"""테이블 단위 PG → CH 복사 오케스트레이션.

두 가지 적재 모드 (테이블 설정 sync_mode 로 선택):
  - append      : 메타에 기록된 마지막 watermark 이후의 row 만 증분 전송.
                  첫 실행(워터마크 없음)은 전체를 한 번 복사한 뒤 watermark 를 세운다.
  - full_reload : 매 실행마다 target 을 TRUNCATE 하고 전체를 다시 적재.

복사는 항상 Python 스트리밍(server-side cursor → batch INSERT)으로 수행한다.
이렇게 해야 batch 단위 / row 단위로 성공·실패를 추적하고 실패 row 를
dead-letter(copy_failed_row)에 보관할 수 있다.

batch INSERT 가 실패하면 binary-split 로 나쁜 row 를 격리한다:
  - 정상 row 는 적재하고, 끝까지 실패하는 단일 row 만 dead-letter 로.
  - 멀티 row batch 가 통째로 실패하면(=row 단위가 아닌 인프라/스키마 문제로 판단)
    즉시 raise 하여 run 을 실패 처리한다.

on_row_error:
  - dead_letter : 실패 row 를 copy_failed_row 에 보관 (기본)
  - skip        : 실패 row 를 버리되 카운트만 (보관 안 함)
  - fail        : row 격리 없이 batch 실패 시 즉시 raise
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime

from .chtypes import quote_ch_identifier, quote_ch_string, quote_pg_identifier
from .config import TableConfig
from .connections import ch_connect, get_connection, pg_connect
from .ddl import build_ch_columns, build_create_table_ddl, extract_ch_key_columns
from .tracking import MetaStore, row_to_json
from .transform import build_transformer
from .watermark import apply_overlap, resolve_sync_since

logger = logging.getLogger("pg2ch.copier")


def _wm_str(v) -> str | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.isoformat()
    return str(v)


@dataclass
class _Totals:
    rows_read: int = 0
    rows_written: int = 0
    rows_failed: int = 0
    batch_count: int = 0
    max_wm: object = None


@dataclass
class RunResult:
    table_id: str
    run_id: int | None
    status: str
    sync_mode: str
    rows_read: int = 0
    rows_written: int = 0
    rows_failed: int = 0
    batch_count: int = 0
    watermark_before: str | None = None
    watermark_after: str | None = None
    duration_ms: int | None = None
    error: str | None = None

    def as_dict(self) -> dict:
        return {k: getattr(self, k) for k in self.__dataclass_fields__}


class TableCopier:
    def __init__(
        self,
        cfg: TableConfig,
        *,
        connections_path: str | None = None,
        airflow_context: dict | None = None,
        logger: logging.Logger | None = None,
    ):
        self.cfg = cfg
        self.connections_path = connections_path
        self.ctx = airflow_context or {}
        self.log = logger or logging.getLogger(f"pg2ch.copier.{cfg.table_id}")

    # ── public entry ─────────────────────────────────────────
    def run(self) -> RunResult:
        """접속 정보를 열어 복사를 수행. Airflow PythonOperator/CLI 진입점."""
        cfg = self.cfg
        src_cfg = get_connection(cfg.source, self.connections_path)
        tgt_cfg = get_connection(cfg.target, self.connections_path)
        meta_cfg = get_connection(cfg.meta, self.connections_path)
        if src_cfg.get("type") not in (None, "postgresql"):
            raise ValueError(f"source '{cfg.source}' must be postgresql")
        if tgt_cfg.get("type") not in (None, "clickhouse"):
            raise ValueError(f"target '{cfg.target}' must be clickhouse")

        pg_conn = ch = meta = None
        try:
            meta = MetaStore.connect(meta_cfg)
            pg_conn = pg_connect(src_cfg)
            ch = ch_connect(tgt_cfg)
            return self.copy(pg_conn, ch, meta, target_default_db=tgt_cfg.get("dbname", "default"))
        finally:
            if ch is not None:
                try:
                    ch.disconnect()
                except Exception:
                    pass
            if pg_conn is not None:
                pg_conn.close()
            if meta is not None:
                meta.close()

    # ── core ─────────────────────────────────────────────────
    def copy(
        self,
        pg_conn,
        ch,
        meta: MetaStore,
        *,
        target_default_db: str = "default",
    ) -> RunResult:
        cfg = self.cfg
        src_schema, src_name = cfg.source_parts()
        tgt_db, tgt_name = cfg.target_parts(target_default_db)
        wm_col = cfg.effective_watermark_column
        ts_col = cfg.timestamp_column
        is_append = cfg.sync_mode == "append"
        sync_since = resolve_sync_since(cfg.sync_since) if cfg.sync_since else None

        # 1) PG 컬럼 introspection
        pg_cols = self._introspect_pg_columns(pg_conn, src_schema, src_name)
        if not pg_cols:
            raise ValueError(
                f"source table {cfg.source_table} not found or has no columns"
            )

        # 2) CH 컬럼 매핑 + 테이블 보장
        key_cols = extract_ch_key_columns(cfg.order_by) | extract_ch_key_columns(
            cfg.primary_key
        )
        ch_columns = build_ch_columns(
            pg_cols, set(cfg.drop_columns), cfg.column_overrides,
            list(key_cols), cfg.use_nullable,
        )
        col_names = [c["name"] for c in ch_columns]

        meta.ensure_schema()
        ddl = build_create_table_ddl(
            tgt_db, tgt_name, ch_columns, cfg.order_by, cfg.partition_by,
            cfg.engine, cfg.primary_key, cfg.indexes, cfg.settings,
        )
        self.log.info("ensuring target table %s.%s", tgt_db, tgt_name)
        ch.execute(ddl)

        # 3) 모드 / cutoff 결정
        resume_wm = (
            meta.get_resume_watermark(cfg.table_id, wm_col)
            if (is_append and wm_col)
            else None
        )

        if is_append and resume_wm is not None:
            cutoff = apply_overlap(
                cfg.source_table, resume_wm,
                overlap_minutes=cfg.overlap_minutes,
                watermark_overlap=cfg.watermark_overlap,
            )
            query, params = self._incremental_query(
                src_schema, src_name, col_names, wm_col, ts_col, cutoff, sync_since
            )
            watermark_before = str(resume_wm)
            self.log.info(
                "%s: append incremental from cutoff=%s", cfg.source_table, cutoff
            )
        else:
            if cfg.sync_mode == "full_reload":
                self.log.info("%s: full_reload — truncating target", cfg.source_table)
                ch.execute(
                    f"TRUNCATE TABLE IF EXISTS "
                    f"{quote_ch_identifier(tgt_db)}.{quote_ch_identifier(tgt_name)}"
                )
            query, params = self._full_query(
                src_schema, src_name, col_names, ts_col, sync_since
            )
            watermark_before = None
            self.log.info(
                "%s: %s full copy", cfg.source_table,
                "append first-run" if is_append else "full_reload",
            )

        # 4) run 시작
        run_id = meta.start_run(
            table_id=cfg.table_id,
            source_table=cfg.source_table,
            target_table=cfg.target_table,
            sync_mode=cfg.sync_mode,
            watermark_column=wm_col if is_append else None,
            watermark_before=watermark_before,
            dag_id=self.ctx.get("dag_id"),
            airflow_run_id=self.ctx.get("run_id"),
            task_id=self.ctx.get("task_id"),
            try_number=self.ctx.get("try_number"),
        )

        totals = _Totals()
        t0 = time.monotonic()
        try:
            self._stream(
                pg_conn, ch, meta, run_id, query, params,
                ch_columns, col_names, wm_col, tgt_db, tgt_name, totals,
            )
            status = "partial" if totals.rows_failed > 0 else "success"
            wm_after = (
                _wm_str(totals.max_wm)
                if (is_append and totals.max_wm is not None)
                else None
            )
            duration_ms = int((time.monotonic() - t0) * 1000)
            meta.finish_run(
                run_id, status=status, watermark_after=wm_after,
                rows_read=totals.rows_read, rows_written=totals.rows_written,
                rows_failed=totals.rows_failed, batch_count=totals.batch_count,
                duration_ms=duration_ms,
            )
            if cfg.optimize_after_sync:
                self._optimize(ch, tgt_db, tgt_name)
            self.log.info(
                "%s: %s done — read=%d written=%d failed=%d batches=%d",
                cfg.source_table, status, totals.rows_read, totals.rows_written,
                totals.rows_failed, totals.batch_count,
            )
            return RunResult(
                table_id=cfg.table_id, run_id=run_id, status=status,
                sync_mode=cfg.sync_mode, rows_read=totals.rows_read,
                rows_written=totals.rows_written, rows_failed=totals.rows_failed,
                batch_count=totals.batch_count, watermark_before=watermark_before,
                watermark_after=wm_after, duration_ms=duration_ms,
            )
        except Exception as e:
            duration_ms = int((time.monotonic() - t0) * 1000)
            self.log.exception("%s: run failed", cfg.source_table)
            meta.finish_run(
                run_id, status="failed",
                watermark_after=None,
                rows_read=totals.rows_read, rows_written=totals.rows_written,
                rows_failed=totals.rows_failed, batch_count=totals.batch_count,
                duration_ms=duration_ms, error=str(e),
            )
            raise

    # ── streaming ────────────────────────────────────────────
    def _stream(
        self, pg_conn, ch, meta, run_id, query, params,
        ch_columns, col_names, wm_col, tgt_db, tgt_name, totals: _Totals,
    ) -> None:
        cfg = self.cfg
        transformer = build_transformer(ch_columns)
        col_insert = ", ".join(quote_ch_identifier(c) for c in col_names)
        insert_sql = (
            f"INSERT INTO {quote_ch_identifier(tgt_db)}."
            f"{quote_ch_identifier(tgt_name)} ({col_insert}) VALUES"
        )
        wm_idx = col_names.index(wm_col) if wm_col and wm_col in col_names else None

        cursor = pg_conn.cursor(name=f"pg2ch_{cfg.table_id}")
        cursor.itersize = cfg.batch_size
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        batch_seq = 0
        while True:
            batch_t0 = time.monotonic()
            fetch_t0 = time.monotonic()
            raw_rows = cursor.fetchmany(cfg.batch_size)
            fetch_ms = int((time.monotonic() - fetch_t0) * 1000)
            if not raw_rows:
                break

            wm_t0 = time.monotonic()
            lo = hi = None
            if wm_idx is not None:
                for r in raw_rows:
                    v = r[wm_idx]
                    if v is None:
                        continue
                    if lo is None or v < lo:
                        lo = v
                    if hi is None or v > hi:
                        hi = v
                if hi is not None and (totals.max_wm is None or hi > totals.max_wm):
                    totals.max_wm = hi
            watermark_ms = int((time.monotonic() - wm_t0) * 1000)

            transform_t0 = time.monotonic()
            xrows = [transformer(r) for r in raw_rows] if transformer else list(raw_rows)
            transform_ms = int((time.monotonic() - transform_t0) * 1000)
            insert_t0 = time.monotonic()
            written, failures = self._insert(
                ch, insert_sql, raw_rows, xrows, col_names, wm_idx
            )
            insert_ms = int((time.monotonic() - insert_t0) * 1000)
            rows_failed = len(failures)
            batch_status = "success" if rows_failed == 0 else "partial"

            meta_t0 = time.monotonic()
            batch_id = meta.record_batch(
                run_id=run_id, table_id=cfg.table_id, batch_seq=batch_seq,
                status=batch_status, rows_in=len(raw_rows), rows_written=written,
                rows_failed=rows_failed, watermark_lo=_wm_str(lo), watermark_hi=_wm_str(hi),
            )
            if failures and cfg.on_row_error == "dead_letter":
                meta.record_failed_rows(
                    run_id=run_id, batch_id=batch_id, table_id=cfg.table_id,
                    batch_seq=batch_seq, failures=failures,
                )
            meta_ms = int((time.monotonic() - meta_t0) * 1000)

            totals.rows_read += len(raw_rows)
            totals.rows_written += written
            totals.rows_failed += rows_failed
            totals.batch_count += 1
            total_ms = int((time.monotonic() - batch_t0) * 1000)
            rows_per_sec = int(len(raw_rows) / max(total_ms / 1000, 0.001))
            self.log.info(
                "%s: batch %d — in=%d written=%d failed=%d "
                "elapsed=%dms rate=%d rows/s "
                "(fetch=%dms watermark=%dms transform=%dms insert=%dms meta=%dms; "
                "total written=%d)",
                cfg.source_table, batch_seq, len(raw_rows), written, rows_failed,
                total_ms, rows_per_sec,
                fetch_ms, watermark_ms, transform_ms, insert_ms, meta_ms,
                totals.rows_written,
            )

            if (
                cfg.max_failed_rows is not None
                and totals.rows_failed > cfg.max_failed_rows
            ):
                raise RuntimeError(
                    f"max_failed_rows ({cfg.max_failed_rows}) exceeded: "
                    f"{totals.rows_failed} rows failed"
                )
            batch_seq += 1

        cursor.close()
        pg_conn.rollback()  # read 트랜잭션 정리

    # ── INSERT with row isolation ────────────────────────────
    def _insert(self, ch, insert_sql, raw_rows, xrows, col_names, wm_idx):
        """(written_count, failures) 반환. failures: [(wm_str, row_json, error)]."""
        if self.cfg.on_row_error == "fail":
            ch.execute(insert_sql, xrows, types_check=True)
            return len(xrows), []

        try:
            ch.execute(insert_sql, xrows, types_check=True)
            return len(xrows), []
        except Exception as first_err:
            written, failures = self._isolate(
                ch, insert_sql, raw_rows, xrows, col_names, wm_idx
            )
            if written == 0 and len(xrows) > 1:
                # 전체 batch 가 row 단위로도 실패 → 인프라/스키마 문제로 판단, fail fast
                raise RuntimeError(
                    f"entire batch of {len(xrows)} rows failed to insert; "
                    f"treating as non-row-level error: {first_err}"
                ) from first_err
            return written, failures

    def _isolate(self, ch, insert_sql, raws, xs, col_names, wm_idx):
        """binary-split 로 나쁜 row 격리."""
        try:
            ch.execute(insert_sql, xs, types_check=True)
            return len(xs), []
        except Exception as e:
            if len(xs) == 1:
                wm = raws[0][wm_idx] if wm_idx is not None else None
                return 0, [(_wm_str(wm), row_to_json(col_names, raws[0]), str(e))]
            mid = len(xs) // 2
            w1, f1 = self._isolate(ch, insert_sql, raws[:mid], xs[:mid], col_names, wm_idx)
            w2, f2 = self._isolate(ch, insert_sql, raws[mid:], xs[mid:], col_names, wm_idx)
            return w1 + w2, f1 + f2

    # ── PG introspection ─────────────────────────────────────
    @staticmethod
    def _introspect_pg_columns(pg_conn, schema: str, table: str) -> list[dict]:
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type, is_nullable,"
                "       numeric_precision, numeric_scale"
                " FROM information_schema.columns"
                " WHERE table_schema = %s AND table_name = %s"
                " ORDER BY ordinal_position",
                (schema, table),
            )
            return [
                {
                    "name": r[0], "pg_type": r[1], "nullable": r[2] == "YES",
                    "precision": r[3], "scale": r[4],
                }
                for r in cur.fetchall()
            ]

    # ── query builders ───────────────────────────────────────
    @staticmethod
    def _incremental_query(src_schema, src_name, col_names, wm_col, ts_col, cutoff, sync_since):
        col_list = ", ".join(quote_pg_identifier(c) for c in col_names)
        conditions = [f"{quote_pg_identifier(wm_col)} > %s"]
        params: list = [cutoff]
        if sync_since and ts_col:
            if ts_col == wm_col:
                if str(sync_since) > str(cutoff):
                    params[0] = sync_since
            else:
                conditions.append(f"{quote_pg_identifier(ts_col)} >= %s")
                params.append(sync_since)
        where = " AND ".join(conditions)
        query = (
            f"SELECT {col_list} FROM {quote_pg_identifier(src_schema)}."
            f"{quote_pg_identifier(src_name)} WHERE {where} "
            f"ORDER BY {quote_pg_identifier(wm_col)}"
        )
        return query, tuple(params)

    @staticmethod
    def _full_query(src_schema, src_name, col_names, ts_col, sync_since):
        # full copy 는 정렬 없이 seq scan 으로 스트리밍한다.
        # PG 의 ORDER BY 는 server-side cursor 와 만나면 첫 row 를 내보내기 전에
        # 전체 테이블 정렬(blocking sort)을 강제해, 대용량 첫 실행에서 데이터가
        # 들어가기 전 긴 선행 대기를 만든다. watermark_after 는 batch running max
        # 로 추적하므로 정렬이 불필요하고, target 정렬은 MergeTree(order_by)가
        # 백그라운드 머지로 처리한다.
        col_list = ", ".join(quote_pg_identifier(c) for c in col_names)
        query = (
            f"SELECT {col_list} FROM {quote_pg_identifier(src_schema)}."
            f"{quote_pg_identifier(src_name)}"
        )
        params = None
        if sync_since and ts_col:
            query += f" WHERE {quote_pg_identifier(ts_col)} >= %s"
            params = (sync_since,)
        return query, params

    # ── post-sync OPTIMIZE ───────────────────────────────────
    def _optimize(self, ch, db_name: str, table_name: str) -> None:
        cfg = self.cfg
        partitions = cfg.optimize_partitions
        mutations_sync = int(cfg.optimize_mutations_sync)
        if partitions is None:
            targets: list = [None]
        elif isinstance(partitions, (str, int)):
            targets = [str(partitions)]
        elif isinstance(partitions, (list, tuple)):
            targets = [str(p) for p in partitions if p is not None and str(p) != ""]
            if not targets:
                targets = [None]
        else:
            raise ValueError(
                "optimize_partitions must be a string, number, or list of those"
            )
        for partition in targets:
            sql = (
                f"OPTIMIZE TABLE {quote_ch_identifier(db_name)}."
                f"{quote_ch_identifier(table_name)}"
            )
            if partition is not None:
                sql += f" PARTITION {quote_ch_string(partition)}"
            sql += f" FINAL SETTINGS mutations_sync = {mutations_sync}"
            self.log.info(
                "%s.%s: OPTIMIZE FINAL%s", db_name, table_name,
                f" partition {partition}" if partition is not None else "",
            )
            ch.execute(sql)
