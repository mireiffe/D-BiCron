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

from .chtypes import (
    quote_ch_identifier,
    quote_ch_string,
    quote_pg_identifier,
    unwrap_ch_type,
)
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


def _chunks(seq, size):
    size = max(1, int(size))
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


# repair 재조회 시 한 번에 거는 key 수. 큰 ``IN (...)`` 리스트는 PostgreSQL 파서가
# 재귀 처리하다 max_stack_depth 를 넘겨 에러가 나므로 작게 끊는다 (단일 컬럼 key 는
# ``= ANY(array)`` 로 나가 이 문제가 없지만, 복합 key 의 ``(a,b) IN (...)`` 를 위해
# 보수적으로 제한한다). insert batch 크기(batch_size)와는 별개다.
_REPAIR_KEY_CHUNK = 1000


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

    @classmethod
    def from_dict(cls, data: dict) -> "RunResult":
        return cls(**{k: data.get(k) for k in cls.__dataclass_fields__})


@dataclass
class CopyPlan:
    table_id: str
    sync_mode: str
    source_table: str
    target_table: str
    planned_mode: str
    rows_to_copy: int | None = None
    watermark_column: str | None = None
    resume_watermark: str | None = None
    copy_cutoff: str | None = None
    sync_since: str | None = None

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
    def run(self, *, finalize_run: bool = True) -> RunResult:
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
            return self.copy(
                pg_conn, ch, meta, target_default_db=tgt_cfg.get("dbname", "default"),
                finalize_run=finalize_run,
            )
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

    def precheck(self) -> CopyPlan:
        """copy 계획(mode/cutoff/watermark)을 산출한다 (row 수는 세지 않음).

        source/meta 접속을 열어 copy 전 fail-fast 검증을 겸한다.
        """
        cfg = self.cfg
        src_cfg = get_connection(cfg.source, self.connections_path)
        meta_cfg = get_connection(cfg.meta, self.connections_path)
        if src_cfg.get("type") not in (None, "postgresql"):
            raise ValueError(f"source '{cfg.source}' must be postgresql")

        pg_conn = meta = None
        try:
            meta = MetaStore.connect(meta_cfg)
            pg_conn = pg_connect(src_cfg)  # source 접속 fail-fast 검증
            return self.inspect_copy_plan(meta)
        finally:
            if pg_conn is not None:
                pg_conn.close()
            if meta is not None:
                meta.close()

    def finalize(self, result: RunResult | dict) -> RunResult:
        """deferred copy run 의 watermark/status 를 최종 확정한다."""
        result = result if isinstance(result, RunResult) else RunResult.from_dict(result)
        if result.run_id is None:
            raise ValueError("cannot finalize copy result without run_id")
        meta_cfg = get_connection(self.cfg.meta, self.connections_path)
        with MetaStore.connect(meta_cfg) as meta:
            meta.ensure_schema()
            meta.finish_run(
                result.run_id,
                status=result.status,
                watermark_after=result.watermark_after,
                rows_read=result.rows_read,
                rows_written=result.rows_written,
                rows_failed=result.rows_failed,
                batch_count=result.batch_count,
                duration_ms=result.duration_ms,
                error=result.error,
            )
        return result

    # ── core ─────────────────────────────────────────────────
    def copy(
        self,
        pg_conn,
        ch,
        meta: MetaStore,
        *,
        target_default_db: str = "default",
        finalize_run: bool = True,
    ) -> RunResult:
        cfg = self.cfg
        wm_col = cfg.effective_watermark_column
        ts_col = cfg.timestamp_column
        is_append = cfg.sync_mode == "append"

        # 1~2) PG introspection + CH 컬럼 매핑 + 대상 테이블 보장
        meta.ensure_schema()
        (src_schema, src_name, tgt_db, tgt_name, ch_columns,
         col_names) = self._prepare_target(pg_conn, ch, target_default_db)

        # 3) 모드 / cutoff 결정
        window = self._copy_window(meta)
        sync_since = window["sync_since"]
        if window["planned_mode"] == "incremental":
            cutoff = window["copy_cutoff"]
            query, params = self._incremental_query(
                src_schema, src_name, ch_columns, wm_col, ts_col, cutoff, sync_since
            )
            watermark_before = window["watermark_before"]
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
                src_schema, src_name, ch_columns, ts_col, sync_since
            )
            watermark_before = window["watermark_before"]
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
            stored_status = status
            stored_watermark_after = wm_after
            if not finalize_run:
                stored_status = "copied_partial" if status == "partial" else "copied"
                stored_watermark_after = None
            meta.finish_run(
                run_id, status=stored_status, watermark_after=stored_watermark_after,
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

    def _prepare_target(self, pg_conn, ch, target_default_db: str, *, ensure_table: bool = True):
        """PG introspection + CH 컬럼 매핑 (+ ensure_table 시 대상 테이블 보장).

        ensure_table=False 면 CREATE TABLE IF NOT EXISTS 를 건너뛴다. 이미 존재가
        보장된 상황(repair — 직전 verify 가 이 테이블을 읽었다)에서 재실행이 불필요할
        뿐 아니라, ClickHouse 쪽 DDL 락/분산 DDL 큐에 걸려 오래 멈출 수 있어 피한다.

        반환: (src_schema, src_name, tgt_db, tgt_name, ch_columns, col_names)
        """
        cfg = self.cfg
        src_schema, src_name = cfg.source_parts()
        tgt_db, tgt_name = cfg.target_parts(target_default_db)
        pg_cols = self._introspect_pg_columns(pg_conn, src_schema, src_name)
        if not pg_cols:
            raise ValueError(
                f"source table {cfg.source_table} not found or has no columns"
            )
        key_cols = extract_ch_key_columns(cfg.order_by) | extract_ch_key_columns(
            cfg.primary_key
        )
        ch_columns = build_ch_columns(
            pg_cols, set(cfg.drop_columns), cfg.column_overrides,
            list(key_cols), cfg.use_nullable,
        )
        col_names = [c["name"] for c in ch_columns]
        if ensure_table:
            ddl = build_create_table_ddl(
                tgt_db, tgt_name, ch_columns, cfg.order_by, cfg.partition_by,
                cfg.engine, cfg.primary_key, cfg.indexes, cfg.settings,
            )
            self.log.info("ensuring target table %s.%s", tgt_db, tgt_name)
            ch.execute(ddl)
        return src_schema, src_name, tgt_db, tgt_name, ch_columns, col_names

    def copy_missing_keys(
        self, pg_conn, ch, meta: MetaStore, *,
        key_cols, keys, target_default_db: str = "default",
    ) -> tuple[int, int]:
        """빠진 key 목록의 row 만 source 에서 다시 읽어 target 에 재적재(self-heal).

        무결성 검사(integrity)가 찾아낸 "target 에 없는 source key" 를 받아 그 row 만
        정확히 다시 넣는다. watermark 는 전진시키지 않는다 — repair run 은
        watermark_before/after 를 NULL 로 남겨 resume/무결성 window 계산에서 제외된다.
        ReplacingMergeTree 계열에서 idempotent(중복 재insert 는 머지로 dedup)하며,
        재적재 중에도 실패하는 row 는 기존 dead-letter 경로로 보관된다.

        반환: (rows_written, rows_failed).
        """
        cfg = self.cfg
        keys = list(keys)
        key_cols = list(key_cols)
        if not keys or not key_cols:
            return 0, 0
        meta.ensure_schema()
        # 대상 테이블은 직전 verify 가 이미 읽었으므로 존재가 보장된다 → CREATE 재실행
        # 생략(불필요 + ClickHouse DDL 락에 걸려 멈추는 것을 피함).
        (src_schema, src_name, tgt_db, tgt_name, ch_columns,
         col_names) = self._prepare_target(
            pg_conn, ch, target_default_db, ensure_table=False
        )
        self.log.info(
            "%s: repair fetching %d missing row(s) into %s.%s",
            cfg.source_table, len(keys), tgt_db, tgt_name,
        )
        transformer = build_transformer(ch_columns)
        col_insert = ", ".join(quote_ch_identifier(c) for c in col_names)
        insert_sql = (
            f"INSERT INTO {quote_ch_identifier(tgt_db)}."
            f"{quote_ch_identifier(tgt_name)} ({col_insert}) VALUES"
        )
        wm_col = cfg.effective_watermark_column
        wm_idx = col_names.index(wm_col) if wm_col and wm_col in col_names else None
        select_list = self._pg_select_list(ch_columns)

        run_id = meta.start_run(
            table_id=cfg.table_id, source_table=cfg.source_table,
            target_table=cfg.target_table, sync_mode=cfg.sync_mode,
            watermark_column=None, watermark_before=None,
            dag_id=self.ctx.get("dag_id"), airflow_run_id=self.ctx.get("run_id"),
            task_id=self.ctx.get("task_id"), try_number=self.ctx.get("try_number"),
        )
        total_written = total_failed = 0
        batch_seq = 0
        t0 = time.monotonic()
        try:
            for chunk in _chunks(keys, _REPAIR_KEY_CHUNK):
                raw_rows = self._fetch_by_keys(
                    pg_conn, src_schema, src_name, select_list, key_cols, chunk
                )
                if not raw_rows:
                    continue
                xrows = (
                    [transformer(r) for r in raw_rows] if transformer
                    else list(raw_rows)
                )
                written, failures = self._insert(
                    ch, insert_sql, raw_rows, xrows, col_names, wm_idx
                )
                rows_failed = len(failures)
                status = "success" if rows_failed == 0 else "partial"
                batch_id = meta.record_batch(
                    run_id=run_id, table_id=cfg.table_id, batch_seq=batch_seq,
                    status=status, rows_in=len(raw_rows), rows_written=written,
                    rows_failed=rows_failed, watermark_lo=None, watermark_hi=None,
                )
                if failures and cfg.on_row_error == "dead_letter":
                    meta.record_failed_rows(
                        run_id=run_id, batch_id=batch_id, table_id=cfg.table_id,
                        batch_seq=batch_seq, failures=failures,
                    )
                total_written += written
                total_failed += rows_failed
                batch_seq += 1
            status = "partial" if total_failed else "success"
            meta.finish_run(
                run_id, status=status, watermark_after=None,
                rows_read=total_written + total_failed, rows_written=total_written,
                rows_failed=total_failed, batch_count=batch_seq,
                duration_ms=int((time.monotonic() - t0) * 1000),
            )
            try:
                pg_conn.rollback()  # read 트랜잭션 정리
            except Exception:
                pass
            self.log.info(
                "%s: repair re-copied %d row(s) (%d failed) for %d missing key(s)",
                cfg.source_table, total_written, total_failed, len(keys),
            )
            return total_written, total_failed
        except Exception as e:
            self.log.exception("%s: repair run failed", cfg.source_table)
            meta.finish_run(
                run_id, status="failed", watermark_after=None,
                rows_read=total_written + total_failed, rows_written=total_written,
                rows_failed=total_failed, batch_count=batch_seq,
                duration_ms=int((time.monotonic() - t0) * 1000), error=str(e),
            )
            raise

    @staticmethod
    def _fetch_by_keys(pg_conn, src_schema, src_name, select_list, key_cols, chunk):
        """key 값 chunk 에 해당하는 source row 를 읽는다.

        단일 컬럼 key 는 ``= ANY(%s)`` (배열 파라미터 = 파서 노드 1개)로 나가 큰
        목록에서도 max_stack_depth 를 넘기지 않는다. 복합 key 는 ``(a,b) IN %s`` 를
        쓰되 호출부에서 chunk 크기를 작게 제한한다(_REPAIR_KEY_CHUNK).
        """
        src_fqn = (
            f"{quote_pg_identifier(src_schema)}.{quote_pg_identifier(src_name)}"
        )
        if len(key_cols) == 1:
            predicate = f"{quote_pg_identifier(key_cols[0])} = ANY(%s)"
            # list → PostgreSQL ARRAY (tuple 이면 IN 구문이 되므로 반드시 list).
            param = ([k[0] if isinstance(k, (tuple, list)) else k for k in chunk],)
        else:
            cols = ", ".join(quote_pg_identifier(c) for c in key_cols)
            predicate = f"({cols}) IN %s"
            param = (tuple(tuple(k) for k in chunk),)
        query = f"SELECT {select_list} FROM {src_fqn} WHERE {predicate}"
        with pg_conn.cursor() as cur:
            cur.execute(query, param)
            return cur.fetchall()

    def inspect_copy_plan(self, meta: MetaStore) -> CopyPlan:
        """현재 resume watermark / sync_since 로 copy 계획(mode/cutoff)을 산출한다.

        대상 row 수는 세지 않는다 (rows_to_copy=None): COUNT(*) 는 큰 테이블에서
        전체/범위 스캔으로 비싸고, 결과는 가시성 용도일 뿐 copy 동작에 쓰이지
        않는다. planned_mode/cutoff/watermark 산출만 수행한다. source 접속 fail-fast
        는 호출부(precheck)에서 접속을 열어 처리한다.
        """
        cfg = self.cfg
        meta.ensure_schema()
        window = self._copy_window(meta)
        return CopyPlan(
            table_id=cfg.table_id,
            sync_mode=cfg.sync_mode,
            source_table=cfg.source_table,
            target_table=cfg.target_table,
            planned_mode=window["planned_mode"],
            rows_to_copy=None,
            watermark_column=cfg.effective_watermark_column if cfg.sync_mode == "append" else None,
            resume_watermark=window["resume_watermark"],
            copy_cutoff=_wm_str(window["copy_cutoff"]),
            sync_since=window["sync_since"],
        )

    def _copy_window(self, meta: MetaStore) -> dict:
        cfg = self.cfg
        wm_col = cfg.effective_watermark_column
        is_append = cfg.sync_mode == "append"
        sync_since = resolve_sync_since(cfg.sync_since) if cfg.sync_since else None
        resume_wm = (
            meta.get_resume_watermark(cfg.table_id, wm_col)
            if (is_append and wm_col)
            else None
        )
        if is_append and resume_wm is not None:
            cutoff = apply_overlap(
                cfg.source_table,
                resume_wm,
                overlap_minutes=cfg.overlap_minutes,
                watermark_overlap=cfg.watermark_overlap,
            )
            return {
                "planned_mode": "incremental",
                "resume_watermark": str(resume_wm),
                "copy_cutoff": cutoff,
                "watermark_before": str(resume_wm),
                "sync_since": sync_since,
            }
        return {
            "planned_mode": "append_first_run" if is_append else "full_reload",
            "resume_watermark": None,
            "copy_cutoff": None,
            "watermark_before": None,
            "sync_since": sync_since,
        }

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
        types_check = self.cfg.insert_types_check
        if self.cfg.on_row_error == "fail":
            ch.execute(insert_sql, xrows, types_check=types_check)
            return len(xrows), []

        try:
            ch.execute(insert_sql, xrows, types_check=types_check)
            return len(xrows), []
        except Exception as first_err:
            written, failures = self._isolate(
                ch, insert_sql, raw_rows, xrows, col_names, wm_idx, types_check
            )
            if written == 0 and len(xrows) > 1:
                # 전체 batch 가 row 단위로도 실패 → 인프라/스키마 문제로 판단, fail fast
                raise RuntimeError(
                    f"entire batch of {len(xrows)} rows failed to insert; "
                    f"treating as non-row-level error: {first_err}"
                ) from first_err
            return written, failures

    def _isolate(self, ch, insert_sql, raws, xs, col_names, wm_idx, types_check):
        """binary-split 로 나쁜 row 격리."""
        try:
            ch.execute(insert_sql, xs, types_check=types_check)
            return len(xs), []
        except Exception as e:
            if len(xs) == 1:
                wm = raws[0][wm_idx] if wm_idx is not None else None
                return 0, [(_wm_str(wm), row_to_json(col_names, raws[0]), str(e))]
            mid = len(xs) // 2
            w1, f1 = self._isolate(
                ch, insert_sql, raws[:mid], xs[:mid], col_names, wm_idx, types_check
            )
            w2, f2 = self._isolate(
                ch, insert_sql, raws[mid:], xs[mid:], col_names, wm_idx, types_check
            )
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
    def _pg_select_item(item) -> str:
        if not isinstance(item, dict):
            return quote_pg_identifier(item)

        name = item["name"]
        quoted = quote_pg_identifier(name)
        alias = f" AS {quoted}"
        pg_type = item["pg_type"]
        base = unwrap_ch_type(item["ch_type"])
        override = item.get("override") or {}

        if override.get("parse_format"):
            return quoted
        if pg_type in ("json", "jsonb") and base == "String":
            return f"{quoted}::text{alias}"
        if pg_type == "bytea" and base == "String":
            return f"encode({quoted}, 'hex'){alias}"
        if pg_type == "boolean" and base == "UInt8":
            return f"{quoted}::int{alias}"
        if base == "String" and pg_type not in (
            "character varying",
            "character",
            "text",
        ):
            return f"{quoted}::text{alias}"
        return quoted

    @classmethod
    def _pg_select_list(cls, items) -> str:
        return ", ".join(cls._pg_select_item(item) for item in items)

    @classmethod
    def _incremental_query(
        cls, src_schema, src_name, select_items, wm_col, ts_col, cutoff, sync_since
    ):
        col_list = cls._pg_select_list(select_items)
        conditions, params = cls._incremental_conditions(
            wm_col, ts_col, cutoff, sync_since
        )
        where = " AND ".join(conditions)
        query = (
            f"SELECT {col_list} FROM {quote_pg_identifier(src_schema)}."
            f"{quote_pg_identifier(src_name)} WHERE {where} "
            f"ORDER BY {quote_pg_identifier(wm_col)}"
        )
        return query, tuple(params)

    @classmethod
    def _full_query(cls, src_schema, src_name, select_items, ts_col, sync_since):
        # full copy 는 정렬 없이 seq scan 으로 스트리밍한다.
        # PG 의 ORDER BY 는 server-side cursor 와 만나면 첫 row 를 내보내기 전에
        # 전체 테이블 정렬(blocking sort)을 강제해, 대용량 첫 실행에서 데이터가
        # 들어가기 전 긴 선행 대기를 만든다. watermark_after 는 batch running max
        # 로 추적하므로 정렬이 불필요하고, target 정렬은 MergeTree(order_by)가
        # 백그라운드 머지로 처리한다.
        col_list = cls._pg_select_list(select_items)
        query = (
            f"SELECT {col_list} FROM {quote_pg_identifier(src_schema)}."
            f"{quote_pg_identifier(src_name)}"
        )
        conditions, params = cls._full_conditions(ts_col, sync_since)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        return query, params

    @staticmethod
    def _incremental_conditions(wm_col, ts_col, cutoff, sync_since):
        conditions = [f"{quote_pg_identifier(wm_col)} > %s"]
        params: list = [cutoff]
        if sync_since and ts_col:
            if ts_col == wm_col:
                if str(sync_since) > str(cutoff):
                    params[0] = sync_since
            else:
                conditions.append(f"{quote_pg_identifier(ts_col)} >= %s")
                params.append(sync_since)
        return conditions, tuple(params)

    @staticmethod
    def _full_conditions(ts_col, sync_since):
        if sync_since and ts_col:
            return [f"{quote_pg_identifier(ts_col)} >= %s"], (sync_since,)
        return [], None

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
