"""PG source retention Job.

pg2ch_sync 가 CH 로 복사한 row 를 PG source 에서 안전하게 삭제합니다.

동작:
  - 같은 pg2ch_config.json 을 읽어 tables[].source_retention 이 있는 항목만 처리
  - 각 테이블의 cutoff = min(now - source_retention, last_synced_ts) 로 보정하여
    sync 가 멈춘 동안 미복제 row 가 삭제되지 않도록 한다.

안전 가드 (case 3 — 정상 sync 상태에서만 실행):
  - timestamp_column 없음 → skip (cutoff 비교 기준 부재)
  - CH watermark row 없음 → skip (sync 가 한 번도 성공하지 않음)
  - 정수형 watermark + wm_col != ts_col 인 경우:
      PG 에서 SELECT MAX(ts_col) WHERE wm_col <= watermark 로 안전 상한을 산출
      MAX 결과가 NULL 이면 skip

설정:
  config 인자로 JSON 경로 지정 (기본: pg2ch_config.json — pg2ch_sync 와 공유)
"""

from __future__ import annotations

import json
from datetime import datetime

from ..db import get_database
from .base import Job, JobResult
from .pg2ch_sync import _resolve_sync_since


class PgRetentionJob(Job):
    name = "pg_retention"
    label = "PG source retention"
    description = "pg2ch_sync 로 복제 완료된 PG row 를 retention 기준으로 삭제"
    default_args: dict = {"config": "pg2ch_config.json"}
    scope = "pipeline"

    _WATERMARK_TABLE = "_pg2ch_watermarks"

    # ── entry point ──────────────────────────────────────────────

    def run(self, *, config: str = "pg2ch_config.json", **kwargs) -> JobResult:
        try:
            from clickhouse_driver import Client as _CHClient  # noqa: F401
        except ImportError:
            return JobResult(
                False,
                "clickhouse-driver 미설치: uv sync --extra clickhouse",
            )

        with open(config) as f:
            cfg = json.load(f)

        defaults = cfg.get("defaults", {})
        tables: list[dict] = cfg["tables"]

        src_db = get_database(cfg["source"])
        tgt_db = get_database(cfg["target"])
        if not src_db:
            return JobResult(False, f"Source DB '{cfg['source']}' not found in databases.json")
        if not tgt_db:
            return JobResult(False, f"Target DB '{cfg['target']}' not found in databases.json")
        if src_db.get("type") != "postgresql":
            return JobResult(False, f"Source must be postgresql, got '{src_db.get('type')}'")
        if tgt_db.get("type") != "clickhouse":
            return JobResult(False, f"Target must be clickhouse, got '{tgt_db.get('type')}'")

        total_purged = 0
        skipped: list[str] = []
        errors: list[str] = []
        processed = 0

        pg_conn = self._pg_connect(src_db)
        ch = self._ch_connect(tgt_db, cfg)
        try:
            for tc in tables:
                merged = {**defaults, **tc}
                src_table = merged.get("source_table", "?")
                if not merged.get("source_retention"):
                    continue
                processed += 1
                try:
                    purged, skip_reason = self._purge_table(
                        pg_conn, ch, src_db, tgt_db, merged, cfg
                    )
                    if skip_reason:
                        skipped.append(f"{src_table}: {skip_reason}")
                    total_purged += purged
                except Exception as e:
                    self.logger.exception("Failed: %s", src_table)
                    errors.append(f"{src_table}: {e}")
        finally:
            ch.disconnect()
            pg_conn.close()

        msg = f"Purged {total_purged} rows across {processed} table(s)"
        if skipped:
            msg += f", skipped {len(skipped)}: " + "; ".join(skipped)
        if errors:
            return JobResult(
                success=False,
                message=msg + f", {len(errors)} error(s): " + "; ".join(errors),
                rows_affected=total_purged,
            )
        return JobResult(success=True, message=msg, rows_affected=total_purged)

    # ── connections ──────────────────────────────────────────────

    @staticmethod
    def _pg_connect(db_cfg: dict):
        import psycopg2

        return psycopg2.connect(
            host=db_cfg["host"],
            port=int(db_cfg.get("port", 5432)),
            dbname=db_cfg["dbname"],
            user=db_cfg.get("user", ""),
            password=db_cfg.get("password", ""),
        )

    @staticmethod
    def _ch_connect(db_cfg: dict, sync_cfg: dict):
        from clickhouse_driver import Client

        port = sync_cfg.get("ch_native_port") or int(db_cfg.get("port", 9000))
        return Client(
            host=db_cfg["host"],
            port=port,
            database=db_cfg["dbname"],
            user=db_cfg.get("user", "default"),
            password=db_cfg.get("password", ""),
        )

    # ── per-table purge ──────────────────────────────────────────

    def _purge_table(
        self,
        pg_conn,
        ch,
        src_db: dict,
        tgt_db: dict,
        tc: dict,
        sync_cfg: dict,
    ) -> tuple[int, str | None]:
        """Returns (purged_rows, skip_reason). skip_reason is None when purge ran."""
        src_table: str = tc["source_table"]
        tgt_table: str = tc["target_table"]
        ts_col: str | None = tc.get("timestamp_column")
        wm_col: str | None = tc.get("watermark_column") or ts_col
        raw_retention: str = tc["source_retention"]
        batch_size: int = tc.get("batch_size", 100_000)

        if not ts_col:
            return 0, "timestamp_column missing"

        retention_cutoff = _resolve_sync_since(raw_retention)

        purge_batch_size = min(batch_size, 10_000)
        raw_purge_batch = tc.get("source_retention_batch_size")
        if raw_purge_batch is not None:
            try:
                purge_batch_size = int(raw_purge_batch)
            except (TypeError, ValueError) as e:
                raise ValueError(
                    f"{src_table}: source_retention_batch_size must be a positive integer"
                ) from e
            if purge_batch_size <= 0:
                raise ValueError(
                    f"{src_table}: source_retention_batch_size must be a positive integer"
                )

        # Watermark 조회 (case 2 가드).
        # pg2ch_sync 와 동일하게 watermark 메타테이블은 target_table 의
        # CH database segment 에 위치 (예: "default.orders" → "default").
        wm_key = f"{sync_cfg['source']}.{src_table}"
        if not wm_col:
            return 0, "no watermark column"
        ch_db = (
            tgt_table.split(".", 1)[0] if "." in tgt_table else tgt_db["dbname"]
        )
        watermark = self._get_watermark(ch, ch_db, wm_key, wm_col)
        if watermark is None:
            return 0, "no watermark recorded (sync never succeeded)"

        # Watermark → timestamp 안전 상한
        last_synced_ts = self._resolve_watermark_to_ts(
            pg_conn, src_table, ts_col, wm_col, watermark
        )
        if last_synced_ts is None:
            return 0, "watermark resolves to no synced rows"

        # 안전 cutoff = min(retention_cutoff, last_synced_ts)
        safe_cutoff = min(retention_cutoff, last_synced_ts)
        if safe_cutoff != retention_cutoff:
            self.logger.warning(
                "%s: retention cutoff %s capped to last synced %s",
                src_table,
                retention_cutoff,
                last_synced_ts,
            )

        src_schema, src_name = (
            src_table.split(".", 1) if "." in src_table else ("public", src_table)
        )

        self.logger.info(
            "%s: purging source rows where %s < %s",
            src_table,
            ts_col,
            safe_cutoff,
        )
        purged = self._purge_source(
            pg_conn, src_schema, src_name, ts_col, safe_cutoff, batch_size=purge_batch_size
        )
        self.logger.info("%s: purged %d source rows", src_table, purged)
        return purged, None

    # ── watermark resolution ─────────────────────────────────────

    def _get_watermark(self, ch, db_name: str, key: str, ts_col: str) -> str | None:
        tbl = self._WATERMARK_TABLE
        try:
            rows = ch.execute(
                f"SELECT value FROM `{db_name}`.`{tbl}` FINAL"
                " WHERE config_key = %(key)s AND timestamp_column = %(ts_col)s"
                " LIMIT 1",
                {"key": key, "ts_col": ts_col},
            )
        except Exception:
            return None
        if rows:
            return rows[0][0]
        return None

    def _resolve_watermark_to_ts(
        self,
        pg_conn,
        src_table: str,
        ts_col: str,
        wm_col: str,
        watermark: str,
    ) -> str | None:
        """Watermark 값을 ts_col timestamp 로 변환.

        wm_col == ts_col: ISO 문자열 그대로 사용.
        wm_col != ts_col: PG 에 SELECT MAX(ts_col) WHERE wm_col <= watermark
                          → wm 까지 sync 된 row 들의 max timestamp.
        """
        if wm_col == ts_col:
            try:
                datetime.fromisoformat(watermark)
                return watermark
            except (ValueError, TypeError):
                return None

        src_schema, src_name = (
            src_table.split(".", 1) if "." in src_table else ("public", src_table)
        )
        with pg_conn.cursor() as cur:
            cur.execute(
                f'SELECT MAX("{ts_col}") FROM "{src_schema}"."{src_name}" '
                f'WHERE "{wm_col}" <= %s',
                (watermark,),
            )
            row = cur.fetchone()
        pg_conn.rollback()  # read-only
        if not row or row[0] is None:
            return None
        ts_val = row[0]
        return ts_val.isoformat() if hasattr(ts_val, "isoformat") else str(ts_val)

    # ── batch DELETE ─────────────────────────────────────────────

    def _purge_source(
        self,
        pg_conn,
        src_schema: str,
        src_name: str,
        ts_col: str,
        cutoff: str,
        batch_size: int = 10_000,
        lock_timeout_ms: int = 5_000,
    ) -> int:
        src_fqn = f'"{src_schema}"."{src_name}"'
        total_deleted = 0

        while True:
            try:
                with pg_conn.cursor() as cur:
                    cur.execute(f"SET LOCAL lock_timeout = '{lock_timeout_ms}ms'")
                    cur.execute(
                        f"DELETE FROM {src_fqn} "
                        f"WHERE ctid = ANY(ARRAY("
                        f'  SELECT ctid FROM {src_fqn}'
                        f'  WHERE "{ts_col}" < %s'
                        f"  LIMIT %s"
                        f"))",
                        (cutoff, batch_size),
                    )
                    deleted = cur.rowcount
                pg_conn.commit()
            except Exception as e:
                pg_conn.rollback()
                err_name = type(e).__name__
                if "LockNotAvailable" in err_name or "lock timeout" in str(e).lower():
                    self.logger.warning(
                        "%s.%s: lock_timeout hit during purge, "
                        "stopping. Deleted %d so far.",
                        src_schema,
                        src_name,
                        total_deleted,
                    )
                    break
                raise

            total_deleted += deleted
            self.logger.info(
                "%s.%s: purged batch %d rows (total %d)",
                src_schema,
                src_name,
                deleted,
                total_deleted,
            )
            if deleted < batch_size:
                break

        return total_deleted
