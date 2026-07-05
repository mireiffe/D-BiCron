"""PG source retention.

copy 와 분리된 전용 DAG(pg2ch_retention)/CLI 에서 실행된다. 삭제 대상은
timestamp_column 기준(``ts < cutoff``)이지만, cutoff 는 finalize 된 watermark
(pg2ch_meta.copy_run)가 가리키는 마지막 synced timestamp 를 넘지 못하게 캡핑한다 —
copy 가 멈춘 동안 retention 만 계속 돌아도 미복제 row 가 삭제되지 않는다.

어떤 테이블을 얼마나 지울지는 config/retention.yaml 의 RetentionPolicy 로 정한다.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from .chtypes import quote_pg_identifier
from .config import RetentionPolicy, TableConfig
from .connections import get_connection, pg_connect
from .tracking import MetaStore
from .watermark import resolve_sync_since

log = logging.getLogger("pg2ch.retention")


@dataclass
class RetentionResult:
    table_id: str
    status: str
    rows_deleted: int = 0
    retention_cutoff: str | None = None
    last_synced_ts: str | None = None
    safe_cutoff: str | None = None
    reason: str | None = None

    def as_dict(self) -> dict:
        return {k: getattr(self, k) for k in self.__dataclass_fields__}


def _parse_dt(value) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        raw = str(value).strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw)
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


class PgRetention:
    def __init__(
        self,
        cfg: TableConfig,
        policy: RetentionPolicy,
        *,
        connections_path: str | None = None,
        logger: logging.Logger | None = None,
    ):
        if policy.table_id != cfg.table_id:
            raise ValueError(
                f"retention policy table_id '{policy.table_id}' does not match "
                f"table config '{cfg.table_id}'"
            )
        self.cfg = cfg
        self.policy = policy
        self.connections_path = connections_path
        self.log = logger or logging.getLogger(f"pg2ch.retention.{cfg.table_id}")

    def run(self) -> RetentionResult:
        cfg = self.cfg
        src_cfg = get_connection(cfg.source, self.connections_path)
        meta_cfg = get_connection(cfg.meta, self.connections_path)
        if src_cfg.get("type") not in (None, "postgresql"):
            raise ValueError(f"source '{cfg.source}' must be postgresql")

        pg_conn = meta = None
        try:
            meta = MetaStore.connect(meta_cfg)
            meta.ensure_schema()
            pg_conn = pg_connect(src_cfg)
            return self.purge(pg_conn, meta)
        finally:
            if pg_conn is not None:
                pg_conn.close()
            if meta is not None:
                meta.close()

    def purge(self, pg_conn, meta: MetaStore) -> RetentionResult:
        cfg = self.cfg
        policy = self.policy
        if cfg.sync_mode != "append":
            return RetentionResult(
                table_id=cfg.table_id,
                status="skipped",
                reason="retention requires append sync_mode",
            )
        if not cfg.timestamp_column:
            raise ValueError(f"{cfg.table_id}: timestamp_column is required")

        wm_col = cfg.effective_watermark_column
        watermark = meta.get_resume_watermark(cfg.table_id, wm_col)
        if watermark is None:
            return RetentionResult(
                table_id=cfg.table_id,
                status="skipped",
                reason="no finalized watermark",
            )

        src_schema, src_name = cfg.source_parts()
        last_synced_ts = self._resolve_watermark_to_ts(
            pg_conn, src_schema, src_name, cfg.timestamp_column, wm_col, watermark
        )
        if last_synced_ts is None:
            return RetentionResult(
                table_id=cfg.table_id,
                status="skipped",
                reason="watermark resolves to no synced timestamp",
            )

        retention_cutoff = resolve_sync_since(str(policy.retention))
        safe_cutoff = self._safe_cutoff(retention_cutoff, last_synced_ts)
        rows_deleted = self._purge_source(
            pg_conn,
            src_schema,
            src_name,
            cfg.timestamp_column,
            safe_cutoff,
            batch_size=int(policy.batch_size),
            lock_timeout_ms=int(policy.lock_timeout_ms),
        )
        self.log.info(
            "%s: retention deleted %d row(s), cutoff=%s safe_cutoff=%s",
            cfg.source_table,
            rows_deleted,
            retention_cutoff,
            safe_cutoff,
        )
        return RetentionResult(
            table_id=cfg.table_id,
            status="success",
            rows_deleted=rows_deleted,
            retention_cutoff=retention_cutoff,
            last_synced_ts=last_synced_ts,
            safe_cutoff=safe_cutoff,
        )

    def _safe_cutoff(self, retention_cutoff: str, last_synced_ts: str) -> str:
        try:
            retention_dt = _parse_dt(retention_cutoff)
            synced_dt = _parse_dt(last_synced_ts)
        except ValueError as e:
            raise ValueError(
                f"{self.cfg.table_id}: retention cutoff and last synced timestamp "
                "must be ISO timestamps"
            ) from e
        return retention_cutoff if retention_dt <= synced_dt else last_synced_ts

    @staticmethod
    def _resolve_watermark_to_ts(
        pg_conn,
        src_schema: str,
        src_name: str,
        ts_col: str,
        wm_col: str,
        watermark: str,
    ) -> str | None:
        if wm_col == ts_col:
            try:
                _parse_dt(watermark)
            except ValueError:
                return None
            return str(watermark)

        src_fqn = f"{quote_pg_identifier(src_schema)}.{quote_pg_identifier(src_name)}"
        with pg_conn.cursor() as cur:
            cur.execute(
                f"SELECT MAX({quote_pg_identifier(ts_col)}) FROM {src_fqn} "
                f"WHERE {quote_pg_identifier(wm_col)} <= %s",
                (watermark,),
            )
            row = cur.fetchone()
        try:
            pg_conn.rollback()
        except Exception:
            pass
        if not row or row[0] is None:
            return None
        value = row[0]
        return value.isoformat() if hasattr(value, "isoformat") else str(value)

    def _purge_source(
        self,
        pg_conn,
        src_schema: str,
        src_name: str,
        ts_col: str,
        cutoff: str,
        *,
        batch_size: int,
        lock_timeout_ms: int,
    ) -> int:
        src_fqn = f"{quote_pg_identifier(src_schema)}.{quote_pg_identifier(src_name)}"
        total_deleted = 0

        while True:
            try:
                with pg_conn.cursor() as cur:
                    cur.execute(f"SET LOCAL lock_timeout = '{lock_timeout_ms}ms'")
                    cur.execute(
                        f"DELETE FROM {src_fqn} "
                        f"WHERE ctid = ANY(ARRAY("
                        f"  SELECT ctid FROM {src_fqn} "
                        f"  WHERE {quote_pg_identifier(ts_col)} < %s "
                        f"  LIMIT %s"
                        f"))",
                        (cutoff, batch_size),
                    )
                    deleted = int(cur.rowcount)
                pg_conn.commit()
            except Exception as e:
                pg_conn.rollback()
                err_name = type(e).__name__
                if "LockNotAvailable" in err_name or "lock timeout" in str(e).lower():
                    self.log.warning(
                        "%s.%s: lock timeout during retention after %d row(s)",
                        src_schema,
                        src_name,
                        total_deleted,
                    )
                    break
                raise

            total_deleted += deleted
            if deleted < batch_size:
                break

        return total_deleted
