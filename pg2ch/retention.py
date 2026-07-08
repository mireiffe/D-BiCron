"""PG source retention.

copy 와 분리된 전용 DAG(pg2ch_retention)/CLI 에서 실행된다. 삭제 기준 컬럼은
기본적으로 테이블의 watermark_column 이고, retention.yaml 테이블 항목의
``column``/``type`` 으로 다른 컬럼을 지정할 수 있다. 삭제 조건은
``col < cutoff`` 이며, retention 값은 컬럼 타입에 따라 해석된다:

  - timestamp     : "180d"(now 기준 상대) 또는 ISO 절대 → cutoff
  - serial/numeric: 숫자 N → 마지막 synced 값 - N (keep-last-N)

cutoff 는 finalize 된 watermark(pg2ch_meta.copy_run)가 가리키는 마지막 synced
값(삭제 기준 컬럼으로 환산)을 넘지 못하게 캡핑한다 — copy 가 멈춘 동안
retention 만 계속 돌아도 미복제 row 가 삭제되지 않는다.

⚠️ 삭제 기준 컬럼을 watermark 와 다르게 지정할 때는 두 컬럼이 함께 증가해야
안전하다 (예: serial id watermark + 삽입 시각 created_at). watermark 순서와
무관하게 갱신되는 컬럼(updated_at 등)을 쓰면, 아직 sync 되지 않은 갱신을 가진
row 가 오래된 삭제 기준 값 때문에 지워질 수 있다.

어떤 테이블을 얼마나 지울지는 config/retention.yaml 의 RetentionPolicy 로 정한다.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

from .chtypes import quote_pg_identifier
from .config import RetentionPolicy, TableConfig
from .connections import get_connection, pg_connect
from .tracking import MetaStore
from .watermark import parse_value, resolve_retention_cutoff, validate_retention_expr

log = logging.getLogger("pg2ch.retention")


def _as_str(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


@dataclass
class RetentionResult:
    table_id: str
    status: str
    rows_deleted: int = 0
    column: str | None = None
    retention_cutoff: str | None = None
    last_synced: str | None = None
    safe_cutoff: str | None = None
    reason: str | None = None

    def as_dict(self) -> dict:
        return {k: getattr(self, k) for k in self.__dataclass_fields__}


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

        wm_col = cfg.watermark_column
        wm_type = cfg.watermark_type
        # 삭제 기준 컬럼/타입: 정책 override 가 없으면 watermark 컬럼을 그대로 쓴다.
        ret_col = policy.column or wm_col
        ret_type = policy.type or wm_type
        # 유효 타입이 정해졌으니 retention 표현을 엄밀히 재검증한다
        # (로드 시점에는 type 미지정 항목을 느슨하게만 검증했다).
        try:
            validate_retention_expr(ret_type, policy.retention)
        except ValueError as e:
            raise ValueError(f"{cfg.table_id}: retention: {e}") from e

        watermark = meta.get_resume_watermark(cfg.table_id, wm_col)
        if watermark is None:
            return RetentionResult(
                table_id=cfg.table_id,
                status="skipped",
                column=ret_col,
                reason="no finalized watermark",
            )
        try:
            wm_value = parse_value(wm_type, watermark)
        except ValueError as e:
            raise ValueError(f"{cfg.table_id}: watermark: {e}") from e

        src_schema, src_name = cfg.source_parts()
        last_synced = self._last_synced_value(
            pg_conn, src_schema, src_name, ret_col, ret_type, wm_col, wm_value
        )
        if last_synced is None:
            return RetentionResult(
                table_id=cfg.table_id,
                status="skipped",
                column=ret_col,
                reason="watermark resolves to no synced value",
            )

        retention_cutoff = resolve_retention_cutoff(
            ret_type, policy.retention, last_synced=last_synced
        )
        # 캡핑: 마지막 synced 값 이후(=아직 미복제일 수 있는 구간)는 지우지 않는다.
        safe_cutoff = min(retention_cutoff, last_synced)
        rows_deleted = self._purge_source(
            pg_conn,
            src_schema,
            src_name,
            ret_col,
            safe_cutoff,
            batch_size=int(policy.batch_size),
            lock_timeout_ms=int(policy.lock_timeout_ms),
        )
        self.log.info(
            "%s: retention deleted %d row(s) by %s, cutoff=%s safe_cutoff=%s",
            cfg.source_table,
            rows_deleted,
            ret_col,
            retention_cutoff,
            safe_cutoff,
        )
        return RetentionResult(
            table_id=cfg.table_id,
            status="success",
            rows_deleted=rows_deleted,
            column=ret_col,
            retention_cutoff=_as_str(retention_cutoff),
            last_synced=_as_str(last_synced),
            safe_cutoff=_as_str(safe_cutoff),
        )

    @staticmethod
    def _last_synced_value(
        pg_conn, src_schema, src_name, ret_col, ret_type, wm_col, wm_value,
    ):
        """watermark 가 가리키는 "마지막 synced 지점"을 삭제 기준 컬럼 값으로 환산.

        삭제 기준 컬럼이 watermark 컬럼과 같으면 watermark 값 그대로, 다르면
        synced 구간(wm <= watermark)의 MAX(ret_col) 을 source 에서 읽는다.
        """
        if ret_col == wm_col:
            return wm_value

        src_fqn = f"{quote_pg_identifier(src_schema)}.{quote_pg_identifier(src_name)}"
        with pg_conn.cursor() as cur:
            cur.execute(
                f"SELECT MAX({quote_pg_identifier(ret_col)}) FROM {src_fqn} "
                f"WHERE {quote_pg_identifier(wm_col)} <= %s",
                (wm_value,),
            )
            row = cur.fetchone()
        try:
            pg_conn.rollback()
        except Exception:
            pass
        if not row or row[0] is None:
            return None
        return parse_value(ret_type, row[0])

    def _purge_source(
        self,
        pg_conn,
        src_schema: str,
        src_name: str,
        ret_col: str,
        cutoff,
        *,
        batch_size: int,
        lock_timeout_ms: int,
    ) -> int:
        """``ret_col < cutoff`` 인 row 를 batch 로 삭제. 삭제 총 row 수 반환.

        ret_col 값을 오름차순으로 **전진(keyset)** 하며 ``(lo, hi]`` 구간씩 지운다.
        lo 가 매 batch 삭제 지점을 넘어 전진하므로 이미 삭제한 앞구간(=dead tuple
        무더기)을 다시 스캔하지 않는다 — 전체 삭제가 선형이라 수억 행에서도 끝난다.
        (예전 ``WHERE ret_col < cutoff LIMIT n`` 반복은 매 batch heap 앞쪽부터 재스캔해
        O(n²) 로 사실상 hang 했다.) 각 batch 는 독립 트랜잭션으로 commit 해 락 보유
        시간을 짧게 유지한다. ret_col 오름차순 인덱스를 전제로 하며(없으면 batch 마다
        정렬), 인덱스가 없으면 경고만 남기고 진행한다.
        """
        src_fqn = f"{quote_pg_identifier(src_schema)}.{quote_pg_identifier(src_name)}"
        col = quote_pg_identifier(ret_col)
        self._warn_if_unindexed(pg_conn, src_schema, src_name, ret_col)

        total_deleted = 0
        batches = 0
        lo = None  # 이미 삭제 끝난 상한 (ret_col <= lo 완료). 오름차순 전진.

        while True:
            try:
                with pg_conn.cursor() as cur:
                    cur.execute(f"SET LOCAL lock_timeout = '{lock_timeout_ms}ms'")
                    # 이번 batch 상한 hi = (lo, cutoff) 구간에서 오름차순 batch_size
                    # 번째 값. lo 직후부터 인덱스로만 걷는다 (앞구간 재스캔 없음).
                    if lo is None:
                        cur.execute(
                            f"SELECT {col} FROM {src_fqn} WHERE {col} < %s "
                            f"ORDER BY {col} OFFSET %s LIMIT 1",
                            (cutoff, batch_size - 1),
                        )
                    else:
                        cur.execute(
                            f"SELECT {col} FROM {src_fqn} "
                            f"WHERE {col} > %s AND {col} < %s "
                            f"ORDER BY {col} OFFSET %s LIMIT 1",
                            (lo, cutoff, batch_size - 1),
                        )
                    probe = cur.fetchone()
                    hi = probe[0] if probe and probe[0] is not None else None

                    # hi 가 있으면 (lo, hi] 삭제, 없으면(잔여 < batch_size) (lo, cutoff)
                    # 잔여 전부 삭제 후 종료. hi <= 값경계라 같은 값의 row 를 쪼개지
                    # 않아 누락 없이 다음 batch 로 넘어간다.
                    if hi is not None:
                        if lo is None:
                            cur.execute(
                                f"DELETE FROM {src_fqn} WHERE {col} <= %s", (hi,)
                            )
                        else:
                            cur.execute(
                                f"DELETE FROM {src_fqn} "
                                f"WHERE {col} > %s AND {col} <= %s",
                                (lo, hi),
                            )
                    else:
                        if lo is None:
                            cur.execute(
                                f"DELETE FROM {src_fqn} WHERE {col} < %s", (cutoff,)
                            )
                        else:
                            cur.execute(
                                f"DELETE FROM {src_fqn} "
                                f"WHERE {col} > %s AND {col} < %s",
                                (lo, cutoff),
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
            batches += 1
            self.log.info(
                "%s.%s: retention batch %d deleted %d row(s) (total=%d, cursor=%s)",
                src_schema, src_name, batches, deleted, total_deleted, _as_str(hi),
            )
            if hi is None:
                break
            lo = hi

        return total_deleted

    def _warn_if_unindexed(self, pg_conn, src_schema, src_name, ret_col) -> None:
        """ret_col 이 어떤 인덱스의 선행 컬럼도 아니면 경고 (best-effort).

        batch 삭제는 ret_col 오름차순 keyset 스캔에 의존하므로 인덱스가 없으면 batch
        마다 정렬이 일어나 오히려 느리다. 권한 등으로 확인 불가 시 조용히 넘어간다.
        """
        found = True  # 확인 실패 시 경고하지 않는다(정상 인덱스 가정)
        try:
            with pg_conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM pg_index i "
                    "JOIN pg_class c ON c.oid = i.indrelid "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "JOIN pg_attribute a "
                    "  ON a.attrelid = c.oid AND a.attnum = i.indkey[0] "
                    "WHERE n.nspname = %s AND c.relname = %s AND a.attname = %s "
                    "LIMIT 1",
                    (src_schema, src_name, ret_col),
                )
                found = cur.fetchone() is not None
        except Exception:
            found = True
        finally:
            try:
                pg_conn.rollback()
            except Exception:
                pass
        if not found:
            self.log.warning(
                "%s.%s: retention column %r is not the leading column of any index; "
                "batched delete will be slow (sorts each batch) — create an index on it",
                src_schema, src_name, ret_col,
            )
