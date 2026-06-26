"""copy 무결성 검사 (retention 전 누락 row 탐지).

최근 N개 copy_run 의 watermark 구간 ``(watermark_before, watermark_after]`` 에 대해
  - PG source 의 row 수 (기대값) 와
  - CH target 의 distinct key 수 (실재값) 를
비교해, target 에 빠진 row 가 있는지 확인한다.

retention(=source 삭제) 직전에 실행하는 안전장치다. overlap 을 걸어도 batch 경계
tie/크래시 등으로 CH 에 안 넘어간 row 가 생길 수 있는데, 그 상태로 retention 이
source 를 지우면 데이터가 영구 유실된다. 이 검사가 누락을 먼저 잡아 retention 을 막는다.

왜 target 은 distinct key 인가:
  overlap 재전송으로 ReplacingMergeTree 에는 같은 row 가 머지 전까지 중복 존재할 수
  있다. 그래서 target 의 ``count(*)`` 는 (다른 row 가 빠졌더라도) 중복이 메워
  누락을 가릴 수 있다. order_by / primary_key(= dedup 키)로 distinct 를 세면 중복에
  영향받지 않고 "논리적 row 수" 를 본다 → CH 가 모자라면 진짜 누락이다.

비용:
  검사 범위를 "최근 run 의 watermark 구간" 으로 한정해, 큰 테이블에서도 전체 스캔
  없이 최근 slice 만 센다(프로젝트가 precheck 에서 COUNT(*) 를 뺀 이유와 같은 맥락).
  하한이 없는 구간(append 첫 전체복사 run)은 전체 스캔이 되므로 건너뛴다.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from .chtypes import quote_ch_identifier, quote_ch_string, quote_pg_identifier
from .config import TableConfig
from .connections import ch_connect, get_connection, pg_connect
from .ddl import extract_ch_key_columns
from .tracking import MetaStore

log = logging.getLogger("pg2ch.integrity")


@dataclass
class IntegrityResult:
    table_id: str
    status: str  # ok | mismatch | skipped | disabled
    enabled: bool
    lookback_runs: int = 0
    windows_checked: int = 0
    source_rows: int = 0
    target_rows: int = 0
    missing_rows: int = 0
    tolerance: int = 0
    windows: list = field(default_factory=list)
    reason: str | None = None

    def as_dict(self) -> dict:
        return {k: getattr(self, k) for k in self.__dataclass_fields__}


def _coerce_watermark(value):
    """메타에 TEXT 로 저장된 watermark 를 비교용 파이썬 타입으로 복원.

    숫자형은 int/Decimal 로, ISO timestamp 는 datetime 으로, 그 외는 문자열 그대로.
    """
    if value is None:
        return None
    if isinstance(value, (datetime, int, Decimal)):
        return value
    s = str(value).strip()
    try:
        d = Decimal(s)
        if d.is_finite():
            return int(d) if d == d.to_integral_value() else d
    except (InvalidOperation, ValueError):
        pass
    raw = s[:-1] + "+00:00" if s.endswith("Z") else s
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return s


def _ch_literal(value) -> str:
    """coerce 된 watermark 를 ClickHouse SQL 리터럴 문자열로.

    datetime 은 UTC naive 로 맞춰 마이크로초까지 보존한 문자열 리터럴로 만든다
    (clickhouse-driver 파라미터 치환이 sub-second 를 떨궈 경계 row 가 빠지는 것을
    피하기 위함). 숫자는 그대로, 그 외는 quote 한 문자열.
    """
    v = _coerce_watermark(value)
    if isinstance(v, datetime):
        dt = v.astimezone(timezone.utc).replace(tzinfo=None) if v.tzinfo else v
        return quote_ch_string(dt.strftime("%Y-%m-%d %H:%M:%S.%f"))
    if isinstance(v, (int, Decimal)):
        return str(v)
    return quote_ch_string(str(v))


class IntegrityChecker:
    def __init__(
        self,
        cfg: TableConfig,
        *,
        connections_path: str | None = None,
        logger: logging.Logger | None = None,
    ):
        self.cfg = cfg
        self.connections_path = connections_path
        self.log = logger or logging.getLogger(f"pg2ch.integrity.{cfg.table_id}")

    def run(self) -> IntegrityResult:
        cfg = self.cfg
        if not cfg.integrity_enabled:
            return IntegrityResult(
                table_id=cfg.table_id, status="disabled", enabled=False,
                reason="integrity_enabled is false",
            )
        if cfg.sync_mode != "append":
            return IntegrityResult(
                table_id=cfg.table_id, status="skipped", enabled=True,
                reason="integrity check requires append sync_mode",
            )

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
            meta.ensure_schema()
            pg_conn = pg_connect(src_cfg)
            ch = ch_connect(tgt_cfg)
            return self.verify(
                pg_conn, ch, meta,
                target_default_db=tgt_cfg.get("dbname", "default"),
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

    def verify(
        self, pg_conn, ch, meta: MetaStore, *, target_default_db: str = "default",
    ) -> IntegrityResult:
        cfg = self.cfg
        if not cfg.integrity_enabled:
            return IntegrityResult(
                table_id=cfg.table_id, status="disabled", enabled=False,
                reason="integrity_enabled is false",
            )
        if cfg.sync_mode != "append":
            return IntegrityResult(
                table_id=cfg.table_id, status="skipped", enabled=True,
                reason="integrity check requires append sync_mode",
            )

        wm_col = cfg.effective_watermark_column
        if not wm_col:
            return IntegrityResult(
                table_id=cfg.table_id, status="skipped", enabled=True,
                reason="no watermark column to check",
            )
        lookback = int(cfg.integrity_lookback_runs)
        tolerance = int(cfg.integrity_tolerance)
        windows = meta.recent_run_windows(cfg.table_id, wm_col, lookback)
        if not windows:
            return IntegrityResult(
                table_id=cfg.table_id, status="skipped", enabled=True,
                lookback_runs=lookback, tolerance=tolerance,
                reason="no finalized run windows to check",
            )

        key_cols = sorted(
            extract_ch_key_columns(cfg.order_by)
            | extract_ch_key_columns(cfg.primary_key)
        )
        if not key_cols:
            self.log.warning(
                "%s: order_by/primary_key has no plain columns; target uses "
                "count() and may not detect rows hidden behind duplicates",
                cfg.table_id,
            )

        src_schema, src_name = cfg.source_parts()
        tgt_db, tgt_name = cfg.target_parts(target_default_db)

        details: list[dict] = []
        checked = 0
        total_src = total_tgt = total_missing = 0
        for w in windows:
            lo, hi = w["watermark_before"], w["watermark_after"]
            if hi is None:
                continue
            if lo is None:
                details.append({
                    "run_id": w["run_id"], "watermark_lo": None, "watermark_hi": hi,
                    "status": "skipped", "reason": "no lower bound (first full copy)",
                })
                continue

            src = self._pg_count(pg_conn, src_schema, src_name, wm_col, lo, hi)
            tgt = self._ch_count(ch, tgt_db, tgt_name, wm_col, key_cols, lo, hi)
            missing = max(0, src - tgt)
            checked += 1
            total_src += src
            total_tgt += tgt
            total_missing += missing
            details.append({
                "run_id": w["run_id"], "watermark_lo": str(lo), "watermark_hi": str(hi),
                "source_rows": src, "target_rows": tgt, "missing": missing,
            })

        if checked == 0:
            return IntegrityResult(
                table_id=cfg.table_id, status="skipped", enabled=True,
                lookback_runs=lookback, tolerance=tolerance, windows=details,
                reason="no checkable windows (all without a lower bound)",
            )

        status = "mismatch" if total_missing > tolerance else "ok"
        result = IntegrityResult(
            table_id=cfg.table_id, status=status, enabled=True,
            lookback_runs=lookback, windows_checked=checked,
            source_rows=total_src, target_rows=total_tgt,
            missing_rows=total_missing, tolerance=tolerance, windows=details,
        )
        if status == "mismatch":
            self.log.warning(
                "%s: integrity MISMATCH — source=%d target=%d missing=%d "
                "across %d window(s) (tolerance=%d): %s",
                cfg.table_id, total_src, total_tgt, total_missing, checked,
                tolerance, details,
            )
        else:
            self.log.info(
                "%s: integrity ok — source=%d target=%d across %d window(s)",
                cfg.table_id, total_src, total_tgt, checked,
            )
        return result

    # ── counts ───────────────────────────────────────────────
    @staticmethod
    def _pg_count(pg_conn, src_schema, src_name, wm_col, lo, hi) -> int:
        src_fqn = f"{quote_pg_identifier(src_schema)}.{quote_pg_identifier(src_name)}"
        sql = (
            f"SELECT count(*) FROM {src_fqn} "
            f"WHERE {quote_pg_identifier(wm_col)} > %s "
            f"AND {quote_pg_identifier(wm_col)} <= %s"
        )
        with pg_conn.cursor() as cur:
            cur.execute(sql, (_coerce_watermark(lo), _coerce_watermark(hi)))
            row = cur.fetchone()
        try:
            pg_conn.rollback()  # read 트랜잭션 정리
        except Exception:
            pass
        return int(row[0]) if row and row[0] is not None else 0

    @staticmethod
    def _ch_count(ch, tgt_db, tgt_name, wm_col, key_cols, lo, hi) -> int:
        tgt_fqn = f"{quote_ch_identifier(tgt_db)}.{quote_ch_identifier(tgt_name)}"
        if key_cols:
            key_expr = ", ".join(quote_ch_identifier(c) for c in key_cols)
            agg = f"uniqExact({key_expr})"
        else:
            agg = "count()"
        sql = (
            f"SELECT {agg} FROM {tgt_fqn} "
            f"WHERE {quote_ch_identifier(wm_col)} > {_ch_literal(lo)} "
            f"AND {quote_ch_identifier(wm_col)} <= {_ch_literal(hi)}"
        )
        res = ch.execute(sql)
        if not res:
            return 0
        return int(res[0][0])
