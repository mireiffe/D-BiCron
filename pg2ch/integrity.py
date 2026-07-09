"""copy 무결성 검사 + 누락 row 자가복구 (retention 안전장치).

최근 N개 copy_run 의 watermark 구간 ``(watermark_before, watermark_after]`` 에서
PG source 와 CH target 을 비교해 target 에 빠진 row 를 찾고, 켜져 있으면 그 row 만
다시 복사(self-heal)한 뒤 retention 을 진행한다. retention(=source 삭제) 직전의
마지막 방어선이다.

비교 식별자는 **watermark 컬럼 하나**다 (order_by/primary_key 는 쓰지 않는다).
dedup 키는 PG/CH 드라이버가 돌려주는 파이썬 타입 표현이 어긋날 수 있어
(Decimal scale, timestamp 정밀도/tz 등) 같은 값이 영원히 "누락" 으로 잡히는
false mismatch 를 만들 수 있다. watermark 는 증분 축이라 양쪽에서 항상 같은
스칼라로 비교된다. 대신 해상도가 watermark 값 단위라, watermark 가 row 를 유일하게
식별하지 않으면(같은 timestamp 를 공유하는 row 다수) 그 값의 row 가 하나라도 남아
있는 한 나머지 누락은 못 본다 — serial/증가 id 처럼 unique 한 watermark 에서 정밀하다.

두 가지 검사 방식 (integrity_method):
  - count   : 값싼 게이트. source ``count(*)`` vs target ``uniqExact(watermark)`` 를
              비교하고, 모자란 구간만 watermark 값 diff 로 무엇이 빠졌는지 찾는다.
  - key_diff: 항상 양쪽 watermark 값 집합을 끌어와 차집합을 구한다. count 가 우연히
              같아 가려지는 경우까지 잡지만 전송 비용이 크다.

파티션 프루닝(integrity_partition_column/period):
  watermark 가 파티션 키가 아니면(예: serial id watermark + ts 파티션) CH 는 wm
  범위 질의에 파티션을 못 쳐내 모든 파티션을 열고, window 가 크면 ``uniqExact`` 가
  수억 distinct 를 RAM 에 올려 메모리 한계를 넘긴다. partition_column(=ts 파티션
  키) + partition_period 를 주면 양쪽 질의에 ``partition_column >= now-period`` 를
  **똑같이** 걸어(동일 조건이라 count/distinct 비교는 그대로 성립) CH 스캔을 최근
  파티션으로 좁힌다. 대신 검사 범위가 그 기간으로 한정된다 — window 의 row 는
  최근 복사분이라 보통 그 기간 안에 든다(늦게 도착한 오래된 ts 는 검사에서 빠질 수
  있으니 period 를 넉넉히 준다).

왜 target 을 distinct watermark 로 보나:
  overlap 재전송으로 ReplacingMergeTree 에 같은 row 가 머지 전까지 중복 존재할 수
  있어, target 의 ``count(*)`` 는 누락을 가릴 수 있다. 중복은 같은 row(=같은
  watermark 값)의 재전송이므로 distinct 로 세면 물리 중복에 영향받지 않는다.

자가복구(repair):
  빠진 watermark 값의 row 를 ``TableCopier.copy_missing_keys`` 로 재적재한다.
  watermark 를 전진시키지 않으므로 resume 로직과 충돌하지 않는다. ReplacingMergeTree
  계열에서만 수행한다(그 외 엔진은 재insert 가 중복을 남길 수 있어 skip). 이미
  dead-letter 로 기록된 row(재복사해도 또 실패)는 누락 대상에서 제외해 무한 재시도를
  막는다.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from .chtypes import quote_ch_identifier, quote_ch_string, quote_pg_identifier
from .config import TableConfig
from .connections import ch_connect, get_connection, pg_connect
from .copier import TableCopier
from .tracking import MetaStore
from .watermark import resolve_since

log = logging.getLogger("pg2ch.integrity")

# 구조화 결과(windows)에 싣는 구간별 누락 watermark 값 샘플 최대 개수. 로그에는 값
# 자체를 남기지 않고(전량 덤프 방지) 개수·분포만 남긴다. 재복사에는 전체 목록
# (_repair_keys)을 쓰되 그건 로그/XCom 에 싣지 않는다.
_MISSING_SAMPLE = 5


@dataclass
class IntegrityResult:
    table_id: str
    status: str  # ok | mismatch | skipped | disabled
    enabled: bool
    method: str = "count"
    lookback_runs: int = 0
    windows_checked: int = 0
    source_rows: int = 0
    target_rows: int = 0
    missing_rows: int = 0  # target 에 없는 source key 수 (dead-letter 제외) = 판정 기준
    deadletter_rows: int = 0  # 누락이지만 이미 dead-letter 로 알려진 수 (판정 제외)
    repaired_rows: int = 0
    repair_attempts_used: int = 0
    tolerance: int = 0
    windows: list = field(default_factory=list)
    reason: str | None = None

    # 재복사 계획 (내부용 — as_dict/XCom 에는 싣지 않음: 목록이 커질 수 있음).
    _repair_keys: list = field(default_factory=list, repr=False)
    _repair_key_cols: list = field(default_factory=list, repr=False)
    # 누락 값들이 걸친 watermark 구간(union). repair fetch 를 이 구간으로 좁힌다.
    _repair_wm_lo: object = field(default=None, repr=False)
    _repair_wm_hi: object = field(default=None, repr=False)

    def as_dict(self) -> dict:
        return {
            k: getattr(self, k)
            for k in self.__dataclass_fields__
            if not k.startswith("_")
        }


def _coerce_watermark(value):
    """메타에 TEXT 로 저장된 watermark 를 비교용 파이썬 타입으로 복원."""
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


def _pg_part_clause(part_col, part_cutoff) -> tuple[str, tuple]:
    """파티션 ts 필터의 PG WHERE 조각 + 파라미터. 미설정이면 빈 조각."""
    if part_col and part_cutoff is not None:
        return f" AND {quote_pg_identifier(part_col)} >= %s", (part_cutoff,)
    return "", ()


def _ch_part_clause(part_col, part_cutoff) -> str:
    """파티션 ts 필터의 CH WHERE 조각 (리터럴 인라인). 미설정이면 빈 문자열."""
    if part_col and part_cutoff is not None:
        return f" AND {quote_ch_identifier(part_col)} >= {_ch_literal(part_cutoff)}"
    return ""


def _canon_scalar(v) -> str | None:
    """key 값 하나를 PG/CH/JSON 어디서 왔든 비교 가능한 문자열로 정규화.

    set 비교(source vs target vs dead-letter)를 위해 타입을 문자열로 통일한다.
    dead-letter 는 JSONB ``->>`` 로 text, source/target 은 드라이버 타입이라
    문자열 기준으로 맞춰야 교차 비교가 성립한다.
    """
    if v is None:
        return None
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, datetime):
        dt = v.astimezone(timezone.utc).replace(tzinfo=None) if v.tzinfo else v
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")
    if isinstance(v, (bytes, memoryview)):
        return bytes(v).hex()
    return str(v)


def _canon_key(values) -> tuple:
    return tuple(_canon_scalar(v) for v in values)


class IntegrityChecker:
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
        self.log = logger or logging.getLogger(f"pg2ch.integrity.{cfg.table_id}")

    # ── public entry ─────────────────────────────────────────
    def run(self, *, repair: bool | None = None) -> IntegrityResult:
        """접속을 열어 검사(+자가복구)를 수행. Airflow verify task / CLI 진입점.

        repair=None 이면 설정(integrity_repair)을 따르고, False 면 검사만 한다.
        """
        cfg = self.cfg
        if not cfg.integrity_enabled:
            return IntegrityResult(
                table_id=cfg.table_id, status="disabled", enabled=False,
                method=cfg.integrity_method, reason="integrity_enabled is false",
            )
        if cfg.sync_mode != "append":
            return IntegrityResult(
                table_id=cfg.table_id, status="skipped", enabled=True,
                method=cfg.integrity_method,
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
            return self._run_with_repair(
                pg_conn, ch, meta,
                target_default_db=tgt_cfg.get("dbname", "default"),
                repair=repair,
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

    def _run_with_repair(
        self, pg_conn, ch, meta: MetaStore, *,
        target_default_db: str, repair: bool | None,
    ) -> IntegrityResult:
        cfg = self.cfg
        do_repair = cfg.integrity_repair if repair is None else repair
        attempts_allowed = 0
        if do_repair:
            if self._repair_supported():
                attempts_allowed = int(cfg.integrity_repair_attempts)
            else:
                self.log.warning(
                    "%s: repair requested but engine %r is not ReplacingMergeTree "
                    "(re-insert could duplicate); skipping repair",
                    cfg.table_id, cfg.engine,
                )

        result = self.verify(pg_conn, ch, meta, target_default_db=target_default_db)
        repaired_total = 0
        attempts_used = 0
        while (
            result.status == "mismatch"
            and attempts_used < attempts_allowed
            and getattr(result, "_repair_keys", None)
        ):
            keys = result._repair_keys
            key_cols = result._repair_key_cols
            self.log.warning(
                "%s: integrity mismatch — repairing %d missing key(s) "
                "(attempt %d/%d)",
                cfg.table_id, len(keys), attempts_used + 1, attempts_allowed,
            )
            copier = TableCopier(
                cfg, connections_path=self.connections_path,
                airflow_context=self.ctx,
            )
            written, _failed = copier.copy_missing_keys(
                pg_conn, ch, meta,
                key_cols=key_cols, keys=keys, target_default_db=target_default_db,
                wm_lo=result._repair_wm_lo, wm_hi=result._repair_wm_hi,
            )
            repaired_total += written
            attempts_used += 1
            if written == 0:
                # 재복사로 아무것도 못 채움(=source 에도 없음) → 더 시도해도 무의미.
                self.log.warning(
                    "%s: repair re-copied 0 rows; missing rows are not recoverable "
                    "from source (deleted?) — giving up", cfg.table_id,
                )
                break
            result = self.verify(
                pg_conn, ch, meta, target_default_db=target_default_db
            )

        result.repaired_rows = repaired_total
        result.repair_attempts_used = attempts_used
        return result

    def _repair_supported(self) -> bool:
        return "replacingmergetree" in (self.cfg.engine or "").lower()

    # ── detection ────────────────────────────────────────────
    def verify(
        self, pg_conn, ch, meta: MetaStore, *, target_default_db: str = "default",
    ) -> IntegrityResult:
        """검사만 수행 (재복사 없음). 누락이 있으면 재복사할 key 를 결과에 실어 둔다."""
        cfg = self.cfg
        method = cfg.integrity_method
        if not cfg.integrity_enabled:
            return IntegrityResult(
                table_id=cfg.table_id, status="disabled", enabled=False,
                method=method, reason="integrity_enabled is false",
            )
        if cfg.sync_mode != "append":
            return IntegrityResult(
                table_id=cfg.table_id, status="skipped", enabled=True,
                method=method, reason="integrity check requires append sync_mode",
            )

        wm_col = cfg.watermark_column
        if not wm_col:
            return IntegrityResult(
                table_id=cfg.table_id, status="skipped", enabled=True,
                method=method, reason="no watermark column to check",
            )
        lookback = int(cfg.integrity_lookback_runs)
        tolerance = int(cfg.integrity_tolerance)
        # 파티션 ts 필터: 설정되면 양쪽 질의에 동일하게 걸어 CH 파티션을 프루닝한다.
        part_col = cfg.integrity_partition_column
        part_cutoff = (
            resolve_since("timestamp", cfg.integrity_partition_period)
            if part_col and cfg.integrity_partition_period
            else None
        )
        windows = meta.recent_run_windows(cfg.table_id, wm_col, lookback)
        if not windows:
            return IntegrityResult(
                table_id=cfg.table_id, status="skipped", enabled=True,
                method=method, lookback_runs=lookback, tolerance=tolerance,
                reason="no finalized run windows to check",
            )

        # 검사/repair 식별자는 watermark 컬럼 하나로 고정한다 (모듈 docstring 참조).
        key_cols = [wm_col]
        deadletter = {
            _canon_key(r) for r in meta.unresolved_failed_keys(cfg.table_id, key_cols)
        }

        src_schema, src_name = cfg.source_parts()
        tgt_db, tgt_name = cfg.target_parts(target_default_db)

        details: list[dict] = []
        repair_keys: list[tuple] = []
        contrib_los: list = []  # 누락이 있는 window 들의 (coerce 된) watermark 하한/상한
        contrib_his: list = []
        checked = 0
        total_src = total_tgt = total_missing = total_deadletter = 0
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

            src_n, tgt_n, missing_keys, dl_hit = self._check_window(
                pg_conn, ch, src_schema, src_name, tgt_db, tgt_name,
                wm_col, key_cols, deadletter, lo, hi, method, part_col, part_cutoff,
            )
            missing = len(missing_keys)
            checked += 1
            total_src += src_n
            total_tgt += tgt_n
            total_missing += missing
            total_deadletter += dl_hit
            repair_keys.extend(missing_keys)
            if missing_keys:
                contrib_los.append(_coerce_watermark(lo))
                contrib_his.append(_coerce_watermark(hi))
            details.append({
                "run_id": w["run_id"], "watermark_lo": str(lo),
                "watermark_hi": str(hi), "source_rows": src_n, "target_rows": tgt_n,
                "missing": missing, "deadletter": dl_hit,
                # 전량이 아니라 소수 샘플만 (진단용). 로그에는 이것도 남기지 않는다.
                "missing_sample": [list(k) for k in missing_keys[:_MISSING_SAMPLE]],
            })

        if checked == 0:
            return IntegrityResult(
                table_id=cfg.table_id, status="skipped", enabled=True,
                method=method, lookback_runs=lookback, tolerance=tolerance,
                windows=details,
                reason="no checkable windows (all without a lower bound)",
            )

        status = "mismatch" if total_missing > tolerance else "ok"
        result = IntegrityResult(
            table_id=cfg.table_id, status=status, enabled=True, method=method,
            lookback_runs=lookback, windows_checked=checked, source_rows=total_src,
            target_rows=total_tgt, missing_rows=total_missing,
            deadletter_rows=total_deadletter, tolerance=tolerance, windows=details,
        )
        # 재복사용 key 는 as_dict()/XCom 에 싣지 않도록 언더스코어 속성으로 부착.
        result._repair_keys = repair_keys
        result._repair_key_cols = key_cols
        # 누락 key 들이 걸친 watermark union 구간 → repair fetch 를 이 범위로 제한.
        result._repair_wm_lo = min(contrib_los) if contrib_los else None
        result._repair_wm_hi = max(contrib_his) if contrib_his else None
        if status == "mismatch":
            # key 를 통째로 남기지 않는다(대량이면 로그 폭발). 개수 + 구간별 분포만.
            distribution = [
                {
                    "run_id": d["run_id"],
                    "window": f'({d["watermark_lo"]}, {d["watermark_hi"]}]',
                    "missing": d["missing"],
                }
                for d in details if d.get("missing")
            ]
            self.log.warning(
                "%s: integrity MISMATCH — missing=%d (deadletter=%d, excluded) "
                "across %d window(s), tolerance=%d; per-window=%s",
                cfg.table_id, total_missing, total_deadletter, checked, tolerance,
                distribution,
            )
        else:
            self.log.info(
                "%s: integrity ok — source=%d target=%d across %d window(s)",
                cfg.table_id, total_src, total_tgt, checked,
            )
        return result

    def _check_window(
        self, pg_conn, ch, src_schema, src_name, tgt_db, tgt_name,
        wm_col, key_cols, deadletter, lo, hi, method, part_col, part_cutoff,
    ):
        """한 구간 검사 → (source_count, target_count, missing_keys, deadletter_hit).

        key_cols 는 항상 ``[watermark]`` 다. count 방식은 값싼 count 게이트를 먼저
        보고 모자랄 때만 watermark 값 diff 로 무엇이 빠졌는지 찾는다. key_diff
        방식은 항상 값 집합을 비교한다. part_col/part_cutoff 가 있으면 양쪽 질의에
        ``part_col >= part_cutoff`` 를 **똑같이** 걸어(동일 조건이라 count/distinct
        비교는 그대로 성립) CH 파티션 프루닝으로 스캔을 최근 파티션으로 좁힌다.
        """
        if method == "count":
            src_n = self._pg_count(
                pg_conn, src_schema, src_name, wm_col, lo, hi, part_col, part_cutoff,
            )
            tgt_n = self._ch_count(
                ch, tgt_db, tgt_name, wm_col, key_cols, lo, hi, part_col, part_cutoff,
            )
            if src_n <= tgt_n:
                return src_n, tgt_n, [], 0
            # count 가 모자란 구간만 정확히 어떤 key 가 빠졌는지 확인.

        src_map = {}
        for row in self._pg_keys(
            pg_conn, src_schema, src_name, wm_col, key_cols, lo, hi,
            part_col, part_cutoff,
        ):
            src_map.setdefault(_canon_key(row), tuple(row))
        tgt_set = {
            _canon_key(row)
            for row in self._ch_keys(
                ch, tgt_db, tgt_name, wm_col, key_cols, lo, hi, part_col, part_cutoff,
            )
        }
        absent = set(src_map) - tgt_set
        dl_hit = absent & deadletter
        missing_canon = absent - deadletter
        missing_keys = [src_map[c] for c in missing_canon]
        return len(src_map), len(tgt_set), missing_keys, len(dl_hit)

    # ── counts / key sets ────────────────────────────────────
    @staticmethod
    def _pg_count(
        pg_conn, src_schema, src_name, wm_col, lo, hi, part_col, part_cutoff,
    ) -> int:
        src_fqn = f"{quote_pg_identifier(src_schema)}.{quote_pg_identifier(src_name)}"
        part_sql, part_params = _pg_part_clause(part_col, part_cutoff)
        sql = (
            f"SELECT count(*) FROM {src_fqn} "
            f"WHERE {quote_pg_identifier(wm_col)} > %s "
            f"AND {quote_pg_identifier(wm_col)} <= %s{part_sql}"
        )
        with pg_conn.cursor() as cur:
            cur.execute(
                sql, (_coerce_watermark(lo), _coerce_watermark(hi), *part_params)
            )
            row = cur.fetchone()
        _rollback(pg_conn)
        return int(row[0]) if row and row[0] is not None else 0

    @staticmethod
    def _ch_count(
        ch, tgt_db, tgt_name, wm_col, key_cols, lo, hi, part_col, part_cutoff,
    ) -> int:
        tgt_fqn = f"{quote_ch_identifier(tgt_db)}.{quote_ch_identifier(tgt_name)}"
        key_expr = ", ".join(quote_ch_identifier(c) for c in key_cols)
        sql = (
            f"SELECT uniqExact({key_expr}) FROM {tgt_fqn} "
            f"WHERE {quote_ch_identifier(wm_col)} > {_ch_literal(lo)} "
            f"AND {quote_ch_identifier(wm_col)} <= {_ch_literal(hi)}"
            f"{_ch_part_clause(part_col, part_cutoff)}"
        )
        res = ch.execute(sql)
        return int(res[0][0]) if res else 0

    @staticmethod
    def _pg_keys(
        pg_conn, src_schema, src_name, wm_col, key_cols, lo, hi, part_col, part_cutoff,
    ):
        src_fqn = f"{quote_pg_identifier(src_schema)}.{quote_pg_identifier(src_name)}"
        cols = ", ".join(quote_pg_identifier(c) for c in key_cols)
        part_sql, part_params = _pg_part_clause(part_col, part_cutoff)
        sql = (
            f"SELECT {cols} FROM {src_fqn} "
            f"WHERE {quote_pg_identifier(wm_col)} > %s "
            f"AND {quote_pg_identifier(wm_col)} <= %s{part_sql}"
        )
        with pg_conn.cursor() as cur:
            cur.execute(
                sql, (_coerce_watermark(lo), _coerce_watermark(hi), *part_params)
            )
            rows = cur.fetchall()
        _rollback(pg_conn)
        return rows

    @staticmethod
    def _ch_keys(
        ch, tgt_db, tgt_name, wm_col, key_cols, lo, hi, part_col, part_cutoff,
    ):
        tgt_fqn = f"{quote_ch_identifier(tgt_db)}.{quote_ch_identifier(tgt_name)}"
        cols = ", ".join(quote_ch_identifier(c) for c in key_cols)
        sql = (
            f"SELECT DISTINCT {cols} FROM {tgt_fqn} "
            f"WHERE {quote_ch_identifier(wm_col)} > {_ch_literal(lo)} "
            f"AND {quote_ch_identifier(wm_col)} <= {_ch_literal(hi)}"
            f"{_ch_part_clause(part_col, part_cutoff)}"
        )
        return ch.execute(sql) or []


def _rollback(pg_conn) -> None:
    try:
        pg_conn.rollback()  # read 트랜잭션 정리
    except Exception:
        pass
