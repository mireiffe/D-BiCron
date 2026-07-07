"""Watermark 타입 인지 유틸 (순수 함수).

watermark 컬럼의 타입은 테이블 설정 ``watermark_type`` 으로 명시된다:
  - serial    : 증가 정수 id (serial/bigserial 등) — 파이썬 int
  - numeric   : 소수 허용 숫자 — 파이썬 Decimal
  - timestamp : 시각 — 파이썬 datetime (tz-aware 는 UTC naive 로 정규화)

증분 copy 의 cutoff / sync_since / watermark_overlap, retention 의 cutoff 계산이
전부 이 타입 기준으로 파싱·비교된다 — 값 문자열을 보고 타입을 추측하지 않는다.

타입별 표현 규칙:
  - sync_since        : timestamp → "30d"/"12h"/"90m" 상대 또는 ISO 절대,
                        serial/numeric → 절대 숫자 (하한값)
  - watermark_overlap : timestamp → "30m" 같은 상대 표현, serial/numeric → 숫자
  - retention         : timestamp → "180d" 상대(now 기준) 또는 ISO 절대,
                        serial/numeric → 숫자 N (마지막 synced 값 - N, keep-last-N)
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

WATERMARK_TYPES = ("serial", "numeric", "timestamp")

_RELATIVE_RE = re.compile(r"^(\d+)\s*([dhm])$", re.IGNORECASE)


def parse_relative_to_timedelta(raw: str) -> timedelta | None:
    """상대 시간 표현('30d'/'12h'/'90m')을 timedelta 로. 아니면 None."""
    m = _RELATIVE_RE.match(raw.strip())
    if not m:
        return None
    amount = int(m.group(1))
    unit = m.group(2).lower()
    return {
        "d": timedelta(days=amount),
        "h": timedelta(hours=amount),
        "m": timedelta(minutes=amount),
    }[unit]


def _normalize_dt(dt: datetime) -> datetime:
    """tz-aware datetime 을 UTC naive 로 정규화 (타입 간 비교 일관성)."""
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _parse_timestamp(raw) -> datetime:
    if isinstance(raw, datetime):
        return _normalize_dt(raw)
    if isinstance(raw, date):
        return datetime(raw.year, raw.month, raw.day)
    s = str(raw).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return _normalize_dt(datetime.fromisoformat(s))
    except ValueError as e:
        raise ValueError(
            f"value {raw!r} is not an ISO timestamp (watermark type is 'timestamp')"
        ) from e


def _parse_number(wm_type: str, raw) -> int | Decimal:
    try:
        d = Decimal(str(raw).strip())
    except (InvalidOperation, ValueError) as e:
        raise ValueError(
            f"value {raw!r} is not a number (watermark type is {wm_type!r})"
        ) from e
    if not d.is_finite():
        raise ValueError(
            f"value {raw!r} is not a number (watermark type is {wm_type!r})"
        )
    if wm_type == "serial":
        if d != d.to_integral_value():
            raise ValueError(
                f"value {raw!r} is not an integer (watermark type is 'serial')"
            )
        return int(d)
    return d


def parse_value(wm_type: str, raw) -> int | Decimal | datetime:
    """watermark 값(메타 TEXT / YAML / 드라이버 값)을 선언된 타입으로 파싱.

    선언된 타입으로 해석할 수 없으면 ValueError — 타입을 추측해 넘어가지 않는다
    (watermark_type 오설정을 즉시 드러내기 위함).
    """
    if raw is None:
        raise ValueError("watermark value is required")
    if isinstance(raw, bool):
        raise ValueError(f"watermark value {raw!r} is not a {wm_type}")
    if wm_type == "timestamp":
        return _parse_timestamp(raw)
    if wm_type in ("serial", "numeric"):
        return _parse_number(wm_type, raw)
    raise ValueError(
        f"unknown watermark type {wm_type!r} (use one of {', '.join(WATERMARK_TYPES)})"
    )


def resolve_since(wm_type: str, raw, *, now: datetime | None = None):
    """sync_since 표현 → 타입에 맞는 하한값.

    timestamp: "30d"/"12h"/"90m" 상대(now 기준 과거) 또는 ISO 절대.
    serial/numeric: 절대 숫자.
    """
    if wm_type == "timestamp" and isinstance(raw, str):
        delta = parse_relative_to_timedelta(raw)
        if delta is not None:
            return _normalize_dt(now or datetime.now()) - delta
    return parse_value(wm_type, raw)


def parse_overlap(wm_type: str, raw) -> timedelta | int | Decimal | None:
    """watermark_overlap 표현 → cutoff 에서 뺄 감산량. 0/None/빈 값은 None(비활성).

    timestamp: "30m"/"12h"/"1d" 상대 표현. serial: 정수. numeric: 숫자.
    """
    if raw is None:
        return None
    if isinstance(raw, bool):
        raise ValueError("watermark_overlap must not be a boolean")
    s = str(raw).strip()
    if s in ("", "0"):
        return None
    if wm_type == "timestamp":
        delta = parse_relative_to_timedelta(s)
        if delta is None:
            raise ValueError(
                f"watermark_overlap {raw!r} must be relative like '30m'/'12h'/'1d' "
                f"(watermark type is 'timestamp')"
            )
        return delta or None
    amount = _parse_number(wm_type, raw)
    if amount < 0:
        raise ValueError("watermark_overlap must be a non-negative number")
    return amount or None


def apply_overlap(wm_type: str, value, raw_overlap):
    """typed watermark 값에 overlap(재전송 lookback)을 적용한 cutoff 반환."""
    delta = parse_overlap(wm_type, raw_overlap)
    return value if delta is None else value - delta


def resolve_retention_cutoff(wm_type: str, retention, *, last_synced, now=None):
    """retention 표현 → 삭제 후보 상한(cutoff, exclusive) 값.

    timestamp     : "180d" 상대(now 기준 과거) 또는 ISO 절대 → datetime
    serial/numeric: 숫자 N → last_synced - N (마지막 synced 값 기준 keep-last-N)
    """
    if wm_type == "timestamp":
        return resolve_since("timestamp", retention, now=now)
    amount = _parse_number(wm_type, retention)
    if amount < 0:
        raise ValueError(
            f"retention {retention!r} must be a non-negative number "
            f"(watermark type is {wm_type!r})"
        )
    return last_synced - amount


def validate_retention_expr(wm_type: str | None, retention) -> None:
    """retention 표현이 타입에 맞는 형식인지 검증 (값 자체는 계산하지 않음).

    wm_type 이 None(설정 로드 시점에 유효 타입 미확정)이면 시간 표현/숫자 중
    어느 한쪽으로 파싱되면 통과 — 엄밀한 검증은 유효 타입이 정해지는 실행
    시점(PgRetention)에 다시 한다.
    """
    if wm_type is not None:
        if wm_type == "timestamp":
            resolve_since("timestamp", retention)
        else:
            amount = _parse_number(wm_type, retention)
            if amount < 0:
                raise ValueError(
                    f"retention {retention!r} must be a non-negative number "
                    f"(watermark type is {wm_type!r})"
                )
        return
    for candidate in ("timestamp", "numeric"):
        try:
            validate_retention_expr(candidate, retention)
            return
        except ValueError:
            continue
    raise ValueError(
        f"retention {retention!r} must be relative like '180d', an ISO timestamp, "
        f"or a non-negative number"
    )
