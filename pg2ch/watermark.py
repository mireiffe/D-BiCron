"""Watermark / sync_since 계산 유틸 (순수 함수).

append 모드의 증분 cutoff 계산에 쓰인다:
  - sync_since : timestamp_column 하한 필터. 상대("30d") 또는 절대 ISO.
  - overlap_minutes : timestamp watermark 를 N분 앞당겨 재전송(중복 허용).
  - watermark_overlap : 숫자형 watermark 를 N만큼 앞당겨 재전송.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation

_RELATIVE_RE = re.compile(r"^(\d+)\s*([dhm])$", re.IGNORECASE)


def parse_relative_to_timedelta(raw: str) -> timedelta | None:
    """상대 시간 표현('30d'/'12h'/'90m')을 timedelta 로. 절대값이면 None."""
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


def resolve_sync_since(raw: str, *, now: datetime | None = None) -> str:
    """sync_since 값을 ISO timestamp 문자열로 변환.

    - 상대: "30d"(일), "12h"(시간), "90m"(분) → now 기준 과거 시각
    - 절대: ISO 8601 timestamp → 그대로 반환
    """
    delta = parse_relative_to_timedelta(raw)
    if delta is not None:
        base = now or datetime.now()
        return (base - delta).isoformat()
    return raw


def parse_watermark_overlap(raw) -> Decimal | None:
    """숫자형 watermark 의 lookback 크기를 Decimal 로. 0/None 이면 None(비활성)."""
    if raw is None:
        return None
    if isinstance(raw, bool):
        raise ValueError("watermark_overlap must be a non-negative number")
    try:
        value = Decimal(str(raw).strip())
    except (InvalidOperation, ValueError) as e:
        raise ValueError("watermark_overlap must be a non-negative number") from e
    if not value.is_finite() or value < 0:
        raise ValueError("watermark_overlap must be a non-negative number")
    if value == 0:
        return None
    return value


def apply_watermark_overlap(src_table: str, watermark, raw_overlap):
    """숫자형 watermark 에 watermark_overlap 을 적용한 cutoff 반환."""
    overlap = parse_watermark_overlap(raw_overlap)
    if overlap is None:
        return watermark
    try:
        value = Decimal(str(watermark).strip())
    except (InvalidOperation, ValueError) as e:
        raise ValueError(
            f"{src_table}: watermark_overlap requires a numeric watermark value"
        ) from e
    if not value.is_finite():
        raise ValueError(
            f"{src_table}: watermark_overlap requires a numeric watermark value"
        )
    cutoff = value - overlap
    if cutoff == cutoff.to_integral_value():
        return int(cutoff)
    return cutoff


def apply_overlap(
    src_table: str,
    watermark: str,
    *,
    overlap_minutes: int = 0,
    watermark_overlap=0,
) -> str | int | Decimal:
    """timestamp/숫자 watermark 양쪽을 처리하는 cutoff 계산.

    timestamp 로 파싱되고 overlap_minutes 가 있으면 시간 overlap 을,
    그렇지 않으면 watermark_overlap(숫자 lookback)을 적용한다.
    """
    if overlap_minutes:
        try:
            wm_dt = datetime.fromisoformat(watermark)
            return (wm_dt - timedelta(minutes=overlap_minutes)).isoformat()
        except (ValueError, TypeError):
            pass  # 숫자형 watermark → 아래 watermark_overlap 으로 처리
    return apply_watermark_overlap(src_table, watermark, watermark_overlap)
