"""PG → ClickHouse 타입 매핑 및 CH 타입 문자열 유틸리티.

순수 함수만 모아 둔 모듈 — DB 커넥션이나 설정에 의존하지 않으므로
단위 테스트가 쉽다. DDL 생성(`ddl.py`)과 row 변환(`transform.py`)이
이 모듈을 공유한다.
"""

from __future__ import annotations

import re
from datetime import timezone

# ── PG → CH 기본 타입 매핑 ──────────────────────────────────────

PG_TO_CH: dict[str, str] = {
    "smallint": "Int16",
    "integer": "Int32",
    "bigint": "Int64",
    "real": "Float32",
    "double precision": "Float64",
    "boolean": "UInt8",
    "character varying": "String",
    "character": "String",
    "text": "String",
    "bytea": "String",
    "date": "Date",
    "timestamp without time zone": "DateTime64(6, 'UTC')",
    "timestamp with time zone": "DateTime64(6, 'UTC')",
    "time without time zone": "String",
    "time with time zone": "String",
    "interval": "String",
    "json": "String",
    "jsonb": "String",
    "uuid": "UUID",
    "inet": "String",
    "cidr": "String",
    "macaddr": "String",
    "money": "Decimal(18,4)",
}


def pg_type_to_ch(
    pg_type: str,
    *,
    nullable: bool = False,
    precision: int | None = None,
    scale: int | None = None,
) -> str:
    """PG data_type 문자열 → CH 타입 문자열.

    numeric 은 precision/scale 을 반영해 Decimal 로, 알 수 없는 타입과
    ARRAY/USER-DEFINED 는 String 으로 떨어진다.
    """
    if pg_type == "numeric":
        p = precision if precision else 18
        s = scale if scale else 4
        base = f"Decimal({p},{s})"
    elif pg_type in ("ARRAY", "USER-DEFINED"):
        base = "String"
    else:
        base = PG_TO_CH.get(pg_type, "String")
    return f"Nullable({base})" if nullable else base


def unwrap_ch_type(ch_type: str) -> str:
    """Nullable / LowCardinality 래퍼를 벗겨 내고 기본 타입만 반환."""
    s = ch_type
    changed = True
    while changed:
        changed = False
        for prefix in ("Nullable(", "LowCardinality("):
            if s.startswith(prefix) and s.endswith(")"):
                s = s[len(prefix) : -1]
                changed = True
    return s


def extract_ch_datetime_tz(ch_type: str) -> str | None:
    """DateTime / DateTime64 타입 문자열에서 timezone 이름 추출. 없으면 None."""
    base = unwrap_ch_type(ch_type)
    m = re.search(r"DateTime(?:64)?\([^)]*'([^']+)'\s*\)", base)
    return m.group(1) if m else None


def ch_datetime_tzinfo(ch_type: str):
    """naive PG timestamp 에 부착할 tzinfo.

    CH 컬럼이 tz 를 가지면 그 tz 로, 없으면 UTC 로 fallback.
    (tz 없는 DateTime 에 naive datetime 을 넘기면 clickhouse-driver 가
     system tz 로 해석해 값이 흔들리므로 UTC 를 부착한다.)
    """
    tz_name = extract_ch_datetime_tz(ch_type)
    if not tz_name or tz_name.upper() == "UTC":
        return timezone.utc
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(tz_name)
    except Exception:
        return timezone.utc


def ch_default_expr(ch_type: str) -> str | None:
    """non-nullable CH 컬럼에 직접 복사(ClickHouse postgresql())할 때 쓰는
    NULL 치환용 기본값 SQL 표현식. 해당 없으면 None."""
    base = unwrap_ch_type(ch_type)
    if base == "String":
        return "''"
    if base == "UUID":
        return "toUUID('00000000-0000-0000-0000-000000000000')"
    if base == "Date":
        return "toDate('1970-01-01')"
    if base == "DateTime" or base.startswith("DateTime("):
        return "toDateTime('1970-01-01 00:00:00')"
    if base.startswith("DateTime64"):
        m = re.match(r"DateTime64\((\d+)", base)
        scale = m.group(1) if m else "6"
        return f"toDateTime64('1970-01-01 00:00:00', {scale})"
    if base.startswith(("Int", "UInt", "Float", "Decimal")):
        return "0"
    return None


# ── 식별자 / 리터럴 인용 ────────────────────────────────────────


def quote_ch_identifier(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def quote_ch_string(value) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def quote_pg_identifier(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def fmt_bytes(n: int) -> str:
    f = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if f < 1024:
            return f"{f:.1f} {unit}" if unit != "B" else f"{int(f)} B"
        f /= 1024
    return f"{f:.1f} TB"
