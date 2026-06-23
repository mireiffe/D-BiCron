"""PG 결과 row → ClickHouse INSERT 호환 row 변환.

clickhouse-driver 가 types_check=True 로 INSERT 할 때 만족시켜야 하는
제약(특히 DateTime tz, non-nullable NULL, json/bool/bytea)을 흡수한다.

`build_transformer(columns)` 는 컬럼 정의 리스트를 받아 변환이 필요하면
`(row: tuple) -> tuple` 함수를, 필요 없으면 None 을 반환한다.
"""

from __future__ import annotations

import json as _json
from datetime import date, datetime
from typing import Callable

from .chtypes import (
    ch_datetime_tzinfo,
    extract_ch_datetime_tz,
    unwrap_ch_type,
)

# non-nullable CH 컬럼에 NULL 유입 시 타입별 기본값
_CH_DEFAULTS: dict[str, object] = {
    "String": "",
    "UUID": "00000000-0000-0000-0000-000000000000",
    "Date": date(1970, 1, 1),
}


def build_transformer(columns: list[dict]):
    """columns: [{name, pg_type, ch_type, [override]}, ...]"""
    transforms: dict[int, Callable] = {}
    null_coerce: dict[int, object] = {}

    for i, col in enumerate(columns):
        pg_t = col["pg_type"]
        ch_type = col["ch_type"]
        base = unwrap_ch_type(ch_type)
        is_nullable = ch_type.startswith("Nullable(")

        # 1) non-nullable 컬럼에 NULL 유입 → 타입별 기본값 치환
        if not is_nullable:
            if base in _CH_DEFAULTS:
                null_coerce[i] = _CH_DEFAULTS[base]
            elif base.startswith(("Int", "UInt", "Float", "Decimal")):
                null_coerce[i] = 0
            elif base.startswith("DateTime"):
                tz_name = extract_ch_datetime_tz(ch_type)
                null_coerce[i] = (
                    datetime(1970, 1, 1, tzinfo=ch_datetime_tzinfo(ch_type))
                    if tz_name
                    else datetime(1970, 1, 1)
                )

        override = col.get("override") or {}
        parse_format = override.get("parse_format")

        # 2) PG text → CH DateTime* (column_overrides.parse_format)
        if (
            parse_format
            and pg_t in ("character varying", "character", "text")
            and base.startswith("DateTime")
        ):
            _fmt = parse_format
            tz_override = override.get("timezone")
            if tz_override:
                try:
                    from zoneinfo import ZoneInfo

                    _tz = ZoneInfo(tz_override)
                except Exception as e:
                    raise ValueError(
                        f"column_overrides[{col['name']}].timezone "
                        f"invalid: {tz_override}"
                    ) from e
            else:
                _tz = ch_datetime_tzinfo(ch_type)
            _name = col["name"]

            def _dtparse(v, _fmt=_fmt, _tz=_tz, _name=_name):
                if v is None:
                    return v
                if not isinstance(v, str):
                    raise ValueError(
                        f"{_name}: expected string for parse_format, "
                        f"got {type(v).__name__}"
                    )
                try:
                    dt = datetime.strptime(v, _fmt)
                except ValueError as e:
                    raise ValueError(
                        f"{_name}: failed to parse {v!r} with format {_fmt!r}: {e}"
                    ) from e
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=_tz)
                return dt

            transforms[i] = _dtparse

        # 3) PG naive timestamp → CH DateTime* (tz 부착)
        elif pg_t == "timestamp without time zone" and base.startswith("DateTime"):
            _tz = ch_datetime_tzinfo(ch_type)

            def _tsconv(v, _tz=_tz):
                if v is not None and getattr(v, "tzinfo", None) is None:
                    return v.replace(tzinfo=_tz)
                return v

            transforms[i] = _tsconv

        # 4) PG json/jsonb → CH String
        elif pg_t in ("json", "jsonb"):

            def _jconv(v):
                if v is not None and not isinstance(v, str):
                    return _json.dumps(v, ensure_ascii=False, default=str)
                return v

            transforms[i] = _jconv

        # 5) PG boolean → CH UInt8
        elif pg_t == "boolean":

            def _bconv(v):
                return int(v) if v is not None else v

            transforms[i] = _bconv

        # 6) PG string → CH numeric (override)
        elif pg_t in ("character varying", "character", "text") and base.startswith(
            ("Int", "UInt", "Float", "Decimal")
        ):
            if base.startswith("Float"):
                _conv = float
            elif base.startswith("Decimal"):
                from decimal import Decimal as _Dec

                _conv = _Dec
            else:
                _conv = int

            def _nconv(v, _c=_conv):
                if v is not None and isinstance(v, str):
                    return _c(v)
                return v

            transforms[i] = _nconv

        # 7) PG numeric(Decimal) → CH Int/Float (override)
        elif pg_t == "numeric" and base.startswith(("Int", "UInt", "Float")):
            _conv = float if base.startswith("Float") else int

            def _dconv(v, _c=_conv):
                if v is not None:
                    return _c(v)
                return v

            transforms[i] = _dconv

        # 8) PG non-string (bytea/interval/inet/array 등) → CH String
        elif base == "String" and pg_t not in (
            "character varying",
            "character",
            "text",
        ):

            def _sconv(v):
                if v is not None and not isinstance(v, str):
                    if isinstance(v, (bytes, memoryview)):
                        return bytes(v).hex()
                    return str(v)
                return v

            transforms[i] = _sconv

    if not transforms and not null_coerce:
        return None

    transform_items = tuple(transforms.items())
    null_items = tuple(null_coerce.items())

    def transform(row: tuple) -> tuple:
        lst = None
        for idx, fn in transform_items:
            old = row[idx] if lst is None else lst[idx]
            new = fn(old)
            if new is not old:
                if lst is None:
                    lst = list(row)
                lst[idx] = new
        for idx, default in null_items:
            value = row[idx] if lst is None else lst[idx]
            if value is None:
                if lst is None:
                    lst = list(row)
                lst[idx] = default
        return row if lst is None else tuple(lst)

    return transform
