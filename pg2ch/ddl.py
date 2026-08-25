"""ClickHouse 컬럼 매핑 + CREATE TABLE DDL 생성.

`build_ch_columns` 가 PG 컬럼 메타 → CH 컬럼 정의로 변환하고,
`build_create_table_ddl` 가 ENGINE / ORDER BY / PRIMARY KEY / INDEX /
PARTITION BY / SETTINGS 를 조립해 DDL 문자열을 만든다.

DDL 생성은 커넥션과 분리(순수 문자열 반환)되어 테스트 가능하다.
실제 실행(`CREATE TABLE IF NOT EXISTS`)은 copier 가 담당한다.
"""

from __future__ import annotations

import re

from .chtypes import (
    extract_ch_array_integer_type,
    pg_type_to_ch,
    quote_ch_identifier,
)


_PG_STRING_TYPES = {"character varying", "character", "text"}


# ── ORDER BY / PRIMARY KEY ─────────────────────────────────────


def extract_ch_key_columns(expr) -> set[str]:
    """list/tuple 형태의 key 식에서 컬럼명 집합 추출 (문자열 식은 무시)."""
    if isinstance(expr, (list, tuple)):
        return {str(col) for col in expr}
    return set()


def format_ch_key_expr(expr, *, name: str) -> str:
    if isinstance(expr, str):
        value = expr.strip()
        if not value:
            raise ValueError(f"{name} must not be empty")
        return value
    if isinstance(expr, (list, tuple)):
        if not expr:
            raise ValueError(f"{name} must not be empty")
        return "(" + ", ".join(quote_ch_identifier(str(c)) for c in expr) + ")"
    raise ValueError(f"{name} must be a string or list of column names")


# ── INDEX ──────────────────────────────────────────────────────


def _format_ch_index_expr(index: dict) -> str:
    if "expr" in index:
        return str(index["expr"]).strip()
    if "expression" in index:
        return str(index["expression"]).strip()
    if "column" in index:
        return quote_ch_identifier(str(index["column"]))
    if "columns" in index:
        columns = index["columns"]
        if isinstance(columns, str):
            return quote_ch_identifier(columns)
        if isinstance(columns, (list, tuple)) and columns:
            quoted = ", ".join(quote_ch_identifier(str(c)) for c in columns)
            return f"({quoted})" if len(columns) > 1 else quoted
    raise ValueError("index must define expr, expression, column, or columns")


def format_ch_index(index) -> str:
    if isinstance(index, str):
        clause = index.strip()
        if not clause:
            raise ValueError("index clause must not be empty")
        return clause if clause.upper().startswith("INDEX ") else f"INDEX {clause}"
    if not isinstance(index, dict):
        raise ValueError("index must be a string or object")
    missing = [k for k in ("name", "type", "granularity") if k not in index]
    if missing:
        raise ValueError(f"index missing required field(s): {', '.join(missing)}")
    expr = _format_ch_index_expr(index)
    if not expr:
        raise ValueError("index expression must not be empty")
    return (
        f"INDEX {quote_ch_identifier(str(index['name']))} {expr} "
        f"TYPE {index['type']} GRANULARITY {index['granularity']}"
    )


def normalize_ch_indexes(indexes) -> list:
    if not indexes:
        return []
    if isinstance(indexes, (dict, str)):
        return [indexes]
    if isinstance(indexes, (list, tuple)):
        return list(indexes)
    raise ValueError("indexes must be a string, object, or list")


# ── SETTINGS ───────────────────────────────────────────────────


def format_ch_setting_value(value) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        raw = value.strip()
        if (
            re.match(r"^-?\d+(\.\d+)?$", raw)
            or raw.startswith("'")
            or raw.startswith("[")
            or raw.startswith("(")
        ):
            return raw
        return "'" + raw.replace("\\", "\\\\").replace("'", "\\'") + "'"
    raise ValueError(f"unsupported ClickHouse setting value: {value!r}")


def format_ch_settings(settings) -> str | None:
    if not settings:
        return None
    if isinstance(settings, str):
        clause = settings.strip()
        if not clause:
            return None
        if clause.upper().startswith("SETTINGS "):
            return clause[len("SETTINGS ") :].strip()
        return clause
    if isinstance(settings, (list, tuple)):
        clauses = [str(i).strip() for i in settings if str(i).strip()]
        return ", ".join(clauses) if clauses else None
    if isinstance(settings, dict):
        clauses = [
            f"{k} = {format_ch_setting_value(v)}" for k, v in settings.items()
        ]
        return ", ".join(clauses) if clauses else None
    raise ValueError("settings must be a string, list, or object")


# ── 컬럼 매핑 ──────────────────────────────────────────────────


def build_ch_columns(
    pg_cols: list[dict],
    drop_columns: set[str],
    column_overrides: dict,
    order_by_cols: list[str],
    use_nullable: bool = True,
) -> list[dict]:
    """PG 컬럼 메타 리스트 → CH 컬럼 정의 리스트.

    pg_cols 원소: {name, pg_type, nullable, precision, scale}
    반환 원소: {name, ch_type, pg_type, [override]}

    - drop_columns 에 든 컬럼은 제외.
    - ORDER BY 컬럼은 Nullable 제거(ClickHouse 제약).
    - use_nullable=False 면 모든 컬럼 non-nullable.
    - column_overrides 값:
        * 문자열: CH 타입 그대로 (예: "LowCardinality(String)")
        * 객체: {"type": "...", "parse_format": "...", "timezone": "..."}
        * text → 정수 배열: {"type": "Array(Int16)", "delimiter": ","}
    """
    order_by_set = set(order_by_cols)
    result: list[dict] = []
    for col in pg_cols:
        name = col["name"]
        if name in drop_columns:
            continue
        override_meta: dict | None = None
        if name in column_overrides:
            ov = column_overrides[name]
            if isinstance(ov, dict):
                if "type" not in ov:
                    raise ValueError(
                        f"column_overrides[{name}] must include 'type'"
                    )
                ch_type = ov["type"]
                override_meta = {k: v for k, v in ov.items() if k != "type"}
                if "delimiter" in ov:
                    delimiter = ov["delimiter"]
                    if not isinstance(delimiter, str) or delimiter == "":
                        raise ValueError(
                            f"column_overrides[{name}].delimiter must be a "
                            f"non-empty string"
                        )
                    if extract_ch_array_integer_type(ch_type) is None:
                        raise ValueError(
                            f"column_overrides[{name}].delimiter requires type "
                            f"Array(Int*) or Array(UInt*)"
                        )
                    if col["pg_type"] not in _PG_STRING_TYPES:
                        raise ValueError(
                            f"column_overrides[{name}].delimiter requires a "
                            f"PostgreSQL text/varchar/char source column, got "
                            f"{col['pg_type']}"
                        )
            else:
                ch_type = ov
        else:
            nullable = use_nullable and col["nullable"] and name not in order_by_set
            ch_type = pg_type_to_ch(
                col["pg_type"],
                nullable=nullable,
                precision=col.get("precision"),
                scale=col.get("scale"),
            )
        entry = {"name": name, "ch_type": ch_type, "pg_type": col["pg_type"]}
        if override_meta:
            entry["override"] = override_meta
        result.append(entry)
    return result


# ── CREATE TABLE DDL ───────────────────────────────────────────


def build_create_table_ddl(
    db_name: str,
    table: str,
    columns: list[dict],
    order_by,
    partition_by: str | None,
    engine: str,
    primary_key=None,
    indexes=None,
    settings=None,
) -> str:
    """CREATE TABLE IF NOT EXISTS DDL 문자열 생성."""
    definitions = [f"{quote_ch_identifier(c['name'])} {c['ch_type']}" for c in columns]
    definitions.extend(format_ch_index(idx) for idx in normalize_ch_indexes(indexes))
    col_defs = ", ".join(definitions)

    ddl = (
        f"CREATE TABLE IF NOT EXISTS {quote_ch_identifier(db_name)}."
        f"{quote_ch_identifier(table)} ({col_defs}) ENGINE = {engine}"
    )
    if partition_by:
        ddl += f" PARTITION BY {partition_by}"
    ddl += f" ORDER BY {format_ch_key_expr(order_by, name='order_by')}"
    if primary_key:
        ddl += f" PRIMARY KEY {format_ch_key_expr(primary_key, name='primary_key')}"
    settings_clause = format_ch_settings(settings)
    if settings_clause:
        ddl += f" SETTINGS {settings_clause}"
    return ddl
