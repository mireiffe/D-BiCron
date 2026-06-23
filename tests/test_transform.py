"""Tests for pg2ch.transform."""

from __future__ import annotations

from decimal import Decimal

from pg2ch.transform import build_transformer


def test_json_to_str():
    fn = build_transformer([{"name": "data", "pg_type": "jsonb", "ch_type": "String"}])
    assert fn is not None
    assert fn(({"key": "val"},)) == ('{"key": "val"}',)


def test_bool_to_int():
    fn = build_transformer([{"name": "flag", "pg_type": "boolean", "ch_type": "UInt8"}])
    assert fn((True,)) == (1,)
    assert fn((False,)) == (0,)


def test_bytes_to_hex():
    fn = build_transformer([{"name": "bin", "pg_type": "bytea", "ch_type": "String"}])
    assert fn((b"\xde\xad",)) == ("dead",)


def test_no_transform_needed():
    fn = build_transformer([{"name": "txt", "pg_type": "text", "ch_type": "Nullable(String)"}])
    assert fn is None


def test_none_passthrough_nullable():
    fn = build_transformer([{"name": "data", "pg_type": "jsonb", "ch_type": "Nullable(String)"}])
    assert fn((None,)) == (None,)


def test_null_coerce_string():
    fn = build_transformer([{"name": "t", "pg_type": "text", "ch_type": "String"}])
    assert fn((None,)) == ("",)
    assert fn(("hello",)) == ("hello",)


def test_null_coerce_int():
    fn = build_transformer([{"name": "n", "pg_type": "integer", "ch_type": "Int32"}])
    assert fn((None,)) == (0,)
    assert fn((42,)) == (42,)


def test_string_to_float_override():
    fn = build_transformer([{"name": "x", "pg_type": "character varying", "ch_type": "Float64"}])
    assert fn(("-22503.95903",)) == (-22503.95903,)
    assert fn((None,)) == (0,)


def test_string_to_int_override():
    fn = build_transformer([{"name": "n", "pg_type": "text", "ch_type": "Int64"}])
    assert fn(("42",)) == (42,)


def test_string_to_decimal_override():
    fn = build_transformer([{"name": "amt", "pg_type": "character varying", "ch_type": "Decimal(18,4)"}])
    assert fn(("123.45",)) == (Decimal("123.45"),)


def test_numeric_to_int_override():
    fn = build_transformer([{"name": "n", "pg_type": "numeric", "ch_type": "Int64"}])
    assert fn((Decimal("42"),)) == (42,)
    assert fn((Decimal("1.7"),)) == (1,)
    assert fn((None,)) == (0,)


def test_numeric_to_decimal_passthrough_nullable_is_none():
    fn = build_transformer([{"name": "amt", "pg_type": "numeric", "ch_type": "Nullable(Decimal(18,4))"}])
    assert fn is None


def test_timestamp_naive_gets_tz():
    from datetime import datetime, timezone

    fn = build_transformer(
        [{"name": "ts", "pg_type": "timestamp without time zone", "ch_type": "DateTime64(6, 'UTC')"}]
    )
    assert fn is not None
    out = fn((datetime(2026, 1, 1, 12, 0, 0),))[0]
    assert out.tzinfo == timezone.utc


def test_parse_format_text_to_datetime():
    from datetime import timezone

    fn = build_transformer(
        [
            {
                "name": "occurred",
                "pg_type": "text",
                "ch_type": "DateTime64(3, 'UTC')",
                "override": {"parse_format": "%Y%m%d %H%M%S"},
            }
        ]
    )
    assert fn is not None
    out = fn(("20260101 120000",))[0]
    assert out.year == 2026 and out.hour == 12
    assert out.tzinfo == timezone.utc
