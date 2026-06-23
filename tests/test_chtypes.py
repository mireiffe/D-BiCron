"""Tests for pg2ch.chtypes."""

from __future__ import annotations

from datetime import timezone

from pg2ch import chtypes as t


class TestPgTypeToCh:
    def test_integer(self):
        assert t.pg_type_to_ch("integer") == "Int32"

    def test_bigint(self):
        assert t.pg_type_to_ch("bigint") == "Int64"

    def test_varchar(self):
        assert t.pg_type_to_ch("character varying") == "String"

    def test_timestamptz(self):
        assert t.pg_type_to_ch("timestamp with time zone") == "DateTime64(6, 'UTC')"

    def test_numeric_default(self):
        assert t.pg_type_to_ch("numeric") == "Decimal(18,4)"

    def test_numeric_custom(self):
        assert t.pg_type_to_ch("numeric", precision=10, scale=2) == "Decimal(10,2)"

    def test_nullable(self):
        assert t.pg_type_to_ch("integer", nullable=True) == "Nullable(Int32)"

    def test_array_and_user_defined(self):
        assert t.pg_type_to_ch("ARRAY") == "String"
        assert t.pg_type_to_ch("USER-DEFINED") == "String"

    def test_unknown_falls_back_to_string(self):
        assert t.pg_type_to_ch("some_exotic_type") == "String"


class TestUnwrap:
    def test_nullable(self):
        assert t.unwrap_ch_type("Nullable(Int32)") == "Int32"

    def test_low_cardinality(self):
        assert t.unwrap_ch_type("LowCardinality(String)") == "String"

    def test_nested(self):
        assert t.unwrap_ch_type("Nullable(LowCardinality(String))") == "String"

    def test_plain(self):
        assert t.unwrap_ch_type("Int64") == "Int64"


class TestDatetimeTz:
    def test_extract_tz(self):
        assert t.extract_ch_datetime_tz("DateTime64(6, 'UTC')") == "UTC"
        assert t.extract_ch_datetime_tz("DateTime('Asia/Seoul')") == "Asia/Seoul"

    def test_extract_none(self):
        assert t.extract_ch_datetime_tz("DateTime64(3)") is None
        assert t.extract_ch_datetime_tz("Int32") is None

    def test_tzinfo_utc(self):
        assert t.ch_datetime_tzinfo("DateTime64(6, 'UTC')") is timezone.utc
        assert t.ch_datetime_tzinfo("DateTime64(3)") is timezone.utc

    def test_tzinfo_named(self):
        tz = t.ch_datetime_tzinfo("DateTime('Asia/Seoul')")
        assert tz is not timezone.utc


class TestDefaultExpr:
    def test_string(self):
        assert t.ch_default_expr("String") == "''"
        assert t.ch_default_expr("LowCardinality(String)") == "''"

    def test_numeric(self):
        assert t.ch_default_expr("Int64") == "0"
        assert t.ch_default_expr("Decimal(18,4)") == "0"

    def test_datetime64(self):
        assert "DateTime64" in t.ch_default_expr("DateTime64(3, 'UTC')")

    def test_nullable_has_no_default_needed(self):
        # Nullable 은 unwrap 후 base 로 판단되므로 표현식은 존재함 — 사용처에서 Nullable 체크
        assert t.ch_default_expr("UUID") is not None


class TestQuoting:
    def test_ch_identifier(self):
        assert t.quote_ch_identifier("col") == "`col`"
        assert t.quote_ch_identifier("a`b") == "`a``b`"

    def test_ch_string(self):
        assert t.quote_ch_string("x") == "'x'"
        assert t.quote_ch_string("O'Brien") == "'O\\'Brien'"

    def test_pg_identifier(self):
        assert t.quote_pg_identifier("col") == '"col"'
        assert t.quote_pg_identifier('a"b') == '"a""b"'


class TestFmtBytes:
    def test_bytes(self):
        assert t.fmt_bytes(512) == "512 B"

    def test_kb(self):
        assert t.fmt_bytes(2048) == "2.0 KB"

    def test_mb(self):
        assert t.fmt_bytes(3 * 1024 * 1024) == "3.0 MB"
