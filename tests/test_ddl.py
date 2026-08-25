"""Tests for pg2ch.ddl."""

from __future__ import annotations

import pytest

from pg2ch import ddl


PG_COLS = [
    {"name": "id", "pg_type": "integer", "nullable": False, "precision": None, "scale": None},
    {"name": "name", "pg_type": "text", "nullable": True, "precision": None, "scale": None},
    {"name": "secret", "pg_type": "text", "nullable": True, "precision": None, "scale": None},
    {"name": "status", "pg_type": "character varying", "nullable": True, "precision": None, "scale": None},
]


class TestBuildChColumns:
    def test_basic_mapping(self):
        result = ddl.build_ch_columns(PG_COLS, set(), {}, [])
        assert result[0] == {"name": "id", "ch_type": "Int32", "pg_type": "integer"}
        assert result[1] == {"name": "name", "ch_type": "Nullable(String)", "pg_type": "text"}

    def test_drop_column(self):
        result = ddl.build_ch_columns(PG_COLS, {"secret"}, {}, [])
        assert "secret" not in [c["name"] for c in result]
        assert len(result) == 3

    def test_string_override(self):
        result = ddl.build_ch_columns(PG_COLS, set(), {"status": "LowCardinality(String)"}, [])
        status = [c for c in result if c["name"] == "status"][0]
        assert status["ch_type"] == "LowCardinality(String)"

    def test_dict_override_with_meta(self):
        ov = {"status": {"type": "DateTime64(3, 'UTC')", "parse_format": "%Y%m%d"}}
        result = ddl.build_ch_columns(PG_COLS, set(), ov, [])
        status = [c for c in result if c["name"] == "status"][0]
        assert status["ch_type"] == "DateTime64(3, 'UTC')"
        assert status["override"] == {"parse_format": "%Y%m%d"}

    def test_text_date_override_with_parse_format(self):
        ov = {"status": {"type": "Date", "parse_format": "%Y%m%d"}}
        result = ddl.build_ch_columns(PG_COLS, set(), ov, [])
        status = [c for c in result if c["name"] == "status"][0]
        assert status["ch_type"] == "Date"
        assert status["override"] == {"parse_format": "%Y%m%d"}

    def test_dict_override_requires_type(self):
        with pytest.raises(ValueError, match="must include 'type'"):
            ddl.build_ch_columns(PG_COLS, set(), {"status": {"parse_format": "x"}}, [])

    def test_delimited_integer_array_override(self):
        cols = [
            {
                "name": "ids",
                "pg_type": "text",
                "nullable": True,
                "precision": None,
                "scale": None,
            }
        ]
        result = ddl.build_ch_columns(
            cols,
            set(),
            {"ids": {"type": "Array(Int16)", "delimiter": ","}},
            [],
        )
        assert result == [
            {
                "name": "ids",
                "pg_type": "text",
                "ch_type": "Array(Int16)",
                "override": {"delimiter": ","},
            }
        ]

    def test_delimited_array_requires_pg_string_source(self):
        cols = [
            {
                "name": "ids",
                "pg_type": "ARRAY",
                "nullable": True,
                "precision": None,
                "scale": None,
            }
        ]
        with pytest.raises(ValueError, match="requires a PostgreSQL text"):
            ddl.build_ch_columns(
                cols,
                set(),
                {"ids": {"type": "Array(Int16)", "delimiter": ","}},
                [],
            )

    def test_parse_format_requires_pg_string_source(self):
        with pytest.raises(ValueError, match="parse_format requires a PostgreSQL text"):
            ddl.build_ch_columns(
                PG_COLS,
                set(),
                {"id": {"type": "Date", "parse_format": "%Y%m%d"}},
                [],
            )

    def test_order_by_removes_nullable(self):
        result = ddl.build_ch_columns(PG_COLS, set(), {}, ["name"])
        name = [c for c in result if c["name"] == "name"][0]
        assert name["ch_type"] == "String"

    def test_use_nullable_false(self):
        result = ddl.build_ch_columns(PG_COLS, set(), {}, [], use_nullable=False)
        assert all(not c["ch_type"].startswith("Nullable(") for c in result)

    def test_use_nullable_false_respects_override(self):
        result = ddl.build_ch_columns(
            PG_COLS, set(), {"name": "Nullable(String)"}, [], use_nullable=False
        )
        name = [c for c in result if c["name"] == "name"][0]
        assert name["ch_type"] == "Nullable(String)"


class TestKeyExpr:
    def test_list(self):
        assert ddl.format_ch_key_expr(["a", "b"], name="order_by") == "(`a`, `b`)"

    def test_string(self):
        assert ddl.format_ch_key_expr("tuple()", name="order_by") == "tuple()"

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            ddl.format_ch_key_expr([], name="order_by")

    def test_extract_columns(self):
        assert ddl.extract_ch_key_columns(["a", "b"]) == {"a", "b"}
        assert ddl.extract_ch_key_columns("expr") == set()


class TestIndex:
    def test_dict_index(self):
        idx = {"name": "idx_x", "column": "x", "type": "set(1000)", "granularity": 1}
        assert ddl.format_ch_index(idx) == "INDEX `idx_x` `x` TYPE set(1000) GRANULARITY 1"

    def test_string_index_passthrough(self):
        assert ddl.format_ch_index("INDEX idx_b b TYPE minmax GRANULARITY 2") == (
            "INDEX idx_b b TYPE minmax GRANULARITY 2"
        )

    def test_missing_field(self):
        with pytest.raises(ValueError, match="missing required field"):
            ddl.format_ch_index({"name": "x", "column": "y"})

    def test_normalize(self):
        assert ddl.normalize_ch_indexes(None) == []
        assert len(ddl.normalize_ch_indexes("INDEX a b TYPE minmax GRANULARITY 1")) == 1


class TestSettings:
    def test_dict(self):
        assert ddl.format_ch_settings({"index_granularity": 8192, "allow_nullable_key": False}) == (
            "index_granularity = 8192, allow_nullable_key = 0"
        )

    def test_string(self):
        assert ddl.format_ch_settings("SETTINGS index_granularity = 8192") == "index_granularity = 8192"

    def test_none(self):
        assert ddl.format_ch_settings(None) is None


class TestCreateTableDdl:
    def test_full(self):
        columns = [
            {"name": "a", "ch_type": "UInt64", "pg_type": "bigint"},
            {"name": "b", "ch_type": "UInt64", "pg_type": "bigint"},
            {"name": "x", "ch_type": "String", "pg_type": "text"},
        ]
        sql = ddl.build_create_table_ddl(
            "tgtdb", "events", columns, ["a", "b"], "toYYYYMM(created_at)", "MergeTree",
            primary_key=["a", "b"],
            indexes=[{"name": "idx_x", "column": "x", "type": "set(1000)", "granularity": 1}],
            settings={"index_granularity": 8192},
        )
        assert "CREATE TABLE IF NOT EXISTS `tgtdb`.`events`" in sql
        assert "INDEX `idx_x` `x` TYPE set(1000) GRANULARITY 1" in sql
        assert "ENGINE = MergeTree" in sql
        assert "PARTITION BY toYYYYMM(created_at)" in sql
        assert "ORDER BY (`a`, `b`)" in sql
        assert "PRIMARY KEY (`a`, `b`)" in sql
        assert "SETTINGS index_granularity = 8192" in sql

    def test_no_partition(self):
        columns = [{"name": "id", "ch_type": "Int64", "pg_type": "bigint"}]
        sql = ddl.build_create_table_ddl(
            "db", "t", columns, ["id"], None, "ReplacingMergeTree"
        )
        assert "PARTITION BY" not in sql
        assert "ORDER BY (`id`)" in sql
        assert "PRIMARY KEY" not in sql
