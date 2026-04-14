"""Tests for dbcron/jobs/enrich/writer.py."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, call

import pytest

from dbcron.jobs.enrich.writer import (
    ensure_target_table,
    ensure_watermark_table,
    get_watermark,
    save_watermark,
    write_rows,
)


# ── helpers ──────────────────────────────────────────────────────


def _mock_engine():
    engine = MagicMock()
    ctx = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=ctx)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine, ctx


def _mock_connect_engine(fetchone_val=None):
    engine = MagicMock()
    ctx = MagicMock()
    ctx.execute.return_value.fetchone.return_value = fetchone_val
    engine.connect.return_value.__enter__ = MagicMock(return_value=ctx)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine, ctx


def _get_sql_text(call_args):
    sql = call_args[0][0]
    return sql.text if hasattr(sql, "text") else str(sql)


# ── ensure_target_table ──────────────────────────────────────────


class TestEnsureTargetTable:
    def _get_ddl(self, db_type, **kwargs):
        engine, ctx = _mock_engine()
        columns = kwargs.get("columns", {"id": "Int64", "name": "String"})
        ensure_target_table(
            engine, db_type,
            kwargs.get("table", "test_table"),
            columns,
            kwargs.get("order_by", ["id"]),
            kwargs.get("engine_clause", "MergeTree"),
            kwargs.get("partition_by"),
        )
        return _get_sql_text(ctx.execute.call_args)

    def test_clickhouse_has_engine(self):
        ddl = self._get_ddl("clickhouse")
        assert "ENGINE = MergeTree" in ddl
        assert "ORDER BY" in ddl

    def test_clickhouse_partition(self):
        ddl = self._get_ddl("clickhouse", partition_by="toYYYYMM(ts)")
        assert "PARTITION BY toYYYYMM(ts)" in ddl

    def test_pg_no_engine(self):
        ddl = self._get_ddl("postgresql")
        assert "ENGINE" not in ddl
        assert "CREATE TABLE IF NOT EXISTS" in ddl

    def test_mssql_if_not_exists(self):
        ddl = self._get_ddl("mssql", table="dbo.test_table")
        assert "IF NOT EXISTS" in ddl
        assert "sys.tables" in ddl

    def test_sqlite_simple(self):
        ddl = self._get_ddl("sqlite")
        assert "CREATE TABLE IF NOT EXISTS" in ddl
        assert "ENGINE" not in ddl


# ── ensure_watermark_table ───────────────────────────────────────


class TestEnsureWatermarkTable:
    def _get_ddl(self, db_type):
        engine, ctx = _mock_engine()
        ensure_watermark_table(engine, db_type)
        return _get_sql_text(ctx.execute.call_args)

    def test_clickhouse_replacing_merge_tree(self):
        ddl = self._get_ddl("clickhouse")
        assert "ReplacingMergeTree" in ddl

    def test_pg_standard_table(self):
        ddl = self._get_ddl("postgresql")
        assert "VARCHAR" in ddl
        assert "ReplacingMergeTree" not in ddl

    def test_mssql_nvarchar(self):
        ddl = self._get_ddl("mssql")
        assert "NVARCHAR" in ddl


# ── watermark get/save ───────────────────────────────────────────


class TestWatermark:
    def test_get_ch_uses_final(self):
        engine, ctx = _mock_connect_engine(("2026-01-01",))
        val = get_watermark(engine, "clickhouse", "k", "wm")
        assert "FINAL" in _get_sql_text(ctx.execute.call_args)
        assert val == "2026-01-01"

    def test_get_pg_no_final(self):
        engine, ctx = _mock_connect_engine(None)
        val = get_watermark(engine, "postgresql", "k", "wm")
        assert "FINAL" not in _get_sql_text(ctx.execute.call_args)
        assert val is None

    def test_save_ch_insert_only(self):
        engine, ctx = _mock_engine()
        save_watermark(engine, "clickhouse", "k", "wm", "val")
        assert ctx.execute.call_count == 1
        assert "INSERT" in _get_sql_text(ctx.execute.call_args)

    def test_save_pg_delete_then_insert(self):
        engine, ctx = _mock_engine()
        save_watermark(engine, "postgresql", "k", "wm", "val")
        assert ctx.execute.call_count == 2
        assert "DELETE" in _get_sql_text(ctx.execute.call_args_list[0])

    def test_save_datetime_value(self):
        engine, ctx = _mock_engine()
        dt = datetime(2026, 1, 1, 12, 0, 0)
        save_watermark(engine, "clickhouse", "k", "wm", dt)
        params = ctx.execute.call_args[0][1]
        assert params["value"] == "2026-01-01T12:00:00"


# ── write_rows ───────────────────────────────────────────────────


class TestWriteRows:
    def test_insert_basic(self):
        engine, ctx = _mock_engine()
        rows = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        n = write_rows(engine, "postgresql", "t", rows)
        assert n == 2
        sql = _get_sql_text(ctx.execute.call_args)
        assert "INSERT INTO" in sql

    def test_insert_empty_rows(self):
        engine, _ = _mock_engine()
        n = write_rows(engine, "postgresql", "t", [])
        assert n == 0

    def test_unknown_strategy_raises(self):
        engine, _ = _mock_engine()
        with pytest.raises(ValueError, match="Unknown write_strategy"):
            write_rows(engine, "postgresql", "t", [{"id": 1}], strategy="bad")

    # ── upsert ───────────────────────────────────────────────

    def test_upsert_pg_on_conflict(self):
        engine, ctx = _mock_engine()
        rows = [{"id": 1, "name": "a", "val": 10}]
        n = write_rows(
            engine, "postgresql", "t", rows,
            strategy="upsert", key_columns=["id"],
        )
        assert n == 1
        sql = _get_sql_text(ctx.execute.call_args)
        assert "ON CONFLICT" in sql
        assert "DO UPDATE SET" in sql
        assert '"name" = EXCLUDED."name"' in sql
        assert '"id" = EXCLUDED."id"' not in sql  # key not in SET

    def test_upsert_ch_plain_insert(self):
        engine, ctx = _mock_engine()
        rows = [{"id": 1, "name": "a"}]
        n = write_rows(
            engine, "clickhouse", "t", rows,
            strategy="upsert", key_columns=["id"],
        )
        assert n == 1
        sql = _get_sql_text(ctx.execute.call_args)
        assert "INSERT INTO" in sql
        assert "ON CONFLICT" not in sql

    def test_upsert_mssql_merge(self):
        engine, ctx = _mock_engine()
        rows = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        n = write_rows(
            engine, "mssql", "t", rows,
            strategy="upsert", key_columns=["id"],
        )
        assert n == 2
        # MSSQL merges row by row
        assert ctx.execute.call_count == 2
        sql = _get_sql_text(ctx.execute.call_args)
        assert "MERGE INTO" in sql

    def test_upsert_sqlite_replace(self):
        engine, ctx = _mock_engine()
        rows = [{"id": 1, "name": "a"}]
        n = write_rows(
            engine, "sqlite", "t", rows,
            strategy="upsert", key_columns=["id"],
        )
        assert n == 1
        sql = _get_sql_text(ctx.execute.call_args)
        assert "INSERT OR REPLACE" in sql

    def test_upsert_no_key_columns_raises(self):
        engine, _ = _mock_engine()
        with pytest.raises(ValueError, match="upsert_key_columns"):
            write_rows(engine, "postgresql", "t", [{"id": 1}], strategy="upsert")

    # ── replace ──────────────────────────────────────────────

    def test_replace_delete_then_insert(self):
        engine, ctx = _mock_engine()
        rows = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        n = write_rows(
            engine, "postgresql", "t", rows,
            strategy="replace", key_columns=["id"],
        )
        assert n == 2
        # 2 DELETE (row by row) + 1 INSERT (batch)
        assert ctx.execute.call_count == 3
        first_sql = _get_sql_text(ctx.execute.call_args_list[0])
        assert "DELETE FROM" in first_sql
        last_sql = _get_sql_text(ctx.execute.call_args_list[2])
        assert "INSERT INTO" in last_sql

    def test_replace_no_key_columns_raises(self):
        engine, _ = _mock_engine()
        with pytest.raises(ValueError, match="upsert_key_columns"):
            write_rows(engine, "postgresql", "t", [{"id": 1}], strategy="replace")


# ── derive_child_rows + write_parent_and_children ────────────────

from dbcron.jobs.enrich.writer import derive_child_rows, write_parent_and_children


class TestDeriveChildRows:
    def test_basic_1_to_n(self):
        parents = [{"id": 1}, {"id": 2}]
        responses = [
            {"details": [{"tag": "a", "value": 1.0}, {"tag": "b", "value": 2.0}]},
            {"details": [{"tag": "c", "value": 3.0}]},
        ]
        child_cfg = {
            "parent_key_column": "id",
            "foreign_key_column": "parent_id",
            "source_array_path": "details",
            "response_mapping": [
                {"json_path": "tag", "column": "tag", "type": "String"},
                {"json_path": "value", "column": "value", "type": "Float64"},
            ],
        }
        rows = derive_child_rows(parents, responses, child_cfg)
        assert len(rows) == 3
        assert rows[0] == {"tag": "a", "value": 1.0, "parent_id": 1}
        assert rows[2] == {"tag": "c", "value": 3.0, "parent_id": 2}

    def test_none_response_skipped(self):
        parents = [{"id": 1}]
        rows = derive_child_rows(
            parents, [None],
            {"parent_key_column": "id", "foreign_key_column": "fk",
             "source_array_path": "items", "response_mapping": []},
        )
        assert rows == []

    def test_empty_array_no_children(self):
        parents = [{"id": 1}]
        responses = [{"items": []}]
        rows = derive_child_rows(
            parents, responses,
            {"parent_key_column": "id", "foreign_key_column": "fk",
             "source_array_path": "items", "response_mapping": []},
        )
        assert rows == []

    def test_no_array_path_uses_response_directly(self):
        parents = [{"id": 1}]
        responses = [{"tag": "x", "val": 10}]
        child_cfg = {
            "parent_key_column": "id",
            "foreign_key_column": "fk",
            "response_mapping": [
                {"json_path": "tag", "column": "tag", "type": "String"},
            ],
        }
        rows = derive_child_rows(parents, responses, child_cfg)
        assert len(rows) == 1
        assert rows[0] == {"tag": "x", "fk": 1}


class TestWriteParentAndChildren:
    def test_parent_and_child(self):
        engine, ctx = _mock_engine()
        parent_rows = [{"id": 1, "name": "a"}]
        responses = [{"details": [{"tag": "x"}]}]
        source_rows = [{"id": 1}]
        child_cfgs = [{
            "target_table": "child_t",
            "parent_key_column": "id",
            "foreign_key_column": "parent_id",
            "source_array_path": "details",
            "response_mapping": [
                {"json_path": "tag", "column": "tag", "type": "String"},
            ],
        }]
        n = write_parent_and_children(
            engine, "postgresql", "parent_t", parent_rows,
            "insert", None, child_cfgs, responses, source_rows, None,
        )
        # 1 parent + 1 child
        assert n == 2
        # 2 execute calls (parent INSERT + child INSERT)
        assert ctx.execute.call_count == 2

    def test_no_children_config(self):
        engine, ctx = _mock_engine()
        parent_rows = [{"id": 1}]
        n = write_parent_and_children(
            engine, "postgresql", "t", parent_rows,
            "insert", None, [], [], [], None,
        )
        assert n == 1

    def test_empty_child_array_parent_only(self):
        engine, ctx = _mock_engine()
        parent_rows = [{"id": 1}]
        responses = [{"items": []}]
        source_rows = [{"id": 1}]
        child_cfgs = [{
            "target_table": "child_t",
            "parent_key_column": "id",
            "foreign_key_column": "fk",
            "source_array_path": "items",
            "response_mapping": [],
        }]
        n = write_parent_and_children(
            engine, "postgresql", "t", parent_rows,
            "insert", None, child_cfgs, responses, source_rows, None,
        )
        assert n == 1  # parent only
