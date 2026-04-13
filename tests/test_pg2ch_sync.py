"""Tests for dbcron/jobs/pg2ch_sync.py."""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from dbcron.jobs.pg2ch_sync import (
    Pg2ChSyncJob,
    _fmt_bytes,
    _pg_type_to_ch,
    _unwrap_ch_type,
)

# ── 1. _pg_type_to_ch ───────────────────────────────────────────


class TestPgTypeToCh:
    def test_integer(self):
        assert _pg_type_to_ch("integer") == "Int32"

    def test_varchar(self):
        assert _pg_type_to_ch("character varying") == "String"

    def test_timestamptz(self):
        assert _pg_type_to_ch("timestamp with time zone") == "DateTime64(6, 'UTC')"

    def test_numeric_default(self):
        assert _pg_type_to_ch("numeric") == "Decimal(18,4)"

    def test_numeric_custom(self):
        assert _pg_type_to_ch("numeric", precision=10, scale=2) == "Decimal(10,2)"

    def test_nullable(self):
        assert _pg_type_to_ch("integer", nullable=True) == "Nullable(Int32)"

    def test_array(self):
        assert _pg_type_to_ch("ARRAY") == "String"

    def test_user_defined(self):
        assert _pg_type_to_ch("USER-DEFINED") == "String"

    def test_unknown_falls_back_to_string(self):
        assert _pg_type_to_ch("some_exotic_type") == "String"


# ── 2. _unwrap_ch_type ──────────────────────────────────────────


class TestUnwrapChType:
    def test_nullable(self):
        assert _unwrap_ch_type("Nullable(Int32)") == "Int32"

    def test_low_cardinality(self):
        assert _unwrap_ch_type("LowCardinality(String)") == "String"

    def test_nested(self):
        assert _unwrap_ch_type("Nullable(LowCardinality(String))") == "String"

    def test_plain(self):
        assert _unwrap_ch_type("Int64") == "Int64"


# ── 3. _fmt_bytes ───────────────────────────────────────────────


class TestFmtBytes:
    def test_bytes(self):
        assert _fmt_bytes(512) == "512 B"

    def test_kb(self):
        assert _fmt_bytes(2048) == "2.0 KB"

    def test_mb(self):
        assert _fmt_bytes(3 * 1024 * 1024) == "3.0 MB"


# ── 4. _build_ch_columns ────────────────────────────────────────


class TestBuildChColumns:
    PG_COLS = [
        {"name": "id", "pg_type": "integer", "nullable": False, "precision": None, "scale": None},
        {"name": "name", "pg_type": "text", "nullable": True, "precision": None, "scale": None},
        {"name": "secret", "pg_type": "text", "nullable": True, "precision": None, "scale": None},
        {"name": "status", "pg_type": "character varying", "nullable": True, "precision": None, "scale": None},
    ]

    def test_basic_mapping(self):
        result = Pg2ChSyncJob._build_ch_columns(self.PG_COLS, set(), {}, [])
        assert len(result) == 4
        assert result[0] == {"name": "id", "ch_type": "Int32", "pg_type": "integer"}
        assert result[1] == {"name": "name", "ch_type": "Nullable(String)", "pg_type": "text"}

    def test_drop_column(self):
        result = Pg2ChSyncJob._build_ch_columns(self.PG_COLS, {"secret"}, {}, [])
        names = [c["name"] for c in result]
        assert "secret" not in names
        assert len(result) == 3

    def test_override(self):
        overrides = {"status": "LowCardinality(String)"}
        result = Pg2ChSyncJob._build_ch_columns(self.PG_COLS, set(), overrides, [])
        status_col = [c for c in result if c["name"] == "status"][0]
        assert status_col["ch_type"] == "LowCardinality(String)"

    def test_order_by_removes_nullable(self):
        """ORDER BY columns must not be Nullable (ClickHouse constraint)."""
        result = Pg2ChSyncJob._build_ch_columns(self.PG_COLS, set(), {}, ["name"])
        name_col = [c for c in result if c["name"] == "name"][0]
        assert name_col["ch_type"] == "String"
        assert "Nullable" not in name_col["ch_type"]


# ── 5. _build_transformer ───────────────────────────────────────


class TestBuildTransformer:
    def test_json_to_str(self):
        columns = [{"name": "data", "pg_type": "jsonb", "ch_type": "String"}]
        fn = Pg2ChSyncJob._build_transformer(columns)
        assert fn is not None
        result = fn(({"key": "val"},))
        assert result == ('{"key": "val"}',)

    def test_bool_to_int(self):
        columns = [{"name": "flag", "pg_type": "boolean", "ch_type": "UInt8"}]
        fn = Pg2ChSyncJob._build_transformer(columns)
        assert fn is not None
        assert fn((True,)) == (1,)
        assert fn((False,)) == (0,)

    def test_bytes_to_hex(self):
        columns = [{"name": "bin", "pg_type": "bytea", "ch_type": "String"}]
        fn = Pg2ChSyncJob._build_transformer(columns)
        assert fn is not None
        assert fn((b"\xde\xad",)) == ("dead",)

    def test_no_transform_needed(self):
        columns = [{"name": "txt", "pg_type": "text", "ch_type": "String"}]
        fn = Pg2ChSyncJob._build_transformer(columns)
        assert fn is None

    def test_none_passthrough(self):
        columns = [{"name": "data", "pg_type": "jsonb", "ch_type": "String"}]
        fn = Pg2ChSyncJob._build_transformer(columns)
        assert fn is not None
        assert fn((None,)) == (None,)


# ── 6. run() validation ─────────────────────────────────────────


def _patch_ch_import():
    """Context manager that makes ``from clickhouse_driver import Client`` succeed
    even when the package is not installed, by injecting a mock module."""
    mock_mod = MagicMock()
    return patch.dict("sys.modules", {"clickhouse_driver": mock_mod})


class TestRunValidation:
    def _make_job(self):
        return Pg2ChSyncJob(config=None)

    def test_clickhouse_driver_missing(self, pg2ch_config):
        """run() returns failure when clickhouse-driver is not installed."""
        import builtins
        import sys

        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "clickhouse_driver":
                raise ImportError("no clickhouse_driver")
            return real_import(name, *args, **kwargs)

        job = self._make_job()
        # Ensure clickhouse_driver is not cached in sys.modules during this test
        saved = sys.modules.pop("clickhouse_driver", None)
        try:
            with patch("builtins.__import__", side_effect=fake_import):
                result = job.run(config=pg2ch_config)
        finally:
            if saved is not None:
                sys.modules["clickhouse_driver"] = saved
        assert not result.success
        assert "clickhouse-driver" in result.message

    def test_source_not_found(self, pg2ch_config, tmp_data_dir):
        job = self._make_job()
        with (
            _patch_ch_import(),
            patch("dbcron.jobs.pg2ch_sync.get_database", side_effect=lambda x: None),
        ):
            result = job.run(config=pg2ch_config)
        assert not result.success
        assert "Source DB" in result.message
        assert "not found" in result.message

    def test_target_not_found(self, pg2ch_config, tmp_data_dir):
        pg_db = {"id": "pg_src", "type": "postgresql", "host": "localhost"}

        def fake_get(db_id):
            if db_id == "pg_src":
                return pg_db
            return None

        job = self._make_job()
        with (
            _patch_ch_import(),
            patch("dbcron.jobs.pg2ch_sync.get_database", side_effect=fake_get),
        ):
            result = job.run(config=pg2ch_config)
        assert not result.success
        assert "Target DB" in result.message
        assert "not found" in result.message

    def test_source_not_pg(self, pg2ch_config, tmp_data_dir):
        src = {"id": "pg_src", "type": "mysql", "host": "localhost"}
        tgt = {"id": "ch_tgt", "type": "clickhouse", "host": "localhost"}

        def fake_get(db_id):
            return src if db_id == "pg_src" else tgt

        job = self._make_job()
        with (
            _patch_ch_import(),
            patch("dbcron.jobs.pg2ch_sync.get_database", side_effect=fake_get),
        ):
            result = job.run(config=pg2ch_config)
        assert not result.success
        assert "Source must be postgresql" in result.message

    def test_target_not_ch(self, pg2ch_config, tmp_data_dir):
        src = {"id": "pg_src", "type": "postgresql", "host": "localhost"}
        tgt = {"id": "ch_tgt", "type": "mysql", "host": "localhost"}

        def fake_get(db_id):
            return src if db_id == "pg_src" else tgt

        job = self._make_job()
        with (
            _patch_ch_import(),
            patch("dbcron.jobs.pg2ch_sync.get_database", side_effect=fake_get),
        ):
            result = job.run(config=pg2ch_config)
        assert not result.success
        assert "Target must be clickhouse" in result.message


# ── 7. watermark ─────────────────────────────────────────────────


class TestWatermark:
    def _make_job(self):
        return Pg2ChSyncJob(config=None)

    def test_ensure_watermark_table(self):
        job = self._make_job()
        ch = MagicMock()
        job._ensure_watermark_table(ch, "tgtdb")
        ch.execute.assert_called_once()
        ddl = ch.execute.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in ddl
        assert "`tgtdb`.`_pg2ch_watermarks`" in ddl
        assert "ReplacingMergeTree" in ddl

    def test_save_and_get_roundtrip(self):
        job = self._make_job()
        ch = MagicMock()

        # save
        job._save_watermark(ch, "tgtdb", "db.tbl", "updated_at", "2026-01-01T00:00:00")
        save_call = ch.execute.call_args
        assert "INSERT INTO" in save_call[0][0]
        inserted = save_call[0][1]
        assert inserted[0][0] == "db.tbl"
        assert inserted[0][1] == "updated_at"
        assert inserted[0][2] == "2026-01-01T00:00:00"

        # get — simulate CH returning the saved value
        ch.reset_mock()
        ch.execute.return_value = [("2026-01-01T00:00:00",)]
        val = job._get_watermark(ch, "tgtdb", "db.tbl", "updated_at")
        assert val == "2026-01-01T00:00:00"
        select_sql = ch.execute.call_args[0][0]
        assert "FINAL" in select_sql

    def test_wrong_column_returns_none(self):
        job = self._make_job()
        ch = MagicMock()
        ch.execute.return_value = []
        val = job._get_watermark(ch, "tgtdb", "db.tbl", "created_at")
        assert val is None

    def test_no_rows_returns_none(self):
        job = self._make_job()
        ch = MagicMock()
        ch.execute.return_value = []
        val = job._get_watermark(ch, "tgtdb", "db.tbl", "updated_at")
        assert val is None

    def test_save_datetime_value(self):
        job = self._make_job()
        ch = MagicMock()
        dt = datetime(2026, 1, 1, 12, 0, 0)
        job._save_watermark(ch, "tgtdb", "db.tbl", "updated_at", dt)
        inserted = ch.execute.call_args[0][1]
        assert inserted[0][2] == "2026-01-01T12:00:00"


# ── 8. default_args and scope, load_config ───────────────────────


class TestJobMeta:
    def test_default_args_contains_config(self):
        assert "config" in Pg2ChSyncJob.default_args
        assert Pg2ChSyncJob.default_args["config"] == "pg2ch_config.json"

    def test_scope_is_pipeline(self):
        assert Pg2ChSyncJob.scope == "pipeline"

    def test_load_config_reads_file(self, pg2ch_config):
        cfg = Pg2ChSyncJob._load_config(pg2ch_config)
        assert cfg["source"] == "pg_src"
        assert cfg["target"] == "ch_tgt"
        assert len(cfg["tables"]) == 1

    def test_load_config_missing_file(self):
        with pytest.raises(FileNotFoundError):
            Pg2ChSyncJob._load_config("/tmp/nonexistent_pg2ch_config_12345.json")
