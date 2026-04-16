"""Tests for dbcron/jobs/pg2ch_sync.py."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from dbcron.jobs.pg2ch_sync import (
    Pg2ChSyncJob,
    _fmt_bytes,
    _parse_relative_to_timedelta,
    _pg_type_to_ch,
    _resolve_sync_since,
    _unwrap_ch_type,
    _validate_source_retention,
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

    def test_use_nullable_false_removes_all_nullable(self):
        """use_nullable=False 이면 모든 컬럼이 non-nullable."""
        result = Pg2ChSyncJob._build_ch_columns(
            self.PG_COLS, set(), {}, [], use_nullable=False
        )
        for col in result:
            assert not col["ch_type"].startswith("Nullable("), (
                f"{col['name']} should not be Nullable"
            )

    def test_use_nullable_true_preserves_nullable(self):
        """use_nullable=True (기본값) 이면 PG nullable 컬럼은 Nullable 유지."""
        result = Pg2ChSyncJob._build_ch_columns(
            self.PG_COLS, set(), {}, [], use_nullable=True
        )
        name_col = [c for c in result if c["name"] == "name"][0]
        assert name_col["ch_type"] == "Nullable(String)"

    def test_use_nullable_false_respects_override(self):
        """use_nullable=False 여도 column_overrides 는 그대로 적용."""
        overrides = {"name": "Nullable(String)"}
        result = Pg2ChSyncJob._build_ch_columns(
            self.PG_COLS, set(), overrides, [], use_nullable=False
        )
        name_col = [c for c in result if c["name"] == "name"][0]
        assert name_col["ch_type"] == "Nullable(String)"


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
        columns = [{"name": "txt", "pg_type": "text", "ch_type": "Nullable(String)"}]
        fn = Pg2ChSyncJob._build_transformer(columns)
        assert fn is None

    def test_none_passthrough_nullable(self):
        columns = [{"name": "data", "pg_type": "jsonb", "ch_type": "Nullable(String)"}]
        fn = Pg2ChSyncJob._build_transformer(columns)
        assert fn is not None
        assert fn((None,)) == (None,)

    def test_null_coerce_string(self):
        """Non-nullable String column should coerce None → empty string."""
        columns = [{"name": "txt", "pg_type": "text", "ch_type": "String"}]
        fn = Pg2ChSyncJob._build_transformer(columns)
        assert fn is not None
        assert fn((None,)) == ("",)
        assert fn(("hello",)) == ("hello",)

    def test_null_coerce_int(self):
        """Non-nullable Int column should coerce None → 0."""
        columns = [{"name": "n", "pg_type": "integer", "ch_type": "Int32"}]
        fn = Pg2ChSyncJob._build_transformer(columns)
        assert fn is not None
        assert fn((None,)) == (0,)
        assert fn((42,)) == (42,)

    def test_null_coerce_skipped_for_nullable(self):
        """Nullable columns should pass None through."""
        columns = [{"name": "n", "pg_type": "integer", "ch_type": "Nullable(Int32)"}]
        fn = Pg2ChSyncJob._build_transformer(columns)
        assert fn is None

    def test_string_to_float_override(self):
        """PG string column overridden to Float64 should cast str → float."""
        columns = [{"name": "x", "pg_type": "character varying", "ch_type": "Float64"}]
        fn = Pg2ChSyncJob._build_transformer(columns)
        assert fn is not None
        assert fn(("-22503.95903",)) == (-22503.95903,)
        assert fn((None,)) == (0,)

    def test_string_to_int_override(self):
        """PG text column overridden to Int64 should cast str → int."""
        columns = [{"name": "n", "pg_type": "text", "ch_type": "Int64"}]
        fn = Pg2ChSyncJob._build_transformer(columns)
        assert fn is not None
        assert fn(("42",)) == (42,)

    def test_string_to_decimal_override(self):
        """PG varchar overridden to Decimal should cast str → Decimal."""
        from decimal import Decimal

        columns = [{"name": "amt", "pg_type": "character varying", "ch_type": "Decimal(18,4)"}]
        fn = Pg2ChSyncJob._build_transformer(columns)
        assert fn is not None
        assert fn(("123.45",)) == (Decimal("123.45"),)

    def test_string_to_nullable_float_override(self):
        """PG string → Nullable(Float64) should cast str → float, pass None."""
        columns = [{"name": "x", "pg_type": "text", "ch_type": "Nullable(Float64)"}]
        fn = Pg2ChSyncJob._build_transformer(columns)
        assert fn is not None
        assert fn(("3.14",)) == (3.14,)
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


# ── 8. _resolve_sync_since ───────────────────────────────────────


class TestResolveSyncSince:
    def test_days(self):
        result = _resolve_sync_since("30d")
        expected = datetime.now() - timedelta(days=30)
        assert abs(datetime.fromisoformat(result) - expected) < timedelta(seconds=2)

    def test_hours(self):
        result = _resolve_sync_since("12h")
        expected = datetime.now() - timedelta(hours=12)
        assert abs(datetime.fromisoformat(result) - expected) < timedelta(seconds=2)

    def test_minutes(self):
        result = _resolve_sync_since("90m")
        expected = datetime.now() - timedelta(minutes=90)
        assert abs(datetime.fromisoformat(result) - expected) < timedelta(seconds=2)

    def test_absolute_passthrough(self):
        ts = "2025-01-01T00:00:00"
        assert _resolve_sync_since(ts) == ts

    def test_whitespace_stripped(self):
        result = _resolve_sync_since("  7d  ")
        expected = datetime.now() - timedelta(days=7)
        assert abs(datetime.fromisoformat(result) - expected) < timedelta(seconds=2)


# ── 9. sync_since ────────────────────────────────────────────────


class TestSyncSince:
    def _make_job(self):
        return Pg2ChSyncJob(config=None)

    def test_sync_since_without_timestamp_column_raises(self):
        """sync_since requires timestamp_column to be set."""
        job = self._make_job()
        tc = {
            "source_table": "public.orders",
            "target_table": "default.orders",
            "sync_since": "2025-01-01T00:00:00",
            "order_by": ["id"],
        }
        src = {"host": "localhost", "port": 5432, "dbname": "src", "user": "u", "password": "p"}
        tgt = {"host": "localhost", "port": 9000, "dbname": "tgt", "user": "default", "password": ""}
        sync_cfg = {"source": "pg_src", "target": "ch_tgt"}
        with pytest.raises(ValueError, match="sync_since requires timestamp_column"):
            job._sync_table(src, tgt, tc, sync_cfg)

    def test_sync_since_applied_in_full_copy(self):
        """Full copy mode should add WHERE ts_col >= sync_since."""
        job = self._make_job()
        ch = MagicMock()
        ch.execute.return_value = []  # _get_watermark → no rows

        pg_conn = MagicMock()
        cursor_mock = MagicMock()
        cursor_mock.fetchmany.return_value = []
        pg_conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor_mock)
        pg_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        # _get_pg_columns mock
        pg_cols = [
            {"name": "id", "pg_type": "integer", "nullable": False, "precision": None, "scale": None},
            {"name": "updated_at", "pg_type": "timestamp without time zone", "nullable": False, "precision": None, "scale": None},
        ]

        tc = {
            "source_table": "public.orders",
            "target_table": "default.orders",
            "timestamp_column": "updated_at",
            "sync_since": "2025-01-01T00:00:00",
            "order_by": ["id"],
            "engine": "ReplacingMergeTree",
        }
        sync_cfg = {"source": "pg_src"}

        with (
            patch.object(job, "_pg_connect", return_value=pg_conn),
            patch.object(job, "_ch_connect", return_value=ch),
            patch.object(job, "_get_pg_columns", return_value=pg_cols),
        ):
            # named cursor for streaming
            stream_cursor = MagicMock()
            stream_cursor.fetchmany.return_value = []
            pg_conn.cursor.return_value = stream_cursor

            job._sync_table(
                {"host": "h", "port": 5432, "dbname": "src", "user": "u", "password": "p"},
                {"host": "h", "port": 9000, "dbname": "tgt", "user": "default", "password": ""},
                tc,
                sync_cfg,
            )

            # Verify the SELECT query includes WHERE ... >= sync_since
            execute_calls = stream_cursor.execute.call_args_list
            assert len(execute_calls) == 1
            query = execute_calls[0][0][0]
            params = execute_calls[0][0][1] if len(execute_calls[0][0]) > 1 else None
            assert 'WHERE "updated_at" >= %s' in query
            assert params == ("2025-01-01T00:00:00",)

    def test_sync_since_overrides_older_watermark(self):
        """When sync_since > watermark cutoff, sync_since should be used."""
        job = self._make_job()
        ch = MagicMock()
        # _get_watermark returns old watermark
        ch.execute.side_effect = [
            None,  # _ensure_ch_table
            None,  # _ensure_watermark_table
            [("2024-06-01T00:00:00",)],  # _get_watermark
        ]

        pg_conn = MagicMock()
        pg_cols = [
            {"name": "id", "pg_type": "integer", "nullable": False, "precision": None, "scale": None},
            {"name": "updated_at", "pg_type": "timestamp without time zone", "nullable": False, "precision": None, "scale": None},
        ]

        tc = {
            "source_table": "public.orders",
            "target_table": "default.orders",
            "timestamp_column": "updated_at",
            "sync_since": "2025-01-01T00:00:00",
            "order_by": ["id"],
            "engine": "ReplacingMergeTree",
        }
        sync_cfg = {"source": "pg_src"}

        with (
            patch.object(job, "_pg_connect", return_value=pg_conn),
            patch.object(job, "_ch_connect", return_value=ch),
            patch.object(job, "_get_pg_columns", return_value=pg_cols),
        ):
            stream_cursor = MagicMock()
            stream_cursor.fetchmany.return_value = []
            pg_conn.cursor.return_value = stream_cursor

            job._sync_table(
                {"host": "h", "port": 5432, "dbname": "src", "user": "u", "password": "p"},
                {"host": "h", "port": 9000, "dbname": "tgt", "user": "default", "password": ""},
                tc,
                sync_cfg,
            )

            # cutoff should be sync_since (2025) not watermark (2024)
            execute_calls = stream_cursor.execute.call_args_list
            query = execute_calls[0][0][0]
            params = execute_calls[0][0][1]
            assert 'WHERE "updated_at" > %s' in query
            assert params == ("2025-01-01T00:00:00",)

    def test_sync_since_ignored_when_watermark_is_newer(self):
        """When watermark > sync_since, watermark should be used."""
        job = self._make_job()
        ch = MagicMock()
        ch.execute.side_effect = [
            None,  # _ensure_ch_table
            None,  # _ensure_watermark_table
            [("2025-06-01T00:00:00",)],  # _get_watermark (newer than sync_since)
        ]

        pg_conn = MagicMock()
        pg_cols = [
            {"name": "id", "pg_type": "integer", "nullable": False, "precision": None, "scale": None},
            {"name": "updated_at", "pg_type": "timestamp without time zone", "nullable": False, "precision": None, "scale": None},
        ]

        tc = {
            "source_table": "public.orders",
            "target_table": "default.orders",
            "timestamp_column": "updated_at",
            "sync_since": "2025-01-01T00:00:00",
            "order_by": ["id"],
            "engine": "ReplacingMergeTree",
        }
        sync_cfg = {"source": "pg_src"}

        with (
            patch.object(job, "_pg_connect", return_value=pg_conn),
            patch.object(job, "_ch_connect", return_value=ch),
            patch.object(job, "_get_pg_columns", return_value=pg_cols),
        ):
            stream_cursor = MagicMock()
            stream_cursor.fetchmany.return_value = []
            pg_conn.cursor.return_value = stream_cursor

            job._sync_table(
                {"host": "h", "port": 5432, "dbname": "src", "user": "u", "password": "p"},
                {"host": "h", "port": 9000, "dbname": "tgt", "user": "default", "password": ""},
                tc,
                sync_cfg,
            )

            # cutoff should be watermark (2025-06), not sync_since (2025-01)
            execute_calls = stream_cursor.execute.call_args_list
            params = execute_calls[0][0][1]
            assert params == ("2025-06-01T00:00:00",)


# ── 10. default_args and scope, load_config ──────────────────────


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


# ── 11. _parse_relative_to_timedelta ────────────────────────────


class TestParseRelativeToTimedelta:
    def test_days(self):
        assert _parse_relative_to_timedelta("30d") == timedelta(days=30)

    def test_hours(self):
        assert _parse_relative_to_timedelta("12h") == timedelta(hours=12)

    def test_minutes(self):
        assert _parse_relative_to_timedelta("90m") == timedelta(minutes=90)

    def test_absolute_returns_none(self):
        assert _parse_relative_to_timedelta("2025-01-01T00:00:00") is None


# ── 12. _validate_source_retention ──────────────────────────────


class TestValidateSourceRetention:
    def test_retention_without_timestamp_column_raises(self):
        with pytest.raises(ValueError, match="requires timestamp_column"):
            _validate_source_retention("public.t", "180d", "90d", None)

    def test_retention_less_than_sync_since_raises(self):
        with pytest.raises(ValueError, match="strictly greater"):
            _validate_source_retention("public.t", "30d", "90d", "ts")

    def test_retention_equal_to_sync_since_raises(self):
        with pytest.raises(ValueError, match="strictly greater"):
            _validate_source_retention("public.t", "90d", "90d", "ts")

    def test_retention_greater_than_sync_since_ok(self):
        _validate_source_retention("public.t", "180d", "90d", "ts")

    def test_retention_without_sync_since_ok(self):
        _validate_source_retention("public.t", "180d", None, "ts")

    def test_mixed_relative_absolute_raises(self):
        with pytest.raises(ValueError, match="must both be relative"):
            _validate_source_retention(
                "public.t", "180d", "2025-01-01T00:00:00", "ts"
            )

    def test_both_absolute_valid(self):
        # retention cutoff older (smaller) than sync_since cutoff
        _validate_source_retention(
            "public.t", "2024-01-01T00:00:00", "2025-01-01T00:00:00", "ts"
        )

    def test_both_absolute_invalid(self):
        # retention cutoff newer — means we'd delete data not yet synced
        with pytest.raises(ValueError, match="must be older"):
            _validate_source_retention(
                "public.t", "2025-06-01T00:00:00", "2025-01-01T00:00:00", "ts"
            )


# ── 13. _purge_source ──────────────────────────────────────────


class TestPurgeSource:
    def _make_job(self):
        return Pg2ChSyncJob(config=None)

    def test_purge_single_batch(self):
        """batch_size 보다 적게 삭제되면 한 번에 종료."""
        job = self._make_job()
        pg_conn = MagicMock()
        cur = MagicMock()
        cur.rowcount = 50
        pg_conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
        pg_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = job._purge_source(
            pg_conn, "public", "orders", "created_at",
            "2024-01-01T00:00:00", batch_size=100,
        )
        assert result == 50
        pg_conn.commit.assert_called_once()

    def test_purge_multi_batch(self):
        """여러 배치에 걸쳐 삭제."""
        job = self._make_job()
        pg_conn = MagicMock()

        cur1 = MagicMock()
        cur1.rowcount = 100
        cur2 = MagicMock()
        cur2.rowcount = 30

        ctx1 = MagicMock()
        ctx1.__enter__ = MagicMock(return_value=cur1)
        ctx1.__exit__ = MagicMock(return_value=False)
        ctx2 = MagicMock()
        ctx2.__enter__ = MagicMock(return_value=cur2)
        ctx2.__exit__ = MagicMock(return_value=False)

        pg_conn.cursor.side_effect = [ctx1, ctx2]

        result = job._purge_source(
            pg_conn, "public", "orders", "created_at",
            "2024-01-01T00:00:00", batch_size=100,
        )
        assert result == 130
        assert pg_conn.commit.call_count == 2

    def test_purge_lock_timeout_stops_gracefully(self):
        """LockNotAvailable 발생 시 graceful stop."""
        job = self._make_job()
        pg_conn = MagicMock()
        cur = MagicMock()
        # 첫 배치 성공, 두 번째에서 lock timeout
        cur_ok = MagicMock()
        cur_ok.rowcount = 100

        ctx_ok = MagicMock()
        ctx_ok.__enter__ = MagicMock(return_value=cur_ok)
        ctx_ok.__exit__ = MagicMock(return_value=False)

        # LockNotAvailable exception
        lock_err = type("LockNotAvailable", (Exception,), {})()
        ctx_fail = MagicMock()
        ctx_fail.__enter__ = MagicMock(side_effect=lock_err)
        ctx_fail.__exit__ = MagicMock(return_value=False)

        pg_conn.cursor.side_effect = [ctx_ok, ctx_fail]

        result = job._purge_source(
            pg_conn, "public", "orders", "created_at",
            "2024-01-01T00:00:00", batch_size=100,
        )
        assert result == 100
        pg_conn.rollback.assert_called_once()

    def test_purge_other_error_reraises(self):
        """lock timeout 이 아닌 에러는 re-raise."""
        job = self._make_job()
        pg_conn = MagicMock()

        ctx_fail = MagicMock()
        ctx_fail.__enter__ = MagicMock(side_effect=RuntimeError("disk full"))
        ctx_fail.__exit__ = MagicMock(return_value=False)
        pg_conn.cursor.return_value = ctx_fail

        with pytest.raises(RuntimeError, match="disk full"):
            job._purge_source(
                pg_conn, "public", "orders", "created_at",
                "2024-01-01T00:00:00",
            )

    def test_purge_sets_lock_timeout(self):
        """SET LOCAL lock_timeout 이 실행되는지 확인."""
        job = self._make_job()
        pg_conn = MagicMock()
        cur = MagicMock()
        cur.rowcount = 0
        pg_conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
        pg_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        job._purge_source(
            pg_conn, "public", "orders", "created_at",
            "2024-01-01T00:00:00", lock_timeout_ms=3000,
        )
        calls = [str(c) for c in cur.execute.call_args_list]
        assert any("lock_timeout" in c and "3000" in c for c in calls)

    def test_purge_uses_correct_cutoff(self):
        """DELETE 쿼리에 retention_cutoff 파라미터가 전달되는지 확인."""
        job = self._make_job()
        pg_conn = MagicMock()
        cur = MagicMock()
        cur.rowcount = 0
        pg_conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
        pg_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        job._purge_source(
            pg_conn, "public", "orders", "created_at",
            "2024-07-01T00:00:00", batch_size=5000,
        )
        delete_call = cur.execute.call_args_list[1]  # 두 번째 execute (첫 번째는 SET LOCAL)
        assert delete_call[0][1] == ("2024-07-01T00:00:00", 5000)


# ── 14. source_retention integration ────────────────────────────


class TestSourceRetentionIntegration:
    def _make_job(self):
        return Pg2ChSyncJob(config=None)

    def test_sync_then_purge(self):
        """sync 후 _purge_source 가 호출되고 결과 tuple 에 포함."""
        job = self._make_job()
        ch = MagicMock()
        ch.execute.return_value = []  # watermark 없음

        pg_conn = MagicMock()
        pg_cols = [
            {"name": "id", "pg_type": "integer", "nullable": False, "precision": None, "scale": None},
            {"name": "updated_at", "pg_type": "timestamp without time zone", "nullable": False, "precision": None, "scale": None},
        ]

        tc = {
            "source_table": "public.orders",
            "target_table": "default.orders",
            "timestamp_column": "updated_at",
            "sync_since": "90d",
            "source_retention": "180d",
            "order_by": ["id"],
            "engine": "ReplacingMergeTree",
        }
        sync_cfg = {"source": "pg_src"}

        with (
            patch.object(job, "_pg_connect", return_value=pg_conn),
            patch.object(job, "_ch_connect", return_value=ch),
            patch.object(job, "_get_pg_columns", return_value=pg_cols),
            patch.object(job, "_purge_source", return_value=42) as mock_purge,
        ):
            stream_cursor = MagicMock()
            stream_cursor.fetchmany.return_value = []
            pg_conn.cursor.return_value = stream_cursor

            synced, purged = job._sync_table(
                {"host": "h", "port": 5432, "dbname": "src", "user": "u", "password": "p"},
                {"host": "h", "port": 9000, "dbname": "tgt", "user": "default", "password": ""},
                tc, sync_cfg,
            )
            assert purged == 42
            mock_purge.assert_called_once()
            # retention_cutoff 이 전달되었는지 확인
            call_args = mock_purge.call_args
            assert call_args[0][0] is pg_conn
            assert call_args[0][1] == "public"
            assert call_args[0][2] == "orders"
            assert call_args[0][3] == "updated_at"

    def test_no_purge_when_retention_not_set(self):
        """source_retention 미설정 시 _purge_source 미호출."""
        job = self._make_job()
        ch = MagicMock()
        ch.execute.return_value = []

        pg_conn = MagicMock()
        pg_cols = [
            {"name": "id", "pg_type": "integer", "nullable": False, "precision": None, "scale": None},
            {"name": "updated_at", "pg_type": "timestamp without time zone", "nullable": False, "precision": None, "scale": None},
        ]

        tc = {
            "source_table": "public.orders",
            "target_table": "default.orders",
            "timestamp_column": "updated_at",
            "order_by": ["id"],
            "engine": "ReplacingMergeTree",
        }
        sync_cfg = {"source": "pg_src"}

        with (
            patch.object(job, "_pg_connect", return_value=pg_conn),
            patch.object(job, "_ch_connect", return_value=ch),
            patch.object(job, "_get_pg_columns", return_value=pg_cols),
            patch.object(job, "_purge_source") as mock_purge,
        ):
            stream_cursor = MagicMock()
            stream_cursor.fetchmany.return_value = []
            pg_conn.cursor.return_value = stream_cursor

            synced, purged = job._sync_table(
                {"host": "h", "port": 5432, "dbname": "src", "user": "u", "password": "p"},
                {"host": "h", "port": 9000, "dbname": "tgt", "user": "default", "password": ""},
                tc, sync_cfg,
            )
            assert purged == 0
            mock_purge.assert_not_called()

    def test_purge_failure_propagates_to_run(self, pg2ch_config, tmp_data_dir):
        """_purge_source 에러가 run() 에러 목록에 포함."""
        job = self._make_job()

        # pg2ch_config 에 source_retention 추가
        with open(pg2ch_config) as f:
            cfg = json.load(f)
        cfg["tables"][0]["sync_since"] = "30d"
        cfg["tables"][0]["source_retention"] = "180d"
        with open(pg2ch_config, "w") as f:
            json.dump(cfg, f)

        with (
            patch.dict("sys.modules", {"clickhouse_driver": MagicMock()}),
            patch("dbcron.jobs.pg2ch_sync.get_database") as mock_get_db,
            patch.object(job, "_sync_table", side_effect=RuntimeError("purge exploded")),
        ):
            mock_get_db.side_effect = lambda db_id: (
                {"id": "pg_src", "type": "postgresql"} if db_id == "pg_src"
                else {"id": "ch_tgt", "type": "clickhouse"}
            )
            result = job.run(config=pg2ch_config)
        assert not result.success
        assert "purge exploded" in result.message

    def test_run_message_includes_purge_count(self, pg2ch_config, tmp_data_dir):
        """run() 결과 메시지에 purge 수가 포함."""
        job = self._make_job()

        with (
            patch.dict("sys.modules", {"clickhouse_driver": MagicMock()}),
            patch("dbcron.jobs.pg2ch_sync.get_database") as mock_get_db,
            patch.object(job, "_sync_table", return_value=(100, 50)),
        ):
            mock_get_db.side_effect = lambda db_id: (
                {"id": "pg_src", "type": "postgresql"} if db_id == "pg_src"
                else {"id": "ch_tgt", "type": "clickhouse"}
            )
            result = job.run(config=pg2ch_config)
        assert result.success
        assert "purged 50 source rows" in result.message


# ── 15. watermark_column separation ────────────────────────────


class TestWatermarkColumn:
    """watermark_column 과 timestamp_column 분리 동작 검증."""

    def _make_job(self):
        return Pg2ChSyncJob(config=None)

    PG_COLS = [
        {"name": "sync_id", "pg_type": "bigint", "nullable": False, "precision": None, "scale": None},
        {"name": "created_at", "pg_type": "timestamp without time zone", "nullable": False, "precision": None, "scale": None},
        {"name": "data", "pg_type": "text", "nullable": True, "precision": None, "scale": None},
    ]

    def test_watermark_column_used_in_incremental_query(self):
        """증분 모드에서 watermark_column 으로 WHERE 절 구성."""
        job = self._make_job()
        ch = MagicMock()
        ch.execute.side_effect = [
            None,  # _ensure_ch_table
            None,  # _ensure_watermark_table
            [("100",)],  # _get_watermark → sync_id 기반
        ]

        pg_conn = MagicMock()
        tc = {
            "source_table": "public.events",
            "target_table": "default.events",
            "timestamp_column": "created_at",
            "watermark_column": "sync_id",
            "order_by": ["sync_id"],
            "engine": "MergeTree",
        }
        sync_cfg = {"source": "pg_src"}

        with (
            patch.object(job, "_pg_connect", return_value=pg_conn),
            patch.object(job, "_ch_connect", return_value=ch),
            patch.object(job, "_get_pg_columns", return_value=self.PG_COLS),
        ):
            stream_cursor = MagicMock()
            stream_cursor.fetchmany.return_value = []
            pg_conn.cursor.return_value = stream_cursor

            job._sync_table(
                {"host": "h", "port": 5432, "dbname": "src", "user": "u", "password": "p"},
                {"host": "h", "port": 9000, "dbname": "tgt", "user": "default", "password": ""},
                tc, sync_cfg,
            )

            query = stream_cursor.execute.call_args[0][0]
            params = stream_cursor.execute.call_args[0][1]
            assert '"sync_id" > %s' in query
            assert 'ORDER BY "sync_id"' in query
            # WHERE 절에 created_at 가 없어야 함 (SELECT 목록에는 있음)
            where_part = query.split("WHERE", 1)[1]
            assert '"created_at"' not in where_part
            assert params == ("100",)

    def test_watermark_column_with_sync_since_adds_both_conditions(self):
        """watermark_column != timestamp_column 일 때 sync_since 는 별도 AND 조건."""
        job = self._make_job()
        ch = MagicMock()
        ch.execute.side_effect = [
            None,  # _ensure_ch_table
            None,  # _ensure_watermark_table
            [("100",)],  # _get_watermark
        ]

        pg_conn = MagicMock()
        tc = {
            "source_table": "public.events",
            "target_table": "default.events",
            "timestamp_column": "created_at",
            "watermark_column": "sync_id",
            "sync_since": "2025-01-01T00:00:00",
            "order_by": ["sync_id"],
            "engine": "MergeTree",
        }
        sync_cfg = {"source": "pg_src"}

        with (
            patch.object(job, "_pg_connect", return_value=pg_conn),
            patch.object(job, "_ch_connect", return_value=ch),
            patch.object(job, "_get_pg_columns", return_value=self.PG_COLS),
        ):
            stream_cursor = MagicMock()
            stream_cursor.fetchmany.return_value = []
            pg_conn.cursor.return_value = stream_cursor

            job._sync_table(
                {"host": "h", "port": 5432, "dbname": "src", "user": "u", "password": "p"},
                {"host": "h", "port": 9000, "dbname": "tgt", "user": "default", "password": ""},
                tc, sync_cfg,
            )

            query = stream_cursor.execute.call_args[0][0]
            params = stream_cursor.execute.call_args[0][1]
            assert '"sync_id" > %s' in query
            assert '"created_at" >= %s' in query
            assert params == ("100", "2025-01-01T00:00:00")

    def test_watermark_saved_with_watermark_column_name(self):
        """워터마크 저장 시 watermark_column 이름으로 기록."""
        job = self._make_job()
        ch = MagicMock()
        ch.execute.side_effect = [
            None,  # _ensure_ch_table
            None,  # _ensure_watermark_table
            [],  # _get_watermark → 없음 (full copy)
            None,  # TRUNCATE
            None,  # INSERT rows
            None,  # _save_watermark
        ]

        pg_conn = MagicMock()
        tc = {
            "source_table": "public.events",
            "target_table": "default.events",
            "timestamp_column": "created_at",
            "watermark_column": "sync_id",
            "order_by": ["sync_id"],
            "engine": "MergeTree",
        }
        sync_cfg = {"source": "pg_src"}

        with (
            patch.object(job, "_pg_connect", return_value=pg_conn),
            patch.object(job, "_ch_connect", return_value=ch),
            patch.object(job, "_get_pg_columns", return_value=self.PG_COLS),
            patch.object(job, "_save_watermark") as mock_save,
        ):
            stream_cursor = MagicMock()
            stream_cursor.fetchmany.side_effect = [
                [(200, "2025-06-01", "hello")],
                [],
            ]
            pg_conn.cursor.return_value = stream_cursor

            job._sync_table(
                {"host": "h", "port": 5432, "dbname": "src", "user": "u", "password": "p"},
                {"host": "h", "port": 9000, "dbname": "tgt", "user": "default", "password": ""},
                tc, sync_cfg,
            )

            mock_save.assert_called_once()
            call_args = mock_save.call_args[0]
            assert call_args[2] == "pg_src.public.events"  # wm_key
            assert call_args[3] == "sync_id"  # wm_col, not ts_col
            assert call_args[4] == 200  # max sync_id value

    def test_retention_purge_uses_timestamp_column(self):
        """source_retention 삭제는 timestamp_column 기준."""
        job = self._make_job()
        ch = MagicMock()
        ch.execute.return_value = []  # no watermark

        pg_conn = MagicMock()
        tc = {
            "source_table": "public.events",
            "target_table": "default.events",
            "timestamp_column": "created_at",
            "watermark_column": "sync_id",
            "sync_since": "30d",
            "source_retention": "180d",
            "order_by": ["sync_id"],
            "engine": "MergeTree",
        }
        sync_cfg = {"source": "pg_src"}

        with (
            patch.object(job, "_pg_connect", return_value=pg_conn),
            patch.object(job, "_ch_connect", return_value=ch),
            patch.object(job, "_get_pg_columns", return_value=self.PG_COLS),
            patch.object(job, "_purge_source", return_value=10) as mock_purge,
        ):
            stream_cursor = MagicMock()
            stream_cursor.fetchmany.return_value = []
            pg_conn.cursor.return_value = stream_cursor

            job._sync_table(
                {"host": "h", "port": 5432, "dbname": "src", "user": "u", "password": "p"},
                {"host": "h", "port": 9000, "dbname": "tgt", "user": "default", "password": ""},
                tc, sync_cfg,
            )

            mock_purge.assert_called_once()
            purge_ts_col = mock_purge.call_args[0][3]
            assert purge_ts_col == "created_at"  # ts_col, not wm_col

    def test_fallback_to_timestamp_column_when_watermark_column_not_set(self):
        """watermark_column 미설정 시 timestamp_column 으로 fallback."""
        job = self._make_job()
        ch = MagicMock()
        ch.execute.side_effect = [
            None,  # _ensure_ch_table
            None,  # _ensure_watermark_table
            [("2025-06-01T00:00:00",)],  # _get_watermark
        ]

        pg_conn = MagicMock()
        tc = {
            "source_table": "public.events",
            "target_table": "default.events",
            "timestamp_column": "created_at",
            # watermark_column 미설정
            "order_by": ["created_at"],
            "engine": "MergeTree",
        }
        sync_cfg = {"source": "pg_src"}

        with (
            patch.object(job, "_pg_connect", return_value=pg_conn),
            patch.object(job, "_ch_connect", return_value=ch),
            patch.object(job, "_get_pg_columns", return_value=self.PG_COLS),
        ):
            stream_cursor = MagicMock()
            stream_cursor.fetchmany.return_value = []
            pg_conn.cursor.return_value = stream_cursor

            job._sync_table(
                {"host": "h", "port": 5432, "dbname": "src", "user": "u", "password": "p"},
                {"host": "h", "port": 9000, "dbname": "tgt", "user": "default", "password": ""},
                tc, sync_cfg,
            )

            query = stream_cursor.execute.call_args[0][0]
            # fallback: created_at 이 워터마크 컬럼으로 사용됨
            assert '"created_at" > %s' in query

    def test_full_copy_orders_by_watermark_column(self):
        """Full copy 모드에서 ORDER BY 는 watermark_column 사용."""
        job = self._make_job()
        ch = MagicMock()
        ch.execute.return_value = []  # no watermark → full copy

        pg_conn = MagicMock()
        tc = {
            "source_table": "public.events",
            "target_table": "default.events",
            "timestamp_column": "created_at",
            "watermark_column": "sync_id",
            "order_by": ["sync_id"],
            "engine": "MergeTree",
        }
        sync_cfg = {"source": "pg_src"}

        with (
            patch.object(job, "_pg_connect", return_value=pg_conn),
            patch.object(job, "_ch_connect", return_value=ch),
            patch.object(job, "_get_pg_columns", return_value=self.PG_COLS),
        ):
            stream_cursor = MagicMock()
            stream_cursor.fetchmany.return_value = []
            pg_conn.cursor.return_value = stream_cursor

            job._sync_table(
                {"host": "h", "port": 5432, "dbname": "src", "user": "u", "password": "p"},
                {"host": "h", "port": 9000, "dbname": "tgt", "user": "default", "password": ""},
                tc, sync_cfg,
            )

            query = stream_cursor.execute.call_args[0][0]
            assert 'ORDER BY "sync_id"' in query
