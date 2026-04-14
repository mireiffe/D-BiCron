"""Tests for dbcron/jobs/pg2pg_sync.py."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from unittest.mock import MagicMock, call, patch

import pytest

from dbcron.jobs.pg2pg_sync import (
    Pg2PgSyncJob,
    _ARRAY_UDT_MAP,
    _fmt_bytes,
    _parse_relative_to_timedelta,
    _reconstruct_pg_type,
    _resolve_sync_since,
    _validate_source_retention,
)

# ── 1. _reconstruct_pg_type ────────────────────────────────────


class TestReconstructPgType:
    def test_varchar_with_length(self):
        col = {"pg_type": "character varying", "max_length": 255}
        assert _reconstruct_pg_type(col) == "character varying(255)"

    def test_varchar_no_length(self):
        col = {"pg_type": "character varying", "max_length": None}
        assert _reconstruct_pg_type(col) == "character varying"

    def test_char_with_length(self):
        col = {"pg_type": "character", "max_length": 10}
        assert _reconstruct_pg_type(col) == "character(10)"

    def test_numeric_with_precision_and_scale(self):
        col = {"pg_type": "numeric", "precision": 10, "scale": 2}
        assert _reconstruct_pg_type(col) == "numeric(10,2)"

    def test_numeric_with_precision_only(self):
        col = {"pg_type": "numeric", "precision": 10, "scale": None}
        assert _reconstruct_pg_type(col) == "numeric(10)"

    def test_numeric_no_precision(self):
        col = {"pg_type": "numeric", "precision": None, "scale": None}
        assert _reconstruct_pg_type(col) == "numeric"

    def test_integer_passthrough(self):
        col = {"pg_type": "integer"}
        assert _reconstruct_pg_type(col) == "integer"

    def test_timestamp_passthrough(self):
        col = {"pg_type": "timestamp with time zone"}
        assert _reconstruct_pg_type(col) == "timestamp with time zone"

    def test_text_passthrough(self):
        col = {"pg_type": "text"}
        assert _reconstruct_pg_type(col) == "text"

    def test_boolean_passthrough(self):
        col = {"pg_type": "boolean"}
        assert _reconstruct_pg_type(col) == "boolean"

    def test_array_int4(self):
        col = {"pg_type": "ARRAY", "udt_name": "_int4"}
        assert _reconstruct_pg_type(col) == "integer[]"

    def test_array_text(self):
        col = {"pg_type": "ARRAY", "udt_name": "_text"}
        assert _reconstruct_pg_type(col) == "text[]"

    def test_array_unknown_falls_back_to_text_array(self):
        col = {"pg_type": "ARRAY", "udt_name": "_exotic"}
        assert _reconstruct_pg_type(col) == "text[]"

    def test_array_no_udt_name(self):
        col = {"pg_type": "ARRAY"}
        assert _reconstruct_pg_type(col) == "text[]"

    def test_user_defined_falls_back_to_text(self):
        col = {"pg_type": "USER-DEFINED", "udt_name": "my_enum"}
        assert _reconstruct_pg_type(col) == "text"


# ── 2. _fmt_bytes ──────────────────────────────────────────────


class TestFmtBytes:
    def test_bytes(self):
        assert _fmt_bytes(512) == "512 B"

    def test_kb(self):
        assert _fmt_bytes(2048) == "2.0 KB"

    def test_mb(self):
        assert _fmt_bytes(3 * 1024 * 1024) == "3.0 MB"


# ── 3. _build_pg_columns ──────────────────────────────────────


class TestBuildPgColumns:
    PG_COLS = [
        {"name": "id", "pg_type": "integer", "nullable": False, "precision": None, "scale": None, "max_length": None, "udt_name": "int4"},
        {"name": "name", "pg_type": "text", "nullable": True, "precision": None, "scale": None, "max_length": None, "udt_name": "text"},
        {"name": "secret", "pg_type": "text", "nullable": True, "precision": None, "scale": None, "max_length": None, "udt_name": "text"},
        {"name": "amount", "pg_type": "numeric", "nullable": True, "precision": 10, "scale": 2, "max_length": None, "udt_name": "numeric"},
    ]

    def test_basic_mapping(self):
        result = Pg2PgSyncJob._build_pg_columns(self.PG_COLS, set(), {})
        assert len(result) == 4
        assert result[0]["name"] == "id"
        assert result[0]["tgt_type"] == "integer"
        assert result[0]["nullable"] is False

    def test_preserves_nullable(self):
        result = Pg2PgSyncJob._build_pg_columns(self.PG_COLS, set(), {})
        name_col = [c for c in result if c["name"] == "name"][0]
        assert name_col["nullable"] is True

    def test_numeric_preserves_precision(self):
        result = Pg2PgSyncJob._build_pg_columns(self.PG_COLS, set(), {})
        amt_col = [c for c in result if c["name"] == "amount"][0]
        assert amt_col["tgt_type"] == "numeric(10,2)"

    def test_drop_column(self):
        result = Pg2PgSyncJob._build_pg_columns(self.PG_COLS, {"secret"}, {})
        names = [c["name"] for c in result]
        assert "secret" not in names
        assert len(result) == 3

    def test_column_override(self):
        overrides = {"amount": "numeric(18,4)"}
        result = Pg2PgSyncJob._build_pg_columns(self.PG_COLS, set(), overrides)
        amt_col = [c for c in result if c["name"] == "amount"][0]
        assert amt_col["tgt_type"] == "numeric(18,4)"


# ── 4. _build_upsert_sql ──────────────────────────────────────


class TestBuildUpsertSql:
    def test_single_conflict_key(self):
        sql = Pg2PgSyncJob._build_upsert_sql(
            "public", "orders", ["id", "name", "amount"], ["id"]
        )
        assert 'ON CONFLICT ("id") DO UPDATE SET' in sql
        assert '"name" = EXCLUDED."name"' in sql
        assert '"amount" = EXCLUDED."amount"' in sql
        # conflict key should NOT appear in SET clause
        assert '"id" = EXCLUDED."id"' not in sql

    def test_composite_conflict_key(self):
        sql = Pg2PgSyncJob._build_upsert_sql(
            "public", "orders", ["tenant_id", "id", "name"], ["tenant_id", "id"]
        )
        assert 'ON CONFLICT ("tenant_id", "id") DO UPDATE SET' in sql
        assert '"name" = EXCLUDED."name"' in sql

    def test_all_columns_are_conflict_key(self):
        sql = Pg2PgSyncJob._build_upsert_sql(
            "public", "orders", ["id", "name"], ["id", "name"]
        )
        assert "DO NOTHING" in sql
        assert "DO UPDATE" not in sql

    def test_values_placeholder(self):
        sql = Pg2PgSyncJob._build_upsert_sql(
            "public", "orders", ["id", "name"], ["id"]
        )
        assert "VALUES %s" in sql


# ── 5. _build_append_sql ──────────────────────────────────────


class TestBuildAppendSql:
    def test_basic(self):
        sql = Pg2PgSyncJob._build_append_sql("public", "orders", ["id", "name"])
        assert sql == 'INSERT INTO "public"."orders" ("id", "name") VALUES %s'


# ── 6. _build_transformer ─────────────────────────────────────


class TestBuildTransformer:
    def test_no_transform_for_basic_types(self):
        columns = [
            {"name": "id", "pg_type": "integer", "tgt_type": "integer"},
            {"name": "name", "pg_type": "text", "tgt_type": "text"},
        ]
        fn = Pg2PgSyncJob._build_transformer(columns)
        assert fn is None

    def test_user_defined_to_text(self):
        columns = [{"name": "status", "pg_type": "USER-DEFINED", "tgt_type": "text"}]
        fn = Pg2PgSyncJob._build_transformer(columns)
        assert fn is not None
        # Enum-like object
        class FakeEnum:
            def __str__(self):
                return "active"
        assert fn((FakeEnum(),)) == ("active",)

    def test_user_defined_none_passthrough(self):
        columns = [{"name": "status", "pg_type": "USER-DEFINED", "tgt_type": "text"}]
        fn = Pg2PgSyncJob._build_transformer(columns)
        assert fn((None,)) == (None,)

    def test_user_defined_string_passthrough(self):
        columns = [{"name": "status", "pg_type": "USER-DEFINED", "tgt_type": "text"}]
        fn = Pg2PgSyncJob._build_transformer(columns)
        assert fn(("active",)) == ("active",)

    def test_no_transform_when_user_defined_not_text(self):
        """USER-DEFINED with non-text target type should not transform."""
        columns = [{"name": "geo", "pg_type": "USER-DEFINED", "tgt_type": "geometry"}]
        fn = Pg2PgSyncJob._build_transformer(columns)
        assert fn is None


# ── 7. run() validation ───────────────────────────────────────


class TestRunValidation:
    def _make_job(self):
        return Pg2PgSyncJob(config=None)

    def test_psycopg2_missing(self, pg2pg_config):
        """run() returns failure when psycopg2 is not installed."""
        import builtins
        import sys

        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "psycopg2":
                raise ImportError("no psycopg2")
            return real_import(name, *args, **kwargs)

        job = self._make_job()
        saved = sys.modules.pop("psycopg2", None)
        try:
            with patch("builtins.__import__", side_effect=fake_import):
                result = job.run(config=pg2pg_config)
        finally:
            if saved is not None:
                sys.modules["psycopg2"] = saved
        assert not result.success
        assert "psycopg2" in result.message

    def test_source_not_found(self, pg2pg_config, tmp_data_dir):
        job = self._make_job()
        with patch("dbcron.jobs.pg2pg_sync.get_database", side_effect=lambda x: None):
            result = job.run(config=pg2pg_config)
        assert not result.success
        assert "Source DB" in result.message
        assert "not found" in result.message

    def test_target_not_found(self, pg2pg_config, tmp_data_dir):
        pg_db = {"id": "pg_src", "type": "postgresql", "host": "localhost"}

        def fake_get(db_id):
            if db_id == "pg_src":
                return pg_db
            return None

        job = self._make_job()
        with patch("dbcron.jobs.pg2pg_sync.get_database", side_effect=fake_get):
            result = job.run(config=pg2pg_config)
        assert not result.success
        assert "Target DB" in result.message
        assert "not found" in result.message

    def test_source_not_pg(self, pg2pg_config, tmp_data_dir):
        src = {"id": "pg_src", "type": "mysql", "host": "localhost"}
        tgt = {"id": "pg_tgt", "type": "postgresql", "host": "localhost"}

        def fake_get(db_id):
            return src if db_id == "pg_src" else tgt

        job = self._make_job()
        with patch("dbcron.jobs.pg2pg_sync.get_database", side_effect=fake_get):
            result = job.run(config=pg2pg_config)
        assert not result.success
        assert "Source must be postgresql" in result.message

    def test_target_not_pg(self, pg2pg_config, tmp_data_dir):
        src = {"id": "pg_src", "type": "postgresql", "host": "localhost"}
        tgt = {"id": "pg_tgt", "type": "clickhouse", "host": "localhost"}

        def fake_get(db_id):
            return src if db_id == "pg_src" else tgt

        job = self._make_job()
        with patch("dbcron.jobs.pg2pg_sync.get_database", side_effect=fake_get):
            result = job.run(config=pg2pg_config)
        assert not result.success
        assert "Target must be postgresql" in result.message

    def test_upsert_without_conflict_key_raises(self):
        job = self._make_job()
        tc = {
            "source_table": "public.orders",
            "target_table": "public.orders",
            "sync_mode": "upsert",
            # no conflict_key
        }
        src = {"host": "localhost", "port": 5432, "dbname": "src", "user": "u", "password": "p"}
        tgt = {"host": "localhost", "port": 5433, "dbname": "tgt", "user": "u", "password": "p"}
        sync_cfg = {"source": "pg_src", "target": "pg_tgt"}
        with pytest.raises(ValueError, match="conflict_key"):
            job._sync_table(src, tgt, tc, sync_cfg)


# ── 8. watermark ───────────────────────────────────────────────


class TestWatermark:
    def _make_job(self):
        return Pg2PgSyncJob(config=None)

    def test_ensure_watermark_table(self):
        job = self._make_job()
        pg_conn = MagicMock()
        cur = MagicMock()
        pg_conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
        pg_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        job._ensure_watermark_table(pg_conn, "public")
        cur.execute.assert_called_once()
        ddl = cur.execute.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in ddl
        assert '"public"."_pg2pg_watermarks"' in ddl
        assert "PRIMARY KEY" in ddl
        pg_conn.commit.assert_called_once()

    def test_save_and_get_roundtrip(self):
        job = self._make_job()

        # save
        pg_conn_save = MagicMock()
        cur_save = MagicMock()
        pg_conn_save.cursor.return_value.__enter__ = MagicMock(return_value=cur_save)
        pg_conn_save.cursor.return_value.__exit__ = MagicMock(return_value=False)

        job._save_watermark(pg_conn_save, "public", "db.tbl", "updated_at", "2026-01-01T00:00:00")
        save_sql = cur_save.execute.call_args[0][0]
        save_params = cur_save.execute.call_args[0][1]
        assert "INSERT INTO" in save_sql
        assert "ON CONFLICT" in save_sql
        assert save_params[0] == "db.tbl"
        assert save_params[1] == "updated_at"
        assert save_params[2] == "2026-01-01T00:00:00"

        # get
        pg_conn_get = MagicMock()
        cur_get = MagicMock()
        cur_get.fetchone.return_value = ("2026-01-01T00:00:00",)
        pg_conn_get.cursor.return_value.__enter__ = MagicMock(return_value=cur_get)
        pg_conn_get.cursor.return_value.__exit__ = MagicMock(return_value=False)

        val = job._get_watermark(pg_conn_get, "public", "db.tbl", "updated_at")
        assert val == "2026-01-01T00:00:00"

    def test_no_rows_returns_none(self):
        job = self._make_job()
        pg_conn = MagicMock()
        cur = MagicMock()
        cur.fetchone.return_value = None
        pg_conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
        pg_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        val = job._get_watermark(pg_conn, "public", "db.tbl", "updated_at")
        assert val is None

    def test_save_datetime_value(self):
        job = self._make_job()
        pg_conn = MagicMock()
        cur = MagicMock()
        pg_conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
        pg_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        dt = datetime(2026, 1, 1, 12, 0, 0)
        job._save_watermark(pg_conn, "public", "db.tbl", "updated_at", dt)
        save_params = cur.execute.call_args[0][1]
        assert save_params[2] == "2026-01-01T12:00:00"


# ── 9. _resolve_sync_since ────────────────────────────────────


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


# ── 10. _parse_relative_to_timedelta ──────────────────────────


class TestParseRelativeToTimedelta:
    def test_days(self):
        assert _parse_relative_to_timedelta("30d") == timedelta(days=30)

    def test_hours(self):
        assert _parse_relative_to_timedelta("12h") == timedelta(hours=12)

    def test_minutes(self):
        assert _parse_relative_to_timedelta("90m") == timedelta(minutes=90)

    def test_absolute_returns_none(self):
        assert _parse_relative_to_timedelta("2025-01-01T00:00:00") is None


# ── 11. _validate_source_retention ─────────────────────────────


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
        _validate_source_retention(
            "public.t", "2024-01-01T00:00:00", "2025-01-01T00:00:00", "ts"
        )

    def test_both_absolute_invalid(self):
        with pytest.raises(ValueError, match="must be older"):
            _validate_source_retention(
                "public.t", "2025-06-01T00:00:00", "2025-01-01T00:00:00", "ts"
            )


# ── 12. sync_since ─────────────────────────────────────────────


class TestSyncSince:
    def _make_job(self):
        return Pg2PgSyncJob(config=None)

    def test_sync_since_without_timestamp_column_raises(self):
        job = self._make_job()
        tc = {
            "source_table": "public.orders",
            "target_table": "public.orders",
            "sync_since": "2025-01-01T00:00:00",
        }
        src = {"host": "localhost", "port": 5432, "dbname": "src", "user": "u", "password": "p"}
        tgt = {"host": "localhost", "port": 5433, "dbname": "tgt", "user": "u", "password": "p"}
        sync_cfg = {"source": "pg_src", "target": "pg_tgt"}
        with pytest.raises(ValueError, match="sync_since requires timestamp_column"):
            job._sync_table(src, tgt, tc, sync_cfg)

    def test_sync_since_applied_in_full_copy_append(self):
        """Full copy (append) mode should add WHERE ts_col >= sync_since."""
        job = self._make_job()

        pg_src = MagicMock()
        pg_tgt = MagicMock()
        tgt_cur = MagicMock()
        pg_tgt.cursor.return_value.__enter__ = MagicMock(return_value=tgt_cur)
        pg_tgt.cursor.return_value.__exit__ = MagicMock(return_value=False)

        pg_cols = [
            {"name": "id", "pg_type": "integer", "nullable": False, "precision": None, "scale": None, "max_length": None, "udt_name": "int4"},
            {"name": "updated_at", "pg_type": "timestamp without time zone", "nullable": False, "precision": None, "scale": None, "max_length": None, "udt_name": "timestamp"},
        ]

        tc = {
            "source_table": "public.orders",
            "target_table": "public.orders",
            "timestamp_column": "updated_at",
            "sync_since": "2025-01-01T00:00:00",
            "sync_mode": "append",
        }
        sync_cfg = {"source": "pg_src"}

        with (
            patch.object(job, "_pg_connect", side_effect=[pg_src, pg_tgt]),
            patch.object(job, "_get_pg_columns", return_value=pg_cols),
            patch.object(job, "_ensure_pg_table"),
            patch.object(job, "_ensure_watermark_table"),
            patch.object(job, "_get_watermark", return_value=None),
            patch("dbcron.jobs.pg2pg_sync.execute_values"),
        ):
            stream_cursor = MagicMock()
            stream_cursor.fetchmany.return_value = []
            pg_src.cursor.return_value = stream_cursor

            job._sync_table(
                {"host": "h", "port": 5432, "dbname": "src", "user": "u", "password": "p"},
                {"host": "h", "port": 5433, "dbname": "tgt", "user": "u", "password": "p"},
                tc, sync_cfg,
            )

            execute_calls = stream_cursor.execute.call_args_list
            assert len(execute_calls) == 1
            query = execute_calls[0][0][0]
            params = execute_calls[0][0][1] if len(execute_calls[0][0]) > 1 else None
            assert 'WHERE "updated_at" >= %s' in query
            assert params == ("2025-01-01T00:00:00",)

    def test_sync_since_overrides_older_watermark(self):
        """When sync_since > watermark cutoff, sync_since should be used."""
        job = self._make_job()

        pg_src = MagicMock()
        pg_tgt = MagicMock()
        tgt_cur = MagicMock()
        pg_tgt.cursor.return_value.__enter__ = MagicMock(return_value=tgt_cur)
        pg_tgt.cursor.return_value.__exit__ = MagicMock(return_value=False)

        pg_cols = [
            {"name": "id", "pg_type": "integer", "nullable": False, "precision": None, "scale": None, "max_length": None, "udt_name": "int4"},
            {"name": "updated_at", "pg_type": "timestamp without time zone", "nullable": False, "precision": None, "scale": None, "max_length": None, "udt_name": "timestamp"},
        ]

        tc = {
            "source_table": "public.orders",
            "target_table": "public.orders",
            "timestamp_column": "updated_at",
            "sync_since": "2025-01-01T00:00:00",
            "sync_mode": "append",
        }
        sync_cfg = {"source": "pg_src"}

        with (
            patch.object(job, "_pg_connect", side_effect=[pg_src, pg_tgt]),
            patch.object(job, "_get_pg_columns", return_value=pg_cols),
            patch.object(job, "_ensure_pg_table"),
            patch.object(job, "_ensure_watermark_table"),
            patch.object(job, "_get_watermark", return_value="2024-06-01T00:00:00"),
            patch("dbcron.jobs.pg2pg_sync.execute_values"),
        ):
            stream_cursor = MagicMock()
            stream_cursor.fetchmany.return_value = []
            pg_src.cursor.return_value = stream_cursor

            job._sync_table(
                {"host": "h", "port": 5432, "dbname": "src", "user": "u", "password": "p"},
                {"host": "h", "port": 5433, "dbname": "tgt", "user": "u", "password": "p"},
                tc, sync_cfg,
            )

            execute_calls = stream_cursor.execute.call_args_list
            query = execute_calls[0][0][0]
            params = execute_calls[0][0][1]
            assert 'WHERE "updated_at" > %s' in query
            assert params == ("2025-01-01T00:00:00",)


# ── 13. sync_mode ──────────────────────────────────────────────


class TestSyncModeAppend:
    def _make_job(self):
        return Pg2PgSyncJob(config=None)

    def test_full_copy_append_truncates_target(self):
        """Full copy + append mode should TRUNCATE target."""
        job = self._make_job()

        pg_src = MagicMock()
        pg_tgt = MagicMock()
        tgt_cur = MagicMock()
        pg_tgt.cursor.return_value.__enter__ = MagicMock(return_value=tgt_cur)
        pg_tgt.cursor.return_value.__exit__ = MagicMock(return_value=False)

        pg_cols = [
            {"name": "id", "pg_type": "integer", "nullable": False, "precision": None, "scale": None, "max_length": None, "udt_name": "int4"},
        ]

        tc = {
            "source_table": "public.orders",
            "target_table": "public.orders",
            "sync_mode": "append",
        }
        sync_cfg = {"source": "pg_src"}

        with (
            patch.object(job, "_pg_connect", side_effect=[pg_src, pg_tgt]),
            patch.object(job, "_get_pg_columns", return_value=pg_cols),
            patch.object(job, "_ensure_pg_table"),
            patch.object(job, "_ensure_watermark_table"),
            patch.object(job, "_get_watermark", return_value=None),
            patch("dbcron.jobs.pg2pg_sync.execute_values"),
        ):
            stream_cursor = MagicMock()
            stream_cursor.fetchmany.return_value = []
            pg_src.cursor.return_value = stream_cursor

            job._sync_table(
                {"host": "h", "port": 5432, "dbname": "src", "user": "u", "password": "p"},
                {"host": "h", "port": 5433, "dbname": "tgt", "user": "u", "password": "p"},
                tc, sync_cfg,
            )

            # Verify TRUNCATE was called on target cursor
            all_tgt_calls = [
                str(c) for c in tgt_cur.execute.call_args_list
            ]
            assert any("TRUNCATE" in c for c in all_tgt_calls)


class TestSyncModeUpsert:
    def _make_job(self):
        return Pg2PgSyncJob(config=None)

    def test_full_copy_upsert_no_truncate(self):
        """Full copy + upsert mode should NOT truncate target."""
        job = self._make_job()

        pg_src = MagicMock()
        pg_tgt = MagicMock()
        tgt_cur = MagicMock()
        pg_tgt.cursor.return_value.__enter__ = MagicMock(return_value=tgt_cur)
        pg_tgt.cursor.return_value.__exit__ = MagicMock(return_value=False)

        pg_cols = [
            {"name": "id", "pg_type": "integer", "nullable": False, "precision": None, "scale": None, "max_length": None, "udt_name": "int4"},
            {"name": "name", "pg_type": "text", "nullable": True, "precision": None, "scale": None, "max_length": None, "udt_name": "text"},
        ]

        tc = {
            "source_table": "public.orders",
            "target_table": "public.orders",
            "sync_mode": "upsert",
            "conflict_key": ["id"],
        }
        sync_cfg = {"source": "pg_src"}

        with (
            patch.object(job, "_pg_connect", side_effect=[pg_src, pg_tgt]),
            patch.object(job, "_get_pg_columns", return_value=pg_cols),
            patch.object(job, "_ensure_pg_table"),
            patch.object(job, "_ensure_conflict_index"),
            patch.object(job, "_ensure_watermark_table"),
            patch.object(job, "_get_watermark", return_value=None),
            patch("dbcron.jobs.pg2pg_sync.execute_values"),
        ):
            stream_cursor = MagicMock()
            stream_cursor.fetchmany.return_value = []
            pg_src.cursor.return_value = stream_cursor

            job._sync_table(
                {"host": "h", "port": 5432, "dbname": "src", "user": "u", "password": "p"},
                {"host": "h", "port": 5433, "dbname": "tgt", "user": "u", "password": "p"},
                tc, sync_cfg,
            )

            # Verify TRUNCATE was NOT called
            all_tgt_calls = [
                str(c) for c in tgt_cur.execute.call_args_list
            ]
            assert not any("TRUNCATE" in c for c in all_tgt_calls)

    def test_upsert_uses_on_conflict_sql(self):
        """Upsert mode should use ON CONFLICT in the INSERT SQL."""
        job = self._make_job()

        pg_src = MagicMock()
        pg_tgt = MagicMock()
        tgt_cur = MagicMock()
        pg_tgt.cursor.return_value.__enter__ = MagicMock(return_value=tgt_cur)
        pg_tgt.cursor.return_value.__exit__ = MagicMock(return_value=False)

        pg_cols = [
            {"name": "id", "pg_type": "integer", "nullable": False, "precision": None, "scale": None, "max_length": None, "udt_name": "int4"},
            {"name": "name", "pg_type": "text", "nullable": True, "precision": None, "scale": None, "max_length": None, "udt_name": "text"},
        ]

        rows_batch = [(1, "Alice"), (2, "Bob")]
        tc = {
            "source_table": "public.orders",
            "target_table": "public.orders",
            "sync_mode": "upsert",
            "conflict_key": ["id"],
        }
        sync_cfg = {"source": "pg_src"}

        with (
            patch.object(job, "_pg_connect", side_effect=[pg_src, pg_tgt]),
            patch.object(job, "_get_pg_columns", return_value=pg_cols),
            patch.object(job, "_ensure_pg_table"),
            patch.object(job, "_ensure_conflict_index"),
            patch.object(job, "_ensure_watermark_table"),
            patch.object(job, "_get_watermark", return_value=None),
            patch("dbcron.jobs.pg2pg_sync.execute_values") as mock_ev,
        ):
            stream_cursor = MagicMock()
            stream_cursor.fetchmany.side_effect = [rows_batch, []]
            pg_src.cursor.return_value = stream_cursor

            job._sync_table(
                {"host": "h", "port": 5432, "dbname": "src", "user": "u", "password": "p"},
                {"host": "h", "port": 5433, "dbname": "tgt", "user": "u", "password": "p"},
                tc, sync_cfg,
            )

            # Verify execute_values was called with ON CONFLICT SQL
            mock_ev.assert_called()
            sql_arg = mock_ev.call_args[0][1]
            assert "ON CONFLICT" in sql_arg


# ── 14. _ensure_pg_table ──────────────────────────────────────


class TestEnsurePgTable:
    def _make_job(self):
        return Pg2PgSyncJob(config=None)

    def test_basic_ddl(self):
        job = self._make_job()
        pg_conn = MagicMock()
        cur = MagicMock()
        pg_conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
        pg_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        columns = [
            {"name": "id", "tgt_type": "integer", "pg_type": "integer", "nullable": False},
            {"name": "name", "tgt_type": "text", "pg_type": "text", "nullable": True},
        ]
        job._ensure_pg_table(pg_conn, "public", "orders", columns, None)
        ddl = cur.execute.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in ddl
        assert '"id" integer NOT NULL' in ddl
        assert '"name" text' in ddl
        assert "NOT NULL" not in ddl.split('"name" text')[1].split(",")[0]

    def test_ddl_with_conflict_key(self):
        job = self._make_job()
        pg_conn = MagicMock()
        cur = MagicMock()
        pg_conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
        pg_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        columns = [
            {"name": "id", "tgt_type": "integer", "pg_type": "integer", "nullable": False},
        ]
        job._ensure_pg_table(pg_conn, "public", "orders", columns, ["id"])
        ddl = cur.execute.call_args[0][0]
        assert 'UNIQUE ("id")' in ddl


# ── 15. _ensure_indexes ───────────────────────────────────────


class TestEnsureIndexes:
    def _make_job(self):
        return Pg2PgSyncJob(config=None)

    def test_single_index(self):
        job = self._make_job()
        pg_conn = MagicMock()
        cur = MagicMock()
        pg_conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
        pg_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        indexes = [{"columns": ["user_id"], "unique": False}]
        job._ensure_indexes(pg_conn, "public", "orders", indexes)
        ddl = cur.execute.call_args[0][0]
        assert "CREATE INDEX IF NOT EXISTS" in ddl
        assert '"user_id"' in ddl
        assert "UNIQUE" not in ddl

    def test_unique_index(self):
        job = self._make_job()
        pg_conn = MagicMock()
        cur = MagicMock()
        pg_conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
        pg_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        indexes = [{"columns": ["email"], "unique": True}]
        job._ensure_indexes(pg_conn, "public", "users", indexes)
        ddl = cur.execute.call_args[0][0]
        assert "CREATE UNIQUE INDEX IF NOT EXISTS" in ddl

    def test_composite_index(self):
        job = self._make_job()
        pg_conn = MagicMock()
        cur = MagicMock()
        pg_conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
        pg_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        indexes = [{"columns": ["tenant_id", "user_id"], "unique": False}]
        job._ensure_indexes(pg_conn, "public", "orders", indexes)
        ddl = cur.execute.call_args[0][0]
        assert '"tenant_id", "user_id"' in ddl


# ── 16. _purge_source ─────────────────────────────────────────


class TestPurgeSource:
    def _make_job(self):
        return Pg2PgSyncJob(config=None)

    def test_purge_single_batch(self):
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
        job = self._make_job()
        pg_conn = MagicMock()

        cur_ok = MagicMock()
        cur_ok.rowcount = 100
        ctx_ok = MagicMock()
        ctx_ok.__enter__ = MagicMock(return_value=cur_ok)
        ctx_ok.__exit__ = MagicMock(return_value=False)

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


# ── 17. source_retention integration ──────────────────────────


class TestSourceRetentionIntegration:
    def _make_job(self):
        return Pg2PgSyncJob(config=None)

    def test_sync_then_purge(self):
        """sync 후 _purge_source 가 호출되고 결과 tuple 에 포함."""
        job = self._make_job()

        pg_src = MagicMock()
        pg_tgt = MagicMock()
        tgt_cur = MagicMock()
        pg_tgt.cursor.return_value.__enter__ = MagicMock(return_value=tgt_cur)
        pg_tgt.cursor.return_value.__exit__ = MagicMock(return_value=False)

        pg_cols = [
            {"name": "id", "pg_type": "integer", "nullable": False, "precision": None, "scale": None, "max_length": None, "udt_name": "int4"},
            {"name": "updated_at", "pg_type": "timestamp without time zone", "nullable": False, "precision": None, "scale": None, "max_length": None, "udt_name": "timestamp"},
        ]

        tc = {
            "source_table": "public.orders",
            "target_table": "public.orders",
            "timestamp_column": "updated_at",
            "sync_since": "90d",
            "source_retention": "180d",
            "sync_mode": "append",
        }
        sync_cfg = {"source": "pg_src"}

        with (
            patch.object(job, "_pg_connect", side_effect=[pg_src, pg_tgt]),
            patch.object(job, "_get_pg_columns", return_value=pg_cols),
            patch.object(job, "_ensure_pg_table"),
            patch.object(job, "_ensure_watermark_table"),
            patch.object(job, "_get_watermark", return_value=None),
            patch.object(job, "_purge_source", return_value=42) as mock_purge,
            patch("dbcron.jobs.pg2pg_sync.execute_values"),
        ):
            stream_cursor = MagicMock()
            stream_cursor.fetchmany.return_value = []
            pg_src.cursor.return_value = stream_cursor

            synced, purged = job._sync_table(
                {"host": "h", "port": 5432, "dbname": "src", "user": "u", "password": "p"},
                {"host": "h", "port": 5433, "dbname": "tgt", "user": "u", "password": "p"},
                tc, sync_cfg,
            )
            assert purged == 42
            mock_purge.assert_called_once()

    def test_no_purge_when_retention_not_set(self):
        job = self._make_job()

        pg_src = MagicMock()
        pg_tgt = MagicMock()
        tgt_cur = MagicMock()
        pg_tgt.cursor.return_value.__enter__ = MagicMock(return_value=tgt_cur)
        pg_tgt.cursor.return_value.__exit__ = MagicMock(return_value=False)

        pg_cols = [
            {"name": "id", "pg_type": "integer", "nullable": False, "precision": None, "scale": None, "max_length": None, "udt_name": "int4"},
            {"name": "updated_at", "pg_type": "timestamp without time zone", "nullable": False, "precision": None, "scale": None, "max_length": None, "udt_name": "timestamp"},
        ]

        tc = {
            "source_table": "public.orders",
            "target_table": "public.orders",
            "timestamp_column": "updated_at",
            "sync_mode": "append",
        }
        sync_cfg = {"source": "pg_src"}

        with (
            patch.object(job, "_pg_connect", side_effect=[pg_src, pg_tgt]),
            patch.object(job, "_get_pg_columns", return_value=pg_cols),
            patch.object(job, "_ensure_pg_table"),
            patch.object(job, "_ensure_watermark_table"),
            patch.object(job, "_get_watermark", return_value=None),
            patch.object(job, "_purge_source") as mock_purge,
            patch("dbcron.jobs.pg2pg_sync.execute_values"),
        ):
            stream_cursor = MagicMock()
            stream_cursor.fetchmany.return_value = []
            pg_src.cursor.return_value = stream_cursor

            synced, purged = job._sync_table(
                {"host": "h", "port": 5432, "dbname": "src", "user": "u", "password": "p"},
                {"host": "h", "port": 5433, "dbname": "tgt", "user": "u", "password": "p"},
                tc, sync_cfg,
            )
            assert purged == 0
            mock_purge.assert_not_called()

    def test_run_message_includes_purge_count(self, pg2pg_config, tmp_data_dir):
        """run() 결과 메시지에 purge 수가 포함."""
        job = self._make_job()

        with (
            patch("dbcron.jobs.pg2pg_sync.get_database") as mock_get_db,
            patch.object(job, "_sync_table", return_value=(100, 50)),
        ):
            mock_get_db.side_effect = lambda db_id: (
                {"id": "pg_src", "type": "postgresql"} if db_id == "pg_src"
                else {"id": "pg_tgt", "type": "postgresql"}
            )
            result = job.run(config=pg2pg_config)
        assert result.success
        assert "purged 50 source rows" in result.message


# ── 18. default_args and scope, load_config ───────────────────


class TestJobMeta:
    def test_default_args_contains_config(self):
        assert "config" in Pg2PgSyncJob.default_args
        assert Pg2PgSyncJob.default_args["config"] == "pg2pg_config.json"

    def test_scope_is_pipeline(self):
        assert Pg2PgSyncJob.scope == "pipeline"

    def test_name(self):
        assert Pg2PgSyncJob.name == "pg2pg_sync"

    def test_load_config_reads_file(self, pg2pg_config):
        cfg = Pg2PgSyncJob._load_config(pg2pg_config)
        assert cfg["source"] == "pg_src"
        assert cfg["target"] == "pg_tgt"
        assert len(cfg["tables"]) == 1

    def test_load_config_missing_file(self):
        with pytest.raises(FileNotFoundError):
            Pg2PgSyncJob._load_config("/tmp/nonexistent_pg2pg_config_12345.json")
