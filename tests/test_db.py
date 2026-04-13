"""Tests for dbcron.db module."""

from __future__ import annotations

import base64
import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from tests.conftest import SAMPLE_DBS

from dbcron.db import (
    URL_BUILDERS,
    create_engine_by_id,
    create_engine_for,
    load_databases,
    get_database,
    resolve_targets,
    should_include_table,
)


# ── URL_BUILDERS ────────────────────────────────────────────────────


class TestURLBuilders:
    """URL generation for each DB type."""

    def test_postgresql_default_port(self):
        cfg = {"host": "db.example.com", "dbname": "mydb", "user": "u", "password": "p"}
        url = URL_BUILDERS["postgresql"](cfg)
        assert url == "postgresql+psycopg2://u:p@db.example.com:5432/mydb"

    def test_postgresql_custom_port(self):
        cfg = {"host": "db.example.com", "port": 5555, "dbname": "mydb", "user": "u", "password": "p"}
        url = URL_BUILDERS["postgresql"](cfg)
        assert url == "postgresql+psycopg2://u:p@db.example.com:5555/mydb"

    def test_postgresql_empty_credentials(self):
        cfg = {"host": "localhost", "dbname": "mydb"}
        url = URL_BUILDERS["postgresql"](cfg)
        assert url == "postgresql+psycopg2://:@localhost:5432/mydb"

    def test_mssql_default_port(self):
        cfg = {"host": "sqlserver", "dbname": "prod", "user": "sa", "password": "pw"}
        url = URL_BUILDERS["mssql"](cfg)
        assert url == "mssql+pymssql://sa:pw@sqlserver:1433/prod"

    def test_mssql_custom_port(self):
        cfg = {"host": "sqlserver", "port": 2000, "dbname": "prod", "user": "sa", "password": "pw"}
        url = URL_BUILDERS["mssql"](cfg)
        assert url == "mssql+pymssql://sa:pw@sqlserver:2000/prod"

    def test_mssql_empty_credentials(self):
        cfg = {"host": "sqlserver", "dbname": "prod"}
        url = URL_BUILDERS["mssql"](cfg)
        assert url == "mssql+pymssql://:@sqlserver:1433/prod"

    def test_sqlite_url(self):
        cfg = {"host": "/data/local.db"}
        url = URL_BUILDERS["sqlite"](cfg)
        assert url == "sqlite:////data/local.db"

    def test_clickhouse_default_port(self):
        cfg = {"host": "ch.local", "dbname": "analytics", "user": "default", "password": ""}
        url = URL_BUILDERS["clickhouse"](cfg)
        assert url == "clickhouse+http://default:@ch.local:8123/analytics"

    def test_clickhouse_custom_port(self):
        cfg = {"host": "ch.local", "port": 9000, "dbname": "analytics", "user": "u", "password": "p"}
        url = URL_BUILDERS["clickhouse"](cfg)
        assert url == "clickhouse+http://u:p@ch.local:9000/analytics"


# ── should_include_table ────────────────────────────────────────────


class TestShouldIncludeTable:
    """fnmatch-based include/exclude filter."""

    def test_no_filters(self):
        assert should_include_table("orders", {}) is True

    def test_include_whitelist_match(self):
        cfg = {"include_tables": ["orders", "users"]}
        assert should_include_table("orders", cfg) is True

    def test_include_whitelist_no_match(self):
        cfg = {"include_tables": ["orders", "users"]}
        assert should_include_table("logs", cfg) is False

    def test_include_glob(self):
        cfg = {"include_tables": ["order*"]}
        assert should_include_table("orders", cfg) is True
        assert should_include_table("order_items", cfg) is True
        assert should_include_table("users", cfg) is False

    def test_exclude(self):
        cfg = {"exclude_tables": ["tmp_*"]}
        assert should_include_table("orders", cfg) is True
        assert should_include_table("tmp_cache", cfg) is False

    def test_include_and_exclude_combined(self):
        cfg = {"include_tables": ["order*"], "exclude_tables": ["order_archive"]}
        assert should_include_table("orders", cfg) is True
        assert should_include_table("order_archive", cfg) is False
        assert should_include_table("users", cfg) is False


# ── resolve_targets ─────────────────────────────────────────────────


class TestResolveTargets:
    """Schedule-target-based DB/table filtering."""

    def test_none_returns_all_dbs(self, tmp_data_dir):
        dbs, _ = resolve_targets(None)
        assert len(dbs) == len(SAMPLE_DBS)

    def test_empty_list_returns_all_dbs(self, tmp_data_dir):
        dbs, _ = resolve_targets([])
        assert len(dbs) == len(SAMPLE_DBS)

    def test_db_only_target(self, tmp_data_dir):
        dbs, filter_fn = resolve_targets([{"db": "pg_src"}])
        assert len(dbs) == 1
        assert dbs[0]["id"] == "pg_src"
        # db-only means all tables pass
        assert filter_fn("pg_src", "any_table", dbs[0]) is True

    def test_db_plus_table_target(self, tmp_data_dir):
        dbs, filter_fn = resolve_targets([{"db": "pg_src", "table": "orders"}])
        assert len(dbs) == 1
        assert filter_fn("pg_src", "orders", dbs[0]) is True
        assert filter_fn("pg_src", "users", dbs[0]) is False

    def test_escalation_db_overrides_table(self, tmp_data_dir):
        """If both db-only and db+table entries exist, db-only wins (all tables)."""
        targets = [
            {"db": "pg_src", "table": "orders"},
            {"db": "pg_src"},  # escalates to full DB
        ]
        dbs, filter_fn = resolve_targets(targets)
        assert len(dbs) == 1
        assert filter_fn("pg_src", "orders", dbs[0]) is True
        assert filter_fn("pg_src", "users", dbs[0]) is True

    def test_multiple_dbs(self, tmp_data_dir):
        targets = [{"db": "pg_src"}, {"db": "ch_tgt"}]
        dbs, _ = resolve_targets(targets)
        ids = {d["id"] for d in dbs}
        assert ids == {"pg_src", "ch_tgt"}

    def test_missing_db_key_skipped(self, tmp_data_dir):
        targets = [{"table": "orders"}]  # no "db" key
        dbs, _ = resolve_targets(targets)
        assert dbs == []

    def test_respects_include_exclude(self, tmp_data_dir):
        """Per-DB include/exclude filters are still applied even with targets."""
        # Modify the sample DB in the fixture to have an exclude rule
        db_file = tmp_data_dir / "databases.json"
        dbs_data = json.loads(db_file.read_text())
        dbs_data[0]["exclude_tables"] = ["secret_*"]
        db_file.write_text(json.dumps(dbs_data))

        dbs, filter_fn = resolve_targets([{"db": "pg_src"}])
        assert filter_fn("pg_src", "orders", dbs[0]) is True
        assert filter_fn("pg_src", "secret_logs", dbs[0]) is False


# ── load_databases ──────────────────────────────────────────────────


class TestLoadDatabases:
    """Loading and b64 password decoding."""

    def test_b64_password_decoded(self, tmp_data_dir):
        raw_pw = "super_secret"
        encoded = base64.b64encode(raw_pw.encode()).decode()
        db_file = tmp_data_dir / "databases.json"
        dbs_data = json.loads(db_file.read_text())
        dbs_data[0]["password"] = encoded
        dbs_data[0]["_enc"] = "b64"
        db_file.write_text(json.dumps(dbs_data))

        result = load_databases()
        assert result[0]["password"] == raw_pw

    def test_missing_file_returns_empty(self, tmp_path):
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        with patch("dbcron.db.DATA_DIR", empty_dir):
            assert load_databases() == []


# ── create_engine_for ───────────────────────────────────────────────


class TestCreateEngineFor:
    """Engine factory edge cases."""

    def test_unsupported_type_raises(self):
        with pytest.raises(ValueError, match="Unsupported DB type"):
            create_engine_for({"type": "oracle", "host": "h", "dbname": "d"})


# ── create_engine_by_id ─────────────────────────────────────────────


class TestCreateEngineById:
    """ID-based engine creation."""

    def test_missing_id_raises(self, tmp_data_dir):
        with pytest.raises(ValueError, match="not found"):
            create_engine_by_id("nonexistent_db")
