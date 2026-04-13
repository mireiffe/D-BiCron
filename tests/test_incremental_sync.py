"""Tests for dbcron/jobs/incremental_sync.py — IncrementalSyncJob."""

from __future__ import annotations

import json
from unittest.mock import patch, MagicMock, call

import pytest

from dbcron.jobs.incremental_sync import IncrementalSyncJob


class TestBuildEngine:
    def test_string_ref_calls_create_engine_by_id(self):
        """1. _build_engine with string ref calls create_engine_by_id."""
        mock_engine = MagicMock()
        sync_cfg = {"source": "pg_src", "target": "ch_tgt"}
        job = IncrementalSyncJob()

        with patch(
            "dbcron.jobs.incremental_sync.create_engine_by_id",
            return_value=mock_engine,
        ) as mock_by_id:
            engine = job._build_engine(sync_cfg, "source")

        mock_by_id.assert_called_once_with("pg_src")
        assert engine is mock_engine

    def test_dict_ref_creates_engine_from_inline_info(self):
        """2. _build_engine with dict ref creates engine from inline connection info."""
        mock_engine = MagicMock()
        sync_cfg = {
            "source": {
                "user": "admin",
                "password": "secret",
                "host": "db.example.com",
                "port": 5432,
                "database": "mydb",
            },
            "target": "ch_tgt",
        }
        job = IncrementalSyncJob()

        with patch(
            "dbcron.jobs.incremental_sync.create_engine",
            return_value=mock_engine,
        ) as mock_ce:
            engine = job._build_engine(sync_cfg, "source")

        expected_url = "postgresql://admin:secret@db.example.com:5432/mydb"
        mock_ce.assert_called_once_with(expected_url, pool_pre_ping=True)
        assert engine is mock_engine

    def test_missing_key_raises_value_error(self):
        """3. _build_engine with missing key raises ValueError."""
        sync_cfg = {"source": "pg_src"}  # no "target" key
        job = IncrementalSyncJob()

        with pytest.raises(ValueError, match="target"):
            job._build_engine(sync_cfg, "target")


class TestIncrementalSyncJobAttributes:
    def test_default_args(self):
        """4. default_args contains {'days': 1, 'config': 'sync_config.json'}."""
        assert IncrementalSyncJob.default_args == {"days": 1, "config": "sync_config.json"}

    def test_scope_is_pipeline(self):
        """5. scope is 'pipeline'."""
        assert IncrementalSyncJob.scope == "pipeline"


class TestLoadSyncConfig:
    def test_reads_valid_json_file(self, tmp_path):
        """6. _load_sync_config reads and returns valid JSON content."""
        config_data = {
            "source": "pg_src",
            "target": "ch_tgt",
            "tables": [
                {
                    "table": "orders",
                    "pk_columns": ["id"],
                    "ts_column": "updated_at",
                }
            ],
        }
        config_path = tmp_path / "sync_config.json"
        config_path.write_text(json.dumps(config_data))

        result = IncrementalSyncJob._load_sync_config(str(config_path))
        assert result == config_data
