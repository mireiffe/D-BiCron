"""Shared test fixtures."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest


# ── Sample data ──────────────────────────────────────────────────

SAMPLE_DBS = [
    {
        "id": "pg_src",
        "type": "postgresql",
        "label": "PG Source",
        "color": "#00e5ff",
        "host": "localhost",
        "port": 5432,
        "dbname": "srcdb",
        "user": "admin",
        "password": "secret",
    },
    {
        "id": "ch_tgt",
        "type": "clickhouse",
        "label": "CH Target",
        "color": "#c8ff00",
        "host": "localhost",
        "port": 9000,
        "dbname": "tgtdb",
        "user": "default",
        "password": "",
    },
    {
        "id": "sqlite_test",
        "type": "sqlite",
        "label": "SQLite Test",
        "color": "#ff2d8a",
        "host": "/tmp/test.db",
        "port": 0,
        "dbname": "test",
        "user": "",
        "password": "",
    },
]

SAMPLE_METADATA = {
    "snapshot_at": "2026-04-13T10:00:00",
    "databases": {
        "pg_src": {
            "tables": {
                "public.users": {
                    "schema": "public",
                    "table": "users",
                    "row_count": 1000,
                    "columns": [
                        {"name": "id", "type": "integer", "nullable": False},
                        {"name": "name", "type": "text", "nullable": True},
                    ],
                    "primary_key": ["id"],
                    "foreign_keys": [],
                    "referenced_by": [],
                },
                "public.orders": {
                    "schema": "public",
                    "table": "orders",
                    "row_count": 5000,
                    "columns": [
                        {"name": "id", "type": "integer", "nullable": False},
                        {"name": "user_id", "type": "integer", "nullable": False},
                        {"name": "amount", "type": "numeric", "nullable": True},
                    ],
                    "primary_key": ["id"],
                    "foreign_keys": [
                        {
                            "column": "user_id",
                            "ref_schema": "public",
                            "ref_table": "users",
                            "ref_column": "id",
                        }
                    ],
                    "referenced_by": [],
                },
            }
        }
    },
}


# ── Fixtures ─────────────────────────────────────────────────────


@pytest.fixture()
def tmp_data_dir(tmp_path):
    """Temporary data directory with databases.json and metadata snapshots."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    # databases.json
    (data_dir / "databases.json").write_text(
        json.dumps(SAMPLE_DBS, ensure_ascii=False, indent=2)
    )

    # metadata snapshots
    (data_dir / "metadata_snapshot.json").write_text(
        json.dumps(SAMPLE_METADATA, ensure_ascii=False, indent=2)
    )

    with patch("dbcron.db.DATA_DIR", data_dir):
        yield data_dir


@pytest.fixture()
def tmp_project_dir(tmp_path):
    """Temporary project root with data/ directory for config files."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "databases.json").write_text(
        json.dumps(SAMPLE_DBS, ensure_ascii=False, indent=2)
    )
    return tmp_path


@pytest.fixture()
def pg2ch_config(tmp_path):
    """Create a temporary pg2ch_config.json and return its path."""
    cfg = {
        "source": "pg_src",
        "target": "ch_tgt",
        "ch_native_port": 9000,
        "tables": [
            {
                "source_table": "public.orders",
                "target_table": "default.orders",
                "watermark_column": "updated_at",
                "engine": "ReplacingMergeTree(updated_at)",
                "order_by": ["id"],
                "partition_by": "toYYYYMM(created_at)",
                "drop_columns": ["internal_note"],
                "column_overrides": {
                    "status": "LowCardinality(String)",
                },
                "batch_size": 1000,
            },
        ],
    }
    path = tmp_path / "pg2ch_config.json"
    path.write_text(json.dumps(cfg, indent=2))
    return str(path)


@pytest.fixture()
def sync_config(tmp_path):
    """Create a temporary sync_config.json and return its path."""
    cfg = {
        "source": "pg_src",
        "target": "sqlite_test",
        "tables": [
            {
                "table": "orders",
                "pk_columns": ["id"],
                "ts_column": "updated_at",
                "source_schema": "public",
                "target_schema": "public",
            },
        ],
    }
    path = tmp_path / "sync_config.json"
    path.write_text(json.dumps(cfg, indent=2))
    return str(path)
