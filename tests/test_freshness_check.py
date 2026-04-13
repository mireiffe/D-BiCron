"""Tests for dbcron/jobs/freshness_check.py — FreshnessCheckJob."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from dbcron.jobs.freshness_check import FreshnessCheckJob


def _make_snapshot(databases: dict) -> dict:
    return {"snapshot_at": "2026-04-13T10:00:00", "databases": databases}


def _table(table_name: str, row_count: int) -> dict:
    return {
        "schema": "public",
        "table": table_name,
        "estimated_row_count": row_count,
        "columns": [{"name": "id", "type": "integer", "nullable": False}],
        "primary_key": ["id"],
        "foreign_keys": [],
        "referenced_by": [],
    }


# Minimal databases.json entries for test DB IDs
_TEST_DBS = [
    {"id": "db1", "type": "postgresql", "host": "localhost", "port": 5432,
     "dbname": "db1", "user": "", "password": ""},
    {"id": "pg_src", "type": "postgresql", "host": "localhost", "port": 5432,
     "dbname": "srcdb", "user": "admin", "password": "secret"},
    {"id": "other_db", "type": "postgresql", "host": "localhost", "port": 5432,
     "dbname": "other", "user": "", "password": ""},
]


@pytest.fixture()
def fresh_dir(tmp_path):
    """Temporary data directory patched as DATA_DIR for both
    freshness_check and db modules."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "databases.json").write_text(
        json.dumps(_TEST_DBS, ensure_ascii=False, indent=2)
    )
    with patch("dbcron.jobs.freshness_check.DATA_DIR", data_dir), \
         patch("dbcron.db.DATA_DIR", data_dir):
        yield data_dir


class TestFreshnessCheckJob:
    def test_missing_snapshots_success(self, fresh_dir):
        """1. Missing snapshot files returns success '비교할 스냅샷이 부족합니다'."""
        job = FreshnessCheckJob()
        result = job.run()
        assert result.success is True
        assert "비교할 스냅샷이 부족합니다" in result.message

    def test_stale_table_detected(self, fresh_dir):
        """2. Table with same non-zero row count across snapshots is stale."""
        snap = _make_snapshot({"db1": {"tables": {
            "public.users": _table("users", 500),
        }}})
        (fresh_dir / "metadata_snapshot.json").write_text(json.dumps(snap))
        (fresh_dir / "metadata_snapshot_prev.json").write_text(json.dumps(snap))

        job = FreshnessCheckJob()
        result = job.run()
        assert result.rows_affected >= 1
        assert "unchanged" in result.message

    def test_zero_row_table_not_stale(self, fresh_dir):
        """3. Table with zero rows is NOT counted as stale."""
        snap = _make_snapshot({"db1": {"tables": {
            "public.empty": _table("empty", 0),
        }}})
        (fresh_dir / "metadata_snapshot.json").write_text(json.dumps(snap))
        (fresh_dir / "metadata_snapshot_prev.json").write_text(json.dumps(snap))

        job = FreshnessCheckJob()
        result = job.run()
        assert result.rows_affected == 0

    def test_changed_table_not_stale(self, fresh_dir):
        """4. Table with changed row count is not stale."""
        prev = _make_snapshot({"db1": {"tables": {
            "public.orders": _table("orders", 100),
        }}})
        cur = _make_snapshot({"db1": {"tables": {
            "public.orders": _table("orders", 200),
        }}})
        (fresh_dir / "metadata_snapshot_prev.json").write_text(json.dumps(prev))
        (fresh_dir / "metadata_snapshot.json").write_text(json.dumps(cur))

        job = FreshnessCheckJob()
        result = job.run()
        assert result.rows_affected == 0

    def test_new_table_in_current_skipped(self, fresh_dir):
        """5. Table present in current but not in previous is skipped."""
        prev = _make_snapshot({"db1": {"tables": {}}})
        cur = _make_snapshot({"db1": {"tables": {
            "public.new_table": _table("new_table", 50),
        }}})
        (fresh_dir / "metadata_snapshot_prev.json").write_text(json.dumps(prev))
        (fresh_dir / "metadata_snapshot.json").write_text(json.dumps(cur))

        job = FreshnessCheckJob()
        result = job.run()
        # New table has no prev counterpart, so it's skipped (not counted as stale)
        assert result.rows_affected == 0

    def test_target_filter_respected(self, fresh_dir):
        """6. Target filter limits scope to specified DB."""
        snap = _make_snapshot({
            "pg_src": {"tables": {
                "public.users": _table("users", 500),
            }},
            "other_db": {"tables": {
                "public.orders": _table("orders", 300),
            }},
        })
        (fresh_dir / "metadata_snapshot.json").write_text(json.dumps(snap))
        (fresh_dir / "metadata_snapshot_prev.json").write_text(json.dumps(snap))

        job = FreshnessCheckJob()
        job._targets = [{"db": "pg_src"}]
        result = job.run()

        # Only pg_src/users should be checked (1 stale), other_db/orders excluded
        assert result.rows_affected == 1

    def test_always_returns_success(self, fresh_dir):
        """7. run() always returns success=True regardless of stale tables."""
        snap = _make_snapshot({"db1": {"tables": {
            "public.users": _table("users", 500),
            "public.orders": _table("orders", 1000),
        }}})
        (fresh_dir / "metadata_snapshot.json").write_text(json.dumps(snap))
        (fresh_dir / "metadata_snapshot_prev.json").write_text(json.dumps(snap))

        job = FreshnessCheckJob()
        result = job.run()
        assert result.success is True
