"""Tests for dbcron/jobs/schema_drift.py — SchemaDriftJob."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from dbcron.jobs.schema_drift import SchemaDriftJob


def _make_snapshot(databases: dict) -> dict:
    return {"snapshot_at": "2026-04-13T10:00:00", "databases": databases}


def _col(name: str, typ: str) -> dict:
    return {"name": name, "type": typ, "nullable": True}


def _table(table_name: str, columns: list[dict], pk: list[str] | None = None,
           row_count: int = 100) -> dict:
    return {
        "schema": "public",
        "table": table_name,
        "row_count": row_count,
        "columns": columns,
        "primary_key": pk or ["id"],
        "foreign_keys": [],
        "referenced_by": [],
    }


# A minimal databases.json entry for test DB IDs
_TEST_DBS = [
    {"id": "db1", "type": "postgresql", "host": "localhost", "port": 5432,
     "dbname": "db1", "user": "", "password": ""},
    {"id": "db_new", "type": "postgresql", "host": "localhost", "port": 5432,
     "dbname": "db_new", "user": "", "password": ""},
    {"id": "db_old", "type": "postgresql", "host": "localhost", "port": 5432,
     "dbname": "db_old", "user": "", "password": ""},
    {"id": "pg_src", "type": "postgresql", "host": "localhost", "port": 5432,
     "dbname": "srcdb", "user": "admin", "password": "secret"},
    {"id": "other_db", "type": "postgresql", "host": "localhost", "port": 5432,
     "dbname": "other", "user": "", "password": ""},
]


@pytest.fixture()
def drift_dir(tmp_path):
    """Provides a temporary data directory patched as DATA_DIR for both
    schema_drift and db modules, with a databases.json containing test DB IDs."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "databases.json").write_text(
        json.dumps(_TEST_DBS, ensure_ascii=False, indent=2)
    )
    with patch("dbcron.jobs.schema_drift.DATA_DIR", data_dir), \
         patch("dbcron.db.DATA_DIR", data_dir):
        yield data_dir


class TestSchemaDriftJob:
    def test_no_current_snapshot_failure(self, drift_dir):
        """1. No current snapshot file returns failure message."""
        job = SchemaDriftJob()
        result = job.run()
        assert result.success is False
        assert "현재 스냅샷이 없습니다" in result.message

    def test_no_previous_snapshot_success(self, drift_dir):
        """2. No previous snapshot returns success '이전 스냅샷이 없어...'."""
        cur = _make_snapshot({"db1": {"tables": {}}})
        (drift_dir / "metadata_snapshot.json").write_text(json.dumps(cur))

        job = SchemaDriftJob()
        result = job.run()
        assert result.success is True
        assert "이전 스냅샷이 없어" in result.message

    def test_new_table_detected(self, drift_dir):
        """3. New table in current snapshot reports [+TABLE]."""
        prev = _make_snapshot({"db1": {"tables": {}}})
        cur = _make_snapshot({"db1": {"tables": {
            "public.users": _table("users", [_col("id", "integer")]),
        }}})
        (drift_dir / "metadata_snapshot_prev.json").write_text(json.dumps(prev))
        (drift_dir / "metadata_snapshot.json").write_text(json.dumps(cur))

        job = SchemaDriftJob()
        result = job.run()
        assert result.rows_affected >= 1

    def test_removed_table_breaking(self, drift_dir):
        """4. Removed table reports [-TABLE] and is breaking."""
        prev = _make_snapshot({"db1": {"tables": {
            "public.users": _table("users", [_col("id", "integer")]),
        }}})
        cur = _make_snapshot({"db1": {"tables": {}}})
        (drift_dir / "metadata_snapshot_prev.json").write_text(json.dumps(prev))
        (drift_dir / "metadata_snapshot.json").write_text(json.dumps(cur))

        job = SchemaDriftJob()
        result = job.run()
        assert result.success is False
        assert "breaking" in result.message

    def test_new_column_detected(self, drift_dir):
        """5. New column reports [+COL]."""
        prev = _make_snapshot({"db1": {"tables": {
            "public.users": _table("users", [_col("id", "integer")]),
        }}})
        cur = _make_snapshot({"db1": {"tables": {
            "public.users": _table("users", [_col("id", "integer"), _col("email", "text")]),
        }}})
        (drift_dir / "metadata_snapshot_prev.json").write_text(json.dumps(prev))
        (drift_dir / "metadata_snapshot.json").write_text(json.dumps(cur))

        job = SchemaDriftJob()
        result = job.run()
        assert result.rows_affected >= 1
        # Non-breaking: new column is additive
        assert result.success is True

    def test_removed_column_breaking(self, drift_dir):
        """6. Removed column reports [-COL] and is breaking."""
        prev = _make_snapshot({"db1": {"tables": {
            "public.users": _table("users", [_col("id", "integer"), _col("email", "text")]),
        }}})
        cur = _make_snapshot({"db1": {"tables": {
            "public.users": _table("users", [_col("id", "integer")]),
        }}})
        (drift_dir / "metadata_snapshot_prev.json").write_text(json.dumps(prev))
        (drift_dir / "metadata_snapshot.json").write_text(json.dumps(cur))

        job = SchemaDriftJob()
        result = job.run()
        assert result.success is False
        assert "breaking" in result.message

    def test_type_change_breaking(self, drift_dir):
        """7. Column type change reports [TYPE] and is breaking."""
        prev = _make_snapshot({"db1": {"tables": {
            "public.users": _table("users", [_col("id", "integer"), _col("age", "integer")]),
        }}})
        cur = _make_snapshot({"db1": {"tables": {
            "public.users": _table("users", [_col("id", "integer"), _col("age", "text")]),
        }}})
        (drift_dir / "metadata_snapshot_prev.json").write_text(json.dumps(prev))
        (drift_dir / "metadata_snapshot.json").write_text(json.dumps(cur))

        job = SchemaDriftJob()
        result = job.run()
        assert result.success is False
        assert "breaking" in result.message

    def test_pk_change_breaking(self, drift_dir):
        """8. Primary key change reports [PK] and is breaking."""
        prev = _make_snapshot({"db1": {"tables": {
            "public.users": _table("users", [_col("id", "integer")], pk=["id"]),
        }}})
        cur = _make_snapshot({"db1": {"tables": {
            "public.users": _table("users", [_col("id", "integer")], pk=["id", "name"]),
        }}})
        (drift_dir / "metadata_snapshot_prev.json").write_text(json.dumps(prev))
        (drift_dir / "metadata_snapshot.json").write_text(json.dumps(cur))

        job = SchemaDriftJob()
        result = job.run()
        assert result.success is False
        assert "breaking" in result.message

    def test_new_db_detected(self, drift_dir):
        """9. New DB in current snapshot reports [NEW DB]."""
        prev = _make_snapshot({})
        cur = _make_snapshot({"db_new": {"tables": {
            "public.t": _table("t", [_col("id", "integer")]),
        }}})
        (drift_dir / "metadata_snapshot_prev.json").write_text(json.dumps(prev))
        (drift_dir / "metadata_snapshot.json").write_text(json.dumps(cur))

        job = SchemaDriftJob()
        result = job.run()
        assert result.rows_affected >= 1
        # New DB is non-breaking
        assert result.success is True

    def test_removed_db_breaking(self, drift_dir):
        """10. Removed DB reports [-DB] and is breaking."""
        prev = _make_snapshot({"db_old": {"tables": {
            "public.t": _table("t", [_col("id", "integer")]),
        }}})
        cur = _make_snapshot({})
        (drift_dir / "metadata_snapshot_prev.json").write_text(json.dumps(prev))
        (drift_dir / "metadata_snapshot.json").write_text(json.dumps(cur))

        job = SchemaDriftJob()
        result = job.run()
        assert result.success is False
        assert "breaking" in result.message

    def test_no_changes_success(self, drift_dir):
        """11. No changes between snapshots returns success '스키마 변경 없음'."""
        snap = _make_snapshot({"db1": {"tables": {
            "public.users": _table("users", [_col("id", "integer")]),
        }}})
        (drift_dir / "metadata_snapshot_prev.json").write_text(json.dumps(snap))
        (drift_dir / "metadata_snapshot.json").write_text(json.dumps(snap))

        job = SchemaDriftJob()
        result = job.run()
        assert result.success is True
        assert "스키마 변경 없음" in result.message

    def test_only_non_breaking_changes_success(self, drift_dir):
        """12. Only non-breaking changes (e.g., new table) returns success=True."""
        prev = _make_snapshot({"db1": {"tables": {}}})
        cur = _make_snapshot({"db1": {"tables": {
            "public.logs": _table("logs", [_col("id", "integer")]),
        }}})
        (drift_dir / "metadata_snapshot_prev.json").write_text(json.dumps(prev))
        (drift_dir / "metadata_snapshot.json").write_text(json.dumps(cur))

        job = SchemaDriftJob()
        result = job.run()
        assert result.success is True

    def test_at_least_one_breaking_fails(self, drift_dir):
        """13. At least one breaking change makes success=False."""
        prev = _make_snapshot({"db1": {"tables": {
            "public.users": _table("users", [_col("id", "integer"), _col("name", "text")]),
        }}})
        # Remove a column (breaking) and add a table (non-breaking)
        cur = _make_snapshot({"db1": {"tables": {
            "public.users": _table("users", [_col("id", "integer")]),
            "public.logs": _table("logs", [_col("id", "integer")]),
        }}})
        (drift_dir / "metadata_snapshot_prev.json").write_text(json.dumps(prev))
        (drift_dir / "metadata_snapshot.json").write_text(json.dumps(cur))

        job = SchemaDriftJob()
        result = job.run()
        assert result.success is False

    def test_target_filter_respected(self, drift_dir):
        """14. Target filter limits scope to specified DB."""
        prev = _make_snapshot({
            "pg_src": {"tables": {
                "public.users": _table("users", [_col("id", "integer")]),
            }},
            "other_db": {"tables": {
                "public.orders": _table("orders", [_col("id", "integer")]),
            }},
        })
        cur = _make_snapshot({
            "pg_src": {"tables": {
                "public.users": _table("users", [_col("id", "integer")]),
            }},
            "other_db": {"tables": {}},  # removed table — would be breaking
        })
        (drift_dir / "metadata_snapshot_prev.json").write_text(json.dumps(prev))
        (drift_dir / "metadata_snapshot.json").write_text(json.dumps(cur))

        job = SchemaDriftJob()
        job._targets = [{"db": "pg_src"}]
        result = job.run()

        # pg_src has no changes, so success
        assert result.success is True
        assert "스키마 변경 없음" in result.message
