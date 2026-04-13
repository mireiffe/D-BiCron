"""Tests for dbcron/jobs/base.py — JobResult and Job base class."""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from dbcron.jobs.base import Job, JobResult


# ── Concrete subclass for testing ───────────────────────────────


class DummyJob(Job):
    name = "dummy"
    label = "Dummy Job"
    description = "A no-op job used in tests"
    default_args = {"days": 7}
    scope = "none"

    def run(self, **kwargs) -> JobResult:
        return JobResult(success=True, message="ok", rows_affected=kwargs.get("rows", 0))


class FailingJob(Job):
    name = "failing"
    label = "Failing Job"
    description = "Always raises"
    scope = "none"

    def run(self, **kwargs) -> JobResult:
        raise RuntimeError("boom")


# ── JobResult ───────────────────────────────────────────────────


class TestJobResultToDict:
    def test_to_dict_all_fields_present(self):
        """1. to_dict returns all expected keys with correct values."""
        started = datetime(2026, 1, 1, 12, 0, 0)
        finished = datetime(2026, 1, 1, 12, 5, 0)
        result = JobResult(
            success=True,
            message="done",
            rows_affected=42,
            started_at=started,
            finished_at=finished,
        )
        d = result.to_dict()
        assert set(d.keys()) == {"success", "message", "rows_affected", "started_at", "finished_at"}
        assert d["success"] is True
        assert d["message"] == "done"
        assert d["rows_affected"] == 42
        assert d["started_at"] == started.isoformat()
        assert d["finished_at"] == finished.isoformat()

    def test_to_dict_with_none_finished_at(self):
        """2. to_dict with None finished_at serialises as None."""
        result = JobResult(success=False, message="pending", finished_at=None)
        d = result.to_dict()
        assert d["finished_at"] is None


# ── Job.execute ─────────────────────────────────────────────────


class TestJobExecute:
    def test_execute_wraps_exception_in_failed_result(self):
        """3. execute wraps exceptions in JobResult(success=False)."""
        job = FailingJob()
        result = job.execute()
        assert result.success is False
        assert "boom" in result.message

    def test_execute_sets_started_and_finished(self):
        """4. execute sets started_at and finished_at timestamps."""
        job = DummyJob()
        result = job.execute()
        assert result.started_at is not None
        assert result.finished_at is not None
        assert result.finished_at >= result.started_at

    def test_execute_extracts_targets_from_kwargs(self):
        """5. execute pops 'targets' from kwargs and stores on instance."""
        job = DummyJob()
        targets = [{"db": "pg_src", "tables": ["orders"]}]
        job.execute(targets=targets)
        assert job._targets == targets


# ── Job.resolve_databases ───────────────────────────────────────


class TestResolveDatabases:
    @patch("dbcron.jobs.base.resolve_targets")
    def test_delegates_to_resolve_targets(self, mock_rt):
        """6. resolve_databases delegates to resolve_targets."""
        mock_filter = MagicMock()
        mock_rt.return_value = ([{"id": "pg_src"}], mock_filter)

        job = DummyJob()
        job._targets = [{"db": "pg_src"}]
        dbs, filt = job.resolve_databases()

        mock_rt.assert_called_once_with([{"db": "pg_src"}])
        assert dbs == [{"id": "pg_src"}]
        assert filt is mock_filter

    @patch("dbcron.jobs.base.resolve_targets")
    def test_caches_result(self, mock_rt):
        """7. resolve_databases caches result on second call."""
        mock_rt.return_value = ([{"id": "pg_src"}], lambda *a: True)

        job = DummyJob()
        job._targets = [{"db": "pg_src"}]
        job.resolve_databases()
        job.resolve_databases()

        assert mock_rt.call_count == 1

    @patch("dbcron.jobs.base.resolve_targets")
    def test_explicit_targets_overrides_cache(self, mock_rt):
        """8. resolve_databases with explicit targets overrides cache."""
        mock_rt.return_value = ([{"id": "ch_tgt"}], lambda *a: True)

        job = DummyJob()
        # Seed the cache
        job._databases = [{"id": "pg_src"}]
        job._table_filter = lambda *a: True

        new_targets = [{"db": "ch_tgt"}]
        dbs, _ = job.resolve_databases(targets=new_targets)

        mock_rt.assert_called_once_with(new_targets)
        assert dbs == [{"id": "ch_tgt"}]


# ── Job.get_connections ─────────────────────────────────────────


class TestGetConnections:
    def test_no_config_kwarg_returns_empty(self):
        """9. get_connections with no config kwarg returns []."""
        assert DummyJob.get_connections() == []

    def test_missing_file_returns_empty(self, tmp_path):
        """10. get_connections with missing file returns []."""
        assert DummyJob.get_connections(config=str(tmp_path / "nope.json")) == []

    def test_invalid_json_returns_empty(self, tmp_path):
        """11. get_connections with invalid JSON returns []."""
        bad = tmp_path / "bad.json"
        bad.write_text("{not valid json!!")
        assert DummyJob.get_connections(config=str(bad)) == []

    def test_no_source_target_keys_returns_empty(self, tmp_path):
        """12. get_connections with no source/target keys returns []."""
        cfg = tmp_path / "cfg.json"
        cfg.write_text(json.dumps({"tables": [{"table": "x"}]}))
        assert DummyJob.get_connections(config=str(cfg)) == []

    def test_dotted_source_table_splits_schema(self, tmp_path):
        """13. Dotted source_table 'public.orders' splits into schema + table."""
        cfg = tmp_path / "cfg.json"
        cfg.write_text(json.dumps({
            "source": "pg_src",
            "target": "ch_tgt",
            "tables": [
                {"source_table": "public.orders", "target_table": "default.orders"},
            ],
        }))
        conns = DummyJob.get_connections(config=str(cfg))
        assert len(conns) == 1
        assert conns[0]["from"] == {"db": "pg_src", "schema": "public", "table": "orders"}
        assert conns[0]["to"] == {"db": "ch_tgt", "schema": "default", "table": "orders"}

    def test_plain_table_with_separate_schema(self, tmp_path):
        """14. Plain table name + separate source_schema field."""
        cfg = tmp_path / "cfg.json"
        cfg.write_text(json.dumps({
            "source": "pg_src",
            "target": "ch_tgt",
            "tables": [
                {
                    "table": "orders",
                    "source_schema": "sales",
                    "target_schema": "warehouse",
                },
            ],
        }))
        conns = DummyJob.get_connections(config=str(cfg))
        assert len(conns) == 1
        assert conns[0]["from"]["schema"] == "sales"
        assert conns[0]["from"]["table"] == "orders"
        assert conns[0]["to"]["schema"] == "warehouse"
        assert conns[0]["to"]["table"] == "orders"

    def test_label_format_includes_cls_name(self, tmp_path):
        """15. Label format includes the job's cls.name."""
        cfg = tmp_path / "cfg.json"
        cfg.write_text(json.dumps({
            "source": "pg_src",
            "target": "ch_tgt",
            "tables": [{"table": "users", "source_schema": "public", "target_schema": "public"}],
        }))
        conns = DummyJob.get_connections(config=str(cfg))
        assert conns[0]["label"] == f"users ({DummyJob.name})"

    def test_skips_entries_missing_source_table_and_table(self, tmp_path):
        """16. Entries without source_table or table key are skipped."""
        cfg = tmp_path / "cfg.json"
        cfg.write_text(json.dumps({
            "source": "pg_src",
            "target": "ch_tgt",
            "tables": [
                {"target_table": "default.orphan"},  # no source_table or table
                {"table": "orders", "source_schema": "public", "target_schema": "public"},
            ],
        }))
        conns = DummyJob.get_connections(config=str(cfg))
        assert len(conns) == 1
        assert conns[0]["from"]["table"] == "orders"
