"""Tests for dbcron/jobs/connection_test.py — ConnectionTestJob."""

from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest

from dbcron.jobs.connection_test import ConnectionTestJob


class TestConnectionTestJob:
    def test_no_databases_returns_failure(self, tmp_data_dir):
        """1. run() with no databases returns failure with '등록된 DB가 없습니다'."""
        # Overwrite databases.json with an empty list
        (tmp_data_dir / "databases.json").write_text("[]")

        job = ConnectionTestJob()
        result = job.run()

        assert result.success is False
        assert "등록된 DB가 없습니다" in result.message

    def test_scope_is_all_dbs(self):
        """2. scope is 'all_dbs'."""
        assert ConnectionTestJob.scope == "all_dbs"

    def test_default_args_is_empty_dict(self):
        """3. default_args is an empty dict."""
        assert ConnectionTestJob.default_args == {}
