"""Tests for dbcron/jobs/enrich/exclusion.py."""

from __future__ import annotations

from unittest.mock import MagicMock

from dbcron.jobs.enrich.exclusion import filter_excluded_rows, get_existing_keys


class TestGetExistingKeys:
    def _mock_engine(self, rows):
        engine = MagicMock()
        ctx = MagicMock()
        ctx.execute.return_value.fetchall.return_value = rows
        engine.connect.return_value.__enter__ = MagicMock(return_value=ctx)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        return engine, ctx

    def test_basic(self):
        engine, ctx = self._mock_engine([(1,), (2,), (3,)])
        result = get_existing_keys(engine, "postgresql", "t", "id")
        assert result == {1, 2, 3}
        sql = ctx.execute.call_args[0][0]
        sql_text = sql.text if hasattr(sql, "text") else str(sql)
        assert "DISTINCT" in sql_text
        assert "FINAL" not in sql_text

    def test_candidate_keys_in_clause(self):
        engine, ctx = self._mock_engine([(1,)])
        result = get_existing_keys(
            engine, "postgresql", "t", "id", candidate_keys=[1, 2, 3]
        )
        assert result == {1}
        sql = ctx.execute.call_args[0][0]
        sql_text = sql.text if hasattr(sql, "text") else str(sql)
        assert "IN" in sql_text

    def test_clickhouse_uses_final(self):
        engine, ctx = self._mock_engine([(10,)])
        get_existing_keys(engine, "clickhouse", "t", "id")
        sql = ctx.execute.call_args[0][0]
        sql_text = sql.text if hasattr(sql, "text") else str(sql)
        assert "FINAL" in sql_text

    def test_empty_target(self):
        engine, _ = self._mock_engine([])
        result = get_existing_keys(engine, "postgresql", "t", "id")
        assert result == set()


class TestFilterExcludedRows:
    def test_basic_filter(self):
        rows = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}, {"id": 3, "v": "c"}]
        result = filter_excluded_rows(rows, {2}, "id")
        assert len(result) == 2
        assert all(r["id"] != 2 for r in result)

    def test_all_excluded(self):
        rows = [{"id": 1}, {"id": 2}]
        result = filter_excluded_rows(rows, {1, 2}, "id")
        assert result == []

    def test_none_excluded(self):
        rows = [{"id": 1}, {"id": 2}]
        result = filter_excluded_rows(rows, set(), "id")
        assert len(result) == 2

    def test_empty_rows(self):
        result = filter_excluded_rows([], {1, 2, 3}, "id")
        assert result == []

    def test_missing_key_column_not_excluded(self):
        rows = [{"id": 1}, {"other": "x"}]
        result = filter_excluded_rows(rows, {1}, "id")
        # {"other": "x"} has no "id" → .get returns None → not in {1}
        assert len(result) == 1
        assert result[0] == {"other": "x"}
