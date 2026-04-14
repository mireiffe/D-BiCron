"""Tests for dbcron/jobs/enrich/context.py."""

from __future__ import annotations

from unittest.mock import MagicMock

from dbcron.jobs.enrich.context import RowContext, run_query_chain


# ── RowContext ───────────────────────────────────────────────────


class TestRowContext:
    def test_flat_merges_source_and_queries(self):
        ctx = RowContext(
            source={"id": 1, "name": "A"},
            queries={"details": {"desc": "hello", "cat": "X"}},
        )
        flat = ctx.flat()
        assert flat["id"] == 1
        assert flat["desc"] == "hello"

    def test_flat_query_overrides_source(self):
        ctx = RowContext(
            source={"id": 1, "name": "A"},
            queries={"q1": {"name": "B"}},
        )
        flat = ctx.flat()
        assert flat["name"] == "B"

    def test_flat_excludes_many_queries(self):
        ctx = RowContext(
            source={"id": 1},
            queries={"multi": [{"x": 1}, {"x": 2}]},
        )
        flat = ctx.flat()
        assert "x" not in flat

    def test_get_simple_column(self):
        ctx = RowContext(source={"id": 1, "name": "A"})
        assert ctx.get("id") == 1

    def test_get_dotted_query(self):
        ctx = RowContext(
            source={"id": 1},
            queries={"details": {"desc": "hello"}},
        )
        assert ctx.get("details.desc") == "hello"

    def test_get_collections_ref(self):
        ctx = RowContext(
            source={"id": 1},
            collections={"images": ["b64a", "b64b"]},
        )
        assert ctx.get("$collections.images") == ["b64a", "b64b"]

    def test_get_missing_returns_none(self):
        ctx = RowContext(source={"id": 1})
        assert ctx.get("nonexistent") is None
        assert ctx.get("q.col") is None
        assert ctx.get("$collections.missing") is None


# ── run_query_chain ──────────────────────────────────────────────


def _mock_engine(rows, col_keys):
    """단일 쿼리 결과를 반환하는 mock 엔진."""
    engine = MagicMock()
    conn = MagicMock()
    result = MagicMock()
    result.keys.return_value = col_keys

    # fetchone/fetchall 지원
    if rows:
        result.fetchone.return_value = rows[0]
    else:
        result.fetchone.return_value = None
    result.fetchall.return_value = rows

    conn.execute.return_value = result
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    return engine, conn


class TestRunQueryChain:
    def test_single_query_bind_from_source(self):
        engine, conn = _mock_engine(
            [("hello", "catX")], ["desc", "cat"]
        )
        source = {"id": 1, "sku": "ABC"}
        chain = [
            {"name": "details", "db": "pg", "sql": "SELECT desc, cat FROM t WHERE id = :id", "bind_from": "source"},
        ]
        ctx = run_query_chain(source, chain, {"pg": engine})
        assert ctx.queries["details"] == {"desc": "hello", "cat": "catX"}
        assert ctx.flat()["desc"] == "hello"

    def test_chained_bind_from_previous(self):
        eng1, _ = _mock_engine([("catX",)], ["category"])
        eng2, _ = _mock_engine([("supplier1",)], ["supplier"])

        source = {"id": 1}
        chain = [
            {"name": "q1", "db": "pg", "sql": "SELECT category FROM t1 WHERE id = :id", "bind_from": "source"},
            {"name": "q2", "db": "pg2", "sql": "SELECT supplier FROM t2 WHERE category = :category", "bind_from": "q1"},
        ]
        ctx = run_query_chain(source, chain, {"pg": eng1, "pg2": eng2})
        assert ctx.queries["q1"] == {"category": "catX"}
        assert ctx.queries["q2"] == {"supplier": "supplier1"}

    def test_many_true_returns_list(self):
        engine, _ = _mock_engine(
            [("sku1",), ("sku2",)], ["sku"]
        )
        source = {"id": 1}
        chain = [
            {"name": "related", "db": "pg", "sql": "SELECT sku FROM t WHERE id = :id",
             "bind_from": "source", "many": True},
        ]
        ctx = run_query_chain(source, chain, {"pg": engine})
        assert ctx.queries["related"] == [{"sku": "sku1"}, {"sku": "sku2"}]

    def test_empty_result_single(self):
        engine, _ = _mock_engine([], ["desc"])
        source = {"id": 1}
        chain = [
            {"name": "details", "db": "pg", "sql": "SELECT desc FROM t WHERE id = :id", "bind_from": "source"},
        ]
        ctx = run_query_chain(source, chain, {"pg": engine})
        assert ctx.queries["details"] == {}

    def test_missing_engine_returns_empty(self):
        source = {"id": 1}
        chain = [
            {"name": "q1", "db": "missing_db", "sql": "SELECT 1", "bind_from": "source"},
        ]
        ctx = run_query_chain(source, chain, {})
        assert ctx.queries["q1"] == {}

    def test_no_chain_returns_source_only(self):
        source = {"id": 1, "name": "A"}
        ctx = run_query_chain(source, [], {})
        assert ctx.source == {"id": 1, "name": "A"}
        assert ctx.queries == {}
        assert ctx.flat() == {"id": 1, "name": "A"}

    def test_query_error_returns_empty(self):
        engine = MagicMock()
        conn = MagicMock()
        conn.execute.side_effect = Exception("DB error")
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        source = {"id": 1}
        chain = [
            {"name": "q1", "db": "pg", "sql": "SELECT bad", "bind_from": "source"},
        ]
        ctx = run_query_chain(source, chain, {"pg": engine})
        assert ctx.queries["q1"] == {}

    def test_bind_from_many_uses_first_row(self):
        eng1, _ = _mock_engine([("a",), ("b",)], ["val"])
        eng2, _ = _mock_engine([("result",)], ["out"])

        source = {"id": 1}
        chain = [
            {"name": "q1", "db": "pg", "sql": "SELECT val FROM t", "bind_from": "source", "many": True},
            {"name": "q2", "db": "pg2", "sql": "SELECT out FROM t2 WHERE val = :val", "bind_from": "q1"},
        ]
        ctx = run_query_chain(source, chain, {"pg": eng1, "pg2": eng2})
        assert ctx.queries["q2"] == {"out": "result"}
        # Verify q2 was bound using first row of q1 ("a")
        call_params = eng2.connect.return_value.__enter__.return_value.execute.call_args[0][1]
        assert call_params["val"] == "a"
