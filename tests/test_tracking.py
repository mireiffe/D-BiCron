"""Tests for pg2ch.tracking (schema DDL, row_to_json, MetaStore w/ fake conn)."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest

from pg2ch.tracking import MetaStore, row_to_json, schema_ddl


class TestSchemaDdl:
    def test_contains_three_tables(self):
        sql = "\n".join(schema_ddl("pg2ch_meta"))
        assert "CREATE SCHEMA IF NOT EXISTS pg2ch_meta" in sql
        assert "pg2ch_meta.copy_run" in sql
        assert "pg2ch_meta.copy_batch" in sql
        assert "pg2ch_meta.copy_failed_row" in sql
        assert "row_data        JSONB" in sql

    def test_custom_schema_name(self):
        sql = "\n".join(schema_ddl("custom_meta"))
        assert "custom_meta.copy_run" in sql

    def test_invalid_schema_rejected(self):
        with pytest.raises(ValueError, match="invalid schema name"):
            schema_ddl("bad-schema;DROP")


class TestRowToJson:
    def test_basic(self):
        out = row_to_json(["a", "b"], (1, "x"))
        assert out == '{"a": 1, "b": "x"}'

    def test_datetime_decimal_bytes(self):
        out = row_to_json(
            ["ts", "amt", "bin"],
            (datetime(2026, 1, 1, 12, 0), Decimal("1.50"), b"\xde\xad"),
        )
        assert "2026-01-01T12:00:00" in out
        assert "1.50" in out
        assert "dead" in out


# ── Fake psycopg2 conn ───────────────────────────────────────────


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.conn.executed.append((sql, params))

    def executemany(self, sql, seq):
        self.conn.executed.append((sql, list(seq)))

    def fetchone(self):
        return self.conn.fetch_queue.pop(0)


class FakeConn:
    def __init__(self):
        self.autocommit = False
        self.executed = []
        self.fetch_queue = []
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.closed = True


class TestMetaStore:
    def test_autocommit_set(self):
        conn = FakeConn()
        MetaStore(conn)
        assert conn.autocommit is True

    def test_ensure_schema_runs_all_statements(self):
        conn = FakeConn()
        MetaStore(conn).ensure_schema()
        assert len(conn.executed) == len(schema_ddl("pg2ch_meta"))

    def test_start_run_returns_id(self):
        conn = FakeConn()
        conn.fetch_queue = [(42,)]
        m = MetaStore(conn)
        rid = m.start_run(
            table_id="orders", source_table="public.orders",
            target_table="default.orders", sync_mode="append",
            watermark_column="updated_at", watermark_before="2025-01-01",
        )
        assert rid == 42
        sql, params = conn.executed[-1]
        assert "INSERT INTO pg2ch_meta.copy_run" in sql
        assert "running" in sql
        assert params[0] == "orders"

    def test_finish_run(self):
        conn = FakeConn()
        m = MetaStore(conn)
        m.finish_run(1, status="success", watermark_after="2025-06-01", rows_written=10, batch_count=1)
        sql, params = conn.executed[-1]
        assert "UPDATE pg2ch_meta.copy_run" in sql
        assert params[0] == "success"

    def test_get_resume_watermark_from_batch_progress(self):
        # finalize 전에 죽은 증분 run 의 마지막 커밋 batch 진행점에서 재개.
        conn = FakeConn()
        conn.fetch_queue = [("2025-06-01T00:00:00", "running")]
        m = MetaStore(conn)
        assert m.get_resume_watermark("orders", "updated_at") == "2025-06-01T00:00:00"
        sql, params = conn.executed[-1]
        assert "copy_batch" in sql
        assert "watermark_before IS NOT NULL" in sql
        assert "ORDER BY b.run_id DESC, b.batch_seq DESC" in sql
        assert params == ("orders", "updated_at")

    def test_get_resume_watermark_batch_blessed_run(self):
        # 정상 종료(success)된 run 의 마지막 batch 도 동일 경로로 읽힌다.
        conn = FakeConn()
        conn.fetch_queue = [("2025-06-01T00:00:00", "success")]
        assert (
            MetaStore(conn).get_resume_watermark("orders", "updated_at")
            == "2025-06-01T00:00:00"
        )

    def test_get_resume_watermark_falls_back_to_blessed(self):
        # 증분 batch 가 없으면(첫 전체복사 직후 등) finalize 된 watermark_after.
        conn = FakeConn()
        conn.fetch_queue = [None, ("2025-05-01T00:00:00",)]
        m = MetaStore(conn)
        assert m.get_resume_watermark("orders", "updated_at") == "2025-05-01T00:00:00"
        sql, params = conn.executed[-1]
        assert "status IN ('success','partial')" in sql
        assert params == ("orders", "updated_at")

    def test_get_resume_watermark_none(self):
        # batch 도 finalize 된 run 도 없으면 None (첫 실행).
        conn = FakeConn()
        conn.fetch_queue = [None, None]
        assert MetaStore(conn).get_resume_watermark("orders", "updated_at") is None

    def test_record_batch_returns_id(self):
        conn = FakeConn()
        conn.fetch_queue = [(7,)]
        m = MetaStore(conn)
        bid = m.record_batch(
            run_id=1, table_id="orders", batch_seq=0, status="partial",
            rows_in=100, rows_written=99, rows_failed=1,
            watermark_lo="a", watermark_hi="z",
        )
        assert bid == 7
        sql, _ = conn.executed[-1]
        assert "INSERT INTO pg2ch_meta.copy_batch" in sql

    def test_record_failed_rows(self):
        conn = FakeConn()
        m = MetaStore(conn)
        m.record_failed_rows(
            run_id=1, batch_id=7, table_id="orders", batch_seq=0,
            failures=[("100", '{"id": 100}', "bad value")],
        )
        sql, rows = conn.executed[-1]
        assert "INSERT INTO pg2ch_meta.copy_failed_row" in sql
        assert "::jsonb" in sql
        assert rows[0][4] == "100"

    def test_record_failed_rows_empty_noop(self):
        conn = FakeConn()
        MetaStore(conn).record_failed_rows(
            run_id=1, batch_id=None, table_id="orders", batch_seq=0, failures=[]
        )
        assert conn.executed == []

    def test_unresolved_failed_count(self):
        conn = FakeConn()
        conn.fetch_queue = [(3,)]
        assert MetaStore(conn).unresolved_failed_count("orders") == 3
