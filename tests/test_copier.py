"""Tests for pg2ch.copier — query builders, row isolation, full copy flow."""

from __future__ import annotations

import pytest

from pg2ch.config import TableConfig
from pg2ch.copier import TableCopier


# ── fakes ────────────────────────────────────────────────────────


class FakeCH:
    """bad(row)->True 인 row 가 포함된 INSERT block 은 통째로 실패시킨다."""

    def __init__(self, bad=None):
        self.bad = bad
        self.executed: list[str] = []
        self.kwargs: list[dict] = []
        self.inserted: list = []

    def execute(self, sql, params=None, **kw):
        self.executed.append(sql)
        self.kwargs.append(kw)
        if sql.startswith("INSERT INTO") and isinstance(params, list):
            if self.bad and any(self.bad(r) for r in params):
                raise ValueError("ch insert failed")
            self.inserted.extend(params)
            return len(params)
        return []

    def disconnect(self):
        pass


class FakeMeta:
    def __init__(self, resume=None):
        self.resume = resume
        self.ensured = False
        self.started: dict | None = None
        self.finished: dict | None = None
        self.finished_history: list[dict] = []
        self.batches: list[dict] = []
        self.failed: list[dict] = []
        self._bid = 0

    def ensure_schema(self):
        self.ensured = True

    def get_resume_watermark(self, table_id, wm_col):
        return self.resume

    def start_run(self, **kw):
        self.started = kw
        return 100

    def finish_run(self, run_id, **kw):
        self.finished = {"run_id": run_id, **kw}
        self.finished_history.append(self.finished)

    def record_batch(self, **kw):
        self._bid += 1
        self.batches.append(kw)
        return self._bid

    def record_failed_rows(self, **kw):
        self.failed.append(kw)


class FakeColCursor:
    def __init__(self, cols):
        self.cols = cols

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        pass

    def fetchall(self):
        return self.cols


class FakeStreamCursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.itersize = None
        self.query = None
        self.params = "UNSET"

    def execute(self, q, p=None):
        self.query = q
        self.params = p

    def fetchmany(self, n):
        chunk = self.rows[:n]
        self.rows = self.rows[n:]
        return chunk

    def close(self):
        pass


class FakePG:
    def __init__(self, cols, rows):
        self.cols = cols
        self.rows = rows
        self.stream: FakeStreamCursor | None = None
        self.rolled_back = False

    def cursor(self, name=None):
        if name is None:
            return FakeColCursor(self.cols)
        self.stream = FakeStreamCursor(self.rows)
        return self.stream

    def rollback(self):
        self.rolled_back = True

    def close(self):
        pass


# id integer NOT NULL, name text NULL
COLS = [
    ("id", "integer", "NO", None, None),
    ("name", "text", "YES", None, None),
]


def _cfg(**over) -> TableConfig:
    d = {
        "table_id": "orders",
        "source": "pg",
        "target": "ch",
        "source_table": "public.orders",
        "target_table": "default.orders",
        "sync_mode": "full_reload",
        "order_by": ["id"],
    }
    d.update(over)
    return TableConfig.from_dict(d)


# ── query builders ───────────────────────────────────────────────


class TestQueryBuilders:
    def test_incremental_query(self):
        q, p = TableCopier._incremental_query(
            "public", "orders", ["id", "name"], "id", None, "1", None
        )
        assert '"id" > %s' in q
        assert 'ORDER BY "id"' in q
        assert p == ("1",)

    def test_incremental_with_separate_sync_since(self):
        q, p = TableCopier._incremental_query(
            "public", "events", ["sync_id", "created_at"], "sync_id", "created_at",
            "100", "2025-01-01T00:00:00",
        )
        assert '"sync_id" > %s' in q
        assert '"created_at" >= %s' in q
        assert p == ("100", "2025-01-01T00:00:00")

    def test_incremental_sync_since_same_column_overrides_older_cutoff(self):
        q, p = TableCopier._incremental_query(
            "public", "orders", ["updated_at"], "updated_at", "updated_at",
            "2024-01-01T00:00:00", "2025-01-01T00:00:00",
        )
        assert p == ("2025-01-01T00:00:00",)

    def test_full_query_no_filter(self):
        q, p = TableCopier._full_query("public", "orders", ["id", "name"], None, None)
        assert q.startswith('SELECT "id", "name" FROM "public"."orders"')
        # full copy 는 server-side cursor 의 blocking sort 를 피하려고 정렬하지 않는다.
        assert "ORDER BY" not in q
        assert p is None

    def test_full_query_with_sync_since(self):
        q, p = TableCopier._full_query(
            "public", "orders", ["id", "ts"], "ts", "2025-01-01T00:00:00"
        )
        assert '"ts" >= %s' in q
        assert "ORDER BY" not in q
        assert p == ("2025-01-01T00:00:00",)

    def test_full_query_pushes_pg_casts_for_expensive_types(self):
        q, _ = TableCopier._full_query(
            "public",
            "events",
            [
                {"name": "id", "pg_type": "integer", "ch_type": "Int32"},
                {"name": "payload", "pg_type": "jsonb", "ch_type": "String"},
                {"name": "raw", "pg_type": "bytea", "ch_type": "String"},
                {"name": "flag", "pg_type": "boolean", "ch_type": "UInt8"},
            ],
            None,
            None,
        )
        assert '"payload"::text AS "payload"' in q
        assert "encode(\"raw\", 'hex') AS \"raw\"" in q
        assert '"flag"::int AS "flag"' in q


# ── row isolation (_insert) ──────────────────────────────────────


class TestInsertIsolation:
    def _copier(self, **over):
        return TableCopier(_cfg(**over))

    def test_all_good(self):
        ch = FakeCH()
        rows = [(1, "a"), (2, "b")]
        written, failures = self._copier()._insert(
            ch, "INSERT INTO `d`.`t` (`id`, `name`) VALUES", rows, rows, ["id", "name"], 0
        )
        assert written == 2 and failures == []
        assert ch.inserted == rows

    def test_isolates_single_bad_row(self):
        ch = FakeCH(bad=lambda r: r[1] == "BAD")
        rows = [(1, "a"), (2, "BAD"), (3, "c")]
        written, failures = self._copier()._insert(
            ch, "INSERT INTO `d`.`t` (`id`, `name`) VALUES", rows, rows, ["id", "name"], 0
        )
        assert written == 2
        assert len(failures) == 1
        wm, row_json, err = failures[0]
        assert wm == "2"
        assert '"name": "BAD"' in row_json
        assert (1, "a") in ch.inserted and (3, "c") in ch.inserted
        assert (2, "BAD") not in ch.inserted

    def test_whole_batch_failure_raises(self):
        ch = FakeCH(bad=lambda r: True)  # 모든 row 실패
        rows = [(1, "a"), (2, "b")]
        with pytest.raises(RuntimeError, match="entire batch"):
            self._copier()._insert(
                ch, "INSERT INTO `d`.`t` (`id`, `name`) VALUES", rows, rows, ["id", "name"], 0
            )

    def test_fail_policy_propagates(self):
        ch = FakeCH(bad=lambda r: r[1] == "BAD")
        rows = [(1, "a"), (2, "BAD")]
        with pytest.raises(ValueError, match="ch insert failed"):
            self._copier(on_row_error="fail")._insert(
                ch, "INSERT INTO `d`.`t` (`id`, `name`) VALUES", rows, rows, ["id", "name"], 0
            )

    def test_insert_types_check_can_be_disabled(self):
        ch = FakeCH()
        rows = [(1, "a"), (2, "b")]
        written, failures = self._copier(insert_types_check=False)._insert(
            ch, "INSERT INTO `d`.`t` (`id`, `name`) VALUES", rows, rows, ["id", "name"], 0
        )
        assert written == 2 and failures == []
        assert ch.kwargs[-1]["types_check"] is False


# ── full copy() flow ─────────────────────────────────────────────


class TestCopyFlow:
    def _run(self, cfg, rows, *, resume=None, bad=None):
        pg = FakePG(COLS, rows)
        ch = FakeCH(bad=bad)
        meta = FakeMeta(resume=resume)
        result = TableCopier(cfg).copy(pg, ch, meta, target_default_db="default")
        return result, pg, ch, meta

    def test_full_reload_truncates_and_succeeds(self):
        cfg = _cfg(sync_mode="full_reload")
        result, pg, ch, meta = self._run(cfg, [(1, "a"), (2, "b")])
        assert any("TRUNCATE TABLE IF EXISTS" in s for s in ch.executed)
        assert result.status == "success"
        assert result.rows_written == 2
        # full_reload → 워터마크 추적 안 함
        assert meta.started["watermark_column"] is None
        assert meta.finished["watermark_after"] is None
        assert pg.rolled_back is True

    def test_creates_target_table(self):
        cfg = _cfg(sync_mode="full_reload", engine="ReplacingMergeTree")
        _, _, ch, _ = self._run(cfg, [(1, "a")])
        assert any("CREATE TABLE IF NOT EXISTS `default`.`orders`" in s for s in ch.executed)

    def test_append_first_run_copies_all_and_sets_watermark(self):
        cfg = _cfg(sync_mode="append", watermark_column="id")
        result, pg, ch, meta = self._run(cfg, [(1, "a"), (2, "b"), (3, "c")], resume=None)
        # append 첫 실행은 TRUNCATE 하지 않는다
        assert not any("TRUNCATE" in s for s in ch.executed)
        assert result.status == "success"
        assert result.watermark_after == "3"
        assert meta.started["watermark_column"] == "id"
        assert meta.started["watermark_before"] is None
        # 첫 실행 full copy 는 ORDER BY 없이 스트리밍한다 (server-side cursor 의
        # blocking sort 로 인한 선행 대기를 피하고, watermark 는 running max 로 추적).
        assert "ORDER BY" not in pg.stream.query

    def test_append_incremental_uses_cutoff(self):
        cfg = _cfg(sync_mode="append", watermark_column="id")
        result, pg, ch, meta = self._run(cfg, [(2, "b"), (3, "c")], resume="1")
        assert pg.stream.query is not None
        assert '"id" > %s' in pg.stream.query
        assert pg.stream.params == ("1",)
        assert result.watermark_before == "1"
        assert result.watermark_after == "3"
        assert result.status == "success"

    def test_precheck_plans_incremental_without_count(self):
        cfg = _cfg(sync_mode="append", watermark_column="id")
        meta = FakeMeta(resume="10")
        plan = TableCopier(cfg).inspect_copy_plan(meta)
        assert plan.planned_mode == "incremental"
        assert plan.resume_watermark == "10"
        assert plan.copy_cutoff == "10"
        assert plan.watermark_column == "id"
        # COUNT(*) 는 수행하지 않는다 — 가시성 용도이고 비싸다.
        assert plan.rows_to_copy is None

    def test_precheck_plans_full_reload(self):
        cfg = _cfg(sync_mode="full_reload")
        meta = FakeMeta(resume=None)
        plan = TableCopier(cfg).inspect_copy_plan(meta)
        assert plan.planned_mode == "full_reload"
        assert plan.rows_to_copy is None
        assert plan.watermark_column is None

    def test_precheck_plans_append_first_run(self):
        cfg = _cfg(sync_mode="append", watermark_column="id")
        meta = FakeMeta(resume=None)
        plan = TableCopier(cfg).inspect_copy_plan(meta)
        assert plan.planned_mode == "append_first_run"
        assert plan.resume_watermark is None
        assert plan.rows_to_copy is None

    def test_deferred_copy_does_not_publish_watermark(self):
        cfg = _cfg(sync_mode="append", watermark_column="id")
        pg = FakePG(COLS, [(1, "a"), (2, "b"), (3, "c")])
        ch = FakeCH()
        meta = FakeMeta(resume=None)
        result = TableCopier(cfg).copy(
            pg, ch, meta, target_default_db="default", finalize_run=False
        )
        assert result.status == "success"
        assert result.watermark_after == "3"
        assert meta.finished["status"] == "copied"
        assert meta.finished["watermark_after"] is None

    def test_partial_run_dead_letters_bad_row(self):
        cfg = _cfg(sync_mode="append", watermark_column="id", on_row_error="dead_letter")
        result, pg, ch, meta = self._run(
            cfg, [(1, "a"), (2, "BAD"), (3, "c")], resume=None, bad=lambda r: r[1] == "BAD"
        )
        assert result.status == "partial"
        assert result.rows_written == 2
        assert result.rows_failed == 1
        # 실패에도 워터마크는 전진 (실패 row 는 dead-letter 에 보관)
        assert result.watermark_after == "3"
        assert len(meta.failed) == 1
        assert meta.failed[0]["failures"][0][0] == "2"
        # batch 상태 partial 로 기록
        assert meta.batches[0]["status"] == "partial"
        assert meta.finished["status"] == "partial"

    def test_skip_policy_does_not_record_failed_rows(self):
        cfg = _cfg(sync_mode="append", watermark_column="id", on_row_error="skip")
        result, pg, ch, meta = self._run(
            cfg, [(1, "a"), (2, "BAD")], resume=None, bad=lambda r: r[1] == "BAD"
        )
        assert result.rows_failed == 1
        assert meta.failed == []  # skip → dead-letter 미기록

    def test_max_failed_rows_aborts(self):
        cfg = _cfg(
            sync_mode="append", watermark_column="id",
            on_row_error="dead_letter", max_failed_rows=0, batch_size=10,
        )
        pg = FakePG(COLS, [(1, "a"), (2, "BAD")])
        ch = FakeCH(bad=lambda r: r[1] == "BAD")
        meta = FakeMeta(resume=None)
        with pytest.raises(RuntimeError, match="max_failed_rows"):
            TableCopier(cfg).copy(pg, ch, meta, target_default_db="default")
        assert meta.finished["status"] == "failed"

    def test_multi_batch(self):
        cfg = _cfg(sync_mode="full_reload", batch_size=1)
        result, pg, ch, meta = self._run(cfg, [(1, "a"), (2, "b"), (3, "c")])
        assert result.batch_count == 3
        assert len(meta.batches) == 3
        assert result.rows_written == 3

    def test_optimize_after_sync(self):
        cfg = _cfg(sync_mode="full_reload", optimize_after_sync=True)
        _, _, ch, _ = self._run(cfg, [(1, "a")])
        assert any(s.startswith("OPTIMIZE TABLE") and "FINAL" in s for s in ch.executed)

    def test_missing_source_table_raises(self):
        cfg = _cfg(sync_mode="full_reload")
        pg = FakePG([], [])  # no columns
        ch = FakeCH()
        meta = FakeMeta()
        with pytest.raises(ValueError, match="not found or has no columns"):
            TableCopier(cfg).copy(pg, ch, meta, target_default_db="default")


# ── copy_missing_keys (integrity self-heal) ──────────────────────


class FakeKeyCursor:
    """introspection + 'WHERE key IN %s' 재조회를 라우팅하는 unnamed 커서."""

    def __init__(self, pg):
        self.pg = pg
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.pg.queries.append((sql, params))
        if sql.strip().startswith("SELECT column_name"):
            self._rows = list(COLS)
        else:  # SELECT ... WHERE "id" IN %s
            wanted = params[0]
            self._rows = [self.pg.rows[i] for i in wanted if i in self.pg.rows]

    def fetchall(self):
        return self._rows


class FakeKeyPG:
    def __init__(self, rows):
        self.rows = rows  # {id: (id, name)}
        self.queries = []
        self.rolled_back = False

    def cursor(self, name=None):
        return FakeKeyCursor(self)

    def rollback(self):
        self.rolled_back = True

    def close(self):
        pass


class TestCopyMissingKeys:
    def test_recopies_and_does_not_advance_watermark(self):
        cfg = _cfg(sync_mode="append", watermark_column="id", engine="ReplacingMergeTree")
        pg = FakeKeyPG({2: (2, "b"), 4: (4, "d")})
        ch = FakeCH()
        meta = FakeMeta()
        written, failed = TableCopier(cfg).copy_missing_keys(
            pg, ch, meta, key_cols=["id"], keys=[(2,), (4,)], target_default_db="default"
        )
        assert written == 2 and failed == 0
        assert (2, "b") in ch.inserted and (4, "d") in ch.inserted
        # repair 는 CREATE TABLE 을 재실행하지 않는다 (테이블 존재 보장 + DDL 락 회피)
        assert not any("CREATE TABLE" in s for s in ch.executed)
        # 단일 컬럼 key 는 = ANY(array) 로 나가야 한다 (큰 IN 리스트 → max_stack_depth 방지)
        fetch = [q for q, _ in pg.queries if "= ANY(" in q]
        assert fetch, "repair fetch must use = ANY(array), not a large IN list"
        # ANY 파라미터는 반드시 list (tuple 이면 IN 구문이 됨)
        any_params = [p for q, p in pg.queries if "= ANY(" in q][0]
        assert isinstance(any_params[0], list)
        # repair run 은 watermark 를 전진시키지 않는다 (resume/무결성 window 제외)
        assert meta.started["watermark_before"] is None
        assert meta.started["watermark_column"] is None
        assert meta.finished["watermark_after"] is None
        assert meta.finished["status"] == "success"
        assert pg.rolled_back is True

    def test_fetch_is_bounded_by_watermark_range(self):
        # wm_lo/wm_hi 를 주면 fetch 가 watermark 구간으로 제한된다 (source seq scan 방지).
        cfg = _cfg(
            sync_mode="append", watermark_column="updated_at",
            engine="ReplacingMergeTree",
        )
        pg = FakeKeyPG({2: (2, "b"), 4: (4, "d")})
        ch = FakeCH()
        meta = FakeMeta()
        TableCopier(cfg).copy_missing_keys(
            pg, ch, meta, key_cols=["id"], keys=[(2,), (4,)],
            target_default_db="default", wm_lo=0, wm_hi=100,
        )
        fetch = [q for q, _ in pg.queries if "= ANY(" in q][0]
        assert '"updated_at" > %s' in fetch and '"updated_at" <= %s' in fetch

    def test_empty_keys_is_noop(self):
        cfg = _cfg(sync_mode="append", watermark_column="id")
        pg = FakeKeyPG({})
        ch = FakeCH()
        meta = FakeMeta()
        assert TableCopier(cfg).copy_missing_keys(
            pg, ch, meta, key_cols=["id"], keys=[], target_default_db="default"
        ) == (0, 0)
        assert meta.started is None  # run 조차 열지 않음

    def test_dead_letters_rows_that_still_fail(self):
        cfg = _cfg(
            sync_mode="append", watermark_column="id",
            engine="ReplacingMergeTree", on_row_error="dead_letter",
        )
        pg = FakeKeyPG({2: (2, "b"), 3: (3, "BAD")})
        ch = FakeCH(bad=lambda r: r[1] == "BAD")
        meta = FakeMeta()
        written, failed = TableCopier(cfg).copy_missing_keys(
            pg, ch, meta, key_cols=["id"], keys=[(2,), (3,)], target_default_db="default"
        )
        assert written == 1 and failed == 1
        assert len(meta.failed) == 1
        assert meta.finished["status"] == "partial"
