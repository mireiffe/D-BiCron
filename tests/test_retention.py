"""Tests for PG source retention."""

from __future__ import annotations

import pytest

from pg2ch.config import RetentionPolicy, TableConfig
from pg2ch.retention import PgRetention


class FakeMeta:
    def __init__(self, resume=None):
        self.resume = resume
        self.ensured = False

    def ensure_schema(self):
        self.ensured = True

    def get_resume_watermark(self, table_id, wm_col):
        return self.resume


class FakeCursor:
    def __init__(self, owner):
        self.owner = owner
        self.rowcount = 0
        self._fetchone = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.owner.executed.append((sql, params))
        if sql.startswith("SELECT MAX"):
            self._fetchone = (self.owner.max_ts,)
        elif sql.startswith("DELETE"):
            self.rowcount = self.owner.delete_counts.pop(0)

    def fetchone(self):
        return self._fetchone


class FakePG:
    def __init__(self, *, max_ts=None, delete_counts=None):
        self.max_ts = max_ts
        self.delete_counts = list(delete_counts or [0])
        self.executed = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def _cfg(**over) -> TableConfig:
    d = {
        "table_id": "events",
        "source": "pg",
        "target": "ch",
        "source_table": "public.events",
        "target_table": "default.events",
        "sync_mode": "append",
        "watermark_column": "id",
        "timestamp_column": "created_at",
        "order_by": ["id"],
    }
    d.update(over)
    return TableConfig.from_dict(d)


def _policy(**over) -> RetentionPolicy:
    d = {
        "table_id": "events",
        "retention": "2026-01-01T00:00:00",
        "batch_size": 10,
    }
    d.update(over)
    return RetentionPolicy(**d)


def test_policy_table_id_must_match_config():
    with pytest.raises(ValueError, match="does not match"):
        PgRetention(_cfg(), _policy(table_id="other"))


def test_retention_skips_non_append():
    cfg = _cfg(sync_mode="full_reload", watermark_column=None)
    pg = FakePG()
    result = PgRetention(cfg, _policy()).purge(pg, FakeMeta(resume="100"))
    assert result.status == "skipped"
    assert result.reason == "retention requires append sync_mode"
    assert pg.executed == []


def test_retention_requires_timestamp_column():
    cfg = _cfg(timestamp_column=None)
    with pytest.raises(ValueError, match="timestamp_column is required"):
        PgRetention(cfg, _policy()).purge(FakePG(), FakeMeta(resume="100"))


def test_retention_skips_without_finalized_watermark():
    cfg = _cfg()
    pg = FakePG()
    result = PgRetention(cfg, _policy()).purge(pg, FakeMeta(resume=None))
    assert result.status == "skipped"
    assert result.reason == "no finalized watermark"
    assert pg.executed == []


def test_retention_purges_in_batches_with_safe_cutoff():
    cfg = _cfg()
    pg = FakePG(max_ts="2026-06-01T00:00:00", delete_counts=[10, 3])
    result = PgRetention(cfg, _policy()).purge(pg, FakeMeta(resume="100"))
    assert result.status == "success"
    assert result.rows_deleted == 13
    assert result.safe_cutoff == "2026-01-01T00:00:00"
    delete_calls = [call for call in pg.executed if call[0].startswith("DELETE")]
    assert len(delete_calls) == 2
    assert delete_calls[0][1] == ("2026-01-01T00:00:00", 10)


def test_retention_cutoff_is_capped_to_last_synced_timestamp():
    cfg = _cfg()
    policy = _policy(retention="2026-06-20T00:00:00")
    pg = FakePG(max_ts="2026-06-01T00:00:00", delete_counts=[0])
    result = PgRetention(cfg, policy).purge(pg, FakeMeta(resume="100"))
    assert result.status == "success"
    assert result.safe_cutoff == "2026-06-01T00:00:00"
    delete_calls = [call for call in pg.executed if call[0].startswith("DELETE")]
    assert delete_calls[0][1] == ("2026-06-01T00:00:00", 10)


def test_same_timestamp_watermark_does_not_query_max_timestamp():
    cfg = _cfg(watermark_column="created_at")
    pg = FakePG(delete_counts=[0])
    result = PgRetention(cfg, _policy()).purge(
        pg,
        FakeMeta(resume="2026-06-01T00:00:00"),
    )
    assert result.status == "success"
    assert not any(sql.startswith("SELECT MAX") for sql, _ in pg.executed)
