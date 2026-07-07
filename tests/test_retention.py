"""Tests for PG source retention (컬럼/타입 인지 cutoff + 캡핑)."""

from __future__ import annotations

from datetime import datetime, timedelta

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
            self._fetchone = (self.owner.max_value,)
        elif sql.startswith("DELETE"):
            self.rowcount = self.owner.delete_counts.pop(0)

    def fetchone(self):
        return self._fetchone


class FakePG:
    def __init__(self, *, max_value=None, delete_counts=None):
        self.max_value = max_value
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
        "watermark_type": "serial",
        "order_by": ["id"],
    }
    d.update(over)
    return TableConfig.from_dict(d)


def _policy(**over) -> RetentionPolicy:
    d = {
        "table_id": "events",
        "retention": "2026-01-01T00:00:00",
        "column": "created_at",
        "type": "timestamp",
        "batch_size": 10,
    }
    d.update(over)
    return RetentionPolicy(**d)


def _deletes(pg):
    return [call for call in pg.executed if call[0].startswith("DELETE")]


def test_policy_table_id_must_match_config():
    with pytest.raises(ValueError, match="does not match"):
        PgRetention(_cfg(), _policy(table_id="other"))


def test_retention_skips_non_append():
    cfg = _cfg(sync_mode="full_reload", watermark_column=None, watermark_type=None)
    pg = FakePG()
    result = PgRetention(cfg, _policy()).purge(pg, FakeMeta(resume="100"))
    assert result.status == "skipped"
    assert result.reason == "retention requires append sync_mode"
    assert pg.executed == []


def test_retention_skips_without_finalized_watermark():
    cfg = _cfg()
    pg = FakePG()
    result = PgRetention(cfg, _policy()).purge(pg, FakeMeta(resume=None))
    assert result.status == "skipped"
    assert result.reason == "no finalized watermark"
    assert pg.executed == []


def test_retention_purges_in_batches_with_safe_cutoff():
    # serial watermark + timestamp 삭제 컬럼: synced 구간의 MAX(created_at) 으로 환산.
    cfg = _cfg()
    pg = FakePG(max_value=datetime(2026, 6, 1), delete_counts=[10, 3])
    result = PgRetention(cfg, _policy()).purge(pg, FakeMeta(resume="100"))
    assert result.status == "success"
    assert result.rows_deleted == 13
    assert result.column == "created_at"
    assert result.safe_cutoff == "2026-01-01T00:00:00"
    delete_calls = _deletes(pg)
    assert len(delete_calls) == 2
    assert '"created_at" < %s' in delete_calls[0][0]
    assert delete_calls[0][1] == (datetime(2026, 1, 1), 10)
    # MAX 환산 쿼리는 typed watermark(int) 로 바인딩된다
    max_calls = [c for c in pg.executed if c[0].startswith("SELECT MAX")]
    assert max_calls[0][1] == (100,)


def test_retention_cutoff_is_capped_to_last_synced_value():
    cfg = _cfg()
    policy = _policy(retention="2026-06-20T00:00:00")
    pg = FakePG(max_value=datetime(2026, 6, 1), delete_counts=[0])
    result = PgRetention(cfg, policy).purge(pg, FakeMeta(resume="100"))
    assert result.status == "success"
    assert result.safe_cutoff == "2026-06-01T00:00:00"
    assert _deletes(pg)[0][1] == (datetime(2026, 6, 1), 10)


def test_retention_relative_expr_uses_now():
    cfg = _cfg()
    policy = _policy(retention="30d")
    far_future_sync = datetime.now() + timedelta(days=365)
    pg = FakePG(max_value=far_future_sync, delete_counts=[0])
    result = PgRetention(cfg, policy).purge(pg, FakeMeta(resume="100"))
    assert result.status == "success"
    cutoff = _deletes(pg)[0][1][0]
    assert abs((datetime.now() - timedelta(days=30)) - cutoff) < timedelta(minutes=5)


def test_retention_on_watermark_column_skips_max_query():
    # 삭제 컬럼 == watermark 컬럼이면 MAX 환산 없이 watermark 값을 그대로 쓴다.
    cfg = _cfg()
    policy = _policy(retention=40, column=None, type=None)
    pg = FakePG(delete_counts=[0])
    result = PgRetention(cfg, policy).purge(pg, FakeMeta(resume="100"))
    assert result.status == "success"
    assert not any(sql.startswith("SELECT MAX") for sql, _ in pg.executed)
    # serial keep-last-N: cutoff = 100 - 40
    assert _deletes(pg)[0][1] == (60, 10)
    assert result.column == "id"
    assert result.safe_cutoff == "60"


def test_retention_serial_zero_deletes_all_synced():
    cfg = _cfg()
    policy = _policy(retention=0, column=None, type=None)
    pg = FakePG(delete_counts=[0])
    result = PgRetention(cfg, policy).purge(pg, FakeMeta(resume="100"))
    assert result.status == "success"
    assert _deletes(pg)[0][1] == (100, 10)


def test_retention_expr_revalidated_against_effective_type():
    # type 미지정 정책은 로드 시점에 느슨히 통과하지만, 유효 타입(serial)이 정해지는
    # 실행 시점에 "180d" 는 거부된다.
    cfg = _cfg()
    policy = _policy(retention="180d", column=None, type=None)
    with pytest.raises(ValueError, match="retention.*not a number"):
        PgRetention(cfg, policy).purge(FakePG(), FakeMeta(resume="100"))


def test_retention_skips_when_no_synced_value():
    cfg = _cfg()
    pg = FakePG(max_value=None, delete_counts=[0])
    result = PgRetention(cfg, _policy()).purge(pg, FakeMeta(resume="100"))
    assert result.status == "skipped"
    assert result.reason == "watermark resolves to no synced value"
    assert _deletes(pg) == []
