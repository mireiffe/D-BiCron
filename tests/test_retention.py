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
        s = sql.lstrip()
        if s.startswith("SELECT MAX"):
            self._fetchone = (self.owner.max_value,)
        elif "pg_index" in s:
            # _warn_if_unindexed 의 인덱스 존재 확인.
            self._fetchone = (1,) if self.owner.indexed else None
        elif s.startswith("SELECT"):
            # _purge_source 의 batch 상한(hi) probe. boundaries 를 순서대로 소비하고,
            # 다 떨어지면(=잔여 < batch_size) None 을 돌려 마지막 batch 로 종료시킨다.
            if self.owner.boundaries:
                self._fetchone = (self.owner.boundaries.pop(0),)
            else:
                self._fetchone = (None,)
        elif s.startswith("DELETE"):
            self.rowcount = self.owner.delete_counts.pop(0) if self.owner.delete_counts else 0

    def fetchone(self):
        return self._fetchone


class FakePG:
    def __init__(self, *, max_value=None, delete_counts=None, boundaries=None, indexed=True):
        self.max_value = max_value
        self.delete_counts = list(delete_counts or [0])
        # 각 batch 의 상한 hi 값(마지막 batch 는 boundaries 소진 → None 으로 종료).
        self.boundaries = list(boundaries or [])
        self.indexed = indexed
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
    # 첫 batch 는 상한 hi(=2025-12-01)까지, 잔여(boundaries 소진)는 마지막 batch.
    cfg = _cfg()
    pg = FakePG(
        max_value=datetime(2026, 6, 1),
        boundaries=[datetime(2025, 12, 1)],  # 첫 batch 상한; 이후 None → 종료 batch
        delete_counts=[10, 3],
    )
    result = PgRetention(cfg, _policy()).purge(pg, FakeMeta(resume="100"))
    assert result.status == "success"
    assert result.rows_deleted == 13
    assert result.column == "created_at"
    assert result.safe_cutoff == "2026-01-01T00:00:00"
    delete_calls = _deletes(pg)
    assert len(delete_calls) == 2
    # 첫 batch: (−∞, hi] 삭제
    assert '"created_at" <= %s' in delete_calls[0][0]
    assert delete_calls[0][1] == (datetime(2025, 12, 1),)
    # 마지막 batch: (lo, cutoff) 잔여 삭제
    assert '"created_at" > %s AND "created_at" < %s' in delete_calls[1][0]
    assert delete_calls[1][1] == (datetime(2025, 12, 1), datetime(2026, 1, 1))
    # 재스캔형 ctid batch 패턴은 더 이상 쓰지 않는다 (O(n²) → hang 회귀 방지)
    assert not any("ctid" in sql for sql, _ in pg.executed)
    # batch 상한 probe 는 ORDER BY + OFFSET(=batch_size-1) 로 인덱스를 태운다
    probes = [c for c in pg.executed if 'ORDER BY "created_at"' in c[0]]
    assert probes and "OFFSET %s" in probes[0][0]
    assert probes[0][1] == (datetime(2026, 1, 1), 9)
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
    # 잔여만 있는 단일 batch: (−∞, cutoff) 삭제, cutoff=safe_cutoff(캡핑됨)
    assert _deletes(pg)[0][1] == (datetime(2026, 6, 1),)


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
    # serial keep-last-N: cutoff = 100 - 40. 잔여만 있는 단일 batch → (−∞, cutoff)
    assert _deletes(pg)[0][1] == (60,)
    assert result.column == "id"
    assert result.safe_cutoff == "60"


def test_retention_serial_zero_deletes_all_synced():
    cfg = _cfg()
    policy = _policy(retention=0, column=None, type=None)
    pg = FakePG(delete_counts=[0])
    result = PgRetention(cfg, policy).purge(pg, FakeMeta(resume="100"))
    assert result.status == "success"
    assert _deletes(pg)[0][1] == (100,)


def test_retention_expr_revalidated_against_effective_type():
    # type 미지정 정책은 로드 시점에 느슨히 통과하지만, 유효 타입(serial)이 정해지는
    # 실행 시점에 "180d" 는 거부된다.
    cfg = _cfg()
    policy = _policy(retention="180d", column=None, type=None)
    with pytest.raises(ValueError, match="retention.*not a number"):
        PgRetention(cfg, policy).purge(FakePG(), FakeMeta(resume="100"))


def test_retention_keyset_walk_advances_lower_bound():
    # 여러 batch: lo 가 hi 로 전진하며 (lo, hi] 구간을 순서대로 지운다.
    cfg = _cfg()
    policy = _policy(retention=0, column=None, type=None)  # serial, cutoff=100
    pg = FakePG(delete_counts=[10, 10, 4], boundaries=[30, 70])
    result = PgRetention(cfg, policy).purge(pg, FakeMeta(resume="100"))
    assert result.status == "success"
    assert result.rows_deleted == 24
    deletes = _deletes(pg)
    assert len(deletes) == 3
    assert deletes[0][1] == (30,)          # (−∞, 30]
    assert deletes[1][1] == (30, 70)       # (30, 70]
    assert deletes[2][1] == (70, 100)      # (70, cutoff) 잔여
    assert pg.commits == 3                 # batch 마다 독립 commit
    # 두 번째 probe 는 lo(=30) 이후만 스캔한다 (앞구간 재스캔 없음)
    probes = [c for c in pg.executed if "OFFSET %s" in c[0]]
    assert probes[1][1] == (30, 100, 9)


def test_retention_warns_when_column_unindexed(caplog):
    cfg = _cfg()
    policy = _policy(retention=0, column=None, type=None)
    pg = FakePG(delete_counts=[0], indexed=False)
    with caplog.at_level("WARNING", logger="pg2ch.retention.events"):
        PgRetention(cfg, policy).purge(pg, FakeMeta(resume="100"))
    assert any("not the leading column of any index" in r.message for r in caplog.records)


def test_retention_no_index_warning_when_indexed(caplog):
    cfg = _cfg()
    policy = _policy(retention=0, column=None, type=None)
    pg = FakePG(delete_counts=[0], indexed=True)
    with caplog.at_level("WARNING", logger="pg2ch.retention.events"):
        PgRetention(cfg, policy).purge(pg, FakeMeta(resume="100"))
    assert not any("leading column" in r.message for r in caplog.records)


def test_retention_skips_when_no_synced_value():
    cfg = _cfg()
    pg = FakePG(max_value=None, delete_counts=[0])
    result = PgRetention(cfg, _policy()).purge(pg, FakeMeta(resume="100"))
    assert result.status == "skipped"
    assert result.reason == "watermark resolves to no synced value"
    assert _deletes(pg) == []
