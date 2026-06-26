"""Tests for pg2ch.integrity (retention 전 무결성 검사)."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pg2ch.config import TableConfig
from pg2ch.integrity import IntegrityChecker, _ch_literal, _coerce_watermark


# ── fakes ────────────────────────────────────────────────────────


class FakeMeta:
    def __init__(self, windows):
        self.windows = windows
        self.ensured = False
        self.last_limit = None

    def ensure_schema(self):
        self.ensured = True

    def recent_run_windows(self, table_id, wm_col, limit):
        self.last_limit = limit
        return self.windows[:limit]


class FakePGCursor:
    def __init__(self, pg):
        self.pg = pg
        self._row = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.pg.executed.append((sql, params))
        self._row = (self.pg.source_counts.pop(0),)

    def fetchone(self):
        return self._row


class FakePG:
    def __init__(self, source_counts):
        self.source_counts = list(source_counts)
        self.executed = []
        self.rollbacks = 0

    def cursor(self):
        return FakePGCursor(self)

    def rollback(self):
        self.rollbacks += 1


class FakeCH:
    def __init__(self, target_counts):
        self.target_counts = list(target_counts)
        self.executed = []

    def execute(self, sql, params=None, **kw):
        self.executed.append(sql)
        return [(self.target_counts.pop(0),)]

    def disconnect(self):
        pass


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
        "integrity_enabled": True,
    }
    d.update(over)
    return TableConfig.from_dict(d)


def _win(run_id, before, after):
    return {"run_id": run_id, "watermark_before": before, "watermark_after": after}


# ── helpers ──────────────────────────────────────────────────────


class TestCoerce:
    def test_int(self):
        assert _coerce_watermark("100") == 100
        assert isinstance(_coerce_watermark("100"), int)

    def test_decimal(self):
        assert _coerce_watermark("100.5") == Decimal("100.5")

    def test_datetime(self):
        assert _coerce_watermark("2026-06-01T12:00:00") == datetime(2026, 6, 1, 12)

    def test_none(self):
        assert _coerce_watermark(None) is None


class TestChLiteral:
    def test_numeric_bare(self):
        assert _ch_literal("12345") == "12345"

    def test_naive_datetime_preserves_micros(self):
        assert _ch_literal("2026-06-01T12:00:00.123456") == "'2026-06-01 12:00:00.123456'"

    def test_aware_datetime_converted_to_utc(self):
        assert _ch_literal("2026-06-01T12:00:00+00:00") == "'2026-06-01 12:00:00.000000'"


# ── verify() ─────────────────────────────────────────────────────


class TestVerify:
    def test_disabled_returns_without_query(self):
        cfg = _cfg(integrity_enabled=False)
        pg, ch = FakePG([]), FakeCH([])
        result = IntegrityChecker(cfg).verify(pg, ch, FakeMeta([]))
        assert result.status == "disabled"
        assert pg.executed == [] and ch.executed == []

    def test_non_append_skipped(self):
        cfg = _cfg()
        cfg.sync_mode = "full_reload"  # 검증 우회: verify 의 방어 분기 확인
        result = IntegrityChecker(cfg).verify(FakePG([]), FakeCH([]), FakeMeta([]))
        assert result.status == "skipped"
        assert "append" in result.reason

    def test_no_windows_skipped(self):
        result = IntegrityChecker(_cfg()).verify(FakePG([]), FakeCH([]), FakeMeta([]))
        assert result.status == "skipped"
        assert result.reason == "no finalized run windows to check"

    def test_match_is_ok(self):
        pg, ch = FakePG([100]), FakeCH([100])
        meta = FakeMeta([_win(5, "1000", "1100")])
        result = IntegrityChecker(_cfg()).verify(pg, ch, meta)
        assert result.status == "ok"
        assert result.missing_rows == 0
        assert result.source_rows == 100 and result.target_rows == 100
        assert result.windows_checked == 1

    def test_target_short_is_mismatch(self):
        pg, ch = FakePG([100]), FakeCH([97])
        meta = FakeMeta([_win(5, "1000", "1100")])
        result = IntegrityChecker(_cfg()).verify(pg, ch, meta)
        assert result.status == "mismatch"
        assert result.missing_rows == 3
        assert result.windows[0]["missing"] == 3

    def test_target_excess_not_flagged(self):
        # overlap 중복으로 target distinct 가 더 많아도(이론상) 누락은 아님.
        pg, ch = FakePG([100]), FakeCH([100])
        result = IntegrityChecker(_cfg()).verify(pg, ch, FakeMeta([_win(5, "1", "9")]))
        assert result.status == "ok"

    def test_tolerance_absorbs_small_shortfall(self):
        pg, ch = FakePG([100]), FakeCH([98])
        result = IntegrityChecker(_cfg(integrity_tolerance=2)).verify(
            pg, ch, FakeMeta([_win(5, "1", "9")])
        )
        assert result.status == "ok"
        assert result.missing_rows == 2

    def test_tolerance_exceeded_is_mismatch(self):
        pg, ch = FakePG([100]), FakeCH([97])
        result = IntegrityChecker(_cfg(integrity_tolerance=2)).verify(
            pg, ch, FakeMeta([_win(5, "1", "9")])
        )
        assert result.status == "mismatch"

    def test_first_full_copy_window_skipped(self):
        # watermark_before=None (첫 전체복사 구간)은 하한 없음 → 스킵.
        pg, ch = FakePG([]), FakeCH([])
        meta = FakeMeta([_win(1, None, "1100")])
        result = IntegrityChecker(_cfg()).verify(pg, ch, meta)
        assert result.status == "skipped"
        assert pg.executed == [] and ch.executed == []
        assert result.windows[0]["status"] == "skipped"

    def test_lookback_aggregates_multiple_windows(self):
        pg = FakePG([50, 40, 30])
        ch = FakeCH([50, 38, 30])  # 두 번째 window 에서 2 누락
        meta = FakeMeta([
            _win(7, "200", "300"),
            _win(6, "100", "200"),
            _win(5, "1", "100"),
        ])
        result = IntegrityChecker(_cfg(integrity_lookback_runs=3)).verify(pg, ch, meta)
        assert meta.last_limit == 3
        assert result.windows_checked == 3
        assert result.source_rows == 120
        assert result.target_rows == 118
        assert result.missing_rows == 2
        assert result.status == "mismatch"

    def test_ch_uses_uniqexact_on_key_and_pg_uses_count(self):
        pg, ch = FakePG([10]), FakeCH([10])
        IntegrityChecker(_cfg(order_by=["id"])).verify(
            pg, ch, FakeMeta([_win(5, "1000", "1100")])
        )
        ch_sql = ch.executed[0]
        assert "uniqExact(`id`)" in ch_sql
        assert "`id` > 1000" in ch_sql and "`id` <= 1100" in ch_sql
        pg_sql, params = pg.executed[0]
        assert "count(*)" in pg_sql
        assert params == (1000, 1100)

    def test_composite_key_distinct(self):
        pg, ch = FakePG([10]), FakeCH([10])
        IntegrityChecker(_cfg(order_by=["a", "b"])).verify(
            pg, ch, FakeMeta([_win(5, "1", "9")])
        )
        assert "uniqExact(`a`, `b`)" in ch.executed[0]


class TestRun:
    def test_disabled_short_circuits_before_connections(self):
        # integrity_enabled=False 면 connections.json 없이도 즉시 반환해야 한다.
        result = IntegrityChecker(_cfg(integrity_enabled=False)).run()
        assert result.status == "disabled"
