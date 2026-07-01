"""Tests for pg2ch.integrity — 검사(count/key_diff) + 누락 row 자가복구."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pg2ch.config import TableConfig
from pg2ch.integrity import IntegrityChecker, _canon_key, _ch_literal, _coerce_watermark


# ── 검사(verify) 용 큐 기반 fakes ──────────────────────────────────


class VPGCur:
    def __init__(self, pg):
        self.pg = pg
        self._r = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.pg.sql.append((sql, params))
        if sql.strip().startswith("SELECT count(*)"):
            self._r = [(self.pg.counts.pop(0),)]
        else:  # SELECT <key cols> FROM ... WHERE wm > .. (window keys)
            self._r = list(self.pg.keys.pop(0))

    def fetchone(self):
        return self._r[0]

    def fetchall(self):
        return self._r


class VPG:
    def __init__(self, counts=(), keys=()):
        self.counts = list(counts)
        self.keys = list(keys)
        self.sql = []

    def cursor(self):
        return VPGCur(self)

    def rollback(self):
        pass


class VCH:
    def __init__(self, counts=(), keys=()):
        self.counts = list(counts)
        self.keys = list(keys)
        self.sql = []

    def execute(self, sql, params=None, **kw):
        self.sql.append(sql)
        s = sql.strip()
        if s.startswith("SELECT uniqExact") or s.startswith("SELECT count()"):
            return [(self.counts.pop(0),)]
        if s.startswith("SELECT DISTINCT"):
            return list(self.keys.pop(0))
        return []

    def disconnect(self):
        pass


class VMeta:
    def __init__(self, windows, deadletter=()):
        self.windows = windows
        self.deadletter = list(deadletter)
        self.limit = None

    def ensure_schema(self):
        pass

    def recent_run_windows(self, table_id, wm_col, limit):
        self.limit = limit
        return self.windows[:limit]

    def unresolved_failed_keys(self, table_id, key_cols):
        return list(self.deadletter)


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
        "engine": "ReplacingMergeTree",
        "integrity_enabled": True,
    }
    d.update(over)
    return TableConfig.from_dict(d)


def _win(run_id, before, after):
    return {"run_id": run_id, "watermark_before": before, "watermark_after": after}


# ── helpers ──────────────────────────────────────────────────────


class TestCoerceAndLiteral:
    def test_coerce_int(self):
        assert _coerce_watermark("100") == 100 and isinstance(_coerce_watermark("100"), int)

    def test_coerce_decimal(self):
        assert _coerce_watermark("100.5") == Decimal("100.5")

    def test_coerce_datetime(self):
        assert _coerce_watermark("2026-06-01T12:00:00") == datetime(2026, 6, 1, 12)

    def test_ch_literal_numeric(self):
        assert _ch_literal("12345") == "12345"

    def test_ch_literal_datetime_micros(self):
        assert _ch_literal("2026-06-01T12:00:00.123456") == "'2026-06-01 12:00:00.123456'"

    def test_ch_literal_aware_to_utc(self):
        assert _ch_literal("2026-06-01T12:00:00+00:00") == "'2026-06-01 12:00:00.000000'"

    def test_canon_key_unifies_types(self):
        # source int 100 과 dead-letter text "100" 이 같은 canon 이어야 한다.
        assert _canon_key((100,)) == _canon_key(("100",)) == ("100",)


# ── verify(): count mode ─────────────────────────────────────────


class TestVerifyCount:
    def test_disabled(self):
        cfg = _cfg(integrity_enabled=False)
        pg, ch = VPG(), VCH()
        r = IntegrityChecker(cfg).verify(pg, ch, VMeta([]))
        assert r.status == "disabled"
        assert pg.sql == [] and ch.sql == []

    def test_non_append_skipped(self):
        cfg = _cfg()
        cfg.sync_mode = "full_reload"  # verify 방어 분기
        r = IntegrityChecker(cfg).verify(VPG(), VCH(), VMeta([]))
        assert r.status == "skipped" and "append" in r.reason

    def test_no_windows_skipped(self):
        r = IntegrityChecker(_cfg()).verify(VPG(), VCH(), VMeta([]))
        assert r.status == "skipped"
        assert r.reason == "no finalized run windows to check"

    def test_match_is_ok_without_keydiff(self):
        pg, ch = VPG(counts=[3]), VCH(counts=[3])
        r = IntegrityChecker(_cfg()).verify(pg, ch, VMeta([_win(5, "0", "10")]))
        assert r.status == "ok" and r.missing_rows == 0
        # count 가 맞으면 key 조회는 하지 않는다
        assert not any("DISTINCT" in s for s in ch.sql)

    def test_shortfall_triggers_keydiff(self):
        pg = VPG(counts=[5], keys=[[(1,), (2,), (3,), (4,), (5,)]])
        ch = VCH(counts=[2], keys=[[(1,), (2,)]])
        r = IntegrityChecker(_cfg()).verify(pg, ch, VMeta([_win(5, "0", "10")]))
        assert r.status == "mismatch"
        assert r.missing_rows == 3
        assert set(r._repair_keys) == {(3,), (4,), (5,)}
        assert any("uniqExact(`id`)" in s for s in ch.sql)
        assert any("DISTINCT" in s for s in ch.sql)

    def test_deadletter_keys_excluded(self):
        pg = VPG(counts=[5], keys=[[(1,), (2,), (3,), (4,), (5,)]])
        ch = VCH(counts=[2], keys=[[(1,), (2,)]])
        meta = VMeta([_win(5, "0", "10")], deadletter=[("4",)])
        r = IntegrityChecker(_cfg()).verify(pg, ch, meta)
        assert r.status == "mismatch"
        assert r.missing_rows == 2  # 4 는 dead-letter → 제외
        assert r.deadletter_rows == 1
        assert set(r._repair_keys) == {(3,), (5,)}

    def test_tolerance_absorbs(self):
        pg = VPG(counts=[5], keys=[[(1,), (2,), (3,), (4,), (5,)]])
        ch = VCH(counts=[3], keys=[[(1,), (2,), (3,)]])
        r = IntegrityChecker(_cfg(integrity_tolerance=2)).verify(
            pg, ch, VMeta([_win(5, "0", "10")])
        )
        assert r.status == "ok" and r.missing_rows == 2

    def test_first_full_copy_window_skipped(self):
        pg, ch = VPG(), VCH()
        r = IntegrityChecker(_cfg()).verify(pg, ch, VMeta([_win(1, None, "10")]))
        assert r.status == "skipped"
        assert pg.sql == [] and ch.sql == []
        assert r.windows[0]["status"] == "skipped"

    def test_lookback_aggregates(self):
        # window1 ok(count), window2 shortfall→keydiff
        pg = VPG(counts=[2, 3], keys=[[(10,), (11,), (12,)]])
        ch = VCH(counts=[2, 1], keys=[[(10,)]])
        meta = VMeta([_win(7, "20", "30"), _win(6, "10", "20")])
        r = IntegrityChecker(_cfg(integrity_lookback_runs=2)).verify(pg, ch, meta)
        assert meta.limit == 2
        assert r.windows_checked == 2
        assert r.missing_rows == 2  # window2 에서 11,12
        assert set(r._repair_keys) == {(11,), (12,)}
        assert r.status == "mismatch"

    def test_missing_sample_is_capped_not_full_dump(self):
        # 누락이 대량이어도 구조화 결과엔 소수 샘플만(로그 폭발 방지), 개수는 전량.
        src = [(i,) for i in range(1, 21)]  # 20 개 전부 누락
        pg = VPG(counts=[20], keys=[src])
        ch = VCH(counts=[0], keys=[[]])
        r = IntegrityChecker(_cfg()).verify(pg, ch, VMeta([_win(5, "0", "100")]))
        assert r.missing_rows == 20
        assert len(r.windows[0]["missing_sample"]) == 5  # 샘플만
        assert len(r._repair_keys) == 20  # 재복사는 전량 대상

    def test_composite_key(self):
        pg = VPG(counts=[2], keys=[[(1, "a"), (2, "b")]])
        ch = VCH(counts=[1], keys=[[(1, "a")]])
        r = IntegrityChecker(_cfg(order_by=["a", "b"])).verify(
            pg, ch, VMeta([_win(5, "0", "10")])
        )
        assert r.missing_rows == 1
        assert set(r._repair_keys) == {(2, "b")}
        assert any("uniqExact(`a`, `b`)" in s for s in ch.sql)


# ── verify(): key_diff mode ──────────────────────────────────────


class TestVerifyKeyDiff:
    def test_always_diffs_no_count(self):
        pg = VPG(keys=[[(1,), (2,), (3,)]])
        ch = VCH(keys=[[(1,), (2,), (3,)]])
        r = IntegrityChecker(_cfg(integrity_method="key_diff")).verify(
            pg, ch, VMeta([_win(5, "0", "10")])
        )
        assert r.status == "ok"
        # count 게이트를 쓰지 않는다
        assert not any("count(*)" in s for (s, _) in pg.sql)
        assert not any("uniqExact" in s for s in ch.sql)

    def test_detects_churn_missing(self):
        # count 는 같지만(둘 다 3) 집합이 달라 누락(3) 이 있는 경우
        pg = VPG(keys=[[(1,), (2,), (3,)]])
        ch = VCH(keys=[[(1,), (2,), (9,)]])  # 9=삭제된 옛 key, 3=누락
        r = IntegrityChecker(_cfg(integrity_method="key_diff")).verify(
            pg, ch, VMeta([_win(5, "0", "10")])
        )
        assert r.status == "mismatch"
        assert set(r._repair_keys) == {(3,)}


# ── key 컬럼이 없을 때(count-only fallback) ──────────────────────


class TestNoKeyColumns:
    def test_string_order_by_falls_back_to_count(self):
        cfg = _cfg(order_by="id")  # 문자열 식 → plain 컬럼 추출 불가
        pg, ch = VPG(counts=[5]), VCH(counts=[3])
        r = IntegrityChecker(cfg).verify(pg, ch, VMeta([_win(5, "0", "10")]))
        assert r.status == "mismatch"
        assert r.missing_rows == 2
        assert r._repair_keys == []  # 재복사 불가 (key 목록 없음)
        assert any("count()" in s for s in ch.sql)


# ── 자가복구 (_run_with_repair) 통합 ─────────────────────────────


class HealPGCur:
    def __init__(self, pg):
        self.pg = pg
        self._rows = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        s = sql.strip()
        if s.startswith("SELECT column_name"):
            self._rows = [
                ("id", "integer", "NO", None, None),
                ("name", "text", "YES", None, None),
            ]
        elif s.startswith("SELECT count(*)"):
            self._rows = [(len(self.pg.keys),)]
        else:  # window keys (verify 의 _pg_keys)
            self._rows = [(k,) for k in self.pg.keys]

    def fetchone(self):
        return self._rows[0]

    def fetchall(self):
        return self._rows


class HealStreamCur:
    """repair 의 named server-side cursor — window 전체 row 를 스트리밍."""

    def __init__(self, pg):
        self._rows = [pg.rows[k] for k in pg.keys if k in pg.rows]
        self.itersize = None

    def execute(self, q, p=None):
        pass

    def fetchmany(self, n):
        chunk = self._rows[:n]
        self._rows = self._rows[n:]
        return chunk

    def close(self):
        pass


class HealPG:
    def __init__(self, keys, rows):
        self.keys = keys
        self.rows = rows

    def cursor(self, name=None):
        # repair 는 named cursor 로 window 를 스트리밍, verify 는 unnamed 로 조회.
        return HealStreamCur(self) if name is not None else HealPGCur(self)

    def rollback(self):
        pass

    def close(self):
        pass


class HealCH:
    def __init__(self, present):
        self.present = set(present)

    def execute(self, sql, params=None, **kw):
        s = sql.strip()
        if s.startswith("SELECT uniqExact") or s.startswith("SELECT count()"):
            return [(len(self.present),)]
        if s.startswith("SELECT DISTINCT"):
            return [(k,) for k in sorted(self.present)]
        if s.startswith("INSERT INTO") and isinstance(params, list):
            for r in params:
                self.present.add(r[0])
            return len(params)
        return []

    def disconnect(self):
        pass


class HealMeta:
    def __init__(self, windows, deadletter=()):
        self.windows = windows
        self.deadletter = list(deadletter)
        self._id = 0
        self.runs = []
        self.finished = []

    def ensure_schema(self):
        pass

    def recent_run_windows(self, table_id, wm_col, limit):
        return self.windows[:limit]

    def unresolved_failed_keys(self, table_id, key_cols):
        return list(self.deadletter)

    def start_run(self, **kw):
        self._id += 1
        self.runs.append(kw)
        return self._id

    def record_batch(self, **kw):
        return 1

    def finish_run(self, run_id, **kw):
        self.finished.append(kw)

    def record_failed_rows(self, **kw):
        pass


class TestRunWithRepair:
    def _rows(self, keys):
        return {k: (k, f"n{k}") for k in keys}

    def test_heals_missing_then_ok(self):
        cfg = _cfg()  # engine=ReplacingMergeTree, repair on
        pg = HealPG([1, 2, 3, 4, 5], self._rows([1, 2, 3, 4, 5]))
        ch = HealCH(present=[1, 2, 3])  # 4,5 missing
        meta = HealMeta([_win(5, "0", "10")])
        r = IntegrityChecker(cfg)._run_with_repair(
            pg, ch, meta, target_default_db="default", repair=None
        )
        assert r.status == "ok"
        assert r.repaired_rows == 2
        assert r.repair_attempts_used == 1
        assert ch.present == {1, 2, 3, 4, 5}
        # repair run 은 watermark 를 전진시키지 않는다
        assert meta.runs[-1]["watermark_before"] is None
        assert meta.runs[-1]["watermark_column"] is None

    def test_no_repair_when_disabled(self):
        cfg = _cfg(integrity_repair=False)
        pg = HealPG([1, 2, 3], self._rows([1, 2, 3]))
        ch = HealCH(present=[1])
        r = IntegrityChecker(cfg)._run_with_repair(
            pg, ch, HealMeta([_win(5, "0", "10")]), target_default_db="default",
            repair=None,
        )
        assert r.status == "mismatch"
        assert r.repaired_rows == 0
        assert ch.present == {1}

    def test_repair_skipped_for_non_replacing_engine(self):
        cfg = _cfg(engine="MergeTree")  # 재insert 가 중복을 남길 수 있어 skip
        pg = HealPG([1, 2, 3], self._rows([1, 2, 3]))
        ch = HealCH(present=[1])
        r = IntegrityChecker(cfg)._run_with_repair(
            pg, ch, HealMeta([_win(5, "0", "10")]), target_default_db="default",
            repair=None,
        )
        assert r.status == "mismatch"
        assert r.repaired_rows == 0
        assert ch.present == {1}

    def test_stops_early_when_repair_makes_no_progress(self):
        # source 에도 없는 row (rows 비어 fetch=0) → 재복사 0 → attempts 소진 전 중단.
        cfg = _cfg(integrity_repair_attempts=3)
        pg = HealPG([1, 2, 3], {})  # window 키는 1,2,3 이나 fetch 는 아무것도 못 줌
        ch = HealCH(present=[1])
        r = IntegrityChecker(cfg)._run_with_repair(
            pg, ch, HealMeta([_win(5, "0", "10")]), target_default_db="default",
            repair=None,
        )
        assert r.status == "mismatch"
        assert r.repaired_rows == 0
        assert r.repair_attempts_used == 1  # 3 회 허용이지만 진전 없어 1 회에 중단

    def test_repair_arg_overrides_config(self):
        cfg = _cfg(integrity_repair=True)
        pg = HealPG([1, 2, 3], self._rows([1, 2, 3]))
        ch = HealCH(present=[1])
        r = IntegrityChecker(cfg)._run_with_repair(
            pg, ch, HealMeta([_win(5, "0", "10")]), target_default_db="default",
            repair=False,  # CLI --no-repair
        )
        assert r.status == "mismatch"
        assert ch.present == {1}


class TestRun:
    def test_disabled_short_circuits_before_connections(self):
        r = IntegrityChecker(_cfg(integrity_enabled=False)).run()
        assert r.status == "disabled"
