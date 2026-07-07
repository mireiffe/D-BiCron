"""Tests for pg2ch.watermark (타입 인지 파싱/변환 유틸)."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

import pytest

from pg2ch import watermark as w


class TestParseRelative:
    def test_days(self):
        assert w.parse_relative_to_timedelta("30d") == timedelta(days=30)

    def test_hours(self):
        assert w.parse_relative_to_timedelta("12h") == timedelta(hours=12)

    def test_minutes(self):
        assert w.parse_relative_to_timedelta("90m") == timedelta(minutes=90)

    def test_absolute_returns_none(self):
        assert w.parse_relative_to_timedelta("2025-01-01T00:00:00") is None


class TestParseValue:
    def test_serial(self):
        assert w.parse_value("serial", "100") == 100
        assert isinstance(w.parse_value("serial", "100"), int)

    def test_serial_rejects_fraction(self):
        with pytest.raises(ValueError, match="not an integer"):
            w.parse_value("serial", "100.5")

    def test_serial_rejects_timestamp(self):
        with pytest.raises(ValueError, match="not a number"):
            w.parse_value("serial", "2025-01-01T00:00:00")

    def test_numeric(self):
        assert w.parse_value("numeric", "100.5") == Decimal("100.5")

    def test_timestamp(self):
        assert w.parse_value("timestamp", "2025-01-01T00:00:00") == datetime(2025, 1, 1)

    def test_timestamp_z_suffix(self):
        assert w.parse_value("timestamp", "2025-01-01T09:00:00Z") == datetime(2025, 1, 1, 9)

    def test_timestamp_tz_normalized_to_utc_naive(self):
        assert w.parse_value("timestamp", "2025-01-01T09:00:00+09:00") == datetime(2025, 1, 1)

    def test_timestamp_datetime_passthrough(self):
        assert w.parse_value("timestamp", datetime(2025, 1, 1)) == datetime(2025, 1, 1)

    def test_timestamp_rejects_number(self):
        with pytest.raises(ValueError, match="not an ISO timestamp"):
            w.parse_value("timestamp", "100")

    def test_unknown_type(self):
        with pytest.raises(ValueError, match="unknown watermark type"):
            w.parse_value("uuid", "x")

    def test_none_rejected(self):
        with pytest.raises(ValueError, match="required"):
            w.parse_value("serial", None)


class TestResolveSince:
    def test_timestamp_relative(self):
        now = datetime(2026, 6, 23)
        assert w.resolve_since("timestamp", "30d", now=now) == now - timedelta(days=30)

    def test_timestamp_absolute(self):
        assert w.resolve_since("timestamp", "2025-01-01T00:00:00") == datetime(2025, 1, 1)

    def test_timestamp_whitespace(self):
        now = datetime(2026, 6, 23)
        assert w.resolve_since("timestamp", "  7d  ", now=now) == now - timedelta(days=7)

    def test_serial_absolute(self):
        assert w.resolve_since("serial", 100000) == 100000

    def test_serial_rejects_relative(self):
        with pytest.raises(ValueError, match="not a number"):
            w.resolve_since("serial", "30d")


class TestParseOverlap:
    def test_zero_and_none_disabled(self):
        assert w.parse_overlap("serial", 0) is None
        assert w.parse_overlap("serial", "0") is None
        assert w.parse_overlap("serial", None) is None
        assert w.parse_overlap("timestamp", 0) is None
        assert w.parse_overlap("timestamp", "") is None

    def test_serial(self):
        assert w.parse_overlap("serial", "1000") == 1000

    def test_numeric(self):
        assert w.parse_overlap("numeric", "0.5") == Decimal("0.5")

    def test_timestamp(self):
        assert w.parse_overlap("timestamp", "30m") == timedelta(minutes=30)

    def test_serial_rejects_negative(self):
        with pytest.raises(ValueError, match="non-negative"):
            w.parse_overlap("serial", -1)

    def test_serial_rejects_duration(self):
        with pytest.raises(ValueError, match="not a number"):
            w.parse_overlap("serial", "3d")

    def test_timestamp_rejects_bare_number(self):
        with pytest.raises(ValueError, match="relative like '30m'"):
            w.parse_overlap("timestamp", 30)

    def test_rejects_bool(self):
        with pytest.raises(ValueError):
            w.parse_overlap("serial", True)


class TestApplyOverlap:
    def test_timestamp(self):
        assert w.apply_overlap(
            "timestamp", datetime(2025, 6, 1), "30m"
        ) == datetime(2025, 5, 31, 23, 30)

    def test_serial(self):
        assert w.apply_overlap("serial", 1000, 20) == 980

    def test_disabled_returns_value(self):
        assert w.apply_overlap("serial", 1000, 0) == 1000
        assert w.apply_overlap("timestamp", datetime(2025, 6, 1), None) == datetime(2025, 6, 1)


class TestResolveRetentionCutoff:
    def test_timestamp_relative(self):
        now = datetime(2026, 7, 1)
        cutoff = w.resolve_retention_cutoff(
            "timestamp", "180d", last_synced=datetime(2026, 6, 1), now=now
        )
        assert cutoff == now - timedelta(days=180)

    def test_timestamp_absolute(self):
        cutoff = w.resolve_retention_cutoff(
            "timestamp", "2026-01-01T00:00:00", last_synced=datetime(2026, 6, 1)
        )
        assert cutoff == datetime(2026, 1, 1)

    def test_serial_keep_last_n(self):
        # serial 은 마지막 synced 값 기준 keep-last-N
        assert w.resolve_retention_cutoff("serial", 100000, last_synced=250000) == 150000

    def test_serial_zero_deletes_all_synced(self):
        assert w.resolve_retention_cutoff("serial", 0, last_synced=250000) == 250000

    def test_serial_rejects_negative(self):
        with pytest.raises(ValueError, match="non-negative"):
            w.resolve_retention_cutoff("serial", -5, last_synced=100)

    def test_serial_rejects_duration(self):
        with pytest.raises(ValueError, match="not a number"):
            w.resolve_retention_cutoff("serial", "180d", last_synced=100)


class TestValidateRetentionExpr:
    def test_typed_timestamp(self):
        w.validate_retention_expr("timestamp", "180d")
        w.validate_retention_expr("timestamp", "2026-01-01T00:00:00")
        with pytest.raises(ValueError):
            w.validate_retention_expr("timestamp", 100000)

    def test_typed_serial(self):
        w.validate_retention_expr("serial", 100000)
        with pytest.raises(ValueError):
            w.validate_retention_expr("serial", "180d")

    def test_untyped_accepts_either_format(self):
        # 유효 타입 미확정(로드 시점) → 시간 표현/숫자 중 한쪽이면 통과
        w.validate_retention_expr(None, "180d")
        w.validate_retention_expr(None, 100000)
        with pytest.raises(ValueError, match="relative like '180d'"):
            w.validate_retention_expr(None, "soon")
