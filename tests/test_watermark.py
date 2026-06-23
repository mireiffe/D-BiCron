"""Tests for pg2ch.watermark."""

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


class TestResolveSyncSince:
    def test_days(self):
        now = datetime(2026, 6, 23, 0, 0, 0)
        assert w.resolve_sync_since("30d", now=now) == (now - timedelta(days=30)).isoformat()

    def test_absolute_passthrough(self):
        ts = "2025-01-01T00:00:00"
        assert w.resolve_sync_since(ts) == ts

    def test_whitespace(self):
        now = datetime(2026, 6, 23)
        assert w.resolve_sync_since("  7d  ", now=now) == (now - timedelta(days=7)).isoformat()


class TestWatermarkOverlap:
    def test_parse_zero_as_disabled(self):
        assert w.parse_watermark_overlap(0) is None
        assert w.parse_watermark_overlap("0") is None
        assert w.parse_watermark_overlap(None) is None

    def test_parse_positive(self):
        assert w.parse_watermark_overlap("1000") == Decimal("1000")

    def test_rejects_negative(self):
        with pytest.raises(ValueError, match="non-negative number"):
            w.parse_watermark_overlap(-1)

    def test_rejects_non_numeric(self):
        with pytest.raises(ValueError, match="non-negative number"):
            w.parse_watermark_overlap("3d")

    def test_rejects_bool(self):
        with pytest.raises(ValueError):
            w.parse_watermark_overlap(True)

    def test_apply_integer(self):
        assert w.apply_watermark_overlap("public.events", "1000", 50) == 950

    def test_apply_decimal(self):
        assert w.apply_watermark_overlap("public.events", "1000.5", "0.5") == 1000

    def test_apply_requires_numeric_watermark(self):
        with pytest.raises(ValueError, match="requires a numeric watermark value"):
            w.apply_watermark_overlap("public.events", "2025-01-01T00:00:00", 100)


class TestApplyOverlap:
    def test_timestamp_overlap_minutes(self):
        assert (
            w.apply_overlap("t", "2025-06-01T00:00:00", overlap_minutes=30)
            == "2025-05-31T23:30:00"
        )

    def test_numeric_watermark_overlap(self):
        assert w.apply_overlap("t", "1000", watermark_overlap=20) == 980

    def test_no_overlap_returns_watermark(self):
        assert w.apply_overlap("t", "1000") == "1000"

    def test_overlap_minutes_ignored_for_numeric_falls_back(self):
        # overlap_minutes 가 설정돼도 숫자 watermark 는 watermark_overlap 으로 처리
        assert w.apply_overlap("t", "1000", overlap_minutes=30, watermark_overlap=20) == 980
