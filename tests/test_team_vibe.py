"""Tests for dbcron/jobs/team_vibe.py — TeamVibeJob."""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from dbcron.jobs.team_vibe import TeamVibeJob, _progress_bar, _generate_member_block


# ── _progress_bar ──────────────────────────────────────────────────


class TestProgressBar:
    def test_zero_percent_all_empty(self):
        """1. _progress_bar(0) returns 20 empty-block characters."""
        result = _progress_bar(0)
        assert result == "░" * 20

    def test_hundred_percent_all_filled(self):
        """2. _progress_bar(100) returns 20 filled-block characters."""
        result = _progress_bar(100)
        assert result == "█" * 20

    def test_fifty_percent_half_and_half(self):
        """3. _progress_bar(50) returns 10 filled + 10 empty."""
        result = _progress_bar(50)
        assert result == "█" * 10 + "░" * 10

    def test_fifty_percent_custom_width(self):
        """4. _progress_bar(50, width=10) returns 5 filled + 5 empty."""
        result = _progress_bar(50, width=10)
        assert result == "█" * 5 + "░" * 5


# ── _generate_member_block ─────────────────────────────────────────


class TestGenerateMemberBlock:
    def test_contains_member_name(self):
        """5. _generate_member_block("Alice") output contains the member name."""
        block = _generate_member_block("Alice")
        assert "Alice" in block


# ── TeamVibeJob.run ────────────────────────────────────────────────


class TestTeamVibeJobRun:
    def test_no_env_uses_defaults_three_members(self):
        """6. run() with no TEAM_MEMBERS env var uses defaults, rows_affected=3."""
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("TEAM_MEMBERS", None)
            job = TeamVibeJob()
            result = job.run()
        assert result.rows_affected == 3

    def test_custom_team_members(self):
        """7. run() with TEAM_MEMBERS="X,Y" returns rows_affected=2."""
        with patch.dict(os.environ, {"TEAM_MEMBERS": "X,Y"}):
            job = TeamVibeJob()
            result = job.run()
        assert result.rows_affected == 2

    def test_always_returns_success(self):
        """8. run() always returns success=True."""
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("TEAM_MEMBERS", None)
            job = TeamVibeJob()
            result = job.run()
        assert result.success is True

    def test_empty_env_uses_defaults(self):
        """9. run() with empty TEAM_MEMBERS="" uses defaults."""
        with patch.dict(os.environ, {"TEAM_MEMBERS": ""}):
            job = TeamVibeJob()
            result = job.run()
        assert result.rows_affected == 3
