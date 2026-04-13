"""Tests for dbcron.main CLI entrypoint.

Each test invokes the CLI via subprocess to test the real argument parsing
and execution flow end-to-end.
"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path

import pytest

PROJECT_ROOT = Path("/home/mireiffe/world/db_manager")
CLI = ["uv", "run", "python", "-m", "dbcron.main"]


def _run(*args: str, env_extra: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    """Run the CLI with the given arguments."""
    env = os.environ.copy()
    if env_extra:
        env.update(env_extra)
    return subprocess.run(
        [*CLI, *args],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
        env=env,
    )


# ── 1. --list-jobs outputs valid JSON with all jobs ─────────────


class TestListJobs:
    def test_list_jobs_valid_json(self):
        """--list-jobs outputs valid JSON containing all registered jobs."""
        result = _run("--list-jobs")
        jobs = json.loads(result.stdout)
        assert isinstance(jobs, list)
        assert len(jobs) > 0
        names = {j["name"] for j in jobs}
        # Verify some known jobs are present
        assert "team_vibe" in names
        assert "connection_test" in names
        assert "pg2ch_sync" in names

    # ── 2. --list-jobs exits 0 ──────────────────────────────────

    def test_list_jobs_exit_0(self):
        """--list-jobs exits with code 0."""
        result = _run("--list-jobs")
        assert result.returncode == 0


# ── 3. No job arg → exit 2 (argparse error) ────────────────────


class TestArgparseErrors:
    def test_no_job_arg_exit_2(self):
        """No job argument causes argparse error (exit 2)."""
        result = _run()
        assert result.returncode == 2
        assert "error" in result.stderr.lower()

    # ── 4. Invalid job name → exit 2 ───────────────────────────

    def test_invalid_job_name_exit_2(self):
        """An invalid job name causes argparse error (exit 2)."""
        result = _run("nonexistent_job_xyz")
        assert result.returncode == 2
        assert "invalid choice" in result.stderr.lower()


# ── 5–7. Dynamic args ──────────────────────────────────────────


class TestDynamicArgs:
    def test_pg2ch_sync_accepts_config(self):
        """pg2ch_sync accepts --config as a dynamic arg from default_args."""
        # Use --list-connections which exercises arg parsing without running the job
        result = _run(
            "pg2ch_sync",
            "--config", "/nonexistent/path.json",
            "--list-connections",
        )
        # Should exit 0 even with nonexistent file (get_connections returns [])
        assert result.returncode == 0

    # ── 6. --days not duplicated ───────────────────────────────

    def test_days_not_duplicated_for_incremental_sync(self):
        """Jobs with 'days' in default_args don't get a duplicate --days.

        incremental_sync has default_args = {"days": 1, "config": "..."},
        so 'days' should be skipped in dynamic arg registration.
        Verify by running with --days (should not conflict).
        """
        result = _run(
            "incremental_sync",
            "--days", "7",
            "--config", "/nonexistent.json",
            "--list-connections",
        )
        assert result.returncode == 0

    # ── 7. Dynamic args passed through to job ──────────────────

    def test_dynamic_args_passed_through(self):
        """Dynamic args are available to the job via --list-connections."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False,
        ) as f:
            json.dump(
                {
                    "source": "src_db",
                    "target": "tgt_db",
                    "tables": [
                        {
                            "source_table": "public.users",
                            "target_table": "default.users",
                            "order_by": ["id"],
                        }
                    ],
                },
                f,
            )
            config_path = f.name

        try:
            result = _run(
                "pg2ch_sync",
                "--config", config_path,
                "--list-connections",
            )
            assert result.returncode == 0
            connections = json.loads(result.stdout)
            assert len(connections) == 1
            assert connections[0]["from"]["db"] == "src_db"
            assert connections[0]["from"]["table"] == "users"
        finally:
            os.unlink(config_path)


# ── 8–9. --list-connections ─────────────────────────────────────


class TestListConnections:
    def test_list_connections_valid_config(self):
        """--list-connections with valid config returns JSON and exit 0."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False,
        ) as f:
            json.dump(
                {
                    "source": "pg_main",
                    "target": "ch_analytics",
                    "tables": [
                        {
                            "source_table": "public.orders",
                            "target_table": "analytics.orders",
                            "order_by": ["id"],
                        }
                    ],
                },
                f,
            )
            config_path = f.name

        try:
            result = _run(
                "pg2ch_sync",
                "--config", config_path,
                "--list-connections",
            )
            assert result.returncode == 0
            data = json.loads(result.stdout)
            assert isinstance(data, list)
            assert len(data) == 1
            assert data[0]["from"]["db"] == "pg_main"
            assert data[0]["to"]["db"] == "ch_analytics"
        finally:
            os.unlink(config_path)

    def test_list_connections_missing_config(self):
        """--list-connections with missing config returns empty array and exit 0."""
        result = _run(
            "pg2ch_sync",
            "--config", "/definitely/does/not/exist.json",
            "--list-connections",
        )
        assert result.returncode == 0
        data = json.loads(result.stdout)
        assert data == []


# ── 10–11. --targets ───────────────────────────────────────────


class TestTargets:
    def test_targets_valid_json(self):
        """--targets with valid JSON is parsed and passed to the job.

        We use connection_test with a nonexistent target DB so it filters
        to an empty list and fails — proving targets was parsed and applied.
        """
        result = _run(
            "connection_test",
            "--targets", '[{"db": "nonexistent_db_for_test"}]',
        )
        # connection_test returns FAILED when no DBs match
        assert result.returncode == 1
        assert "FAILED:" in result.stderr

    def test_targets_invalid_json(self):
        """--targets with invalid JSON causes argparse error (exit 2)."""
        result = _run(
            "team_vibe",
            "--targets", "not-valid-json{{{",
        )
        assert result.returncode == 2
        assert "must be valid json" in result.stderr.lower()


# ── 12–13. One-shot execution ──────────────────────────────────


class TestOneShot:
    def test_one_shot_success(self):
        """One-shot execution with team_vibe succeeds (exit 0, stdout has OK:)."""
        result = _run("team_vibe")
        assert result.returncode == 0
        assert "OK:" in result.stdout

    def test_one_shot_failure(self):
        """One-shot execution that fails exits 1 with FAILED: on stderr.

        connection_test targeting a nonexistent DB returns success=False.
        """
        result = _run(
            "connection_test",
            "--targets", '[{"db": "nonexistent_db_for_test"}]',
        )
        assert result.returncode == 1
        assert "FAILED:" in result.stderr


# ── 14. Config load failure is graceful ────────────────────────


class TestConfigGraceful:
    def test_config_load_failure_graceful(self):
        """When .env is missing / env vars unset, jobs that don't need
        infra config still run without crashing.

        team_vibe doesn't need S3 config so it should succeed even if
        load_config() raises EnvironmentError (caught as config=None).
        """
        # Strip all S3-related env vars to force load_config to rely on defaults
        env_strip = {
            k: v for k, v in os.environ.items()
            if not k.startswith("S3_")
        }
        result = subprocess.run(
            [*CLI, "team_vibe"],
            capture_output=True,
            text=True,
            cwd=str(PROJECT_ROOT),
            env=env_strip,
        )
        assert result.returncode == 0
        assert "OK:" in result.stdout
