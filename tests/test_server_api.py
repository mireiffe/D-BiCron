"""Integration tests for the WebUI Express server REST API.

Starts ``node server.js`` as a subprocess, exercises every endpoint via httpx,
and tears the server down at the end of the session.

Run with:
    uv run pytest tests/test_server_api.py -v
Skip with:
    uv run pytest -m "not api"
"""

from __future__ import annotations

import json
import os
import random
import shutil
import signal
import socket
import subprocess
import tempfile
import time
from pathlib import Path

import httpx
import pytest

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent          # /home/.../db_manager
WEBUI_DIR = PROJECT_ROOT / "dbcron" / "webui"
SERVER_JS = WEBUI_DIR / "server.js"
SAMPLE_DBS_DIR = PROJECT_ROOT / "data" / "sample_dbs"

# A valid job name that exists in the registry and is cheap to run
KNOWN_JOB = "connection_test"

pytestmark = pytest.mark.api


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _free_port() -> int:
    """Find an unused TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_server(url: str, timeout: float = 15.0) -> None:
    """Poll the server until it responds or *timeout* seconds elapse."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            r = httpx.get(f"{url}/api/jobs", timeout=2.0)
            if r.status_code == 200:
                return
        except httpx.ConnectError:
            pass
        time.sleep(0.3)
    raise RuntimeError(f"Server at {url} did not become ready within {timeout}s")


# ---------------------------------------------------------------------------
# Session-scoped fixture: start / stop the Express server
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def server_url():
    """Start the Node.js server and yield its base URL.

    * Creates a temporary ``data/`` directory with seed files.
    * Passes a random high port via ``PORT`` env var.
    * Kills the server on teardown.
    """
    port = _free_port()
    tmp_dir = tempfile.mkdtemp(prefix="dbcron_test_")
    data_dir = os.path.join(tmp_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    # --- seed data files ---

    # databases.json — use the real sample SQLite DBs
    sample_dbs = [
        {
            "id": "users_db",
            "type": "sqlite",
            "label": "Users DB",
            "color": "#00e5ff",
            "host": str(SAMPLE_DBS_DIR / "users.db"),
            "port": 0,
            "dbname": "users",
            "user": "",
            "password": "",
        },
        {
            "id": "shop_db",
            "type": "sqlite",
            "label": "Shop DB",
            "color": "#c8ff00",
            "host": str(SAMPLE_DBS_DIR / "shop.db"),
            "port": 0,
            "dbname": "shop",
            "user": "",
            "password": "",
        },
    ]
    with open(os.path.join(data_dir, "databases.json"), "w") as f:
        json.dump(sample_dbs, f)

    # schedules.json — empty
    with open(os.path.join(data_dir, "schedules.json"), "w") as f:
        json.dump({"nextId": 1, "schedules": []}, f)

    # history.json — empty
    with open(os.path.join(data_dir, "history.json"), "w") as f:
        json.dump([], f)

    # running.json — empty
    with open(os.path.join(data_dir, "running.json"), "w") as f:
        json.dump({"nextRunId": 1, "jobs": []}, f)

    # annotations.json — empty
    with open(os.path.join(data_dir, "annotations.json"), "w") as f:
        json.dump({}, f)

    # metadata_snapshot.json — minimal snapshot so /api/metadata returns 200
    snapshot = {
        "snapshot_at": "2026-04-13T00:00:00",
        "databases": {
            "users_db": {
                "tables": {
                    "main.users": {
                        "schema": "main",
                        "table": "users",
                        "row_count": 10,
                        "columns": [
                            {"name": "id", "type": "INTEGER", "nullable": False},
                            {"name": "name", "type": "TEXT", "nullable": True},
                        ],
                        "primary_key": ["id"],
                        "foreign_keys": [],
                        "referenced_by": [],
                    }
                }
            }
        },
    }
    with open(os.path.join(data_dir, "metadata_snapshot.json"), "w") as f:
        json.dump(snapshot, f)

    # Symlink node_modules into the project root so the server can resolve
    # express / node-cron from its require() calls.  The actual node_modules
    # live at PROJECT_ROOT/node_modules which the server already resolves
    # because __dirname is dbcron/webui and Node walks up.

    env = os.environ.copy()
    env["PORT"] = str(port)
    env["NODE_PATH"] = str(PROJECT_ROOT / "node_modules")

    # Use a wrapper cwd = PROJECT_ROOT so that DATA_DIR resolves to our temp
    # data.  The server computes DATA_DIR = PROJECT_ROOT + "/data", so we
    # set PROJECT_ROOT-equivalent by creating a symlink structure.
    #
    # Actually: the server resolves PROJECT_ROOT as path.resolve(__dirname, "../..").
    # __dirname is the *real* webui dir, so PROJECT_ROOT always points to
    # the real project root.  To redirect data/ we symlink tmp_dir/data to
    # the temp data directory — but that's already where it is.
    #
    # The cleanest approach: symlink the real project structure into tmp_dir,
    # then override the data/ dir.
    #
    # Simpler approach: we symlink everything *except* data/ from the real
    # project into tmp_dir and start the server from there.

    # Build a thin project mirror with our temp data/
    mirror_root = os.path.join(tmp_dir, "project")
    os.makedirs(mirror_root, exist_ok=True)

    # Symlink all top-level entries except data/
    for entry in os.listdir(str(PROJECT_ROOT)):
        if entry == "data":
            continue
        src = os.path.join(str(PROJECT_ROOT), entry)
        dst = os.path.join(mirror_root, entry)
        if not os.path.exists(dst):
            os.symlink(src, dst)

    # Symlink our temp data dir as mirror_root/data
    os.symlink(data_dir, os.path.join(mirror_root, "data"))

    # The server resolves PROJECT_ROOT = path.resolve(__dirname, "../..").
    # __dirname = mirror_root/dbcron/webui (via symlink to real webui dir).
    # So PROJECT_ROOT = mirror_root.  That means DATA_DIR = mirror_root/data
    # which is our temp data dir.  Perfect.

    # But __dirname follows symlinks, so it would still be the real dir.
    # To avoid that, we need to physically create the dbcron/webui path
    # and copy/symlink server.js into it.
    webui_mirror = os.path.join(mirror_root, "dbcron", "webui")
    # Remove the symlink to dbcron/ first
    dbcron_link = os.path.join(mirror_root, "dbcron")
    if os.path.islink(dbcron_link):
        os.unlink(dbcron_link)
    os.makedirs(webui_mirror, exist_ok=True)

    # Symlink dbcron contents except webui/
    real_dbcron = str(PROJECT_ROOT / "dbcron")
    for entry in os.listdir(real_dbcron):
        if entry == "webui":
            continue
        src = os.path.join(real_dbcron, entry)
        dst = os.path.join(mirror_root, "dbcron", entry)
        if not os.path.exists(dst):
            os.symlink(src, dst)

    # Symlink webui contents except server.js (copy server.js so __dirname is correct)
    real_webui = str(WEBUI_DIR)
    for entry in os.listdir(real_webui):
        src = os.path.join(real_webui, entry)
        dst = os.path.join(webui_mirror, entry)
        if not os.path.exists(dst):
            os.symlink(src, dst)

    # The server.js symlink is fine — Node resolves __dirname to the
    # *symlink location*, not the target, when the file is accessed via symlink.
    # Actually, Node resolves __dirname to the *real path*. So we need to
    # copy server.js instead.
    server_link = os.path.join(webui_mirror, "server.js")
    if os.path.islink(server_link):
        os.unlink(server_link)
    shutil.copy2(str(SERVER_JS), server_link)

    server_js_path = os.path.join(webui_mirror, "server.js")

    proc = subprocess.Popen(
        ["node", server_js_path],
        env=env,
        cwd=mirror_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        preexec_fn=os.setsid,
    )

    base_url = f"http://127.0.0.1:{port}"
    try:
        _wait_for_server(base_url)
    except RuntimeError:
        # Dump server output for debugging
        proc.kill()
        out = proc.stdout.read().decode(errors="replace") if proc.stdout else ""
        raise RuntimeError(
            f"Server failed to start on port {port}.\n--- server output ---\n{out}"
        )

    yield base_url

    # Teardown: kill the entire process group
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except (ProcessLookupError, OSError):
        pass
    proc.wait(timeout=5)
    shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Convenience client fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def client(server_url: str) -> httpx.Client:
    """A shared httpx.Client bound to the test server."""
    with httpx.Client(base_url=server_url, timeout=10.0) as c:
        yield c


# ===================================================================
# JOBS
# ===================================================================

class TestJobs:
    """Tests for /api/jobs endpoints."""

    def test_list_jobs(self, client: httpx.Client):
        """GET /api/jobs returns a JSON array with required fields."""
        r = client.get("/api/jobs")
        assert r.status_code == 200
        jobs = r.json()
        assert isinstance(jobs, list)
        assert len(jobs) > 0
        for job in jobs:
            assert "name" in job
            assert "label" in job
            assert "description" in job
            assert "defaultArgs" in job
            assert "scope" in job

    def test_jobs_stats(self, client: httpx.Client):
        """GET /api/jobs/stats returns 200."""
        r = client.get("/api/jobs/stats")
        assert r.status_code == 200
        assert isinstance(r.json(), dict)

    def test_run_job_valid(self, client: httpx.Client):
        """POST /api/jobs/{name}/run with a known job returns 200 + runId."""
        r = client.post(f"/api/jobs/{KNOWN_JOB}/run", json={})
        assert r.status_code == 200
        body = r.json()
        assert "runId" in body
        assert isinstance(body["runId"], int)
        # Wait briefly for the job to finish so subsequent tests are not blocked
        time.sleep(2)

    def test_run_job_not_found(self, client: httpx.Client):
        """POST /api/jobs/nonexistent/run returns 404."""
        r = client.post("/api/jobs/nonexistent_job_xyz/run", json={})
        assert r.status_code == 404
        assert "error" in r.json()

    def test_run_job_already_running(self, client: httpx.Client):
        """POST when job already running with same args returns 409."""
        # Start a job that takes a while — use metadata_snapshot which
        # connects to DBs.  We fire it twice rapidly.
        job = "metadata_snapshot"
        r1 = client.post(f"/api/jobs/{job}/run", json={})
        # It may succeed or fail depending on timing; what matters is the
        # second call while the first is still running.
        if r1.status_code == 200:
            r2 = client.post(f"/api/jobs/{job}/run", json={})
            assert r2.status_code == 409
            assert "error" in r2.json()
            # Wait for the running job to finish
            time.sleep(3)
        else:
            # Job was already running from a previous test; just verify the
            # conflict response.
            assert r1.status_code == 409

    def test_run_same_job_different_args_concurrent(self, client: httpx.Client):
        """POST same job with different args should NOT conflict (200+200)."""
        job = "metadata_snapshot"
        # Fire with two different arg sets — they should run concurrently.
        r1 = client.post(f"/api/jobs/{job}/run", json={"tag": "a"})
        r2 = client.post(f"/api/jobs/{job}/run", json={"tag": "b"})
        # Both should be accepted (not 409) because args differ.
        assert r1.status_code == 200, f"First run failed: {r1.text}"
        assert r2.status_code == 200, f"Second run (different args) should not conflict: {r2.text}"
        # Wait for both to finish
        time.sleep(3)


# ===================================================================
# SCHEDULES
# ===================================================================

class TestSchedules:
    """Tests for /api/schedules CRUD."""

    def test_list_schedules_empty(self, client: httpx.Client):
        """GET /api/schedules returns 200 with an array."""
        r = client.get("/api/schedules")
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    def test_create_schedule_valid(self, client: httpx.Client):
        """POST /api/schedules with valid data returns 201."""
        payload = {
            "jobName": KNOWN_JOB,
            "cronExpr": "0 3 * * *",
            "args": {},
        }
        r = client.post("/api/schedules", json=payload)
        assert r.status_code == 201
        body = r.json()
        assert "id" in body
        assert body["jobName"] == KNOWN_JOB
        # Cleanup
        client.delete(f"/api/schedules/{body['id']}")

    def test_create_schedule_unknown_job(self, client: httpx.Client):
        """POST /api/schedules with unknown job returns 400."""
        payload = {
            "jobName": "totally_fake_job",
            "cronExpr": "0 3 * * *",
            "args": {},
        }
        r = client.post("/api/schedules", json=payload)
        assert r.status_code == 400
        assert "error" in r.json()

    def test_create_schedule_invalid_cron(self, client: httpx.Client):
        """POST /api/schedules with invalid cron returns 400."""
        payload = {
            "jobName": KNOWN_JOB,
            "cronExpr": "not a cron",
            "args": {},
        }
        r = client.post("/api/schedules", json=payload)
        assert r.status_code == 400
        assert "error" in r.json()

    def test_create_schedule_with_targets(self, client: httpx.Client):
        """POST /api/schedules with targets persists them correctly."""
        targets = [{"db": "users_db", "schema": "main", "table": "users"}]
        payload = {
            "jobName": KNOWN_JOB,
            "cronExpr": "30 2 * * *",
            "args": {},
            "targets": targets,
        }
        r = client.post("/api/schedules", json=payload)
        assert r.status_code == 201
        body = r.json()
        assert body["targets"] is not None
        assert len(body["targets"]) == 1
        assert body["targets"][0]["db"] == "users_db"

        # Verify via GET
        r2 = client.get("/api/schedules")
        found = [s for s in r2.json() if s["id"] == body["id"]]
        assert len(found) == 1
        assert found[0]["targets"] is not None
        assert found[0]["targets"][0]["db"] == "users_db"

        # Cleanup
        client.delete(f"/api/schedules/{body['id']}")

    def test_create_schedule_with_depends_on(self, client: httpx.Client):
        """POST /api/schedules with dependsOn creates without cron validation."""
        payload = {
            "jobName": KNOWN_JOB,
            "dependsOn": "metadata_snapshot",
            "args": {},
        }
        r = client.post("/api/schedules", json=payload)
        assert r.status_code == 201
        body = r.json()
        assert body.get("dependsOn") == "metadata_snapshot"
        # Cleanup
        client.delete(f"/api/schedules/{body['id']}")

    def test_update_schedule(self, client: httpx.Client):
        """PUT /api/schedules/:id updates the schedule."""
        # Create first
        payload = {
            "jobName": KNOWN_JOB,
            "cronExpr": "0 1 * * *",
            "args": {},
        }
        r = client.post("/api/schedules", json=payload)
        assert r.status_code == 201
        sched_id = r.json()["id"]

        # Update
        r2 = client.put(f"/api/schedules/{sched_id}", json={"cronExpr": "0 5 * * *"})
        assert r2.status_code == 200
        assert r2.json()["cron"] == "0 5 * * *"

        # Cleanup
        client.delete(f"/api/schedules/{sched_id}")

    def test_update_schedule_not_found(self, client: httpx.Client):
        """PUT /api/schedules/999 returns 404."""
        r = client.put("/api/schedules/999", json={"cronExpr": "0 5 * * *"})
        assert r.status_code == 404

    def test_delete_schedule(self, client: httpx.Client):
        """DELETE /api/schedules/:id deletes the schedule."""
        # Create
        payload = {
            "jobName": KNOWN_JOB,
            "cronExpr": "0 2 * * *",
            "args": {},
        }
        r = client.post("/api/schedules", json=payload)
        sched_id = r.json()["id"]

        # Delete
        r2 = client.delete(f"/api/schedules/{sched_id}")
        assert r2.status_code == 200
        assert r2.json()["deleted"] == sched_id

        # Confirm gone
        schedules = client.get("/api/schedules").json()
        assert all(s["id"] != sched_id for s in schedules)

    def test_delete_schedule_not_found(self, client: httpx.Client):
        """DELETE /api/schedules/999 returns 404."""
        r = client.delete("/api/schedules/999")
        assert r.status_code == 404

    def test_pause_schedule(self, client: httpx.Client):
        """PUT /api/schedules/:id/pause sets paused=true."""
        # Create
        payload = {
            "jobName": KNOWN_JOB,
            "cronExpr": "0 4 * * *",
            "args": {},
        }
        r = client.post("/api/schedules", json=payload)
        sched_id = r.json()["id"]

        r2 = client.put(f"/api/schedules/{sched_id}/pause")
        assert r2.status_code == 200
        assert r2.json()["paused"] is True

        # Verify via list
        schedules = client.get("/api/schedules").json()
        found = [s for s in schedules if s["id"] == sched_id]
        assert found[0]["paused"] is True

        # Cleanup
        client.delete(f"/api/schedules/{sched_id}")

    def test_resume_schedule(self, client: httpx.Client):
        """PUT /api/schedules/:id/resume sets paused=false."""
        # Create and pause
        payload = {
            "jobName": KNOWN_JOB,
            "cronExpr": "0 4 * * *",
            "args": {},
        }
        r = client.post("/api/schedules", json=payload)
        sched_id = r.json()["id"]
        client.put(f"/api/schedules/{sched_id}/pause")

        # Resume
        r2 = client.put(f"/api/schedules/{sched_id}/resume")
        assert r2.status_code == 200
        assert r2.json()["paused"] is False

        # Cleanup
        client.delete(f"/api/schedules/{sched_id}")


# ===================================================================
# RUNNING
# ===================================================================

class TestRunning:
    """Tests for /api/running endpoints."""

    def test_list_running(self, client: httpx.Client):
        """GET /api/running returns 200 with an array."""
        r = client.get("/api/running")
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    def test_get_running_by_id(self, client: httpx.Client):
        """GET /api/running/:id returns details when job is active."""
        # Fire a job and immediately check running state
        r = client.post(f"/api/jobs/{KNOWN_JOB}/run", json={})
        if r.status_code == 200:
            run_id = r.json()["runId"]
            r2 = client.get(f"/api/running/{run_id}")
            # It might have already finished, so accept 200 or 404
            assert r2.status_code in (200, 404)
            if r2.status_code == 200:
                body = r2.json()
                assert body["runId"] == run_id
                assert body["jobName"] == KNOWN_JOB
            # Wait for completion
            time.sleep(2)

    def test_get_running_not_found(self, client: httpx.Client):
        """GET /api/running/999 returns 404."""
        r = client.get("/api/running/999")
        assert r.status_code == 404
        assert "error" in r.json()


# ===================================================================
# DATABASES
# ===================================================================

class TestDatabases:
    """Tests for /api/databases CRUD."""

    def test_list_databases(self, client: httpx.Client):
        """GET /api/databases returns 200 with seeded entries."""
        r = client.get("/api/databases")
        assert r.status_code == 200
        dbs = r.json()
        assert isinstance(dbs, list)
        assert len(dbs) >= 2
        ids = [d["id"] for d in dbs]
        assert "users_db" in ids

    def test_create_database(self, client: httpx.Client):
        """POST /api/databases with valid data returns 201."""
        payload = {
            "id": "test_new_db",
            "type": "postgresql",
            "label": "Test New",
            "host": "localhost",
            "port": 5432,
            "dbname": "testdb",
            "user": "admin",
            "password": "secret",
        }
        r = client.post("/api/databases", json=payload)
        assert r.status_code == 201
        assert r.json()["id"] == "test_new_db"

        # Cleanup
        client.delete("/api/databases/test_new_db")

    def test_create_database_missing_fields(self, client: httpx.Client):
        """POST /api/databases with missing required fields returns 400."""
        payload = {"label": "Incomplete"}
        r = client.post("/api/databases", json=payload)
        assert r.status_code == 400
        assert "error" in r.json()

    def test_create_database_duplicate(self, client: httpx.Client):
        """POST /api/databases with existing id returns 409."""
        r = client.post(
            "/api/databases",
            json={"id": "users_db", "host": "x", "dbname": "x"},
        )
        assert r.status_code == 409
        assert "error" in r.json()

    def test_update_database(self, client: httpx.Client):
        """PUT /api/databases/:id updates the record."""
        # Create
        client.post(
            "/api/databases",
            json={"id": "upd_db", "host": "h1", "dbname": "d1"},
        )

        r = client.put(
            "/api/databases/upd_db",
            json={"label": "Updated Label", "host": "h2"},
        )
        assert r.status_code == 200
        assert r.json()["id"] == "upd_db"

        # Verify
        dbs = client.get("/api/databases").json()
        upd = [d for d in dbs if d["id"] == "upd_db"][0]
        assert upd["label"] == "Updated Label"
        assert upd["host"] == "h2"

        # Cleanup
        client.delete("/api/databases/upd_db")

    def test_update_database_not_found(self, client: httpx.Client):
        """PUT /api/databases/nonexistent returns 404."""
        r = client.put(
            "/api/databases/nonexistent_db_xyz",
            json={"label": "X"},
        )
        assert r.status_code == 404

    def test_delete_database(self, client: httpx.Client):
        """DELETE /api/databases/:id removes the entry."""
        # Create
        client.post(
            "/api/databases",
            json={"id": "del_db", "host": "h", "dbname": "d"},
        )

        r = client.delete("/api/databases/del_db")
        assert r.status_code == 200
        assert r.json()["deleted"] == "del_db"

        # Confirm gone
        dbs = client.get("/api/databases").json()
        assert all(d["id"] != "del_db" for d in dbs)

    def test_delete_database_not_found(self, client: httpx.Client):
        """DELETE /api/databases/nonexistent returns 404."""
        r = client.delete("/api/databases/nonexistent_db_xyz")
        assert r.status_code == 404


# ===================================================================
# PIPELINE & CANVAS
# ===================================================================

class TestPipelineCanvas:
    """Tests for pipeline config, schedule status, lineage, metadata, history, events."""

    def test_pipeline_config(self, client: httpx.Client):
        """GET /api/pipeline-config returns 200 with pipelines and entry_points."""
        r = client.get("/api/pipeline-config")
        assert r.status_code == 200
        body = r.json()
        assert "pipelines" in body
        assert "entry_points" in body

    def test_schedule_status(self, client: httpx.Client):
        """GET /api/schedule-status returns 200 with an array."""
        r = client.get("/api/schedule-status")
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    def test_lineage(self, client: httpx.Client):
        """GET /api/lineage/:db/:table returns upstream/downstream."""
        r = client.get("/api/lineage/users_db/main.users")
        assert r.status_code == 200
        body = r.json()
        assert "upstream" in body
        assert "downstream" in body
        assert isinstance(body["upstream"], list)
        assert isinstance(body["downstream"], list)

    def test_metadata_snapshot(self, client: httpx.Client):
        """GET /api/metadata returns the seeded snapshot."""
        r = client.get("/api/metadata")
        assert r.status_code == 200
        body = r.json()
        assert "snapshot_at" in body
        assert "databases" in body

    def test_history(self, client: httpx.Client):
        """GET /api/history returns 200 with an array."""
        r = client.get("/api/history")
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    def test_events_sse(self, client: httpx.Client):
        """GET /api/events returns SSE stream with correct content-type."""
        # SSE is a streaming endpoint; we just check the initial response
        with httpx.stream("GET", f"{client._base_url}/api/events", timeout=3.0) as r:
            assert r.status_code == 200
            content_type = r.headers.get("content-type", "")
            assert "text/event-stream" in content_type

    def test_timeline(self, client: httpx.Client):
        """GET /api/timeline returns 200 with an array of events."""
        r = client.get("/api/timeline")
        assert r.status_code == 200
        body = r.json()
        assert isinstance(body, list)

    def test_freshness(self, client: httpx.Client):
        """GET /api/freshness returns 200 (empty when no log exists)."""
        r = client.get("/api/freshness")
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    def test_metadata_drift_no_prev(self, client: httpx.Client):
        """GET /api/metadata/drift returns drift list (empty when no previous snapshot)."""
        r = client.get("/api/metadata/drift")
        assert r.status_code == 200
        body = r.json()
        assert "drift" in body
        assert isinstance(body["drift"], list)


# ===================================================================
# ANNOTATIONS
# ===================================================================

class TestAnnotations:
    """Tests for /api/annotations endpoints."""

    def test_put_annotation(self, client: httpx.Client):
        """PUT /api/annotations/:key saves the annotation."""
        payload = {"owner": "alice", "notes": "Main user table"}
        r = client.put("/api/annotations/users_db.main.users", json=payload)
        assert r.status_code == 200
        body = r.json()
        assert body["owner"] == "alice"
        assert body["notes"] == "Main user table"
        assert "updatedAt" in body

    def test_get_all_annotations(self, client: httpx.Client):
        """GET /api/annotations returns all saved annotations."""
        # Ensure at least one exists
        client.put(
            "/api/annotations/test_key",
            json={"owner": "bob"},
        )
        r = client.get("/api/annotations")
        assert r.status_code == 200
        body = r.json()
        assert isinstance(body, dict)
        assert "test_key" in body

    def test_get_annotation_by_key(self, client: httpx.Client):
        """GET /api/annotations/:key returns the specific annotation."""
        # Ensure it exists
        client.put(
            "/api/annotations/specific_key",
            json={"owner": "carol", "runbook": "https://example.com"},
        )
        r = client.get("/api/annotations/specific_key")
        assert r.status_code == 200
        body = r.json()
        assert body["owner"] == "carol"
        assert body["runbook"] == "https://example.com"
