const express = require("express");
const cron = require("node-cron");
const fs = require("fs");
const { spawn, execFileSync } = require("child_process");
const path = require("path");

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const PORT = process.env.PORT || 3999;
const PROJECT_ROOT = path.resolve(__dirname, "../..");
const WEBHOOK_URL = process.env.WEBHOOK_URL || "";
const JOB_TIMEOUT_MS = Number(process.env.JOB_TIMEOUT_MS) || 30 * 60_000; // 30min default
const JOB_MAX_RETRIES = Number(process.env.JOB_MAX_RETRIES) || 0; // 0 = no retry

// -------------------------------------------------------------------
// Available jobs — add new entries when you create a new Job subclass
// -------------------------------------------------------------------
let AVAILABLE_JOBS = [];
try {
  const out = execFileSync(
    path.join(PROJECT_ROOT, ".venv", "bin", "python"),
    ["-m", "dbcron.main", "--list-jobs"],
    { cwd: PROJECT_ROOT, timeout: 10000 },
  );
  AVAILABLE_JOBS = JSON.parse(out);
  console.log(`Loaded ${AVAILABLE_JOBS.length} job(s) from registry`);
} catch (err) {
  console.error("Failed to load jobs from registry:", err.message);
}

// -------------------------------------------------------------------
// Schedule persistence (filesystem-based)
// -------------------------------------------------------------------
const DATA_DIR = path.join(PROJECT_ROOT, "data");
const SCHEDULES_FILE = path.join(DATA_DIR, "schedules.json");
const RUNNING_FILE = path.join(DATA_DIR, "running.json");
const HISTORY_FILE = path.join(DATA_DIR, "history.json");
const DATABASES_FILE = path.join(DATA_DIR, "databases.json");
const HISTORY_RETENTION_MS =
  (Number(process.env.HISTORY_RETENTION_HOURS) || 168) * 3600_000;

function loadSchedules() {
  try {
    const raw = fs.readFileSync(SCHEDULES_FILE, "utf-8");
    return JSON.parse(raw);
  } catch {
    return { nextId: 1, schedules: [] };
  }
}

function saveSchedules() {
  const entries = [];
  for (const [id, s] of scheduledTasks) {
    entries.push({ id, jobName: s.jobName, cron: s.cron, args: s.args, createdAt: s.createdAt });
  }
  fs.mkdirSync(DATA_DIR, { recursive: true });
  const data = JSON.stringify({ nextId, schedules: entries }, null, 2);
  fs.writeFileSync(SCHEDULES_FILE, data, "utf-8");
}

function loadHistory() {
  try {
    const raw = fs.readFileSync(HISTORY_FILE, "utf-8");
    return JSON.parse(raw);
  } catch {
    return [];
  }
}

function saveHistory() {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(HISTORY_FILE, JSON.stringify(runHistory, null, 2), "utf-8");
}

function pruneHistory() {
  const cutoff = Date.now() - HISTORY_RETENTION_MS;
  const before = runHistory.length;
  while (runHistory.length && new Date(runHistory[runHistory.length - 1].finishedAt).getTime() < cutoff) {
    runHistory.pop();
  }
  if (runHistory.length !== before) saveHistory();
}

function loadRunning() {
  try {
    const raw = fs.readFileSync(RUNNING_FILE, "utf-8");
    return JSON.parse(raw);
  } catch {
    return { nextRunId: 1, jobs: [] };
  }
}

function saveRunning() {
  const jobs = [];
  for (const [, r] of runningJobs) {
    jobs.push({ runId: r.runId, jobName: r.jobName, args: r.args, pid: r.pid, startedAt: r.startedAt });
  }
  fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(RUNNING_FILE, JSON.stringify({ nextRunId, jobs }, null, 2), "utf-8");
}

function isProcessAlive(pid) {
  try { process.kill(pid, 0); return true; } catch { return false; }
}

// Runtime state
const scheduledTasks = new Map(); // id -> { cron, jobName, args, task, createdAt }
const runningJobs = new Map();    // runId -> { jobName, args, startedAt, stdout, stderr }
const runHistory = loadHistory(); // persisted history (pruned by retention)

// Restore running jobs from previous session
const persistedRunning = loadRunning();
let nextRunId = persistedRunning.nextRunId;
for (const r of persistedRunning.jobs) {
  if (isProcessAlive(r.pid)) {
    runningJobs.set(r.runId, {
      runId: r.runId, jobName: r.jobName, args: r.args,
      pid: r.pid, startedAt: r.startedAt,
      stdout: "(recovered — logs unavailable)", stderr: "",
      recovered: true,
    });
  } else {
    runHistory.unshift({
      runId: r.runId, jobName: r.jobName, args: r.args, pid: r.pid,
      success: false, stdout: "", stderr: "(server restarted while job was running)",
      finishedAt: new Date().toISOString(),
    });
  }
}
if (persistedRunning.jobs.length) {
  const alive = [...runningJobs.values()].filter((r) => r.recovered).length;
  const dead = persistedRunning.jobs.length - alive;
  console.log(`Recovered running jobs: ${alive} alive, ${dead} finished`);
  saveRunning();
}
pruneHistory();
console.log(`Loaded ${runHistory.length} history entry(ies), retention ${process.env.HISTORY_RETENTION_HOURS || 168}h`);

// Restore persisted schedules
const persisted = loadSchedules();
let nextId = persisted.nextId;
for (const s of persisted.schedules) {
  const task = cron.schedule(s.cron, () => {
    startJob(s.jobName, s.args);
  });
  scheduledTasks.set(s.id, {
    cron: s.cron,
    jobName: s.jobName,
    args: s.args || {},
    task,
    createdAt: s.createdAt,
  });
}
if (persisted.schedules.length) {
  console.log(`Restored ${persisted.schedules.length} schedule(s) from disk`);
}

// -------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------
function isJobRunning(jobName) {
  for (const [, r] of runningJobs) {
    if (r.jobName === jobName) return true;
  }
  return false;
}

function startJob(jobName, args = {}, _retryCount = 0) {
  if (isJobRunning(jobName)) {
    console.log(`Skipped ${jobName}: already running`);
    return null;
  }

  const runId = nextRunId++;
  const tracker = {
    runId,
    jobName,
    args,
    startedAt: new Date().toISOString(),
    stdout: "",
    stderr: "",
    retryCount: _retryCount,
  };
  runningJobs.set(runId, tracker);

  const cliArgs = ["-u", "-m", "dbcron.main", jobName];
  for (const [k, v] of Object.entries(args)) {
    cliArgs.push(`--${k}`, String(v));
  }

  const proc = spawn(
    path.join(PROJECT_ROOT, ".venv", "bin", "python"),
    cliArgs,
    { cwd: PROJECT_ROOT },
  );
  tracker.pid = proc.pid;
  tracker.proc = proc;
  saveRunning();

  // Timeout kill
  const timeoutId = setTimeout(() => {
    console.log(`Timeout: killing ${jobName} (PID ${proc.pid}) after ${JOB_TIMEOUT_MS}ms`);
    tracker.stderr += `\n[TIMEOUT] Job killed after ${Math.round(JOB_TIMEOUT_MS / 1000)}s`;
    proc.kill("SIGTERM");
    setTimeout(() => { try { proc.kill("SIGKILL"); } catch {} }, 3000);
  }, JOB_TIMEOUT_MS);

  proc.on("error", (err) => {
    clearTimeout(timeoutId);
    runningJobs.delete(runId);
    saveRunning();
    runHistory.unshift({
      runId, jobName, args, pid: proc.pid,
      success: false, stdout: "", stderr: err.message,
      finishedAt: new Date().toISOString(),
    });
    saveHistory();
  });

  proc.stdout.on("data", (d) => (tracker.stdout += d));
  proc.stderr.on("data", (d) => (tracker.stderr += d));

  proc.on("close", (code) => {
    clearTimeout(timeoutId);
    runningJobs.delete(runId);
    saveRunning();
    const entry = {
      runId, jobName, args, pid: proc.pid,
      success: code === 0,
      stdout: tracker.stdout.trim(),
      stderr: tracker.stderr.trim(),
      finishedAt: new Date().toISOString(),
      retryCount: _retryCount,
    };
    runHistory.unshift(entry);
    saveHistory();
    broadcastSSE("job_complete", { runId, jobName, success: entry.success });

    // Trigger dependent jobs on success
    if (entry.success) {
      for (const [, s] of scheduledTasks) {
        if (s.dependsOn === jobName) {
          console.log(`Triggering dependent job: ${s.jobName} (depends on ${jobName})`);
          startJob(s.jobName, s.args);
        }
      }
    }

    // Retry on failure
    if (!entry.success && _retryCount < JOB_MAX_RETRIES) {
      const delay = Math.min(5000 * 2 ** _retryCount, 60000); // exponential backoff, max 60s
      console.log(`Retry ${_retryCount + 1}/${JOB_MAX_RETRIES} for ${jobName} in ${delay}ms`);
      setTimeout(() => startJob(jobName, args, _retryCount + 1), delay);
    }

    // Webhook notification on final failure (after all retries exhausted)
    if (!entry.success && _retryCount >= JOB_MAX_RETRIES && WEBHOOK_URL) {
      const payload = JSON.stringify({
        event: "job_failed",
        job: jobName,
        runId,
        pid: proc.pid,
        exitCode: code,
        error: entry.stderr.slice(0, 500) || entry.stdout.slice(0, 500),
        finishedAt: entry.finishedAt,
        retries: _retryCount,
      });
      fetch(WEBHOOK_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: payload,
      }).catch((err) => console.error("Webhook failed:", err.message));
    }
  });

  return runId;
}

// -------------------------------------------------------------------
// REST API
// -------------------------------------------------------------------

// List available jobs
app.get("/api/jobs", (_req, res) => {
  res.json(AVAILABLE_JOBS);
});

// Job statistics derived from history
app.get("/api/jobs/stats", (_req, res) => {
  const stats = {};
  for (const h of runHistory) {
    if (!stats[h.jobName]) {
      stats[h.jobName] = { lastSuccess: null, lastFailure: null, failStreak: 0, durations: [] };
    }
    const s = stats[h.jobName];
    if (h.success) {
      if (!s.lastSuccess) s.lastSuccess = h.finishedAt;
    } else {
      if (!s.lastFailure) s.lastFailure = h.finishedAt;
    }
  }
  // Calculate fail streaks (consecutive failures from most recent)
  for (const jobName of Object.keys(stats)) {
    let streak = 0;
    for (const h of runHistory) {
      if (h.jobName !== jobName) continue;
      if (!h.success) streak++;
      else break;
    }
    stats[jobName].failStreak = streak;
  }
  // Next run times from schedules
  for (const [, s] of scheduledTasks) {
    if (!stats[s.jobName]) stats[s.jobName] = { lastSuccess: null, lastFailure: null, failStreak: 0 };
  }
  res.json(stats);
});

// Run a job immediately (fire-and-forget)
app.post("/api/jobs/:name/run", (req, res) => {
  const jobMeta = AVAILABLE_JOBS.find((j) => j.name === req.params.name);
  if (!jobMeta) return res.status(404).json({ error: "Unknown job" });

  const args = { ...jobMeta.defaultArgs, ...req.body };
  const runId = startJob(req.params.name, args);
  if (runId === null) return res.status(409).json({ error: "Job already running" });
  res.json({ runId });
});

// List scheduled tasks
app.get("/api/schedules", (_req, res) => {
  const list = [];
  for (const [id, s] of scheduledTasks) {
    list.push({ id, jobName: s.jobName, cron: s.cron, args: s.args, createdAt: s.createdAt, paused: !!s.paused, dependsOn: s.dependsOn || null });
  }
  res.json(list);
});

// Create a schedule
app.post("/api/schedules", (req, res) => {
  const { jobName, cronExpr, args, dependsOn } = req.body;

  if (!AVAILABLE_JOBS.find((j) => j.name === jobName)) {
    return res.status(400).json({ error: "Unknown job" });
  }
  if (!dependsOn && !cron.validate(cronExpr)) {
    return res.status(400).json({ error: "Invalid cron expression" });
  }

  const id = nextId++;
  let task = null;
  if (!dependsOn) {
    task = cron.schedule(cronExpr, () => {
      startJob(jobName, args);
    });
  }

  scheduledTasks.set(id, {
    cron: cronExpr || "",
    jobName,
    args: args || {},
    task,
    dependsOn: dependsOn || null,
    createdAt: new Date().toISOString(),
  });
  saveSchedules();

  res.status(201).json({ id, jobName, cron: cronExpr, dependsOn });
});

// Pause/resume a schedule
app.put("/api/schedules/:id/pause", (req, res) => {
  const id = Number(req.params.id);
  const entry = scheduledTasks.get(id);
  if (!entry) return res.status(404).json({ error: "Schedule not found" });
  if (entry.task) entry.task.stop();
  entry.paused = true;
  saveSchedules();
  res.json({ id, paused: true });
});

app.put("/api/schedules/:id/resume", (req, res) => {
  const id = Number(req.params.id);
  const entry = scheduledTasks.get(id);
  if (!entry) return res.status(404).json({ error: "Schedule not found" });
  if (entry.task) entry.task.start();
  entry.paused = false;
  saveSchedules();
  res.json({ id, paused: false });
});

// Update a schedule
app.put("/api/schedules/:id", (req, res) => {
  const id = Number(req.params.id);
  const entry = scheduledTasks.get(id);
  if (!entry) return res.status(404).json({ error: "Schedule not found" });

  const { cronExpr, args } = req.body;
  if (cronExpr && !cron.validate(cronExpr)) {
    return res.status(400).json({ error: "Invalid cron expression" });
  }

  entry.task.stop();
  const newCron = cronExpr || entry.cron;
  const newArgs = args !== undefined ? args : entry.args;
  const task = cron.schedule(newCron, () => {
    startJob(entry.jobName, newArgs);
  });
  scheduledTasks.set(id, {
    cron: newCron,
    jobName: entry.jobName,
    args: newArgs,
    task,
    createdAt: entry.createdAt,
  });
  saveSchedules();
  res.json({ id, jobName: entry.jobName, cron: newCron });
});

// Delete a schedule
app.delete("/api/schedules/:id", (req, res) => {
  const id = Number(req.params.id);
  const entry = scheduledTasks.get(id);
  if (!entry) return res.status(404).json({ error: "Schedule not found" });

  entry.task.stop();
  scheduledTasks.delete(id);
  saveSchedules();
  res.json({ deleted: id });
});

// Running jobs
app.get("/api/running", (_req, res) => {
  const list = [];
  for (const [, r] of runningJobs) {
    list.push({ runId: r.runId, jobName: r.jobName, args: r.args, pid: r.pid, startedAt: r.startedAt, stdout: r.stdout, stderr: r.stderr });
  }
  res.json(list);
});

app.get("/api/running/:id", (req, res) => {
  const r = runningJobs.get(Number(req.params.id));
  if (!r) return res.status(404).json({ error: "Not running" });
  res.json({ runId: r.runId, jobName: r.jobName, args: r.args, pid: r.pid, startedAt: r.startedAt, stdout: r.stdout, stderr: r.stderr });
});

// Kill a running job
app.delete("/api/running/:id", (req, res) => {
  const r = runningJobs.get(Number(req.params.id));
  if (!r) return res.status(404).json({ error: "Not running" });
  if (r.proc) {
    r.proc.kill("SIGTERM");
    setTimeout(() => { try { r.proc.kill("SIGKILL"); } catch {} }, 3000);
  } else {
    // Recovered job — kill by PID directly
    try { process.kill(r.pid, "SIGTERM"); } catch {}
    setTimeout(() => { try { process.kill(r.pid, "SIGKILL"); } catch {} }, 3000);
    runningJobs.delete(Number(req.params.id));
    saveRunning();
  }
  res.json({ killed: r.runId, pid: r.pid });
});

// Run history
app.get("/api/history", (_req, res) => {
  res.json(runHistory);
});

// SSE: real-time event stream for job state changes
const sseClients = new Set();
app.get("/api/events", (req, res) => {
  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    Connection: "keep-alive",
  });
  res.write("data: connected\n\n");
  sseClients.add(res);
  req.on("close", () => sseClients.delete(res));
});
function broadcastSSE(event, data) {
  const msg = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const c of sseClients) { try { c.write(msg); } catch {} }
}

// -------------------------------------------------------------------
// Database registration (CRUD)
// -------------------------------------------------------------------

function loadDatabases() {
  try {
    const dbs = JSON.parse(fs.readFileSync(DATABASES_FILE, "utf-8"));
    // Decode obfuscated passwords
    return dbs.map((d) => ({
      ...d,
      password: d._enc === "b64" && d.password ? Buffer.from(d.password, "base64").toString() : (d.password || ""),
    }));
  } catch {
    return [];
  }
}

function saveDatabases(dbs) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  // Obfuscate passwords (base64) to avoid plaintext on disk
  const stored = dbs.map((d) => ({
    ...d,
    password: d.password ? Buffer.from(d.password).toString("base64") : "",
    _enc: "b64",
  }));
  fs.writeFileSync(DATABASES_FILE, JSON.stringify(stored, null, 2), "utf-8");
}

app.get("/api/databases", (_req, res) => {
  const dbs = loadDatabases();
  // Strip passwords from response
  res.json(dbs.map(({ password, ...rest }) => ({ ...rest, hasPassword: !!password })));
});

app.post("/api/databases", (req, res) => {
  const { id, type, label, color, host, port, dbname, user, password, include_tables, exclude_tables } = req.body;
  if (!id || !host || !dbname) {
    return res.status(400).json({ error: "id, host, dbname are required" });
  }
  const dbs = loadDatabases();
  if (dbs.find((d) => d.id === id)) {
    return res.status(409).json({ error: "Database ID already exists" });
  }
  const DEFAULTS = { postgresql: 5432, mssql: 1433, clickhouse: 8123, sqlite: 0 };
  const dbType = type || "postgresql";
  const entry = {
    id,
    type: dbType,
    label: label || id,
    color: color || "#00e5ff",
    host,
    port: Number(port) || DEFAULTS[dbType] || 5432,
    dbname,
    user: user || "",
    password: password || "",
  };
  if (include_tables?.length) entry.include_tables = include_tables;
  if (exclude_tables?.length) entry.exclude_tables = exclude_tables;
  dbs.push(entry);
  saveDatabases(dbs);
  res.status(201).json({ id });
});

app.put("/api/databases/:id", (req, res) => {
  const dbs = loadDatabases();
  const idx = dbs.findIndex((d) => d.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: "Not found" });
  const { type, label, color, host, port, dbname, user, password, include_tables, exclude_tables } = req.body;
  if (type !== undefined) dbs[idx].type = type;
  if (label !== undefined) dbs[idx].label = label;
  if (color !== undefined) dbs[idx].color = color;
  if (host !== undefined) dbs[idx].host = host;
  if (port !== undefined) dbs[idx].port = Number(port) || 5432;
  if (dbname !== undefined) dbs[idx].dbname = dbname;
  if (user !== undefined) dbs[idx].user = user;
  if (password !== undefined) dbs[idx].password = password;
  if (include_tables !== undefined) {
    if (include_tables.length) dbs[idx].include_tables = include_tables;
    else delete dbs[idx].include_tables;
  }
  if (exclude_tables !== undefined) {
    if (exclude_tables.length) dbs[idx].exclude_tables = exclude_tables;
    else delete dbs[idx].exclude_tables;
  }
  saveDatabases(dbs);
  res.json({ id: req.params.id });
});

app.delete("/api/databases/:id", (req, res) => {
  const dbs = loadDatabases();
  const filtered = dbs.filter((d) => d.id !== req.params.id);
  if (filtered.length === dbs.length) return res.status(404).json({ error: "Not found" });
  saveDatabases(filtered);
  res.json({ deleted: req.params.id });
});

// -------------------------------------------------------------------
// Canvas APIs — metadata snapshot + pipeline config
// -------------------------------------------------------------------

// Serve cached metadata snapshot (generated by metadata_snapshot job)
app.get("/api/metadata", (_req, res) => {
  const snapshotPath = path.join(DATA_DIR, "metadata_snapshot.json");
  try {
    const raw = fs.readFileSync(snapshotPath, "utf-8");
    res.json(JSON.parse(raw));
  } catch {
    res.status(404).json({
      error: "No metadata snapshot available. Run the metadata_snapshot job first.",
    });
  }
});

// Schema drift: compare current vs previous snapshot
app.get("/api/metadata/drift", (_req, res) => {
  const curPath = path.join(DATA_DIR, "metadata_snapshot.json");
  const prevPath = path.join(DATA_DIR, "metadata_snapshot_prev.json");
  let cur, prev;
  try {
    cur = JSON.parse(fs.readFileSync(curPath, "utf-8"));
  } catch {
    return res.status(404).json({ error: "No current snapshot" });
  }
  try {
    prev = JSON.parse(fs.readFileSync(prevPath, "utf-8"));
  } catch {
    return res.json({ drift: [], prev_snapshot_at: null, cur_snapshot_at: cur.snapshot_at });
  }

  const drift = [];
  for (const [dbId, dbCur] of Object.entries(cur.databases || {})) {
    const dbPrev = (prev.databases || {})[dbId];
    if (!dbPrev) {
      drift.push({ db: dbId, type: "db_added", detail: `Database added` });
      continue;
    }
    const curTables = dbCur.tables || {};
    const prevTables = dbPrev.tables || {};

    // Added tables
    for (const tKey of Object.keys(curTables)) {
      if (!prevTables[tKey]) {
        drift.push({ db: dbId, table: tKey, type: "table_added", breaking: false });
      }
    }
    // Removed tables
    for (const tKey of Object.keys(prevTables)) {
      if (!curTables[tKey]) {
        drift.push({ db: dbId, table: tKey, type: "table_removed", breaking: true });
      }
    }
    // Changed tables
    for (const [tKey, tCur] of Object.entries(curTables)) {
      const tPrev = prevTables[tKey];
      if (!tPrev) continue;
      const curCols = Object.fromEntries((tCur.columns || []).map((c) => [c.name, c]));
      const prevCols = Object.fromEntries((tPrev.columns || []).map((c) => [c.name, c]));

      for (const [cName, cCur] of Object.entries(curCols)) {
        if (!prevCols[cName]) {
          drift.push({ db: dbId, table: tKey, column: cName, type: "column_added", breaking: false });
        } else {
          const cPrev = prevCols[cName];
          if (cCur.type !== cPrev.type) {
            drift.push({ db: dbId, table: tKey, column: cName, type: "type_changed", from: cPrev.type, to: cCur.type, breaking: true });
          }
          if (cCur.nullable !== cPrev.nullable) {
            drift.push({ db: dbId, table: tKey, column: cName, type: "nullable_changed", from: cPrev.nullable, to: cCur.nullable, breaking: !cCur.nullable });
          }
        }
      }
      for (const cName of Object.keys(prevCols)) {
        if (!curCols[cName]) {
          drift.push({ db: dbId, table: tKey, column: cName, type: "column_removed", breaking: true });
        }
      }

      // PK changes
      const curPk = JSON.stringify(tCur.primary_key);
      const prevPk = JSON.stringify(tPrev.primary_key);
      if (curPk !== prevPk) {
        drift.push({ db: dbId, table: tKey, type: "pk_changed", breaking: true });
      }
    }
  }
  // Removed DBs
  for (const dbId of Object.keys(prev.databases || {})) {
    if (!(cur.databases || {})[dbId]) {
      drift.push({ db: dbId, type: "db_removed", breaking: true });
    }
  }

  res.json({ drift, prev_snapshot_at: prev.snapshot_at, cur_snapshot_at: cur.snapshot_at });
});

// Timeline: merged history + drift events
app.get("/api/timeline", (_req, res) => {
  const events = [];
  // Job runs
  for (const h of runHistory) {
    events.push({
      type: "job_run",
      ts: h.finishedAt,
      job: h.jobName,
      success: h.success,
      message: h.success ? (h.stdout || "").slice(0, 100) : (h.stderr || "").slice(0, 100),
    });
  }
  // Schema drift
  const prevPath = path.join(DATA_DIR, "metadata_snapshot_prev.json");
  const curPath = path.join(DATA_DIR, "metadata_snapshot.json");
  try {
    const cur = JSON.parse(fs.readFileSync(curPath, "utf-8"));
    const prev = JSON.parse(fs.readFileSync(prevPath, "utf-8"));
    if (prev.snapshot_at && cur.snapshot_at) {
      // Count drift changes
      let driftCount = 0, breaking = 0;
      for (const [dbId, dbCur] of Object.entries(cur.databases || {})) {
        const dbPrev = (prev.databases || {})[dbId];
        if (!dbPrev) { driftCount++; continue; }
        for (const [tKey, tCur] of Object.entries(dbCur.tables || {})) {
          const tPrev = (dbPrev.tables || {})[tKey];
          if (!tPrev) { driftCount++; continue; }
          const curCols = new Set((tCur.columns || []).map(c => c.name + ":" + c.type));
          const prevCols = new Set((tPrev.columns || []).map(c => c.name + ":" + c.type));
          for (const c of curCols) { if (!prevCols.has(c)) { driftCount++; } }
          for (const c of prevCols) { if (!curCols.has(c)) { driftCount++; breaking++; } }
        }
      }
      if (driftCount > 0) {
        events.push({
          type: "schema_drift",
          ts: cur.snapshot_at,
          changes: driftCount,
          breaking,
          message: `${driftCount} schema change(s)${breaking ? `, ${breaking} breaking` : ""}`,
        });
      }
    }
  } catch {}

  events.sort((a, b) => new Date(b.ts) - new Date(a.ts));
  res.json(events.slice(0, 200));
});

// Annotations: owner, notes, runbook per table/job
const ANNOTATIONS_FILE = path.join(DATA_DIR, "annotations.json");

function loadAnnotations() {
  try { return JSON.parse(fs.readFileSync(ANNOTATIONS_FILE, "utf-8")); } catch { return {}; }
}
function saveAnnotations(data) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(ANNOTATIONS_FILE, JSON.stringify(data, null, 2), "utf-8");
}

app.get("/api/annotations", (_req, res) => res.json(loadAnnotations()));
app.get("/api/annotations/:key", (req, res) => {
  const data = loadAnnotations();
  res.json(data[req.params.key] || {});
});
app.put("/api/annotations/:key", (req, res) => {
  const data = loadAnnotations();
  data[req.params.key] = { ...data[req.params.key], ...req.body, updatedAt: new Date().toISOString() };
  saveAnnotations(data);
  res.json(data[req.params.key]);
});

// Lineage: upstream/downstream dependencies for a table
app.get("/api/lineage/:db/:table", (req, res) => {
  const cfgPath = path.join(PROJECT_ROOT, process.env.PIPELINE_CONFIG || "pipeline_config.json");
  let cfg = { entry_points: [], pipelines: [] };
  try { cfg = { ...cfg, ...JSON.parse(fs.readFileSync(cfgPath, "utf-8")) }; } catch {}

  // Resolve auto connections
  const syncCfgPath = path.join(PROJECT_ROOT, process.env.SYNC_CONFIG || "sync_config.json");
  let syncCfg = null;
  try { syncCfg = JSON.parse(fs.readFileSync(syncCfgPath, "utf-8")); } catch {}
  const allConns = [];
  for (const p of cfg.pipelines || []) {
    if (p.connections === "auto" && syncCfg?.tables) {
      const srcDb = syncCfg.source || "unknown";
      const tgtDb = syncCfg.target || "unknown";
      const fromDb = typeof srcDb === "string" ? srcDb : srcDb.database || "source";
      const toDb = typeof tgtDb === "string" ? tgtDb : tgtDb.database || "target";
      for (const t of syncCfg.tables) {
        allConns.push({ from: `${t.source_schema || "public"}.${t.table}`, fromDb, to: `${t.target_schema || "public"}.${t.table}`, toDb, label: p.description || "" });
      }
    } else if (Array.isArray(p.connections)) {
      for (const c of p.connections) {
        allConns.push({ from: `${c.from.schema}.${c.from.table}`, fromDb: c.from.db, to: `${c.to.schema}.${c.to.table}`, toDb: c.to.db, label: c.label || "" });
      }
    }
  }

  const targetDb = req.params.db;
  const targetTable = req.params.table;

  const upstream = [];
  const downstream = [];
  for (const c of allConns) {
    if (c.toDb === targetDb && c.to === targetTable) upstream.push({ db: c.fromDb, table: c.from, label: c.label });
    if (c.fromDb === targetDb && c.from === targetTable) downstream.push({ db: c.toDb, table: c.to, label: c.label });
  }
  // Entry points
  const entryUpstream = [];
  for (const ep of cfg.entry_points || []) {
    for (const t of ep.targets || []) {
      if (t.db === targetDb && `${t.schema}.${t.table}` === targetTable) {
        entryUpstream.push({ name: ep.name, type: ep.type, description: ep.description || "" });
      }
    }
  }

  res.json({ upstream, downstream, entryPoints: entryUpstream });
});

// Freshness log: row count history per table
app.get("/api/freshness", (req, res) => {
  const p = path.join(DATA_DIR, "freshness_log.jsonl");
  try {
    const lines = fs.readFileSync(p, "utf-8").trim().split("\n").filter(Boolean);
    const entries = lines.map((l) => JSON.parse(l));
    // Optional filter by db/table
    const { db, table } = req.query;
    const filtered = entries.filter((e) => (!db || e.db === db) && (!table || e.table === table));
    res.json(filtered.slice(-500)); // Last 500 entries
  } catch {
    res.json([]);
  }
});

// Serve connection test results
app.get("/api/connection-test", (_req, res) => {
  const p = path.join(DATA_DIR, "connection_test.json");
  try {
    res.json(JSON.parse(fs.readFileSync(p, "utf-8")));
  } catch {
    res.status(404).json({ error: "No connection test results. Run the connection_test job first." });
  }
});

// Serve pipeline config with auto-derived connections from sync_config
app.get("/api/pipeline-config", (_req, res) => {
  let cfg = { databases: {}, entry_points: [], pipelines: [] };
  const cfgPath = path.join(
    PROJECT_ROOT,
    process.env.PIPELINE_CONFIG || "pipeline_config.json",
  );
  try {
    cfg = { ...cfg, ...JSON.parse(fs.readFileSync(cfgPath, "utf-8")) };
  } catch {}

  // Merge dynamic databases into config (overrides file-based databases section)
  const dbs = loadDatabases();
  if (dbs.length) {
    cfg.databases = {};
    for (const d of dbs) {
      cfg.databases[d.id] = { label: d.label, color: d.color };
    }
  }

  // Resolve "auto" pipelines from sync_config.json
  const syncCfgPath = path.join(
    PROJECT_ROOT,
    process.env.SYNC_CONFIG || "sync_config.json",
  );
  let syncCfg = null;
  try {
    syncCfg = JSON.parse(fs.readFileSync(syncCfgPath, "utf-8"));
  } catch {}

  if (syncCfg && cfg.pipelines) {
    for (const p of cfg.pipelines) {
      if (p.connections === "auto" && syncCfg.tables) {
        const srcDb = syncCfg.source || "unknown";
        const tgtDb = syncCfg.target || "unknown";
        const fromDb = typeof srcDb === "string" ? srcDb : srcDb.database || "source";
        const toDb = typeof tgtDb === "string" ? tgtDb : tgtDb.database || "target";
        p.connections = syncCfg.tables.map((t) => ({
          from: {
            db: fromDb,
            schema: t.source_schema || "public",
            table: t.table,
          },
          to: {
            db: toDb,
            schema: t.target_schema || "public",
            table: t.table,
          },
          label: `${t.table} (incremental_sync)`,
        }));
      }
    }
  }

  res.json(cfg);
});

// Prune expired history entries every hour
setInterval(pruneHistory, 3600_000);

// -------------------------------------------------------------------
app.listen(PORT, "0.0.0.0", () => {
  console.log(`db_manager WebUI running on http://0.0.0.0:${PORT}`);
});
