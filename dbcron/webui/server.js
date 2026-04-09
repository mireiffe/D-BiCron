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
const runHistory = []; // last 100 results

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

function startJob(jobName, args = {}) {
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

  proc.on("error", (err) => {
    runningJobs.delete(runId);
    saveRunning();
    runHistory.unshift({
      runId, jobName, args, pid: proc.pid,
      success: false, stdout: "", stderr: err.message,
      finishedAt: new Date().toISOString(),
    });
    if (runHistory.length > 100) runHistory.pop();
  });

  proc.stdout.on("data", (d) => (tracker.stdout += d));
  proc.stderr.on("data", (d) => (tracker.stderr += d));

  proc.on("close", (code) => {
    runningJobs.delete(runId);
    saveRunning();
    runHistory.unshift({
      runId, jobName, args, pid: proc.pid,
      success: code === 0,
      stdout: tracker.stdout.trim(),
      stderr: tracker.stderr.trim(),
      finishedAt: new Date().toISOString(),
    });
    if (runHistory.length > 100) runHistory.pop();
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
    list.push({ id, jobName: s.jobName, cron: s.cron, args: s.args, createdAt: s.createdAt });
  }
  res.json(list);
});

// Create a schedule
app.post("/api/schedules", (req, res) => {
  const { jobName, cronExpr, args } = req.body;

  if (!AVAILABLE_JOBS.find((j) => j.name === jobName)) {
    return res.status(400).json({ error: "Unknown job" });
  }
  if (!cron.validate(cronExpr)) {
    return res.status(400).json({ error: "Invalid cron expression" });
  }

  const id = nextId++;
  const task = cron.schedule(cronExpr, () => {
    startJob(jobName, args);
  });

  scheduledTasks.set(id, {
    cron: cronExpr,
    jobName,
    args: args || {},
    task,
    createdAt: new Date().toISOString(),
  });
  saveSchedules();

  res.status(201).json({ id, jobName, cron: cronExpr });
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

// -------------------------------------------------------------------
app.listen(PORT, "0.0.0.0", () => {
  console.log(`db_manager WebUI running on http://0.0.0.0:${PORT}`);
});
