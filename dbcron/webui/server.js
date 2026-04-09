const express = require("express");
const cron = require("node-cron");
const { spawn } = require("child_process");
const path = require("path");

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const PORT = process.env.PORT || 3999;
const PROJECT_ROOT = path.resolve(__dirname, "../..");

// -------------------------------------------------------------------
// Available jobs — add new entries when you create a new Job subclass
// -------------------------------------------------------------------
const AVAILABLE_JOBS = [];

// In-memory state
const scheduledTasks = new Map(); // id -> { cron, jobName, args, task, createdAt }
const runningJobs = new Map();    // runId -> { jobName, args, startedAt, stdout, stderr }
const runHistory = []; // last 100 results
let nextId = 1;
let nextRunId = 1;

// -------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------
function startJob(jobName, args = {}) {
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

  proc.on("error", (err) => {
    runningJobs.delete(runId);
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

  res.status(201).json({ id, jobName, cron: cronExpr });
});

// Delete a schedule
app.delete("/api/schedules/:id", (req, res) => {
  const id = Number(req.params.id);
  const entry = scheduledTasks.get(id);
  if (!entry) return res.status(404).json({ error: "Schedule not found" });

  entry.task.stop();
  scheduledTasks.delete(id);
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
  r.proc.kill("SIGTERM");
  // Give it 3s, then force kill
  setTimeout(() => { try { r.proc.kill("SIGKILL"); } catch {} }, 3000);
  res.json({ killed: r.runId, pid: r.pid });
});

// Run history
app.get("/api/history", (_req, res) => {
  res.json(runHistory);
});

// -------------------------------------------------------------------
app.listen(PORT, () => {
  console.log(`db_manager WebUI running on http://localhost:${PORT}`);
});
