const express = require("express");
const cron = require("node-cron");
const { spawn } = require("child_process");
const path = require("path");

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const PORT = process.env.PORT || 3000;
const PROJECT_ROOT = path.resolve(__dirname, "../..");

// -------------------------------------------------------------------
// Available jobs — add new entries when you create a new Job subclass
// -------------------------------------------------------------------
const AVAILABLE_JOBS = [];

// In-memory state
const scheduledTasks = new Map(); // id -> { cron, jobName, args, task, createdAt }
const runHistory = []; // last 100 results
let nextId = 1;

// -------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------
function runJob(jobName, args = {}) {
  return new Promise((resolve, reject) => {
    const cliArgs = ["run", "python", "-m", "dbcron.main", jobName];
    for (const [k, v] of Object.entries(args)) {
      cliArgs.push(`--${k}`, String(v));
    }

    const proc = spawn("uv", cliArgs, { cwd: PROJECT_ROOT });
    let stdout = "";
    let stderr = "";

    proc.on("error", (err) => {
      reject({ jobName, args, success: false, stdout: "", stderr: err.message, finishedAt: new Date().toISOString() });
    });
    proc.stdout.on("data", (d) => (stdout += d));
    proc.stderr.on("data", (d) => (stderr += d));

    proc.on("close", (code) => {
      const entry = {
        jobName,
        args,
        success: code === 0,
        stdout: stdout.trim(),
        stderr: stderr.trim(),
        finishedAt: new Date().toISOString(),
      };
      runHistory.unshift(entry);
      if (runHistory.length > 100) runHistory.pop();
      code === 0 ? resolve(entry) : reject(entry);
    });
  });
}

// -------------------------------------------------------------------
// REST API
// -------------------------------------------------------------------

// List available jobs
app.get("/api/jobs", (_req, res) => {
  res.json(AVAILABLE_JOBS);
});

// Run a job immediately (one-shot)
app.post("/api/jobs/:name/run", async (req, res) => {
  const jobMeta = AVAILABLE_JOBS.find((j) => j.name === req.params.name);
  if (!jobMeta) return res.status(404).json({ error: "Unknown job" });

  const args = { ...jobMeta.defaultArgs, ...req.body };
  try {
    const result = await runJob(req.params.name, args);
    res.json(result);
  } catch (err) {
    res.status(500).json(err);
  }
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
    runJob(jobName, args).catch(() => {});
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

// Run history
app.get("/api/history", (_req, res) => {
  res.json(runHistory);
});

// -------------------------------------------------------------------
app.listen(PORT, () => {
  console.log(`db_manager WebUI running on http://localhost:${PORT}`);
});
