/**
 * services/scheduler.js
 * Pipeline scheduler using node-cron.
 * State is stored in the SQLite database — no separate JSON file.
 * Runs inside the API server process so it shares the DB connection.
 *
 * Cron schedules:
 *   weather     — every 6 hours       (0 *\/6 * * *)
 *   fuel        — every day at 07:00  (0 7 * * *)
 *   commodities — every Monday 08:00  (0 8 * * 1)
 *   tariffs     — 1st of month 09:00  (0 9 1 * *)
 */

const cron   = require("node-cron");
const { spawnSync } = require("child_process");
const path   = require("path");
const fs     = require("fs");

const PROJECT_ROOT = path.join(__dirname, "../..");
const LOG_DIR      = path.join(PROJECT_ROOT, "logs");

// ── Schedule definitions ──────────────────────────────────────────────────────
const SCHEDULES = [
  {
    source:      "weather",
    cron:        "0 */6 * * *",       // every 6 hours
    description: "Port weather forecasts — Open-Meteo",
    interval:    "every 6 hours",
  },
  {
    source:      "fuel",
    cron:        "0 7 * * *",         // daily 07:00
    description: "Fuel prices — EIA + ECB + World Bank",
    interval:    "daily at 07:00",
  },
  {
    source:      "commodities",
    cron:        "0 8 * * 1",         // every Monday 08:00
    description: "Commodity prices — World Bank CMO",
    interval:    "weekly (Monday 08:00)",
  },
  {
    source:      "tariffs",
    cron:        "0 9 1 * *",         // 1st of month 09:00
    description: "Tariff rates — WTO TAO",
    interval:    "monthly (1st at 09:00)",
  },
];

// ── Schema for schedule state table ──────────────────────────────────────────
const SETUP_SQL = `
  CREATE TABLE IF NOT EXISTS pipeline_schedule (
    source        TEXT PRIMARY KEY,
    cron_expr     TEXT NOT NULL,
    description   TEXT,
    interval_text TEXT,
    last_run_at   TEXT,
    last_status   TEXT,
    last_exit_code INTEGER,
    next_run_at   TEXT,
    is_running    INTEGER DEFAULT 0,
    run_count     INTEGER DEFAULT 0,
    fail_count    INTEGER DEFAULT 0
  );
`;

// ── Helpers ───────────────────────────────────────────────────────────────────
function getNextRunTime(cronExpr) {
  try {
    // Calculate next run from cron expression
    // node-cron doesn't expose next() directly so we approximate
    const now    = new Date();
    const parts  = cronExpr.split(" ");
    const minute = parseInt(parts[0]) || 0;
    const hour   = parts[1] === "*" ? now.getHours()
                 : parts[1].startsWith("*/") ? now.getHours()
                 : parseInt(parts[1]);

    const next = new Date(now);
    next.setSeconds(0);
    next.setMilliseconds(0);

    if (cronExpr === "0 */6 * * *") {
      // Every 6 hours — next multiple of 6
      const nextHour = Math.ceil((now.getHours() + 1) / 6) * 6;
      next.setHours(nextHour % 24, 0, 0, 0);
      if (nextHour >= 24) next.setDate(next.getDate() + 1);
    } else if (cronExpr === "0 7 * * *") {
      next.setHours(7, 0, 0, 0);
      if (next <= now) next.setDate(next.getDate() + 1);
    } else if (cronExpr === "0 8 * * 1") {
      // Next Monday 08:00
      const daysUntilMonday = (8 - next.getDay()) % 7 || 7;
      next.setDate(next.getDate() + daysUntilMonday);
      next.setHours(8, 0, 0, 0);
    } else if (cronExpr === "0 9 1 * *") {
      // 1st of next month 09:00
      next.setMonth(next.getMonth() + 1, 1);
      next.setHours(9, 0, 0, 0);
    }

    return next.toISOString();
  } catch (_) {
    return null;
  }
}

function logToFile(source, message) {
  try {
    fs.mkdirSync(LOG_DIR, { recursive: true });
    const logFile = path.join(LOG_DIR,
      `pipeline_${new Date().toISOString().slice(0, 10)}.log`);
    const line = `${new Date().toISOString()} [scheduler:${source}] ${message}\n`;
    fs.appendFileSync(logFile, line);
  } catch (_) {}
}

// ── Core runner ───────────────────────────────────────────────────────────────
function runSource(db, source, manual = false) {
  // Mark as running
  db.prepare(`
    UPDATE pipeline_schedule
    SET is_running = 1, last_run_at = datetime('now')
    WHERE source = ?
  `).run(source);

  logToFile(source, `Starting (${manual ? "manual" : "scheduled"})`);
  console.log(`[Scheduler] ${source} starting (${manual ? "manual" : "scheduled"})`);

  const script = path.join(PROJECT_ROOT, "run_pipeline.py");
  const env    = { ...process.env };

  try {
    const result = spawnSync("python", [script, "--source", source], {
      cwd:         PROJECT_ROOT,
      env,
      timeout:     5 * 60 * 1000,  // 5 minute timeout per source
      encoding:    "utf8",
      windowsHide: true,
    });

    const exitCode = result.status ?? 1;
    const success  = exitCode === 0;
    const output   = (result.stdout || "") + (result.stderr || "");

    logToFile(source, `Finished — exit ${exitCode}`);
    if (output) logToFile(source, output.slice(0, 500));

    console.log(`[Scheduler] ${source} finished — exit ${exitCode}`);

    // Update state
    db.prepare(`
      UPDATE pipeline_schedule SET
        is_running    = 0,
        last_status   = ?,
        last_exit_code = ?,
        next_run_at   = ?,
        run_count     = run_count + 1,
        fail_count    = fail_count + ?
      WHERE source = ?
    `).run(
      success ? "success" : "failed",
      exitCode,
      getNextRunTime(SCHEDULES.find(s => s.source === source)?.cron || ""),
      success ? 0 : 1,
      source
    );

    return { success, exitCode, output };

  } catch (err) {
    logToFile(source, `Error: ${err.message}`);
    console.error(`[Scheduler] ${source} error: ${err.message}`);

    db.prepare(`
      UPDATE pipeline_schedule SET
        is_running = 0, last_status = 'error',
        fail_count = fail_count + 1
      WHERE source = ?
    `).run(source);

    return { success: false, error: err.message };
  }
}

// ── Setup and start ───────────────────────────────────────────────────────────
const jobs = [];

function start(db) {
  // Create table if needed
  db.exec(SETUP_SQL);

  // Upsert schedule definitions
  const upsert = db.prepare(`
    INSERT INTO pipeline_schedule (source, cron_expr, description, interval_text, next_run_at)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(source) DO UPDATE SET
      cron_expr     = excluded.cron_expr,
      description   = excluded.description,
      interval_text = excluded.interval_text
  `);

  for (const s of SCHEDULES) {
    upsert.run(s.source, s.cron, s.description, s.interval,
               getNextRunTime(s.cron));
  }

  // Register cron jobs
  for (const s of SCHEDULES) {
    const job = cron.schedule(s.cron, () => {
      // Check not already running
      const row = db.prepare(
        "SELECT is_running FROM pipeline_schedule WHERE source = ?"
      ).get(s.source);
      if (row?.is_running) {
        console.log(`[Scheduler] ${s.source} already running — skipping`);
        return;
      }
      runSource(db, s.source, false);
    }, {
      timezone: "UTC",
    });

    jobs.push({ source: s.source, job });
    console.log(`[Scheduler] Registered: ${s.source} (${s.cron}) — ${s.interval}`);
  }

  console.log(`[Scheduler] Started — ${jobs.length} jobs registered`);
}

function stop() {
  for (const { job } of jobs) job.stop();
  jobs.length = 0;
  console.log("[Scheduler] Stopped");
}

function getStatus(db) {
  return db.prepare(`
    SELECT source, cron_expr, description, interval_text,
           last_run_at, last_status, last_exit_code,
           next_run_at, is_running, run_count, fail_count
    FROM pipeline_schedule
    ORDER BY source
  `).all();
}

function triggerNow(db, source) {
  const row = db.prepare(
    "SELECT is_running FROM pipeline_schedule WHERE source = ?"
  ).get(source);

  if (!row) return { success: false, error: `Unknown source: ${source}` };
  if (row.is_running) return { success: false, error: `${source} is already running` };

  // Run synchronously in this context (returns when done)
  return runSource(db, source, true);
}

module.exports = { start, stop, getStatus, triggerNow };