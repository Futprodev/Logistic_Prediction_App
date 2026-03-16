/**
 * server.js
 * Local Express API for the Logistics Intelligence Platform.
 * Runs on localhost:3001 — consumed by the Electron frontend.
 *
 * Start:  node server.js
 * Dev:    npx nodemon server.js
 */

const express    = require("express");
const cors       = require("cors");
const path       = require("path");
const Database   = require("better-sqlite3");
const { calculateLandedCost, compareRoutes } = require("./services/landedCost");
const { getAlerts, acknowledgeAlert, getAlertSummary } = require("./services/alerts");
const scheduler = require("./services/scheduler");

const app  = express();
const PORT = process.env.PORT || 3001;
const DB_PATH = process.env.LIP_DB_PATH
  || path.join(__dirname, "../data/lip.db");

// ── Middleware ────────────────────────────────────────────────────────────────
app.use(cors());
app.use(express.json());

// ── Database connection ───────────────────────────────────────────────────────
let db;
try {
  db = new Database(DB_PATH, { readonly: false });
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  console.log(`Connected to database: ${DB_PATH}`);
} catch (err) {
  console.error(`Failed to open database at ${DB_PATH}: ${err.message}`);
  console.error("Run: python run_pipeline.py --setup-only   to create the database first");
  process.exit(1);
}

// ── Request logger ────────────────────────────────────────────────────────────
app.use((req, res, next) => {
  const start = Date.now();
  res.on("finish", () => {
    console.log(`${req.method} ${req.path} → ${res.statusCode} (${Date.now() - start}ms)`);
  });
  next();
});

// ── Error wrapper ─────────────────────────────────────────────────────────────
function wrap(fn) {
  return (req, res) => {
    try {
      const result = fn(req, res);
      if (result && typeof result.then === "function") {
        result.catch(err => {
          console.error(err);
          res.status(500).json({ error: err.message });
        });
      }
    } catch (err) {
      console.error(err);
      res.status(400).json({ error: err.message });
    }
  };
}


// ════════════════════════════════════════════════════════════
//  ROUTES
// ════════════════════════════════════════════════════════════

// ── Health check ──────────────────────────────────────────────────────────────
app.get("/health", (req, res) => {
  const counts = {};
  const tables = [
    "raw_commodity_prices", "raw_fuel_prices", "fact_tariff_rates",
    "raw_weather_port", "raw_lpi_scores", "pred_rate_alerts"
  ];
  for (const t of tables) {
    try {
      counts[t] = db.prepare(`SELECT COUNT(*) as n FROM ${t}`).get().n;
    } catch {
      counts[t] = null;
    }
  }
  res.json({
    status: "ok",
    db_path: DB_PATH,
    table_counts: counts,
    timestamp: new Date().toISOString(),
  });
});


// ── POST /api/v1/landed-cost ──────────────────────────────────────────────────
/**
 * Calculate landed cost for a single shipment.
 *
 * Body:
 *   origin_port_code  string   e.g. "SHA"
 *   dest_port_code    string   e.g. "RTM"
 *   hs_code           string   e.g. "720839"
 *   cargo_weight_kg   number   e.g. 24000
 *   fob_value_usd     number?  null = estimate from commodity price
 *   incoterm          string?  "CIF" (default) or "FOB"
 *   currency_out      string?  "USD" (default)
 */
app.post("/api/v1/landed-cost", wrap((req, res) => {
  const { origin_port_code, dest_port_code, hs_code, cargo_weight_kg } = req.body;

  if (!origin_port_code || !dest_port_code || !hs_code) {
    return res.status(400).json({
      error: "origin_port_code, dest_port_code, and hs_code are required"
    });
  }
  if (!cargo_weight_kg && !req.body.fob_value_usd) {
    return res.status(400).json({
      error: "Either cargo_weight_kg or fob_value_usd must be provided"
    });
  }

  const startMs = Date.now();
  const result  = calculateLandedCost(db, req.body);
  result.computed_in_ms = Date.now() - startMs;

  res.json(result);
}));


// ── POST /api/v1/landed-cost/compare ─────────────────────────────────────────
/**
 * Compare landed cost across multiple destination ports.
 *
 * Body: same as /landed-cost plus:
 *   alternative_dest_ports  string[]  e.g. ["HAM", "ANR", "PIR"]
 */
app.post("/api/v1/landed-cost/compare", wrap((req, res) => {
  const result = compareRoutes(db, req.body);
  res.json(result);
}));


// ── GET /api/v1/commodities ───────────────────────────────────────────────────
/**
 * Latest price for all commodities, with spike flags.
 * Query params:
 *   spike_only=true   return only commodities with spike_flag=1
 *   limit=50
 */
app.get("/api/v1/commodities", wrap((req, res) => {
  const spikeOnly = req.query.spike_only === "true";
  const limit     = parseInt(req.query.limit) || 50;

  const rows = db.prepare(`
    SELECT commodity_name, date, price_usd, unit,
           mom_change_pct, yoy_change_pct, spike_flag
    FROM (
      SELECT commodity_name, date, price_usd, unit,
             mom_change_pct, yoy_change_pct, spike_flag,
             ROW_NUMBER() OVER (PARTITION BY commodity_name ORDER BY date DESC) AS rn
      FROM fact_commodity_prices
    )
    WHERE rn = 1
    ${spikeOnly ? "AND spike_flag = 1" : ""}
    ORDER BY commodity_name
    LIMIT ?
  `).all(limit);

  res.json({ count: rows.length, commodities: rows });
}));


// ── GET /api/v1/commodities/:name/history ────────────────────────────────────
/**
 * Price history for a specific commodity.
 * Query params:
 *   months=36   how many months of history 
 */

app.get("/api/v1/commodities/:name/history", wrap((req, res) => {
  const months = parseInt(req.query.months) || 36;
  const name   = req.params.name;

  // Try requested date range first
  let rows = db.prepare(`
    SELECT date, price_usd, mom_change_pct, yoy_change_pct, spike_flag
    FROM fact_commodity_prices
    WHERE commodity_name = ?
      AND date >= date('now', '-' || ? || ' months')
    ORDER BY date ASC
  `).all(name, months);

  // If nothing in range, fall back to all available data for this commodity
  if (!rows.length) {
    rows = db.prepare(`
      SELECT date, price_usd, mom_change_pct, yoy_change_pct, spike_flag
      FROM fact_commodity_prices
      WHERE commodity_name = ?
      ORDER BY date ASC
    `).all(name);
  }

  // If still nothing, the commodity genuinely doesn't exist
  if (!rows.length) {
    return res.status(404).json({ error: `Commodity '${name}' not found` });
  }

  res.json({
    commodity_name: name,
    months_requested: months,
    count: rows.length,
    history: rows,
    note: rows[rows.length - 1].date < new Date(Date.now() - months * 30 * 86400000)
      .toISOString().slice(0, 10)
      ? `Data available up to ${rows[rows.length - 1].date} only`
      : undefined,
  });
}));

// ── GET /api/v1/fuel ──────────────────────────────────────────────────────────
/**
 * Latest fuel prices with moving averages.
 */
app.get("/api/v1/fuel", wrap((req, res) => {
  const rows = db.prepare(`
    SELECT fuel_type, date, price_usd, unit, ma_30d, ma_180d, delta_vs_180d_pct
    FROM fact_fuel_prices
    WHERE date = (
      SELECT MAX(date) FROM fact_fuel_prices f2
      WHERE f2.fuel_type = fact_fuel_prices.fuel_type
    )
    ORDER BY fuel_type
  `).all();

  res.json({ fuel_prices: rows });
}));


// ── GET /api/v1/fuel/:type/history ───────────────────────────────────────────
app.get("/api/v1/fuel/:type/history", wrap((req, res) => {
  const weeks = parseInt(req.query.weeks) || 52;
  const rows  = db.prepare(`
    SELECT date, price_usd, ma_30d, ma_180d, delta_vs_180d_pct
    FROM fact_fuel_prices
    WHERE fuel_type = ?
      AND date >= date('now', ? || ' days')
    ORDER BY date ASC
  `).all(req.params.type, `-${weeks * 7}`);

  res.json({ fuel_type: req.params.type, count: rows.length, history: rows });
}));


// ── GET /api/v1/tariffs ───────────────────────────────────────────────────────
/**
 * Tariff lookup.
 * Query params:
 *   reporter  ISO code of importing country  e.g. "USA"
 *   hs_code   HS code prefix                 e.g. "72"
 *   partner   ISO code of exporting country  (optional)
 */
app.get("/api/v1/tariffs", wrap((req, res) => {
  const { reporter, hs_code, partner } = req.query;
  if (!reporter || !hs_code) {
    return res.status(400).json({ error: "reporter and hs_code are required" });
  }

  const rows = db.prepare(`
    SELECT reporter_iso, partner_iso, hs_code, hs_description,
           tariff_type, rate_pct, fta_name, year, rate_change_flag
    FROM fact_tariff_rates
    WHERE reporter_iso = ?
      AND hs_code LIKE ?
      ${partner ? "AND (partner_iso = ? OR partner_iso IS NULL)" : ""}
    ORDER BY tariff_type, rate_pct ASC
    LIMIT 50
  `).all(...[reporter, `${hs_code.slice(0,2)}%`, ...(partner ? [partner] : [])]);

  res.json({ count: rows.length, tariffs: rows });
}));


// ── GET /api/v1/ports ─────────────────────────────────────────────────────────
app.get("/api/v1/ports", wrap((req, res) => {
  const rows = db.prepare(`
    SELECT port_code, port_name, country_iso, lat, lon,
           avg_dwell_days, delay_index, overall_grade
    FROM dim_ports
    ORDER BY port_name
  `).all();
  res.json({ count: rows.length, ports: rows });
}));


// ── GET /api/v1/ports/:code/weather ──────────────────────────────────────────
app.get("/api/v1/ports/:code/weather", wrap((req, res) => {
  const rows = db.prepare(`
    SELECT date, avg_wind_speed_kmh, max_wave_height_m, delay_risk_weather
    FROM fact_port_weather
    WHERE port_code = ?
    ORDER BY date ASC
  `).all(req.params.code.toUpperCase());

  res.json({ port_code: req.params.code.toUpperCase(), forecast: rows });
}));


// ── GET /api/v1/lpi ───────────────────────────────────────────────────────────
/**
 * LPI scores.
 * Query params:
 *   year=2023    filter by year (default latest)
 *   limit=20
 */
app.get("/api/v1/lpi", wrap((req, res) => {
  const year  = req.query.year || null;
  const limit = parseInt(req.query.limit) || 20;

  const rows = db.prepare(`
    SELECT country_name, year, lpi_score, customs_score,
           infrastructure_score, timeliness_score, tracking_score
    FROM raw_lpi_scores
    WHERE (? IS NULL OR year = ?)
    ORDER BY lpi_score DESC
    LIMIT ?
  `).all(year, year, limit);

  res.json({ count: rows.length, lpi: rows });
}));


// ── GET /api/v1/alerts ────────────────────────────────────────────────────────
app.get("/api/v1/alerts", wrap((req, res) => {
  const alerts  = getAlerts(db, {
    limit: parseInt(req.query.limit) || 50,
    unacknowledged_only: req.query.unacknowledged_only === "true",
    type: req.query.type || null,
  });
  const summary = getAlertSummary(db);
  res.json({ summary, alerts });
}));


// ── POST /api/v1/alerts/:id/acknowledge ──────────────────────────────────────
app.post("/api/v1/alerts/:id/acknowledge", wrap((req, res) => {
  const ok = acknowledgeAlert(db, req.params.id);
  res.json({ success: ok });
}));

app.post("/api/v1/alerts/:id/unacknowledge", wrap((req, res) => {
  const result = db.prepare(`
    UPDATE pred_rate_alerts
    SET acknowledged = 0, acknowledged_at = NULL
    WHERE alert_id = ?
  `).run(req.params.id);
  res.json({ success: result.changes > 0 });
}));


// ── GET /api/v1/macro ─────────────────────────────────────────────────────────
app.get("/api/v1/macro", wrap((req, res) => {
  const rows = db.prepare(`
    SELECT series_id, series_name, date, value, source
    FROM raw_macro_indicators
    WHERE date = (
      SELECT MAX(date) FROM raw_macro_indicators m2
      WHERE m2.series_id = raw_macro_indicators.series_id
    )
    ORDER BY series_id
  `).all();
  res.json({ macro: rows });
}));

// Scheduler

app.get("/api/v1/scheduler", wrap((req, res) => {
  res.json({ schedules: scheduler.getStatus(db) });
}));

app.post("/api/v1/scheduler/:source/run", wrap((req, res) => {
  const { source } = req.params;
  const valid = ["weather", "fuel", "commodities", "tariffs"];
  if (!valid.includes(source)) {
    return res.status(400).json({ error: `Unknown source. Valid: ${valid.join(", ")}` });
  }
  setImmediate(() => scheduler.triggerNow(db, source));
  res.json({ success: true, message: `${source} pipeline triggered` });
}));

// ── 404 ───────────────────────────────────────────────────────────────────────
app.use((req, res) => {
  res.status(404).json({
    error: "Not found",
    available_endpoints: [
      "GET  /health",
      "POST /api/v1/landed-cost",
      "POST /api/v1/landed-cost/compare",
      "GET  /api/v1/commodities",
      "GET  /api/v1/commodities/:name/history",
      "GET  /api/v1/fuel",
      "GET  /api/v1/fuel/:type/history",
      "GET  /api/v1/tariffs?reporter=USA&hs_code=72",
      "GET  /api/v1/ports",
      "GET  /api/v1/ports/:code/weather",
      "GET  /api/v1/lpi",
      "GET  /api/v1/alerts",
      "POST /api/v1/alerts/:id/acknowledge",
      "GET  /api/v1/macro",
    ],
  });
});


// ── Start ─────────────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`\nLogistics Intelligence API running on http://localhost:${PORT}`);
  console.log(`Health check: http://localhost:${PORT}/health\n`);
  scheduler.start(db);
});

module.exports = app;