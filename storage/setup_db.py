"""
storage/setup_db.py
Creates and migrates the SQLite database schema for the Logistics Intelligence Platform.
Run once on first setup, then safe to re-run (uses CREATE TABLE IF NOT EXISTS).
"""

import sqlite3
import os
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DB_PATH = os.environ.get("LIP_DB_PATH", "./data/lip.db")


def get_conn(db_path: str = DB_PATH) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


RAW_TABLES = """
-- ─────────────────────────────────────────────────────────────
--  RAW TABLES  (ingested exactly as received, never modified)
-- ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS raw_commodity_prices (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    commodity_name  TEXT NOT NULL,
    date            TEXT NOT NULL,          -- YYYY-MM-01
    price_usd       REAL,
    unit            TEXT,
    source          TEXT DEFAULT 'world_bank_cmo',
    ingested_at     TEXT DEFAULT (datetime('now')),
    UNIQUE(commodity_name, date, source)
);

CREATE TABLE IF NOT EXISTS raw_fuel_prices (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    fuel_type       TEXT NOT NULL,          -- 'brent', 'wti', 'diesel', 'bunker_380'
    region          TEXT,
    date            TEXT NOT NULL,          -- YYYY-MM-DD
    price_usd       REAL,
    unit            TEXT DEFAULT 'per_barrel',
    source          TEXT,                   -- 'eia' or 'fred'
    ingested_at     TEXT DEFAULT (datetime('now')),
    UNIQUE(fuel_type, date, source)
);

CREATE TABLE IF NOT EXISTS raw_macro_indicators (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    series_id       TEXT NOT NULL,          -- FRED series e.g. 'DCOILBRENTEU'
    series_name     TEXT,
    date            TEXT NOT NULL,          -- YYYY-MM-DD
    value           REAL,
    source          TEXT DEFAULT 'fred',
    ingested_at     TEXT DEFAULT (datetime('now')),
    UNIQUE(series_id, date)
);

CREATE TABLE IF NOT EXISTS raw_tariff_rates (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    reporter_iso    TEXT NOT NULL,
    partner_iso     TEXT,                   -- NULL = MFN (applies to all)
    hs_code         TEXT NOT NULL,
    hs_description  TEXT,
    tariff_type     TEXT,                   -- 'MFN_applied', 'preferential', 'bound'
    rate_pct        REAL,
    specific_rate   REAL,
    fta_name        TEXT,
    year            INTEGER NOT NULL,
    source          TEXT DEFAULT 'wto_tao',
    ingested_at     TEXT DEFAULT (datetime('now')),
    UNIQUE(reporter_iso, partner_iso, hs_code, tariff_type, year)
);

CREATE TABLE IF NOT EXISTS raw_weather_port (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    port_code       TEXT NOT NULL,
    date            TEXT NOT NULL,          -- YYYY-MM-DD
    hour            INTEGER DEFAULT 0,
    wind_speed_kmh  REAL,
    wind_direction  REAL,
    wave_height_m   REAL,
    swell_height_m  REAL,
    visibility_km   REAL,
    precipitation_mm REAL,
    condition_code  TEXT,
    source          TEXT DEFAULT 'open_meteo',
    ingested_at     TEXT DEFAULT (datetime('now')),
    UNIQUE(port_code, date, hour, source)
);

CREATE TABLE IF NOT EXISTS raw_ocean_conditions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    lat             REAL NOT NULL,
    lon             REAL NOT NULL,
    date            TEXT NOT NULL,
    wave_height_m   REAL,
    wave_period_s   REAL,
    current_speed_ms REAL,
    sea_surface_temp REAL,
    source          TEXT DEFAULT 'cmems',
    ingested_at     TEXT DEFAULT (datetime('now')),
    UNIQUE(lat, lon, date, source)
);

CREATE TABLE IF NOT EXISTS raw_teu_traffic (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    country_iso     TEXT NOT NULL,
    country_name    TEXT,
    year            INTEGER NOT NULL,
    teu_volume      REAL,
    source          TEXT DEFAULT 'world_bank_wdi',
    ingested_at     TEXT DEFAULT (datetime('now')),
    UNIQUE(country_iso, year)
);

CREATE TABLE IF NOT EXISTS raw_trade_flows (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    reporter_iso    TEXT,
    reporter_name   TEXT,
    partner_iso     TEXT,
    partner_name    TEXT,
    flow_code       TEXT,                   -- 'X' export, 'M' import
    hs_code         TEXT,
    hs_description  TEXT,
    year            INTEGER,
    fob_value_usd   REAL,
    net_weight_kg   REAL,
    source          TEXT DEFAULT 'un_comtrade',
    ingested_at     TEXT DEFAULT (datetime('now')),
    UNIQUE(reporter_iso, partner_iso, hs_code, year, flow_code)
);

CREATE TABLE IF NOT EXISTS raw_vessel_safety (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    imo_number      TEXT NOT NULL,
    vessel_name     TEXT,
    flag_state      TEXT,
    vessel_type     TEXT,
    gross_tonnage   REAL,
    build_year      INTEGER,
    last_inspection TEXT,
    detention_count INTEGER DEFAULT 0,
    deficiency_count INTEGER DEFAULT 0,
    source          TEXT DEFAULT 'equasis',
    ingested_at     TEXT DEFAULT (datetime('now')),
    UNIQUE(imo_number, source)
);

CREATE TABLE IF NOT EXISTS raw_lpi_scores (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    country_name    TEXT NOT NULL,
    country_iso     TEXT,
    year            INTEGER NOT NULL,
    lpi_score       REAL,
    customs_score   REAL,
    infrastructure_score REAL,
    intl_shipments_score REAL,
    logistics_quality_score REAL,
    timeliness_score REAL,
    tracking_score  REAL,
    source          TEXT DEFAULT 'world_bank_lpi',
    ingested_at     TEXT DEFAULT (datetime('now')),
    UNIQUE(country_iso, year)
);
"""

VALIDATED_TABLES = """
-- ─────────────────────────────────────────────────────────────
--  VALIDATED TABLES  (passed schema + range checks)
-- ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS validated_commodity_prices (
    id INTEGER PRIMARY KEY, commodity_name TEXT, date TEXT,
    price_usd REAL, unit TEXT, source TEXT, ingested_at TEXT,
    validation_status TEXT, validation_notes TEXT
);

CREATE TABLE IF NOT EXISTS validated_tariff_rates (
    id INTEGER PRIMARY KEY, reporter_iso TEXT, partner_iso TEXT,
    hs_code TEXT, tariff_type TEXT, rate_pct REAL, year INTEGER,
    source TEXT, ingested_at TEXT,
    validation_status TEXT, validation_notes TEXT
);

CREATE TABLE IF NOT EXISTS validated_fuel_prices (
    id INTEGER PRIMARY KEY, fuel_type TEXT, date TEXT,
    price_usd REAL, unit TEXT, source TEXT, ingested_at TEXT,
    validation_status TEXT, validation_notes TEXT
);
"""

FACT_TABLES = """
-- ─────────────────────────────────────────────────────────────
--  FACT TABLES  (cleaned, normalised, analytics-ready)
-- ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fact_commodity_prices (
    commodity_id        TEXT NOT NULL,
    commodity_name      TEXT NOT NULL,
    date                TEXT NOT NULL,
    price_usd           REAL NOT NULL,
    unit                TEXT,
    mom_change_pct      REAL,
    yoy_change_pct      REAL,
    spike_flag          INTEGER DEFAULT 0,  -- 1 if MoM>15% or YoY>40%
    source              TEXT,
    updated_at          TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (commodity_id, date)
);

CREATE TABLE IF NOT EXISTS fact_tariff_rates (
    tariff_id           TEXT PRIMARY KEY,   -- hash of key fields
    reporter_iso        TEXT NOT NULL,
    partner_iso         TEXT,
    hs_code             TEXT NOT NULL,
    hs_description      TEXT,
    tariff_type         TEXT NOT NULL,
    rate_pct            REAL,
    specific_rate       REAL,
    fta_name            TEXT,
    year                INTEGER NOT NULL,
    hs_precision        INTEGER DEFAULT 6,  -- 6=exact, 4=fallback
    prev_rate_pct       REAL,
    rate_change_flag    INTEGER DEFAULT 0,
    updated_at          TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS fact_fuel_prices (
    fuel_type           TEXT NOT NULL,
    date                TEXT NOT NULL,
    price_usd           REAL NOT NULL,
    unit                TEXT,
    ma_30d              REAL,               -- 30-day moving average
    ma_180d             REAL,               -- 180-day moving average
    delta_vs_180d_pct   REAL,               -- % deviation from 180d avg
    source              TEXT,
    updated_at          TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (fuel_type, date)
);

CREATE TABLE IF NOT EXISTS fact_port_weather (
    port_code           TEXT NOT NULL,
    date                TEXT NOT NULL,
    avg_wind_speed_kmh  REAL,
    max_wave_height_m   REAL,
    delay_risk_weather  INTEGER DEFAULT 0,  -- 1 if conditions cause delays
    source              TEXT,
    updated_at          TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (port_code, date)
);

CREATE TABLE IF NOT EXISTS fact_teu_traffic (
    country_iso         TEXT NOT NULL,
    country_name        TEXT,
    year                INTEGER NOT NULL,
    teu_volume          REAL,
    yoy_growth_pct      REAL,
    source              TEXT,
    updated_at          TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (country_iso, year)
);
"""

DIM_TABLES = """
-- ─────────────────────────────────────────────────────────────
--  DIMENSION TABLES
-- ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dim_ports (
    port_code           TEXT PRIMARY KEY,
    port_name           TEXT NOT NULL,
    country_iso         TEXT,
    country_name        TEXT,
    lat                 REAL,
    lon                 REAL,
    annual_teu          REAL,
    max_vessel_class    TEXT,
    avg_dwell_days      REAL,
    delay_index         REAL,
    overall_grade       TEXT,
    updated_at          TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS dim_countries (
    iso_code            TEXT PRIMARY KEY,
    country_name        TEXT NOT NULL,
    region              TEXT,
    lpi_score           REAL,
    lpi_rank            INTEGER,
    customs_score       REAL,
    infrastructure_score REAL,
    logistics_score     REAL,
    timeliness_score    REAL,
    updated_at          TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS dim_commodities (
    commodity_id        TEXT PRIMARY KEY,
    commodity_name      TEXT NOT NULL,
    category            TEXT,               -- 'energy', 'metals', 'agriculture', 'timber'
    unit                TEXT,
    hs_chapters         TEXT,               -- JSON array of related HS chapters
    price_sensitivity   TEXT                -- 'high', 'medium', 'low' — how much it moves
);

CREATE TABLE IF NOT EXISTS dim_hs_codes (
    hs_code             TEXT PRIMARY KEY,
    hs_description      TEXT,
    hs_chapter          TEXT,               -- first 2 digits
    chapter_description TEXT,
    section             TEXT,
    related_commodity   TEXT                -- links to dim_commodities.commodity_id
);
"""

PREDICTION_TABLES = """
-- ─────────────────────────────────────────────────────────────
--  PREDICTION OUTPUT TABLES
-- ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS pred_landed_cost_estimates (
    estimate_id             TEXT PRIMARY KEY,
    origin_port_code        TEXT,
    dest_port_code          TEXT,
    hs_code                 TEXT,
    cargo_weight_kg         REAL,
    cargo_value_fob         REAL,
    freight_cost_usd        REAL,
    import_duty_usd         REAL,
    tariff_rate_pct         REAL,
    tariff_type_used        TEXT,
    fta_name                TEXT,
    insurance_usd           REAL,
    port_handling_usd       REAL,
    customs_brokerage_usd   REAL,
    total_landed_cost_usd   REAL,
    confidence_interval_pct REAL,
    commodity_price_flag    INTEGER DEFAULT 0,
    tariff_change_flag      INTEGER DEFAULT 0,
    fta_opportunity_flag    INTEGER DEFAULT 0,
    fuel_spike_flag         INTEGER DEFAULT 0,
    alerts_json             TEXT,           -- JSON array of alert objects
    model_version           TEXT,
    requested_at            TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS pred_rate_alerts (
    alert_id                TEXT PRIMARY KEY,
    alert_type              TEXT NOT NULL,  -- 'COMMODITY_SPIKE', 'TARIFF_CHANGE', 'FTA_OPPORTUNITY', 'FUEL_SPIKE'
    severity                TEXT,           -- 'INFO', 'WARNING', 'ALERT'
    entity_id               TEXT,           -- commodity_id or route_id etc.
    message                 TEXT,
    detail_json             TEXT,
    triggered_at            TEXT DEFAULT (datetime('now')),
    acknowledged            INTEGER DEFAULT 0,
    acknowledged_at         TEXT
);
"""

INDEXES = """
-- ─────────────────────────────────────────────────────────────
--  INDEXES  (query performance)
-- ─────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_raw_commodity_date ON raw_commodity_prices(date);
CREATE INDEX IF NOT EXISTS idx_raw_commodity_name ON raw_commodity_prices(commodity_name);
CREATE INDEX IF NOT EXISTS idx_raw_tariff_reporter ON raw_tariff_rates(reporter_iso, partner_iso);
CREATE INDEX IF NOT EXISTS idx_raw_tariff_hs ON raw_tariff_rates(hs_code);
CREATE INDEX IF NOT EXISTS idx_raw_fuel_date ON raw_fuel_prices(date, fuel_type);
CREATE INDEX IF NOT EXISTS idx_raw_weather_port_date ON raw_weather_port(port_code, date);
CREATE INDEX IF NOT EXISTS idx_fact_commodity ON fact_commodity_prices(commodity_id, date);
CREATE INDEX IF NOT EXISTS idx_fact_tariff_lookup ON fact_tariff_rates(reporter_iso, partner_iso, hs_code);
CREATE INDEX IF NOT EXISTS idx_fact_fuel_date ON fact_fuel_prices(fuel_type, date);
CREATE INDEX IF NOT EXISTS idx_pred_estimates_route ON pred_landed_cost_estimates(origin_port_code, dest_port_code);
CREATE INDEX IF NOT EXISTS idx_pred_alerts_type ON pred_rate_alerts(alert_type, triggered_at);
"""


def setup(db_path: str = DB_PATH):
    log.info(f"Setting up database at: {db_path}")
    conn = get_conn(db_path)
    cur = conn.cursor()

    for name, ddl in [
        ("raw tables", RAW_TABLES),
        ("validated tables", VALIDATED_TABLES),
        ("fact tables", FACT_TABLES),
        ("dimension tables", DIM_TABLES),
        ("prediction tables", PREDICTION_TABLES),
    ]:
        log.info(f"Creating {name}...")
        for stmt in ddl.strip().split(";"):
            # Strip whitespace and leading comment lines
            lines = [l for l in stmt.strip().splitlines() if not l.strip().startswith("--")]
            stmt = "\n".join(lines).strip()
            if stmt:
                cur.execute(stmt)
        conn.commit()  # commit after each block so indexes can find tables

    log.info("Creating indexes...")
    for stmt in INDEXES.strip().split(";"):
        stmt = stmt.strip()
        if stmt and not stmt.startswith("--"):
            try:
                cur.execute(stmt)
            except Exception as e:
                log.warning(f"Index creation skipped: {e}")

    conn.commit()
    conn.close()
    log.info("Database setup complete.")


if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else DB_PATH
    setup(path)
