"""
ingestion/ingest_weather_and_ports.py
Fetches weather forecasts for major ports via Open-Meteo (no API key needed).
Also loads LPI scores and TEU traffic from World Bank API.

Run:  python ingestion/ingest_weather_and_ports.py
"""

import os
import sys
import logging
import requests
import sqlite3
import json
import time
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))
from storage.setup_db import get_conn

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DB_PATH = os.environ.get("LIP_DB_PATH", "./data/lip.db")

# Load port config
CONFIG_PATH = Path(__file__).parent.parent / "config" / "sources.json"
with open(CONFIG_PATH) as f:
    CONFIG = json.load(f)

PORTS = CONFIG["ports"]["major_ports"]

# ── Open-Meteo Marine API ─────────────────────────────────────────────────────
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
MARINE_API_URL = "https://marine-api.open-meteo.com/v1/marine"


def fetch_port_weather(port: dict) -> dict | None:
    """Fetch 7-day forecast for a port. No API key needed."""
    params = {
        "latitude": port["lat"],
        "longitude": port["lon"],
        "daily": [
            "wind_speed_10m_max",
            "wind_direction_10m_dominant",
            "precipitation_sum",
            "visibility_mean",
        ],
        "timezone": "UTC",
        "forecast_days": 7,
    }
    try:
        resp = requests.get(OPEN_METEO_URL, params=params, timeout=15)
        if resp.status_code == 200:
            return resp.json()
        log.warning(f"Open-Meteo returned {resp.status_code} for {port['code']}")
        return None
    except Exception as e:
        log.warning(f"Weather fetch failed for {port['code']}: {e}")
        return None


def fetch_marine_conditions(port: dict) -> dict | None:
    """Fetch marine forecasts (wave height, swell) via Open-Meteo Marine API."""
    params = {
        "latitude": port["lat"],
        "longitude": port["lon"],
        "daily": [
            "wave_height_max",
            "wave_direction_dominant",
            "wave_period_max",
            "swell_wave_height_max",
        ],
        "timezone": "UTC",
        "forecast_days": 7,
    }
    try:
        resp = requests.get(MARINE_API_URL, params=params, timeout=15)
        if resp.status_code == 200:
            return resp.json()
        return None
    except Exception as e:
        log.warning(f"Marine fetch failed for {port['code']}: {e}")
        return None


def upsert_weather(port_code: str, weather_data: dict, marine_data: dict,
                   conn: sqlite3.Connection) -> int:
    inserted = 0
    cur = conn.cursor()

    if not weather_data or "daily" not in weather_data:
        return 0

    daily = weather_data["daily"]
    dates = daily.get("time", [])
    winds = daily.get("wind_speed_10m_max", [None] * len(dates))
    dirs = daily.get("wind_direction_10m_dominant", [None] * len(dates))
    precips = daily.get("precipitation_sum", [None] * len(dates))

    marine_daily = marine_data.get("daily", {}) if marine_data else {}
    waves = marine_daily.get("wave_height_max", [None] * len(dates))
    swells = marine_daily.get("swell_wave_height_max", [None] * len(dates))

    for i, date in enumerate(dates):
        wind = winds[i] if i < len(winds) else None
        wave = waves[i] if i < len(waves) else None
        swell = swells[i] if i < len(swells) else None
        precip = precips[i] if i < len(precips) else None

        cur.execute("""
            INSERT OR IGNORE INTO raw_weather_port
                (port_code, date, wind_speed_kmh, wind_direction, wave_height_m,
                 swell_height_m, precipitation_mm, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'open_meteo')
        """, (port_code, date, wind, dirs[i] if i < len(dirs) else None,
              wave, swell, precip))
        if cur.rowcount > 0:
            inserted += 1

    conn.commit()
    return inserted


def build_fact_port_weather(conn: sqlite3.Connection):
    """Aggregate weather into daily delay-risk scores per port."""
    conn.execute("DELETE FROM fact_port_weather")
    conn.execute("""
        INSERT INTO fact_port_weather
            (port_code, date, avg_wind_speed_kmh, max_wave_height_m, delay_risk_weather, source)
        SELECT
            port_code,
            date,
            ROUND(AVG(wind_speed_kmh), 1),
            ROUND(MAX(wave_height_m), 2),
            CASE
                WHEN MAX(wave_height_m) > 4.0 THEN 1
                WHEN AVG(wind_speed_kmh) > 60 THEN 1
                ELSE 0
            END AS delay_risk_weather,
            'open_meteo'
        FROM raw_weather_port
        GROUP BY port_code, date
    """)
    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM fact_port_weather").fetchone()[0]
    risks = conn.execute("SELECT COUNT(*) FROM fact_port_weather WHERE delay_risk_weather = 1").fetchone()[0]
    log.info(f"fact_port_weather: {count} records, {risks} high-risk weather days")


def load_lpi_from_file(xlsx_path: str, conn: sqlite3.Connection) -> int:
    """Load LPI data from the local Excel file already downloaded."""
    import pandas as pd

    if not Path(xlsx_path).exists():
        log.warning(f"LPI file not found: {xlsx_path}")
        return 0

    year_map = {"2007": 2007, "2010": 2010, "2012": 2012,
                "2014": 2014, "2016": 2016, "2018": 2018, "2023": 2023}
    inserted = 0
    cur = conn.cursor()

    for sheet_name, year in year_map.items():
        try:
            raw = pd.read_excel(xlsx_path, sheet_name=sheet_name)

            if year == 2023:
                # Clean format
                raw.columns = [str(c).strip() for c in raw.columns]
                for _, row in raw.iterrows():
                    try:
                        cur.execute("""
                            INSERT OR IGNORE INTO raw_lpi_scores
                                (country_name, year, lpi_score, customs_score,
                                 infrastructure_score, timeliness_score, tracking_score, source)
                            VALUES (?, ?, ?, ?, ?, ?, ?, 'world_bank_lpi')
                        """, (
                            str(row.get("Economy", "")).strip(),
                            year,
                            float(row.get("LPI Score", 0)) if pd.notna(row.get("LPI Score")) else None,
                            float(row.get("Customs Score", 0)) if pd.notna(row.get("Customs Score")) else None,
                            float(row.get("Infrastructure Score", 0)) if pd.notna(row.get("Infrastructure Score")) else None,
                            float(row.get("Timeliness Score", 0)) if pd.notna(row.get("Timeliness Score")) else None,
                            float(row.get("Tracking and Tracing Score", 0)) if pd.notna(row.get("Tracking and Tracing Score")) else None,
                        ))
                        if cur.rowcount > 0:
                            inserted += 1
                    except Exception:
                        pass
            else:
                # Older format — header on row 1
                raw2 = raw.iloc[1:].reset_index(drop=True)
                for _, row in raw2.iterrows():
                    try:
                        name = str(row.iloc[0]).strip()
                        score = float(row.iloc[2]) if pd.notna(row.iloc[2]) else None
                        if not name or name == "nan" or score is None:
                            continue
                        cur.execute("""
                            INSERT OR IGNORE INTO raw_lpi_scores
                                (country_name, year, lpi_score, source)
                            VALUES (?, ?, ?, 'world_bank_lpi')
                        """, (name, year, score))
                        if cur.rowcount > 0:
                            inserted += 1
                    except Exception:
                        pass
        except Exception as e:
            log.warning(f"Could not parse LPI sheet {sheet_name}: {e}")

    conn.commit()
    log.info(f"LPI data: {inserted} records loaded from {xlsx_path}")
    return inserted


def seed_dim_countries(conn: sqlite3.Connection):
    """Build dim_countries from raw LPI data."""
    conn.execute("DELETE FROM dim_countries")
    conn.execute("""
        INSERT OR REPLACE INTO dim_countries (iso_code, country_name, lpi_score, timeliness_score)
        SELECT
            UPPER(SUBSTR(country_name, 1, 3)) as iso_code,
            country_name,
            AVG(lpi_score),
            AVG(timeliness_score)
        FROM raw_lpi_scores
        WHERE year = (SELECT MAX(year) FROM raw_lpi_scores)
          AND country_name IS NOT NULL
        GROUP BY country_name
    """)
    conn.commit()

    # Seed dim_ports from config
    for port in PORTS:
        conn.execute("""
            INSERT OR IGNORE INTO dim_ports
                (port_code, port_name, country_iso, lat, lon)
            VALUES (?, ?, '', ?, ?)
        """, (port["code"], port["name"], port["lat"], port["lon"]))
    conn.commit()
    log.info(f"dim_countries seeded. dim_ports seeded with {len(PORTS)} ports.")


def run(db_path: str = DB_PATH, lpi_file: str = None):
    conn = get_conn(db_path)

    # ── Weather ──────────────────────────────────────────────────────────────
    log.info(f"Fetching weather for {len(PORTS)} ports...")
    total_weather = 0
    for port in PORTS:
        weather = fetch_port_weather(port)
        marine = fetch_marine_conditions(port)
        n = upsert_weather(port["code"], weather, marine, conn)
        log.info(f"  {port['code']} ({port['name']}): {n} weather records")
        total_weather += n
        time.sleep(0.2)  # Rate limit: be respectful

    if total_weather > 0:
        build_fact_port_weather(conn)

    # ── LPI from local file ──────────────────────────────────────────────────
    lpi_path = lpi_file or "/mnt/project/International_LPI_from_2007_to_2023_0.xlsx"
    if Path(lpi_path).exists():
        load_lpi_from_file(lpi_path, conn)
        seed_dim_countries(conn)
    else:
        log.warning(f"LPI file not at {lpi_path} — skipping. Pass --lpi-file to specify path.")

    conn.close()
    log.info("Weather + port data ingestion complete.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=DB_PATH)
    parser.add_argument("--lpi-file", default=None, help="Path to LPI .xlsx file")
    args = parser.parse_args()
    run(db_path=args.db, lpi_file=args.lpi_file)
