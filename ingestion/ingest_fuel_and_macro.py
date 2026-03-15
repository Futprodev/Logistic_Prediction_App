"""
ingestion/ingest_fuel_and_macro.py
Ingests fuel prices and macro indicators.
FRED bypassed entirely — uses EIA, World Bank API, and ECB instead.
All three confirmed HTTP 200 in diagnostics.

Sources:
  EIA API        → weekly Brent + WTI (free key at eia.gov/opendata)
  World Bank API → annual commodity prices + macro (no key needed)
  ECB Data Portal→ daily EUR/USD rate (no key needed)

Run:
    python ingestion/ingest_fuel_and_macro.py
    set EIA_API_KEY=your_key && python ingestion/ingest_fuel_and_macro.py
"""

import os
import sys
import logging
import requests
import sqlite3
import time
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))
from storage.setup_db import get_conn

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DB_PATH = os.environ.get("LIP_DB_PATH", "./data/lip.db")
EIA_KEY = os.environ.get("EIA_API_KEY", "DEMO_KEY")
WB_BASE = "https://api.worldbank.org/v2"

# ── World Bank indicators (confirmed HTTP 200) ────────────────────────────────
# Format: "label": (wb_indicator_code, series_id, unit, category)
WB_INDICATORS = {
    "Brent Crude":       ("PUIB_USD",          "brent",          "per_barrel", "fuel"),
    "WTI Crude":         ("PUIL_USD",           "wti",            "per_barrel", "fuel"),
    "Natural Gas US":    ("PNGAS_USD",          "natgas_us",      "per_mmbtu",  "fuel"),
    "Coal Australia":    ("PCOAL_USD",          "coal_aus",       "per_tonne",  "fuel"),
    "Aluminum":          ("PALUM_USD",          "aluminum",       "per_tonne",  "commodity"),
    "Copper":            ("PCOPP_USD",          "copper",         "per_tonne",  "commodity"),
    "Iron Ore":          ("PIORECR_USD",        "iron_ore",       "per_tonne",  "commodity"),
    "GDP Growth World":  ("NY.GDP.MKTP.KD.ZG",  "gdp_growth_wld", "pct",        "macro"),
    "Trade % GDP":       ("NE.TRD.GNFS.ZS",     "trade_pct_gdp",  "pct",        "macro"),
    "CPI World":         ("FP.CPI.TOTL.ZG",     "cpi_world",      "pct",        "macro"),
    "FX USD Official":   ("PA.NUS.FCRF",         "fx_usd_official","rate",       "macro"),
}

# ── EIA products ──────────────────────────────────────────────────────────────
EIA_PRODUCTS = {
    "EPCBRENT": ("brent", "per_barrel"),
    "EPCWTI":   ("wti",   "per_barrel"),
}


# ─────────────────────────────────────────────────────────────────────────────
#  FETCH FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def fetch_eia(product_code: str, weeks: int = 156) -> list:
    if EIA_KEY == "DEMO_KEY":
        log.info("  EIA using DEMO_KEY (limited). Get free key: eia.gov/opendata/register.php")
    try:
        resp = requests.get(
            "https://api.eia.gov/v2/petroleum/pri/spt/data/",
            params={
                "api_key": EIA_KEY,
                "frequency": "weekly",
                "data[0]": "value",
                "facets[product][]": product_code,
                "sort[0][column]": "period",
                "sort[0][direction]": "desc",
                "length": weeks,
            },
            timeout=20
        )
        if resp.status_code != 200:
            log.warning(f"  EIA {product_code}: HTTP {resp.status_code}")
            return []
        records = resp.json().get("response", {}).get("data", [])
        return records
    except Exception as e:
        log.warning(f"  EIA {product_code} failed: {e}")
        return []


def fetch_wb(indicator: str, mrv: int = 30) -> list:
    try:
        resp = requests.get(
            f"{WB_BASE}/country/WLD/indicator/{indicator}",
            params={"format": "json", "mrv": mrv, "per_page": mrv},
            timeout=15
        )
        if resp.status_code != 200:
            log.warning(f"  WB {indicator}: HTTP {resp.status_code}")
            return []
        data = resp.json()
        if len(data) < 2 or not data[1]:
            return []
        return [r for r in data[1] if r.get("value") is not None]
    except Exception as e:
        log.warning(f"  WB {indicator} failed: {e}")
        return []


def fetch_ecb_eurusd(days: int = 730) -> list:
    """ECB Statistical Data Warehouse — no key, confirmed accessible."""
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    end   = datetime.now().strftime("%Y-%m-%d")
    try:
        resp = requests.get(
            "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A",
            params={"format": "jsondata", "startPeriod": start,
                    "endPeriod": end, "detail": "dataonly"},
            headers={"Accept": "application/json"},
            timeout=15
        )
        if resp.status_code != 200:
            log.warning(f"  ECB API: HTTP {resp.status_code}")
            return []
        data = resp.json()
        series = data["dataSets"][0]["series"]
        dates  = data["structure"]["dimensions"]["observation"][0]["values"]
        records = []
        for _, s in series.items():
            for idx, vals in s.get("observations", {}).items():
                if vals[0] is not None:
                    records.append({"date": dates[int(idx)]["id"], "value": vals[0]})
        return records
    except Exception as e:
        log.warning(f"  ECB EUR/USD failed: {e}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
#  UPSERT FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def upsert_fuel(records: list, fuel_type: str, unit: str,
                source: str, conn: sqlite3.Connection) -> int:
    inserted = 0
    cur = conn.cursor()
    for rec in records:
        date  = str(rec.get("period") or rec.get("date") or "")
        value = rec.get("value")
        if not date or value is None:
            continue
        # WB annual: "2023" → "2023-01-01"
        if len(date) == 4 and date.isdigit():
            date = f"{date}-01-01"
        try:
            cur.execute("""
                INSERT OR IGNORE INTO raw_fuel_prices
                    (fuel_type, date, price_usd, unit, source)
                VALUES (?, ?, ?, ?, ?)
            """, (fuel_type, date, float(value), unit, source))
            if cur.rowcount > 0:
                inserted += 1
        except Exception:
            pass
    conn.commit()
    return inserted


def upsert_macro(records: list, series_id: str,
                  name: str, conn: sqlite3.Connection) -> int:
    inserted = 0
    cur = conn.cursor()
    for rec in records:
        date  = str(rec.get("date") or rec.get("period") or "")
        value = rec.get("value")
        if not date or value is None:
            continue
        if len(date) == 4 and date.isdigit():
            date = f"{date}-01-01"
        try:
            cur.execute("""
                INSERT OR IGNORE INTO raw_macro_indicators
                    (series_id, series_name, date, value, source)
                VALUES (?, ?, ?, ?, 'world_bank_api')
            """, (series_id, name, date, float(value)))
            if cur.rowcount > 0:
                inserted += 1
        except Exception:
            pass
    conn.commit()
    return inserted


def upsert_fx(records: list, pair: str, source: str, conn: sqlite3.Connection) -> int:
    inserted = 0
    cur = conn.cursor()
    for rec in records:
        date  = str(rec.get("date") or rec.get("period") or "")
        value = rec.get("value")
        if not date or value is None:
            continue
        try:
            cur.execute("""
                INSERT OR IGNORE INTO raw_macro_indicators
                    (series_id, series_name, date, value, source)
                VALUES (?, ?, ?, ?, ?)
            """, (f"FX_{pair}", f"FX rate {pair}", date, float(value), source))
            if cur.rowcount > 0:
                inserted += 1
        except Exception:
            pass
    conn.commit()
    return inserted


# ─────────────────────────────────────────────────────────────────────────────
#  FACT TABLE + ALERTS
# ─────────────────────────────────────────────────────────────────────────────

def build_fact_fuel(conn: sqlite3.Connection):
    conn.execute("DELETE FROM fact_fuel_prices")
    conn.execute("""
        INSERT OR REPLACE INTO fact_fuel_prices
            (fuel_type, date, price_usd, unit, ma_30d, ma_180d, delta_vs_180d_pct, source)
        WITH ordered AS (
            SELECT fuel_type, date, price_usd, unit, source,
                AVG(price_usd) OVER (
                    PARTITION BY fuel_type ORDER BY date
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) AS ma_30d,
                AVG(price_usd) OVER (
                    PARTITION BY fuel_type ORDER BY date
                    ROWS BETWEEN 179 PRECEDING AND CURRENT ROW
                ) AS ma_180d
            FROM raw_fuel_prices
            WHERE price_usd IS NOT NULL AND price_usd > 0
        )
        SELECT fuel_type, date, price_usd, unit,
            ROUND(ma_30d, 2), ROUND(ma_180d, 2),
            CASE WHEN ma_180d > 0
                THEN ROUND((price_usd - ma_180d) / ma_180d * 100, 2)
                ELSE NULL END,
            source
        FROM ordered
    """)
    conn.commit()
    count  = conn.execute("SELECT COUNT(*) FROM fact_fuel_prices").fetchone()[0]
    spikes = conn.execute(
        "SELECT COUNT(*) FROM fact_fuel_prices WHERE delta_vs_180d_pct > 20"
    ).fetchone()[0]
    log.info(f"fact_fuel_prices: {count} records, {spikes} spike records (>20% above 180d avg)")


def generate_fuel_alerts(conn: sqlite3.Connection):
    import hashlib
    spikes = conn.execute("""
        SELECT fuel_type, date, price_usd, ma_180d, delta_vs_180d_pct
        FROM fact_fuel_prices
        WHERE delta_vs_180d_pct > 20
          AND date >= date('now', '-30 days')
        ORDER BY date DESC LIMIT 10
    """).fetchall()
    for fuel, date, price, avg, delta in spikes:
        msg = (f"Fuel spike: {fuel} at ${price:.2f} — "
               f"{delta:+.1f}% above 6-month avg (${avg:.2f})")
        aid = hashlib.md5(f"FUEL_SPIKE_{fuel}_{date}".encode()).hexdigest()
        conn.execute("""
            INSERT OR IGNORE INTO pred_rate_alerts
                (alert_id, alert_type, severity, entity_id, message, detail_json)
            VALUES (?, 'FUEL_SPIKE', 'WARNING', ?, ?, json_object(
                'fuel_type',?,'date',?,'price',?,'avg_180d',?,'delta_pct',?))
        """, (aid, fuel, msg, fuel, date, price, avg, delta))
    conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────

def run(db_path: str = DB_PATH):
    conn  = get_conn(db_path)
    total = 0

    # 1. EIA weekly fuel prices
    log.info("── EIA weekly fuel prices ──────────────────────────────")
    for product_code, (fuel_type, unit) in EIA_PRODUCTS.items():
        records = fetch_eia(product_code, weeks=156)
        n = upsert_fuel(records, fuel_type, unit, "eia", conn)
        log.info(f"  {fuel_type}: {n} new records (EIA)")
        total += n
        time.sleep(0.3)

    # 2. World Bank commodity + macro
    log.info("── World Bank indicators ───────────────────────────────")
    for label, (wb_code, series_id, unit, category) in WB_INDICATORS.items():
        records = fetch_wb(wb_code, mrv=30)
        if not records:
            log.warning(f"  {label}: no data")
            continue
        if category == "fuel":
            n = upsert_fuel(records, series_id, unit, "world_bank_api", conn)
        else:
            n = upsert_macro(records, series_id, label, conn)
        log.info(f"  {label}: {n} new records")
        total += n
        time.sleep(0.2)

    # 3. ECB EUR/USD daily
    log.info("── ECB EUR/USD exchange rate ────────────────────────────")
    eurusd = fetch_ecb_eurusd(days=730)
    n = upsert_fx(eurusd, "EURUSD", "ecb", conn)
    log.info(f"  EUR/USD: {n} new records (ECB)")
    total += n

    # 4. Rebuild fact tables
    if total > 0:
        log.info("── Rebuilding fact tables ──────────────────────────────")
        build_fact_fuel(conn)
        generate_fuel_alerts(conn)
    else:
        log.info("No new records inserted — fact tables unchanged")

    # Summary
    fuel_total  = conn.execute("SELECT COUNT(*) FROM raw_fuel_prices").fetchone()[0]
    macro_total = conn.execute("SELECT COUNT(*) FROM raw_macro_indicators").fetchone()[0]
    log.info(f"── Summary ─────────────────────────────────────────────")
    log.info(f"  raw_fuel_prices:       {fuel_total:>6} total rows")
    log.info(f"  raw_macro_indicators:  {macro_total:>6} total rows")
    log.info(f"  New records this run:  {total:>6}")
    conn.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=DB_PATH)
    args = parser.parse_args()
    run(db_path=args.db)