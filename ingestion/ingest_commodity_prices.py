"""
ingestion/ingest_commodity_prices.py
Downloads and ingests the World Bank Commodity Price Monitor (CMO).
Covers 70+ commodities monthly — steel, aluminum, copper, lumber, energy.

Run:  python ingestion/ingest_commodity_prices.py
      python ingestion/ingest_commodity_prices.py --db ./data/lip.db --backfill
"""

import os
import sys
import logging
import argparse
import hashlib
import requests
import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime
from io import BytesIO

sys.path.insert(0, str(Path(__file__).parent.parent))
from storage.setup_db import get_conn

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────────────

CMO_URL = (
    "https://thedocs.worldbank.org/en/doc/"
    "18675f1d1639c7a34d463f59263ba0a2-0050012025/related/CMO-Historical-Data-Monthly.xlsx"
)

# Commodities we specifically care about for the Landed Cost Calculator
PRIORITY_COMMODITIES = {
    "Steel, HRC": "steel_hrc",
    "Steel, CRC": "steel_crc",
    "Steel, Rebar": "steel_rebar",
    "Aluminum": "aluminum",
    "Copper": "copper",
    "Zinc": "zinc",
    "Nickel": "nickel",
    "Lead": "lead",
    "Tin": "tin",
    "Lumber, soft (logs)": "lumber_soft",
    "Rubber, SGP/MYS": "rubber",
    "Cotton, A Index": "cotton",
    "Coal, Australia": "coal_aus",
    "Coal, South Africa": "coal_zaf",
    "Crude oil, average": "crude_avg",
    "Crude oil, Brent": "crude_brent",
    "Crude oil, WTI": "crude_wti",
    "Natural gas, US": "natgas_us",
    "Natural gas, Europe": "natgas_eu",
    "LNG, Japan": "lng_japan",
}

DB_PATH = os.environ.get("LIP_DB_PATH", "./data/lip.db")


def download_cmo(url: str = CMO_URL) -> pd.ExcelFile:
    log.info(f"Downloading CMO data from World Bank...")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    log.info(f"Downloaded {len(resp.content) / 1024:.1f} KB")
    return pd.ExcelFile(BytesIO(resp.content))


def parse_monthly_sheet(xl: pd.ExcelFile) -> pd.DataFrame:
    """Parse the 'Monthly Prices' sheet from the CMO Excel file."""
    sheet_names = xl.sheet_names
    log.info(f"Sheets in CMO file: {sheet_names}")

    # The monthly sheet is usually named 'Monthly Prices' or similar
    monthly_sheet = next(
        (s for s in sheet_names if "monthly" in s.lower() or "price" in s.lower()),
        sheet_names[0]
    )
    log.info(f"Using sheet: {monthly_sheet}")

    # Read raw — header is usually on row 4 or 5 (World Bank format)
    raw = xl.parse(monthly_sheet, header=None)

    # Find the row with date markers (years like 1960, 1961...)
    # or 'Jan', 'Feb' patterns
    header_row = None
    for i, row in raw.iterrows():
        row_vals = [str(v) for v in row if pd.notna(v)]
        if any(str(v).strip().isdigit() and 1950 <= int(str(v).strip()) <= 2030
               for v in row if pd.notna(v)):
            if sum(1 for v in row if pd.notna(v)) > 10:
                header_row = i
                break

    if header_row is None:
        # Fallback: assume row 4
        header_row = 4

    log.info(f"Header row detected at index: {header_row}")

    # Re-read with proper header
    df = xl.parse(monthly_sheet, header=header_row)
    return df


def normalise_to_long(df: pd.DataFrame) -> pd.DataFrame:
    """Convert wide-format CMO data to long format (commodity, date, price)."""
    records = []

    # First column is commodity names
    commodity_col = df.columns[0]

    for _, row in df.iterrows():
        commodity = str(row[commodity_col]).strip() if pd.notna(row[commodity_col]) else None
        if not commodity or commodity in ("nan", "", "Commodity"):
            continue

        # All other columns are date-keyed
        for col in df.columns[1:]:
            val = row[col]
            if pd.isna(val):
                continue
            try:
                price = float(val)
            except (ValueError, TypeError):
                continue

            # Parse the column header as a date
            date_str = str(col).strip()
            try:
                # Handle formats like '2024M01', '2024-01', 'Jan-24', '2024/01'
                if "M" in date_str and len(date_str) == 7:
                    # '2024M01' format
                    dt = datetime.strptime(date_str, "%YM%m")
                elif "-" in date_str and len(date_str) == 7:
                    dt = datetime.strptime(date_str, "%Y-%m")
                elif "/" in date_str:
                    dt = datetime.strptime(date_str, "%Y/%m")
                else:
                    # Try pandas date parsing
                    dt = pd.to_datetime(date_str, errors="coerce")
                    if pd.isna(dt):
                        continue
                date_out = dt.strftime("%Y-%m-01")
            except Exception:
                continue

            records.append({
                "commodity_name": commodity,
                "date": date_out,
                "price_usd": price,
            })

    df_long = pd.DataFrame(records)
    log.info(f"Parsed {len(df_long)} price records for {df_long['commodity_name'].nunique()} commodities")
    return df_long


def infer_unit(commodity_name: str) -> str:
    name_lower = commodity_name.lower()
    if any(x in name_lower for x in ("crude", "oil", "petroleum")):
        return "per_barrel"
    elif any(x in name_lower for x in ("gas", "lng", "lng")):
        return "per_mmbtu"
    elif any(x in name_lower for x in ("steel", "aluminum", "copper", "zinc", "nickel", "lead", "tin", "coal")):
        return "per_metric_tonne"
    elif any(x in name_lower for x in ("lumber", "timber", "log")):
        return "per_cubic_metre"
    elif "cotton" in name_lower:
        return "per_kg"
    elif "rubber" in name_lower:
        return "per_kg"
    return "per_unit"


def upsert_to_db(df: pd.DataFrame, conn: sqlite3.Connection, backfill: bool = False):
    """Insert new records into raw_commodity_prices. Skip duplicates."""
    if not backfill:
        # Only last 24 months if not backfilling
        cutoff = pd.Timestamp.now() - pd.DateOffset(months=24)
        df = df[pd.to_datetime(df["date"]) >= cutoff]
        log.info(f"Filtered to last 24 months: {len(df)} records")

    inserted = 0
    skipped = 0
    cur = conn.cursor()

    for _, row in df.iterrows():
        unit = infer_unit(row["commodity_name"])
        try:
            cur.execute("""
                INSERT OR IGNORE INTO raw_commodity_prices
                    (commodity_name, date, price_usd, unit, source)
                VALUES (?, ?, ?, ?, 'world_bank_cmo')
            """, (row["commodity_name"], row["date"], row["price_usd"], unit))
            if cur.rowcount > 0:
                inserted += 1
            else:
                skipped += 1
        except Exception as e:
            log.warning(f"Insert failed for {row['commodity_name']} {row['date']}: {e}")

    conn.commit()
    log.info(f"Inserted: {inserted} new records, Skipped (already exist): {skipped}")
    return inserted


def build_fact_table(conn: sqlite3.Connection):
    """
    Build fact_commodity_prices from validated raw data.
    Computes MoM, YoY changes and spike flags.
    """
    log.info("Building fact_commodity_prices...")

    conn.execute("DELETE FROM fact_commodity_prices")

    conn.execute("""
        INSERT OR REPLACE INTO fact_commodity_prices
            (commodity_id, commodity_name, date, price_usd, unit, mom_change_pct, yoy_change_pct, spike_flag, source)
        WITH base AS (
            SELECT
                LOWER(REPLACE(REPLACE(commodity_name, ' ', '_'), ',', '')) AS commodity_id,
                commodity_name,
                date,
                price_usd,
                unit,
                source,
                LAG(price_usd) OVER (PARTITION BY commodity_name ORDER BY date) AS prev_month_price,
                LAG(price_usd, 12) OVER (PARTITION BY commodity_name ORDER BY date) AS prev_year_price
            FROM raw_commodity_prices
            WHERE price_usd IS NOT NULL AND price_usd > 0
        )
        SELECT
            commodity_id,
            commodity_name,
            date,
            price_usd,
            unit,
            CASE WHEN prev_month_price > 0
                THEN ROUND((price_usd - prev_month_price) / prev_month_price * 100, 2)
                ELSE NULL END AS mom_change_pct,
            CASE WHEN prev_year_price > 0
                THEN ROUND((price_usd - prev_year_price) / prev_year_price * 100, 2)
                ELSE NULL END AS yoy_change_pct,
            CASE
                WHEN prev_month_price > 0 AND ABS((price_usd - prev_month_price) / prev_month_price) > 0.15 THEN 1
                WHEN prev_year_price > 0 AND ABS((price_usd - prev_year_price) / prev_year_price) > 0.40 THEN 1
                ELSE 0
            END AS spike_flag,
            source
        FROM base
    """)

    conn.commit()

    count = conn.execute("SELECT COUNT(*) FROM fact_commodity_prices").fetchone()[0]
    spikes = conn.execute("SELECT COUNT(*) FROM fact_commodity_prices WHERE spike_flag = 1").fetchone()[0]
    log.info(f"fact_commodity_prices: {count} records built, {spikes} spike flags set")


def generate_alerts(conn: sqlite3.Connection):
    """Check for current commodity spikes and insert alerts."""
    import uuid

    # Get most recent spike flags for priority commodities
    recent_spikes = conn.execute("""
        SELECT commodity_name, date, price_usd, mom_change_pct, yoy_change_pct
        FROM fact_commodity_prices
        WHERE spike_flag = 1
          AND date >= date('now', '-35 days')
        ORDER BY date DESC
    """).fetchall()

    new_alerts = 0
    for row in recent_spikes:
        commodity, date, price, mom, yoy = row
        msg = f"{commodity}: price ${price:.2f} | MoM {mom:+.1f}% | YoY {yoy:+.1f}%" if mom else f"{commodity}: spike detected at ${price:.2f}"
        alert_id = hashlib.md5(f"COMMODITY_SPIKE_{commodity}_{date}".encode()).hexdigest()

        conn.execute("""
            INSERT OR IGNORE INTO pred_rate_alerts
                (alert_id, alert_type, severity, entity_id, message, detail_json)
            VALUES (?, 'COMMODITY_SPIKE', 'WARNING', ?, ?, json_object(
                'commodity', ?, 'date', ?, 'price_usd', ?, 'mom_pct', ?, 'yoy_pct', ?
            ))
        """, (alert_id, commodity, msg, commodity, date, price, mom, yoy))

        if conn.execute("SELECT changes()").fetchone()[0] > 0:
            new_alerts += 1

    conn.commit()
    log.info(f"Generated {new_alerts} new commodity spike alerts")


def run(db_path: str = DB_PATH, backfill: bool = False, local_file: str = None):
    conn = get_conn(db_path)

    try:
        if local_file:
            log.info(f"Using local file: {local_file}")
            xl = pd.ExcelFile(local_file)
        else:
            xl = download_cmo()

        df = parse_monthly_sheet(xl)
        df_long = normalise_to_long(df)

        if df_long.empty:
            log.error("No data parsed from CMO file. Check sheet format.")
            return

        inserted = upsert_to_db(df_long, conn, backfill=backfill)

        if inserted > 0 or backfill:
            build_fact_table(conn)
            generate_alerts(conn)

        log.info("Commodity price ingestion complete.")

    except requests.RequestException as e:
        log.error(f"Download failed: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest World Bank Commodity Prices")
    parser.add_argument("--db", default=DB_PATH, help="Path to SQLite database")
    parser.add_argument("--backfill", action="store_true", help="Load full historical data (slow)")
    parser.add_argument("--file", default=None, help="Use local .xlsx file instead of downloading")
    args = parser.parse_args()

    run(db_path=args.db, backfill=args.backfill, local_file=args.file)
