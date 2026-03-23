"""
fix_commodity_sheet.py
Run this from your project folder to:
1. Inspect the CMO Excel file structure (so we can see the correct sheet/layout)
2. Automatically fix and re-run commodity ingestion

Usage:
    python fix_commodity_sheet.py --inspect
    python fix_commodity_sheet.py --fix
"""

import sys
import os
import argparse
import logging
import requests
import pandas as pd
from pathlib import Path
from io import BytesIO
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DB_PATH  = os.environ.get("LIP_DB_PATH", "./data/lip.db")
CMO_URL  = ("https://thedocs.worldbank.org/en/doc/"
            "18675f1d1639c7a34d463f59263ba0a2-0050012025/related/CMO-Historical-Data-Monthly.xlsx")


def download_cmo() -> pd.ExcelFile:
    log.info("Downloading CMO file...")
    resp = requests.get(CMO_URL, timeout=60)
    resp.raise_for_status()
    log.info(f"Downloaded {len(resp.content)/1024:.0f} KB")
    return pd.ExcelFile(BytesIO(resp.content))


def inspect(xl: pd.ExcelFile):
    """Print full structure of every sheet so we know exactly how to parse it."""
    print(f"\nSheets in file: {xl.sheet_names}\n")

    for sheet_name in xl.sheet_names:
        print(f"{'='*60}")
        print(f"SHEET: {sheet_name}")
        print(f"{'='*60}")
        raw = xl.parse(sheet_name, header=None, nrows=15)
        print(f"Shape (first 15 rows): {raw.shape}")
        for i, row in raw.iterrows():
            vals = []
            for v in row:
                s = str(v)
                if s != 'nan':
                    vals.append(s[:25])
            if vals:
                print(f"  row {i:2d}: {vals[:8]}")
        print()


def parse_to_long(xl: pd.ExcelFile, sheet_name: str = "Monthly Prices",
                  cutoff_year: int = 2000) -> pd.DataFrame:
    """
    Parse the CMO 'Monthly Prices' sheet.

    Confirmed structure (from --inspect):
      row 0-3 : title / metadata text  — skip
      row 4   : commodity names        — these become our column labels
      row 5   : units e.g. ($/bbl)     — skip
      row 6+  : data, col 0 = date in '1960M01' format, col 1+ = prices
    """
    raw = xl.parse(sheet_name, header=None)
    log.info(f"Sheet '{sheet_name}': {raw.shape[0]} rows x {raw.shape[1]} cols")

    # Row 4 = commodity names (confirmed)
    COMMODITY_ROW = 4
    DATA_START_ROW = 6          # row 5 is units, data begins row 6

    # Build commodity name list from row 4 (skip col 0 which is the date column)
    commodity_row = raw.iloc[COMMODITY_ROW]
    commodities = {}  # col_index -> name
    for col_idx in range(1, len(commodity_row)):
        name = str(commodity_row.iloc[col_idx]).strip()
        if name and name != 'nan':
            commodities[col_idx] = name

    log.info(f"Found {len(commodities)} commodity columns: "
             f"{list(commodities.values())[:6]}...")

    # Parse data rows
    records = []
    skipped_dates = 0

    for row_idx in range(DATA_START_ROW, len(raw)):
        row = raw.iloc[row_idx]

        # Col 0 = date string e.g. '2024M03'
        raw_date = str(row.iloc[0]).strip()
        if not raw_date or raw_date == 'nan':
            continue

        # Parse '2024M03' format
        if len(raw_date) == 7 and 'M' in raw_date:
            try:
                y, m = raw_date.split('M')
                year = int(y)
                if year < cutoff_year:
                    continue
                date_str = f"{y}-{m.zfill(2)}-01"
            except ValueError:
                skipped_dates += 1
                continue
        else:
            skipped_dates += 1
            continue

        # Extract price for each commodity column
        for col_idx, commodity_name in commodities.items():
            try:
                val = row.iloc[col_idx]
                # Skip ellipsis placeholders ('…') and blanks
                if str(val).strip() in ('', 'nan', '…', '...', 'n.a.', 'N/A'):
                    continue
                price = float(val)
                if price > 0:
                    records.append({
                        'commodity_name': commodity_name,
                        'date': date_str,
                        'price_usd': round(price, 6),
                    })
            except (ValueError, TypeError, IndexError):
                pass

    df = pd.DataFrame(records)

    if skipped_dates:
        log.info(f"Skipped {skipped_dates} non-date rows")

    if not df.empty:
        log.info(f"Parsed {len(df):,} price records "
                 f"for {df['commodity_name'].nunique()} commodities")
        log.info(f"Date range: {df['date'].min()} to {df['date'].max()}")
    else:
        log.error("No records parsed — run --inspect to debug")

    return df


def infer_unit(name: str) -> str:
    n = name.lower()
    if any(x in n for x in ('crude', 'oil', 'brent', 'wti', 'petroleum')):
        return 'per_barrel'
    if any(x in n for x in ('gas', 'lng')):
        return 'per_mmbtu'
    if any(x in n for x in ('steel', 'aluminum', 'copper', 'zinc', 'nickel',
                              'lead', 'tin', 'coal', 'iron')):
        return 'per_metric_tonne'
    if any(x in n for x in ('lumber', 'log', 'timber')):
        return 'per_cubic_metre'
    if any(x in n for x in ('cotton', 'rubber', 'rice', 'wheat', 'sugar')):
        return 'per_kg'
    return 'per_unit'


def load_to_db(df: pd.DataFrame, db_path: str) -> int:
    import sqlite3
    sys.path.insert(0, '.')
    from storage.setup_db import get_conn
    conn = get_conn(db_path)

    # Wipe stale data first (the 2006-era records)
    old_count = conn.execute("SELECT COUNT(*) FROM raw_commodity_prices").fetchone()[0]
    conn.execute("DELETE FROM raw_commodity_prices")
    conn.commit()
    log.info(f"Cleared {old_count} stale commodity records")

    inserted = 0
    cur = conn.cursor()
    for _, row in df.iterrows():
        unit = infer_unit(row['commodity_name'])
        cur.execute("""
            INSERT OR IGNORE INTO raw_commodity_prices
                (commodity_name, date, price_usd, unit, source)
            VALUES (?, ?, ?, ?, 'world_bank_cmo')
        """, (row['commodity_name'], row['date'], row['price_usd'], unit))
        if cur.rowcount > 0:
            inserted += 1

    conn.commit()
    log.info(f"Inserted {inserted:,} records into raw_commodity_prices")

    # Rebuild fact table
    log.info("Rebuilding fact_commodity_prices...")
    conn.execute("DELETE FROM fact_commodity_prices")
    conn.execute("""
        INSERT OR REPLACE INTO fact_commodity_prices
            (commodity_id, commodity_name, date, price_usd, unit,
             mom_change_pct, yoy_change_pct, spike_flag, source)
        WITH base AS (
            SELECT
                LOWER(REPLACE(REPLACE(REPLACE(commodity_name,' ','_'),',',''),'/','_')) AS commodity_id,
                commodity_name, date, price_usd, unit, source,
                LAG(price_usd) OVER (PARTITION BY commodity_name ORDER BY date) AS prev_m,
                LAG(price_usd, 12) OVER (PARTITION BY commodity_name ORDER BY date) AS prev_y
            FROM raw_commodity_prices
            WHERE price_usd IS NOT NULL AND price_usd > 0
        )
        SELECT commodity_id, commodity_name, date, price_usd, unit,
            CASE WHEN prev_m > 0 THEN ROUND((price_usd-prev_m)/prev_m*100,2) END,
            CASE WHEN prev_y > 0 THEN ROUND((price_usd-prev_y)/prev_y*100,2) END,
            CASE
                WHEN prev_m > 0 AND ABS((price_usd-prev_m)/prev_m) > 0.15 THEN 1
                WHEN prev_y > 0 AND ABS((price_usd-prev_y)/prev_y) > 0.40 THEN 1
                ELSE 0 END,
            source
        FROM base
    """)
    conn.commit()

    count  = conn.execute("SELECT COUNT(*) FROM fact_commodity_prices").fetchone()[0]
    spikes = conn.execute("SELECT COUNT(*) FROM fact_commodity_prices WHERE spike_flag=1").fetchone()[0]
    latest = conn.execute("SELECT MAX(date) FROM fact_commodity_prices").fetchone()[0]
    log.info(f"fact_commodity_prices: {count:,} records, {spikes} spikes, latest: {latest}")

    # Generate spike alerts
    import hashlib
    recent = conn.execute("""
        SELECT commodity_name, date, price_usd, mom_change_pct, yoy_change_pct
        FROM fact_commodity_prices
        WHERE spike_flag = 1 AND date >= date('now', '-60 days')
    """).fetchall()
    new_alerts = 0
    for name, d, price, mom, yoy in recent:
        msg = f"{name}: ${price:.2f} | MoM {mom:+.1f}%" if mom else f"{name}: spike at ${price:.2f}"
        aid = hashlib.md5(f"COMMODITY_SPIKE_{name}_{d}".encode()).hexdigest()
        conn.execute("""
            INSERT OR IGNORE INTO pred_rate_alerts
                (alert_id, alert_type, severity, entity_id, message, detail_json)
            VALUES (?, 'COMMODITY_SPIKE', 'WARNING', ?, ?, json_object(
                'commodity',?,'date',?,'price',?,'mom_pct',?,'yoy_pct',?))
        """, (aid, name, msg, name, d, price, mom, yoy))
        if conn.execute("SELECT changes()").fetchone()[0]:
            new_alerts += 1
    conn.commit()
    log.info(f"Generated {new_alerts} new commodity spike alerts")
    conn.close()
    return inserted


def run_fix(db_path: str):
    xl = download_cmo()
    sheet = "Monthly Prices"   # confirmed from --inspect
    log.info(f"Using sheet: '{sheet}'")
    df = parse_to_long(xl, sheet, cutoff_year=2000)

    if df.empty:
        log.error("Parsing failed. Run with --inspect to see the raw sheet structure.")
        return

    latest = df['date'].max()
    current_year = datetime.now().year
    if int(latest[:4]) < current_year - 2:
        log.error(f"Latest date {latest} is still too old — aborting to avoid overwriting good data.")
        return

    load_to_db(df, db_path)
    log.info("Done. Run: python run_pipeline.py --status")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fix CMO commodity price ingestion")
    parser.add_argument("--db", default=DB_PATH)
    parser.add_argument("--inspect", action="store_true",
                        help="Print sheet structure without modifying DB")
    parser.add_argument("--fix", action="store_true",
                        help="Download, parse correctly, and reload into DB")
    args = parser.parse_args()

    if args.inspect:
        xl = download_cmo()
        inspect(xl)
    elif args.fix:
        run_fix(args.db)
    else:
        parser.print_help()
        print("\nRun --inspect first to see sheet names, then --fix to reload.")