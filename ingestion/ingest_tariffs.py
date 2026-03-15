"""
ingestion/ingest_tariffs.py
Downloads and ingests WTO Tariff Analysis Online (TAO) data.
Provides applied MFN and preferential tariff rates by HS code and country pair.

Run:  python ingestion/ingest_tariffs.py
      python ingestion/ingest_tariffs.py --reporter USA --hs-chapters 72,73,84,85
"""

import os
import sys
import csv
import hashlib
import logging
import argparse
import requests
import sqlite3
import json
from pathlib import Path
from datetime import datetime
from io import StringIO

sys.path.insert(0, str(Path(__file__).parent.parent))
from storage.setup_db import get_conn

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DB_PATH = os.environ.get("LIP_DB_PATH", "./data/lip.db")

# ── Top trading country pairs to pre-load ─────────────────────────────────────
# Format: (reporter_iso, partner_iso) — reporter = importer
TOP_PAIRS = [
    ("USA", "CHN"), ("USA", "DEU"), ("USA", "JPN"), ("USA", "KOR"), ("USA", "VNM"),
    ("USA", "IND"), ("USA", "MEX"), ("USA", "GBR"), ("USA", "TWN"), ("USA", "BRA"),
    ("DEU", "CHN"), ("DEU", "USA"), ("DEU", "NLD"), ("DEU", "FRA"), ("DEU", "POL"),
    ("CHN", "USA"), ("CHN", "DEU"), ("CHN", "JPN"), ("CHN", "KOR"), ("CHN", "AUS"),
    ("GBR", "USA"), ("GBR", "DEU"), ("GBR", "CHN"), ("GBR", "NLD"), ("GBR", "FRA"),
    ("JPN", "CHN"), ("JPN", "USA"), ("JPN", "KOR"), ("JPN", "AUS"), ("JPN", "SGP"),
    ("KOR", "CHN"), ("KOR", "USA"), ("KOR", "VNM"), ("KOR", "JPN"), ("KOR", "AUS"),
    ("AUS", "CHN"), ("AUS", "JPN"), ("AUS", "KOR"), ("AUS", "USA"), ("AUS", "SGP"),
    ("SGP", "CHN"), ("SGP", "MYS"), ("SGP", "USA"), ("SGP", "IND"), ("SGP", "IDN"),
    ("NLD", "DEU"), ("NLD", "BEL"), ("NLD", "GBR"), ("NLD", "CHN"), ("NLD", "USA"),
]

# Priority HS chapters for the Landed Cost Calculator
# Chapter 72=Iron/Steel, 73=Steel articles, 76=Aluminum, 74=Copper
# 84=Machinery, 85=Electrical, 87=Vehicles, 61-62=Apparel, 27=Fuel
PRIORITY_HS_CHAPTERS = [
    "27", "72", "73", "74", "76", "84", "85", "87", "61", "62", "39", "94"
]

# Known FTA agreements for preferential rate detection
KNOWN_FTAS = {
    ("USA", "CAN"): "USMCA",
    ("USA", "MEX"): "USMCA",
    ("USA", "KOR"): "KORUS",
    ("USA", "AUS"): "AUSFTA",
    ("USA", "SGP"): "USSFTA",
    ("DEU", "KOR"): "EU-Korea",
    ("DEU", "CAN"): "CETA",
    ("DEU", "JPN"): "EU-Japan",
    ("DEU", "SGP"): "EU-Singapore",
    ("GBR", "AUS"): "UK-Australia",
    ("GBR", "JPN"): "UKJFTA",
    ("JPN", "AUS"): "JAEPA",
    ("JPN", "SGP"): "JSEPA",
    ("CHN", "AUS"): "ChAFTA",
    ("CHN", "KOR"): "CK-FTA",
    ("SGP", "AUS"): "SAFTA",
    ("KOR", "AUS"): "KAFTA",
    ("AUS", "NZL"): "ANZCERTA",
}

# Static fallback tariff rates by HS chapter (MFN averages from WTO data)
# Used when API is unavailable — ensure offline functionality
STATIC_MFN_FALLBACK = {
    # (reporter_iso, hs_chapter): avg_mfn_rate_pct
    ("USA", "27"): 0.0,    ("USA", "72"): 0.0,    ("USA", "73"): 0.8,
    ("USA", "74"): 1.2,    ("USA", "76"): 3.0,    ("USA", "84"): 1.8,
    ("USA", "85"): 1.6,    ("USA", "87"): 2.5,    ("USA", "61"): 13.2,
    ("USA", "62"): 13.6,   ("USA", "39"): 4.8,    ("USA", "94"): 0.0,
    ("DEU", "27"): 0.0,    ("DEU", "72"): 0.0,    ("DEU", "73"): 1.7,
    ("DEU", "74"): 3.0,    ("DEU", "76"): 4.0,    ("DEU", "84"): 1.7,
    ("DEU", "85"): 2.7,    ("DEU", "87"): 4.5,    ("DEU", "61"): 12.0,
    ("DEU", "62"): 12.0,   ("DEU", "39"): 6.5,    ("DEU", "94"): 2.7,
    ("CHN", "27"): 0.0,    ("CHN", "72"): 2.0,    ("CHN", "73"): 8.5,
    ("CHN", "74"): 3.0,    ("CHN", "76"): 9.0,    ("CHN", "84"): 5.5,
    ("CHN", "85"): 6.9,    ("CHN", "87"): 15.0,   ("CHN", "61"): 16.0,
    ("CHN", "62"): 16.0,   ("CHN", "39"): 9.0,    ("CHN", "94"): 7.5,
    ("JPN", "27"): 0.0,    ("JPN", "72"): 0.0,    ("JPN", "73"): 2.5,
    ("JPN", "74"): 3.0,    ("JPN", "76"): 3.9,    ("JPN", "84"): 0.0,
    ("JPN", "85"): 0.0,    ("JPN", "87"): 0.0,    ("JPN", "61"): 10.9,
    ("JPN", "62"): 10.9,   ("JPN", "39"): 3.9,    ("JPN", "94"): 0.0,
    ("AUS", "27"): 0.0,    ("AUS", "72"): 0.0,    ("AUS", "73"): 0.0,
    ("AUS", "74"): 0.0,    ("AUS", "76"): 5.0,    ("AUS", "84"): 0.0,
    ("AUS", "85"): 0.0,    ("AUS", "87"): 5.0,    ("AUS", "61"): 10.0,
    ("AUS", "62"): 10.0,   ("AUS", "39"): 5.0,    ("AUS", "94"): 5.0,
    ("SGP", "27"): 0.0,    ("SGP", "72"): 0.0,    ("SGP", "73"): 0.0,
    ("SGP", "74"): 0.0,    ("SGP", "76"): 0.0,    ("SGP", "84"): 0.0,
    ("SGP", "85"): 0.0,    ("SGP", "87"): 0.0,    ("SGP", "61"): 0.0,
    ("SGP", "62"): 0.0,    ("SGP", "39"): 0.0,    ("SGP", "94"): 0.0,
}


def fetch_wto_tariffs_api(reporter: str, hs_chapter: str, year: int = None) -> list[dict]:
    """
    Attempt to fetch tariff data from WTO TAO API.
    The WTO provides a public data portal — this hits the bulk download endpoint.
    Falls back to static data if unavailable.
    """
    year = year or datetime.now().year - 1

    # WTO TAO provides data via their REST API
    url = "https://tao.wto.org/api/Data/DownloadCSV"
    params = {
        "reporterCode": reporter,
        "year": year,
        "productCode": hs_chapter,
        "measureType": "MFN_Applied",
    }

    try:
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code == 200 and len(resp.content) > 100:
            reader = csv.DictReader(StringIO(resp.text))
            return list(reader)
        else:
            log.warning(f"WTO API returned {resp.status_code} for {reporter}/{hs_chapter} — using fallback")
            return []
    except requests.RequestException as e:
        log.warning(f"WTO API unavailable: {e} — using static fallback data")
        return []


def load_static_fallback(reporter: str, hs_chapters: list[str], year: int) -> list[dict]:
    """Load static MFN averages when live API is unavailable."""
    records = []
    for chapter in hs_chapters:
        rate = STATIC_MFN_FALLBACK.get((reporter, chapter))
        if rate is not None:
            records.append({
                "reporter_iso": reporter,
                "partner_iso": None,  # MFN applies to all
                "hs_code": chapter.zfill(2) + "0000",  # 6-digit placeholder
                "hs_description": f"HS Chapter {chapter} (avg MFN rate)",
                "tariff_type": "MFN_applied",
                "rate_pct": rate,
                "specific_rate": None,
                "fta_name": None,
                "year": year,
                "hs_precision": 2,  # chapter-level only
                "source": "static_fallback",
            })
    return records


def build_preferential_records(reporter: str, year: int) -> list[dict]:
    """
    Generate preferential rate records for known FTAs.
    Uses known-zero or near-zero rates for FTA partners.
    """
    records = []
    for (rep, partner), fta_name in KNOWN_FTAS.items():
        if rep != reporter:
            continue
        for chapter in PRIORITY_HS_CHAPTERS:
            # FTA preferential rates are typically 0% or much lower than MFN
            mfn_rate = STATIC_MFN_FALLBACK.get((reporter, chapter), 5.0)
            pref_rate = max(0.0, mfn_rate * 0.1)  # FTA usually ~90% reduction
            records.append({
                "reporter_iso": reporter,
                "partner_iso": partner,
                "hs_code": chapter.zfill(2) + "0000",
                "hs_description": f"HS Chapter {chapter} — {fta_name} preferential",
                "tariff_type": "preferential",
                "rate_pct": pref_rate,
                "specific_rate": None,
                "fta_name": fta_name,
                "year": year,
                "hs_precision": 2,
                "source": "fta_static",
            })
    return records


def upsert_tariffs(records: list[dict], conn: sqlite3.Connection):
    inserted = 0
    cur = conn.cursor()
    for rec in records:
        tariff_id = hashlib.md5(
            f"{rec['reporter_iso']}|{rec.get('partner_iso', '')}|{rec['hs_code']}|{rec['tariff_type']}|{rec['year']}".encode()
        ).hexdigest()

        cur.execute("""
            INSERT OR IGNORE INTO raw_tariff_rates
                (reporter_iso, partner_iso, hs_code, hs_description,
                 tariff_type, rate_pct, specific_rate, fta_name, year, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            rec["reporter_iso"], rec.get("partner_iso"), rec["hs_code"],
            rec.get("hs_description"), rec["tariff_type"], rec.get("rate_pct"),
            rec.get("specific_rate"), rec.get("fta_name"), rec["year"],
            rec.get("source", "wto_tao")
        ))
        if cur.rowcount > 0:
            inserted += 1

    conn.commit()
    return inserted


def build_fact_tariff_table(conn: sqlite3.Connection):
    """Build fact_tariff_rates with change detection vs prior year."""
    log.info("Building fact_tariff_rates...")
    conn.execute("DELETE FROM fact_tariff_rates")

    conn.execute("""
        INSERT OR REPLACE INTO fact_tariff_rates
            (tariff_id, reporter_iso, partner_iso, hs_code, hs_description,
             tariff_type, rate_pct, specific_rate, fta_name, year, hs_precision,
             prev_rate_pct, rate_change_flag)
        WITH current_year AS (
            SELECT MAX(year) as yr FROM raw_tariff_rates
        ),
        current_rates AS (
            SELECT r.*,
                LOWER(REPLACE(r.reporter_iso||r.partner_iso||r.hs_code||r.tariff_type||CAST(r.year AS TEXT),' ','')) AS tariff_id,
                6 as hs_precision
            FROM raw_tariff_rates r, current_year cy WHERE r.year = cy.yr
        ),
        prior_rates AS (
            SELECT reporter_iso, partner_iso, hs_code, tariff_type, rate_pct as prev_rate
            FROM raw_tariff_rates
            WHERE year = (SELECT MAX(year)-1 FROM raw_tariff_rates)
        )
        SELECT
            c.tariff_id,
            c.reporter_iso, c.partner_iso, c.hs_code, c.hs_description,
            c.tariff_type, c.rate_pct, c.specific_rate, c.fta_name, c.year, c.hs_precision,
            p.prev_rate AS prev_rate_pct,
            CASE
                WHEN p.prev_rate IS NOT NULL AND ABS(c.rate_pct - p.prev_rate) > 5 THEN 1
                ELSE 0
            END AS rate_change_flag
        FROM current_rates c
        LEFT JOIN prior_rates p ON
            c.reporter_iso = p.reporter_iso AND
            (c.partner_iso = p.partner_iso OR (c.partner_iso IS NULL AND p.partner_iso IS NULL)) AND
            c.hs_code = p.hs_code AND c.tariff_type = p.tariff_type
    """)

    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM fact_tariff_rates").fetchone()[0]
    changes = conn.execute("SELECT COUNT(*) FROM fact_tariff_rates WHERE rate_change_flag = 1").fetchone()[0]
    log.info(f"fact_tariff_rates: {count} records, {changes} rate changes flagged")


def generate_tariff_alerts(conn: sqlite3.Connection):
    """Generate alerts for tariff changes and FTA opportunities."""
    import uuid

    # Tariff change alerts
    changed = conn.execute("""
        SELECT reporter_iso, hs_code, tariff_type, rate_pct, prev_rate_pct
        FROM fact_tariff_rates
        WHERE rate_change_flag = 1 AND partner_iso IS NULL
        LIMIT 50
    """).fetchall()

    for reporter, hs, ttype, new_rate, old_rate in changed:
        direction = "increased" if new_rate > old_rate else "decreased"
        msg = f"{reporter} tariff on HS {hs}: {old_rate:.1f}% → {new_rate:.1f}% ({direction})"
        alert_id = hashlib.md5(f"TARIFF_CHANGE_{reporter}_{hs}_{new_rate}".encode()).hexdigest()
        severity = "ALERT" if direction == "increased" else "INFO"
        conn.execute("""
            INSERT OR IGNORE INTO pred_rate_alerts
                (alert_id, alert_type, severity, entity_id, message, detail_json)
            VALUES (?, 'TARIFF_CHANGE', ?, ?, ?, json_object(
                'reporter', ?, 'hs_code', ?, 'old_rate', ?, 'new_rate', ?, 'direction', ?
            ))
        """, (alert_id, severity, f"{reporter}_{hs}", msg,
              reporter, hs, old_rate, new_rate, direction))

    # FTA opportunity alerts
    fta_opps = conn.execute("""
        SELECT f.reporter_iso, f.partner_iso, f.hs_code, f.rate_pct as pref_rate,
               m.rate_pct as mfn_rate, f.fta_name,
               (m.rate_pct - f.rate_pct) as saving_pp
        FROM fact_tariff_rates f
        JOIN fact_tariff_rates m ON
            f.reporter_iso = m.reporter_iso AND
            f.hs_code = m.hs_code AND
            m.partner_iso IS NULL AND m.tariff_type = 'MFN_applied'
        WHERE f.tariff_type = 'preferential'
          AND (m.rate_pct - f.rate_pct) > 3
        LIMIT 100
    """).fetchall()

    for reporter, partner, hs, pref, mfn, fta, saving in fta_opps:
        msg = f"FTA opportunity: {reporter}←{partner} on HS {hs} via {fta}: {mfn:.1f}% → {pref:.1f}% (save {saving:.1f}pp)"
        alert_id = hashlib.md5(f"FTA_OPP_{reporter}_{partner}_{hs}".encode()).hexdigest()
        conn.execute("""
            INSERT OR IGNORE INTO pred_rate_alerts
                (alert_id, alert_type, severity, entity_id, message, detail_json)
            VALUES (?, 'FTA_OPPORTUNITY', 'INFO', ?, ?, json_object(
                'reporter', ?, 'partner', ?, 'hs_code', ?, 'mfn_rate', ?, 'pref_rate', ?, 'fta_name', ?, 'saving_pp', ?
            ))
        """, (alert_id, f"{reporter}_{partner}_{hs}", msg,
              reporter, partner, hs, mfn, pref, fta, saving))

    conn.commit()
    log.info(f"Generated tariff change alerts for {len(changed)} changes, {len(fta_opps)} FTA opportunities")


def run(db_path: str = DB_PATH, reporters: list[str] = None, hs_chapters: list[str] = None):
    conn = get_conn(db_path)
    year = datetime.now().year - 1

    reporters = reporters or list({p[0] for p in TOP_PAIRS})
    hs_chapters = hs_chapters or PRIORITY_HS_CHAPTERS

    log.info(f"Ingesting tariffs for {len(reporters)} reporters, {len(hs_chapters)} HS chapters, year {year}")
    total_inserted = 0

    for reporter in reporters:
        log.info(f"Processing: {reporter}")

        # Try live API first
        api_records = []
        for chapter in hs_chapters:
            api_data = fetch_wto_tariffs_api(reporter, chapter, year)
            api_records.extend(api_data)

        if api_records:
            log.info(f"  Got {len(api_records)} records from WTO API")
            total_inserted += upsert_tariffs(api_records, conn)
        else:
            # Fall back to static data
            static_records = load_static_fallback(reporter, hs_chapters, year)
            pref_records = build_preferential_records(reporter, year)
            all_records = static_records + pref_records
            log.info(f"  Using static fallback: {len(all_records)} records")
            total_inserted += upsert_tariffs(all_records, conn)

    log.info(f"Total inserted: {total_inserted}")
    build_fact_tariff_table(conn)
    generate_tariff_alerts(conn)
    conn.close()
    log.info("Tariff ingestion complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest WTO Tariff Data")
    parser.add_argument("--db", default=DB_PATH)
    parser.add_argument("--reporters", nargs="+", default=None, help="ISO codes e.g. USA DEU CHN")
    parser.add_argument("--hs-chapters", nargs="+", default=None, help="HS chapters e.g. 72 73 84")
    args = parser.parse_args()
    run(db_path=args.db, reporters=args.reporters, hs_chapters=args.hs_chapters)
