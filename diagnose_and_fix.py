"""
diagnose_and_fix.py
Run this from your project root to check what's working, what's broken,
and automatically fix the issues found in the pipeline run.

Usage:
    python diagnose_and_fix.py
    python diagnose_and_fix.py --lpi-file "C:/path/to/International_LPI_from_2007_to_2023_0.xlsx"
    python diagnose_and_fix.py --fix-all
"""

import os
import sys
import json
import sqlite3
import argparse
import requests
import time
from pathlib import Path
from datetime import datetime

DB_PATH = os.environ.get("LIP_DB_PATH", "./data/lip.db")

# ── Colour helpers for terminal output ───────────────────────────────────────
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
BLUE   = "\033[94m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def ok(msg):    print(f"  {GREEN}✓{RESET}  {msg}")
def warn(msg):  print(f"  {YELLOW}⚠{RESET}  {msg}")
def fail(msg):  print(f"  {RED}✗{RESET}  {msg}")
def info(msg):  print(f"  {BLUE}→{RESET}  {msg}")
def header(msg): print(f"\n{BOLD}{msg}{RESET}\n{'─'*60}")


# ════════════════════════════════════════════════════════════
#  CHECK 1 — DATABASE
# ════════════════════════════════════════════════════════════
def check_database(db_path: str) -> dict:
    header("CHECK 1 — Database & Table Row Counts")

    if not Path(db_path).exists():
        fail(f"Database not found at: {db_path}")
        return {}

    ok(f"Database exists: {db_path} ({Path(db_path).stat().st_size / 1024:.1f} KB)")

    conn = sqlite3.connect(db_path)
    expected_tables = [
        "raw_commodity_prices", "raw_fuel_prices", "raw_tariff_rates",
        "raw_weather_port", "raw_lpi_scores", "raw_macro_indicators",
        "fact_commodity_prices", "fact_fuel_prices", "fact_tariff_rates",
        "fact_port_weather", "dim_countries", "dim_ports",
        "pred_landed_cost_estimates", "pred_rate_alerts",
    ]

    results = {}
    missing = []
    empty = []

    for table in expected_tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            results[table] = count
            if count == 0:
                empty.append(table)
                warn(f"{table:40} {count:>8} rows  ← EMPTY")
            else:
                ok(f"{table:40} {count:>8,} rows")
        except Exception:
            missing.append(table)
            fail(f"{table:40}  NOT FOUND")

    # Latest dates
    print()
    info("Latest data dates:")
    date_checks = [
        ("raw_commodity_prices", "MAX(date)"),
        ("raw_fuel_prices",      "MAX(date)"),
        ("raw_weather_port",     "MAX(date)"),
        ("raw_lpi_scores",       "MAX(ingested_at)"),
        ("raw_macro_indicators", "MAX(date)"),
        ("pred_rate_alerts",     "MAX(triggered_at)"),
    ]
    for table, expr in date_checks:
        try:
            val = conn.execute(f"SELECT {expr} FROM {table}").fetchone()[0]
            status = ok if val else warn
            status(f"{table:40} {val or 'no data':>25}")
        except Exception:
            pass

    conn.close()
    return {"missing": missing, "empty": empty, "counts": results}


# ════════════════════════════════════════════════════════════
#  CHECK 2 — NETWORK CONNECTIVITY
# ════════════════════════════════════════════════════════════
def check_network() -> dict:
    header("CHECK 2 — Network Connectivity to Data Sources")

    endpoints = [
        ("World Bank CMO (new URL)",
         "https://thedocs.worldbank.org/en/doc/18675f1d1639c7a34d463f59263ba0a2-0050012025/related/CMO-Historical-Data-Monthly.xlsx",
         "head"),
        ("World Bank CMO (old URL — should 404)",
         "https://thedocs.worldbank.org/en/doc/18675ac6c71c4d83748e3b4e8c69cb74-0350012024/CMO-Historical-Data-Monthly.xlsx",
         "head"),
        ("FRED (St. Louis Fed)",
         "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DCOILBRENTEU",
         "head"),
        ("Open-Meteo (weather)",
         "https://api.open-meteo.com/v1/forecast?latitude=1.29&longitude=103.85&current=wind_speed_10m&timezone=UTC",
         "get"),
        ("World Bank API (TEU data)",
         "https://api.worldbank.org/v2/country/SGP/indicator/IS.SHP.GOOD.TU?format=json&mrv=1",
         "get"),
        ("EIA API (demo key)",
         "https://api.eia.gov/v2/petroleum/pri/spt/data/?api_key=DEMO_KEY&length=1",
         "get"),
        ("WTO TAO",
         "https://tao.wto.org",
         "head"),
    ]

    results = {}
    for name, url, method in endpoints:
        try:
            fn = requests.head if method == "head" else requests.get
            resp = fn(url, timeout=10, allow_redirects=True,
                      headers={"User-Agent": "LIP-Diagnostics/1.0"})
            if resp.status_code < 400:
                ok(f"{name:35} HTTP {resp.status_code}")
                results[name] = True
            else:
                fail(f"{name:35} HTTP {resp.status_code}")
                results[name] = False
        except requests.Timeout:
            fail(f"{name:35} TIMEOUT (>10s)")
            results[name] = False
        except Exception as e:
            fail(f"{name:35} ERROR: {type(e).__name__}")
            results[name] = False

    # FRED specific diagnosis
    fred_ok = results.get("FRED (St. Louis Fed)", False)
    if not fred_ok:
        print()
        warn("FRED is timing out. This is usually one of:")
        info("  a) Corporate/university firewall blocking fred.stlouisfed.org")
        info("  b) VPN interference — try disconnecting VPN and retrying")
        info("  c) Temporary FRED outage — check https://fred.stlouisfed.org")
        info("  Fix: Run  python diagnose_and_fix.py --fix-fred-fallback")
        info("       to use the World Bank API as a Brent price fallback instead")

    return results


# ════════════════════════════════════════════════════════════
#  CHECK 3 — SOURCE FILE AVAILABILITY
# ════════════════════════════════════════════════════════════
def check_source_files(lpi_file: str = None) -> dict:
    header("CHECK 3 — Local Source Files")

    results = {}

    # LPI file — check common locations
    lpi_candidates = [
        lpi_file,
        "./International_LPI_from_2007_to_2023_0.xlsx",
        "./data/International_LPI_from_2007_to_2023_0.xlsx",
        "../International_LPI_from_2007_to_2023_0.xlsx",
    ]
    lpi_candidates = [p for p in lpi_candidates if p]

    found_lpi = None
    for path in lpi_candidates:
        if path and Path(path).exists():
            found_lpi = path
            ok(f"LPI file found:  {path}")
            break

    if not found_lpi:
        fail("LPI file NOT found in any of these locations:")
        for p in lpi_candidates:
            info(f"  {p}")
        warn("Download from: https://lpi.worldbank.org/sites/default/files/International_LPI_from_2007_to_2023_0.xlsx")
        warn("Then run: python diagnose_and_fix.py --lpi-file 'C:/your/path/to/LPI.xlsx' --fix-lpi")
    results["lpi_file"] = found_lpi

    # Trade CSV
    trade_candidates = [
        "./TradeData_3_7_2026_15_6_51.csv",
        "./data/TradeData_3_7_2026_15_6_51.csv",
    ]
    for path in trade_candidates:
        if Path(path).exists():
            size = Path(path).stat().st_size / 1024
            ok(f"Trade CSV found: {path} ({size:.0f} KB)")
            results["trade_csv"] = path
            break
    else:
        warn("Trade CSV not found — not required for core pipeline but useful for trade flow analysis")
        results["trade_csv"] = None

    return results


# ════════════════════════════════════════════════════════════
#  CHECK 4 — DATA QUALITY
# ════════════════════════════════════════════════════════════
def check_data_quality(db_path: str) -> dict:
    header("CHECK 4 — Data Quality Spot Checks")

    if not Path(db_path).exists():
        fail("No database to check")
        return {}

    conn = sqlite3.connect(db_path)
    issues = []

    # Fuel prices — check range
    try:
        rows = conn.execute("""
            SELECT fuel_type, MIN(price_usd), MAX(price_usd), COUNT(*), MIN(date), MAX(date)
            FROM raw_fuel_prices GROUP BY fuel_type
        """).fetchall()
        if rows:
            ok("Fuel prices — range check:")
            for fuel, mn, mx, cnt, d_min, d_max in rows:
                if mn < 10 or mx > 500:
                    warn(f"  {fuel}: ${mn:.2f}–${mx:.2f} ({cnt} records, {d_min} → {d_max})  ← suspicious range")
                    issues.append(f"Fuel price range suspicious: {fuel}")
                else:
                    ok(f"  {fuel}: ${mn:.2f}–${mx:.2f} ({cnt} records, {d_min} → {d_max})")
        else:
            warn("No fuel price data to check")
    except Exception as e:
        fail(f"Fuel price check failed: {e}")

    # Tariff rates — check range and coverage
    try:
        tariff_stats = conn.execute("""
            SELECT reporter_iso, COUNT(*) as n, MIN(rate_pct), MAX(rate_pct),
                   SUM(CASE WHEN tariff_type='preferential' THEN 1 ELSE 0 END) as pref_count
            FROM fact_tariff_rates
            GROUP BY reporter_iso
            ORDER BY n DESC
        """).fetchall()
        if tariff_stats:
            ok(f"Tariff rates — {len(tariff_stats)} reporters:")
            for reporter, n, mn, mx, pref in tariff_stats:
                ok(f"  {reporter}: {n} records, rates {mn:.1f}%–{mx:.1f}%, {pref} preferential")
        else:
            warn("No tariff data found")
    except Exception as e:
        fail(f"Tariff check failed: {e}")

    # Weather — check ports and dates
    try:
        weather_stats = conn.execute("""
            SELECT COUNT(DISTINCT port_code) as ports, COUNT(*) as records,
                   MIN(date) as earliest, MAX(date) as latest
            FROM raw_weather_port
        """).fetchone()
        if weather_stats and weather_stats[1] > 0:
            ports, records, earliest, latest = weather_stats
            ok(f"Weather data: {ports} ports, {records} records ({earliest} → {latest})")
            if ports < 10:
                warn(f"Only {ports}/15 ports have weather data")
                issues.append("Weather coverage incomplete")
        else:
            warn("No weather data found")
    except Exception as e:
        fail(f"Weather check failed: {e}")

    # Alerts — sanity check
    try:
        alerts = conn.execute("""
            SELECT alert_type, severity, COUNT(*) FROM pred_rate_alerts
            GROUP BY alert_type, severity
        """).fetchall()
        if alerts:
            ok(f"Alerts generated:")
            for atype, sev, cnt in alerts:
                ok(f"  {atype:30} {sev:10} {cnt:>4}")
        else:
            warn("No alerts generated yet")
    except Exception as e:
        fail(f"Alert check failed: {e}")

    conn.close()
    return {"issues": issues}


# ════════════════════════════════════════════════════════════
#  FIX: Update CMO URL in config and re-run commodity ingestion
# ════════════════════════════════════════════════════════════
def fix_commodity_url(db_path: str):
    header("FIX — Updating World Bank CMO URL and re-ingesting")

    # New confirmed URL from World Bank
    NEW_URL = "https://thedocs.worldbank.org/en/doc/18675f1d1639c7a34d463f59263ba0a2-0050012025/related/CMO-Historical-Data-Monthly.xlsx"

    config_path = Path("./config/sources.json")
    if config_path.exists():
        with open(config_path) as f:
            config = json.load(f)
        config["sources"]["wb_commodity_prices"]["url"] = NEW_URL
        with open(config_path, "w") as f:
            json.dump(config, f, indent=2)
        ok(f"Updated config/sources.json with new URL")
    else:
        warn("config/sources.json not found — skipping config update")

    # Patch the ingestion script directly
    ingest_path = Path("./ingestion/ingest_commodity_prices.py")
    if ingest_path.exists():
        content = ingest_path.read_text()
        old_url = "18675ac6c71c4d83748e3b4e8c69cb74-0350012024"
        new_url_fragment = "18675f1d1639c7a34d463f59263ba0a2-0050012025/related"
        if old_url in content:
            content = content.replace(
                "https://thedocs.worldbank.org/en/doc/18675ac6c71c4d83748e3b4e8c69cb74-0350012024/CMO-Historical-Data-Monthly.xlsx",
                NEW_URL
            )
            ingest_path.write_text(content)
            ok("Patched ingest_commodity_prices.py with new URL")
        elif new_url_fragment in content:
            ok("ingest_commodity_prices.py already has the new URL")
        else:
            warn("Could not find old URL in script — patch manually")
            info(f"Set CMO_URL = '{NEW_URL}'")

    # Now run ingestion
    info("Running commodity price ingestion with corrected URL...")
    try:
        sys.path.insert(0, ".")
        from storage.setup_db import setup
        setup(db_path)
        from ingestion.ingest_commodity_prices import run as run_commodities
        run_commodities(db_path=db_path, backfill=True)
        ok("Commodity prices ingested successfully")
    except Exception as e:
        fail(f"Commodity ingestion failed: {e}")
        info("Try manually: python run_pipeline.py --source commodities --backfill")


# ════════════════════════════════════════════════════════════
#  FIX: Load LPI from local file
# ════════════════════════════════════════════════════════════
def fix_lpi(db_path: str, lpi_file: str):
    header("FIX — Loading LPI data from local file")

    if not lpi_file or not Path(lpi_file).exists():
        fail(f"LPI file not found: {lpi_file}")
        info("Download from: https://lpi.worldbank.org/sites/default/files/International_LPI_from_2007_to_2023_0.xlsx")
        return

    try:
        sys.path.insert(0, ".")
        from storage.setup_db import setup, get_conn
        setup(db_path)
        conn = get_conn(db_path)
        from ingestion.ingest_weather_and_ports import load_lpi_from_file, seed_dim_countries
        n = load_lpi_from_file(lpi_file, conn)
        seed_dim_countries(conn)
        conn.close()
        ok(f"LPI loaded: {n} records")

        # Check result
        conn2 = sqlite3.connect(db_path)
        lpi_count = conn2.execute("SELECT COUNT(*) FROM raw_lpi_scores").fetchone()[0]
        country_count = conn2.execute("SELECT COUNT(*) FROM dim_countries").fetchone()[0]
        conn2.close()
        ok(f"raw_lpi_scores: {lpi_count} records")
        ok(f"dim_countries:  {country_count} records")
    except Exception as e:
        fail(f"LPI load failed: {e}")
        import traceback; traceback.print_exc()


# ════════════════════════════════════════════════════════════
#  FIX: FRED fallback via World Bank API
# ════════════════════════════════════════════════════════════
def fix_fred_fallback(db_path: str):
    header("FIX — Loading fuel/macro data via World Bank API (FRED fallback)")

    info("Fetching Brent crude from World Bank API instead of FRED...")

    # World Bank has Brent crude as indicator PBRENT
    url = "https://api.worldbank.org/v2/country/all/indicator/CRUDE_BRENT?format=json&mrv=60&per_page=100"
    # Also try the energy commodity indicator
    alt_url = "https://api.worldbank.org/v2/country/WLD/indicator/PBRENT?format=json&mrv=36&per_page=100"

    try:
        resp = requests.get(alt_url, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            if len(data) > 1 and data[1]:
                ok(f"World Bank Brent API returned {len(data[1])} records")
                sys.path.insert(0, ".")
                from storage.setup_db import get_conn
                conn = get_conn(db_path)
                inserted = 0
                for rec in data[1]:
                    if rec.get("value") and rec.get("date"):
                        date_str = f"{rec['date']}-01-01"
                        conn.execute("""
                            INSERT OR IGNORE INTO raw_fuel_prices
                                (fuel_type, date, price_usd, unit, source)
                            VALUES ('brent', ?, ?, 'per_barrel', 'world_bank_api')
                        """, (date_str, float(rec["value"])))
                        if conn.execute("SELECT changes()").fetchone()[0]:
                            inserted += 1
                conn.commit()
                conn.close()
                ok(f"Inserted {inserted} annual Brent price records from World Bank API")
                info("Note: Annual data only — for weekly data, an EIA API key is recommended")
            else:
                warn("World Bank Brent API returned empty data")
        else:
            fail(f"World Bank API returned HTTP {resp.status_code}")
    except Exception as e:
        fail(f"World Bank API fallback failed: {e}")

    info("For weekly fuel data, get a free EIA key at: https://www.eia.gov/opendata/register.php")
    info("Then set: set EIA_API_KEY=your_key  (Windows)  or  export EIA_API_KEY=your_key  (Mac/Linux)")


# ════════════════════════════════════════════════════════════
#  SUMMARY REPORT
# ════════════════════════════════════════════════════════════
def print_action_plan(db_results, network_results, file_results):
    header("ACTION PLAN — What to do next")

    actions = []

    counts = db_results.get("counts", {})

    if counts.get("raw_commodity_prices", 0) == 0:
        actions.append(("HIGH", "Commodity prices empty",
                        "python diagnose_and_fix.py --fix-commodities"))

    if counts.get("raw_lpi_scores", 0) == 0:
        lpi_path = file_results.get("lpi_file")
        if lpi_path:
            actions.append(("HIGH", "LPI scores empty — file found",
                            f'python diagnose_and_fix.py --fix-lpi --lpi-file "{lpi_path}"'))
        else:
            actions.append(("HIGH", "LPI scores empty — download file first",
                            "1) Download LPI xlsx from lpi.worldbank.org\n"
                            '     2) python diagnose_and_fix.py --fix-lpi --lpi-file "path/to/file.xlsx"'))

    if not network_results.get("FRED (St. Louis Fed)", False):
        if counts.get("raw_fuel_prices", 0) < 50:
            actions.append(("MEDIUM", "FRED blocked/timing out, fuel data sparse",
                            "python diagnose_and_fix.py --fix-fred-fallback\n"
                            "     OR get EIA key: https://www.eia.gov/opendata/register.php"))

    if counts.get("raw_macro_indicators", 0) == 0:
        actions.append(("LOW", "Macro indicators empty (FRED blocked)",
                        "Try disabling VPN, then: python run_pipeline.py --source fuel"))

    if not actions:
        ok("No critical issues found — database looks healthy!")
        info("Run 'python run_pipeline.py --status' anytime to check row counts")
        return

    for priority, issue, fix in actions:
        color = RED if priority == "HIGH" else YELLOW if priority == "MEDIUM" else BLUE
        print(f"\n  {color}{BOLD}[{priority}]{RESET} {issue}")
        print(f"  {BLUE}Fix:{RESET} {fix}")


# ════════════════════════════════════════════════════════════
#  MAIN
# ════════════════════════════════════════════════════════════
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LIP Pipeline Diagnostics & Auto-Fix")
    parser.add_argument("--db", default=DB_PATH)
    parser.add_argument("--lpi-file", default=None, help="Path to LPI xlsx file")
    parser.add_argument("--fix-all", action="store_true", help="Run all available fixes")
    parser.add_argument("--fix-commodities", action="store_true", help="Fix commodity URL and re-ingest")
    parser.add_argument("--fix-lpi", action="store_true", help="Load LPI from local file")
    parser.add_argument("--fix-fred-fallback", action="store_true", help="Use World Bank API instead of FRED")
    args = parser.parse_args()

    print(f"\n{BOLD}{'═'*60}{RESET}")
    print(f"{BOLD}  LOGISTICS INTELLIGENCE PLATFORM — DIAGNOSTICS{RESET}")
    print(f"{BOLD}  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{RESET}")
    print(f"{BOLD}{'═'*60}{RESET}")

    db_results    = check_database(args.db)
    network_results = check_network()
    file_results  = check_source_files(args.lpi_file)
    quality_results = check_data_quality(args.db)

    if args.fix_all or args.fix_commodities:
        fix_commodity_url(args.db)

    if args.fix_all or args.fix_lpi:
        lpi_path = args.lpi_file or file_results.get("lpi_file")
        fix_lpi(args.db, lpi_path)

    if args.fix_all or args.fix_fred_fallback:
        fix_fred_fallback(args.db)

    if not any([args.fix_all, args.fix_commodities, args.fix_lpi, args.fix_fred_fallback]):
        print_action_plan(db_results, network_results, file_results)

    print(f"\n{BOLD}{'═'*60}{RESET}\n")