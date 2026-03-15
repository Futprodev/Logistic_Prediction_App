#!/usr/bin/env python3
"""
run_pipeline.py
Master ingestion orchestrator for the Logistics Intelligence Platform.
Runs all data sources in dependency order, logs results, handles failures gracefully.

Usage:
    python run_pipeline.py                    # Run all sources
    python run_pipeline.py --source fuel      # Run one source only
    python run_pipeline.py --setup-only       # Just create DB schema
    python run_pipeline.py --backfill         # Full historical load

Environment variables:
    LIP_DB_PATH      Path to SQLite database (default: ./data/lip.db)
    EIA_API_KEY      EIA API key (get free at eia.gov/opendata) — optional
    COMTRADE_API_KEY UN Comtrade key — optional
    CMEMS_API_KEY    Copernicus Marine key — optional
"""

import os
import sys
import time
import logging
import argparse
import traceback
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"./logs/pipeline_{datetime.now().strftime('%Y%m%d')}.log",
                           mode="a") if Path("./logs").exists() or not Path("./logs").mkdir(parents=True, exist_ok=True) else logging.StreamHandler()
    ]
)
log = logging.getLogger("pipeline")

DB_PATH = os.environ.get("LIP_DB_PATH", "./data/lip.db")


def step(name: str, fn, **kwargs) -> bool:
    """Run a pipeline step with error handling and timing."""
    log.info(f"{'─'*60}")
    log.info(f"▶  STARTING: {name}")
    start = time.time()
    try:
        fn(**kwargs)
        elapsed = time.time() - start
        log.info(f"✓  DONE: {name} ({elapsed:.1f}s)")
        return True
    except Exception as e:
        elapsed = time.time() - start
        log.error(f"✗  FAILED: {name} ({elapsed:.1f}s) — {e}")
        log.debug(traceback.format_exc())
        return False


def run_all(db_path: str, backfill: bool = False, lpi_file: str = None):
    results = {}

    # ── 1. Database setup (always first) ─────────────────────────────────
    from storage.setup_db import setup
    ok = step("Database Schema Setup", setup, db_path=db_path)
    results["db_setup"] = ok
    if not ok:
        log.error("Cannot proceed without database. Exiting.")
        sys.exit(1)

    # ── 2. Dimension / reference data (no external deps) ─────────────────
    from ingestion.ingest_weather_and_ports import run as run_weather
    results["weather_ports_lpi"] = step(
        "Weather (Open-Meteo) + LPI + Port Dimensions",
        run_weather,
        db_path=db_path,
        lpi_file=lpi_file or "/mnt/project/International_LPI_from_2007_to_2023_0.xlsx"
    )

    # ── 3. Commodity prices (World Bank) ─────────────────────────────────
    from ingestion.ingest_commodity_prices import run as run_commodities
    results["commodity_prices"] = step(
        "World Bank Commodity Prices",
        run_commodities,
        db_path=db_path,
        backfill=backfill
    )

    # ── 4. Fuel prices + macro (FRED + EIA) ──────────────────────────────
    from ingestion.ingest_fuel_and_macro import run as run_fuel
    results["fuel_macro"] = step(
        "Fuel Prices + Macro Indicators (FRED/EIA)",
        run_fuel,
        db_path=db_path
    )

    # ── 5. Tariffs (WTO TAO) ──────────────────────────────────────────────
    from ingestion.ingest_tariffs import run as run_tariffs
    results["tariffs"] = step(
        "WTO Tariff Rates",
        run_tariffs,
        db_path=db_path
    )

    # ── Summary ───────────────────────────────────────────────────────────
    log.info(f"{'═'*60}")
    log.info("PIPELINE SUMMARY")
    log.info(f"{'═'*60}")
    for name, ok in results.items():
        status = "✓ OK" if ok else "✗ FAILED"
        log.info(f"  {status:10} {name}")

    passed = sum(1 for v in results.values() if v)
    log.info(f"\n  {passed}/{len(results)} steps completed successfully")

    if passed < len(results):
        log.warning("Some steps failed — check logs above. Partial data may be available.")

    return results


def print_status(db_path: str):
    """Print a quick data status report."""
    try:
        import sqlite3
        conn = sqlite3.connect(db_path)

        tables = [
            ("raw_commodity_prices", "Commodity prices"),
            ("raw_fuel_prices", "Fuel prices"),
            ("raw_tariff_rates", "Tariff rates"),
            ("raw_weather_port", "Port weather"),
            ("raw_lpi_scores", "LPI scores"),
            ("fact_commodity_prices", "Fact: commodity"),
            ("fact_tariff_rates", "Fact: tariffs"),
            ("fact_fuel_prices", "Fact: fuel"),
            ("pred_rate_alerts", "Active alerts"),
        ]

        print(f"\n{'─'*50}")
        print(f"  DATABASE STATUS: {db_path}")
        print(f"{'─'*50}")
        for table, label in tables:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f"  {label:30} {count:>8,} rows")
            except Exception:
                print(f"  {label:30} {'not found':>8}")

        # Latest data dates
        print(f"\n{'─'*50}")
        print("  LATEST DATA DATES")
        print(f"{'─'*50}")
        checks = [
            ("raw_commodity_prices", "MAX(date)", "Latest commodity price"),
            ("raw_fuel_prices", "MAX(date)", "Latest fuel price"),
            ("raw_weather_port", "MAX(date)", "Latest weather"),
            ("pred_rate_alerts", "MAX(triggered_at)", "Latest alert"),
        ]
        for table, agg, label in checks:
            try:
                val = conn.execute(f"SELECT {agg} FROM {table}").fetchone()[0]
                print(f"  {label:30} {val or 'no data':>20}")
            except Exception:
                pass

        conn.close()
    except Exception as e:
        print(f"Could not read database: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Logistics Intelligence Platform — Data Pipeline")
    parser.add_argument("--db", default=DB_PATH, help="Path to SQLite database")
    parser.add_argument("--source", choices=["all", "fuel", "commodities", "tariffs", "weather"],
                        default="all", help="Which source to run")
    parser.add_argument("--setup-only", action="store_true", help="Only create DB schema, no ingestion")
    parser.add_argument("--backfill", action="store_true", help="Load full historical data")
    parser.add_argument("--lpi-file", default=None, help="Path to LPI .xlsx file")
    parser.add_argument("--status", action="store_true", help="Show database status and exit")
    args = parser.parse_args()

    # Make dirs
    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    Path("./logs").mkdir(parents=True, exist_ok=True)

    if args.status:
        print_status(args.db)
        sys.exit(0)

    if args.setup_only:
        from storage.setup_db import setup
        setup(args.db)
        print_status(args.db)
        sys.exit(0)

    if args.source == "all":
        run_all(db_path=args.db, backfill=args.backfill, lpi_file=args.lpi_file)
    elif args.source == "fuel":
        from storage.setup_db import setup
        setup(args.db)
        from ingestion.ingest_fuel_and_macro import run
        run(db_path=args.db)
    elif args.source == "commodities":
        from storage.setup_db import setup
        setup(args.db)
        from ingestion.ingest_commodity_prices import run
        run(db_path=args.db, backfill=args.backfill)
    elif args.source == "tariffs":
        from storage.setup_db import setup
        setup(args.db)
        from ingestion.ingest_tariffs import run
        run(db_path=args.db)
    elif args.source == "weather":
        from storage.setup_db import setup
        setup(args.db)
        from ingestion.ingest_weather_and_ports import run
        run(db_path=args.db, lpi_file=args.lpi_file)

    print_status(args.db)
