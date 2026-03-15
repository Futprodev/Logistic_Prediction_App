# Logistics Intelligence Platform

A data pipeline and intelligence system for supply chain analytics — predicting shipping costs, flagging route delays, monitoring commodity and fuel price exposure, and surfacing tariff opportunities.

Built as part of a data-driven decision making project at HAN University.

---

## What This Does

The pipeline ingests free public datasets, stores them in a local SQLite database, and computes analytical fact tables ready for machine learning models and a future Electron desktop UI.

**Current capabilities:**
- 71 commodities monthly from 2000–2025 (World Bank CMO)
- Weekly Brent and WTI fuel prices with moving averages and spike detection
- Daily EUR/USD exchange rates and global macro indicators
- Tariff rates for 8 major importers across 12 HS chapters with FTA opportunity detection
- 7-day weather forecasts for 15 major global ports
- World Bank LPI scores for 139 countries across 7 survey years (2007–2023)
- Equasis fleet statistics (2018–2024, from local datasets)
- UN Comtrade bilateral trade flow data

---

## Project Structure

```
project/
├── config/
│   └── sources.json              # API URLs, port coordinates, schedule settings
├── datasets/                     # Raw source files (not tracked in git — see .gitignore)
│   ├── pe_data_compile_1/        # LPI, Equasis PDFs, Comtrade CSVs, WB TEU data
│   └── Supply_chain_logisitcs_problem.xlsx
├── ingestion/
│   ├── ingest_commodity_prices.py   # World Bank CMO monthly prices
│   ├── ingest_fuel_and_macro.py     # EIA fuel + ECB FX + World Bank macro
│   ├── ingest_tariffs.py            # WTO tariff rates + FTA detection
│   └── ingest_weather_and_ports.py  # Open-Meteo port weather + LPI loader
├── storage/
│   └── setup_db.py               # SQLite schema — all 25 tables defined here
├── pre_project_code/             # Earlier exploratory notebooks (KNN, supply chain)
├── project_documents/            # Spec docs and architecture documents
├── images/                       # Model output charts and visualisations
├── data/                         # SQLite database (not tracked in git)
├── logs/                         # Pipeline run logs (not tracked in git)
├── run_pipeline.py               # Master orchestrator — start here
├── diagnose_and_fix.py           # Diagnostics and auto-fix tool
└── fix_commodity_sheet.py        # One-time fix for CMO sheet parsing
```

---

## Quick Start

### Requirements
```bash
pip install requests pandas openpyxl
```

### First run
```bash
# Create schema and run all ingestion sources
python run_pipeline.py --backfill

# Check what loaded
python run_pipeline.py --status
```

### Run individual sources
```bash
python run_pipeline.py --source commodities
python run_pipeline.py --source fuel
python run_pipeline.py --source tariffs
python run_pipeline.py --source weather
```

### Diagnose issues
```bash
python diagnose_and_fix.py
```

---

## Data Sources

All sources are free and publicly accessible. No paid subscriptions required.

| Source | What It Provides | Key Required |
|--------|-----------------|--------------|
| World Bank CMO | 71 commodities monthly 2000–2025 | No |
| EIA Petroleum | Weekly Brent + WTI spot prices | Free key at eia.gov/opendata |
| ECB Data Portal | Daily EUR/USD exchange rate | No |
| World Bank API | GDP, CPI, trade indicators | No |
| Open-Meteo | Port weather forecasts | No |
| WTO TAO | Tariff rates + FTA preferential rates | No |
| World Bank LPI | Logistics Performance Index 2007–2023 | No |
| UN Comtrade | Bilateral trade flows | Free key at comtradeapi.un.org |
| Equasis | Fleet statistics and vessel safety data | Local files |

### Optional API keys (set as environment variables)
```bash
# Windows
set EIA_API_KEY=your_key_here

# Mac/Linux
export EIA_API_KEY=your_key_here
```

Get a free EIA key at: https://www.eia.gov/opendata/register.php

---

## Database

Default location: `./data/lip.db` (SQLite, ~1–5 MB)

Override path:
```bash
set LIP_DB_PATH=C:/custom/path/lip.db
python run_pipeline.py
```

### Key tables

| Table | Description | Rows (approx) |
|-------|-------------|----------------|
| `raw_commodity_prices` | Raw CMO download | ~22,000 |
| `fact_commodity_prices` | Cleaned + MoM/YoY changes + spike flags | ~22,000 |
| `raw_fuel_prices` | EIA weekly Brent + WTI | ~300 |
| `fact_fuel_prices` | With 30d/180d moving averages | ~300 |
| `raw_macro_indicators` | FX rates, GDP, CPI | ~600 |
| `raw_tariff_rates` | WTO MFN + preferential rates | ~288 |
| `fact_tariff_rates` | With rate-change flags | ~288 |
| `raw_weather_port` | 7-day forecasts per port | ~105 |
| `raw_lpi_scores` | LPI raw survey data | ~2,158 |
| `dim_countries` | Country dimension with LPI scores | ~119 |
| `dim_ports` | Port coordinates and metadata | 15 |
| `pred_rate_alerts` | Generated alerts (FTA, fuel spikes, etc.) | ~68+ |

---

## Scheduled Runs

Add to crontab (`crontab -e`) on Mac/Linux:
```
0 6 * * *   cd /path/to/project && python run_pipeline.py --source weather
0 7 * * 1   cd /path/to/project && python run_pipeline.py --source fuel
0 8 2 * *   cd /path/to/project && python run_pipeline.py --source commodities
0 9 2 * *   cd /path/to/project && python run_pipeline.py --source tariffs
```

On Windows use Task Scheduler pointing to `run_pipeline.py`.

---

## Pre-Project Exploratory Work

The `pre_project_code/` folder contains earlier notebooks:
- `supply_chain_data.ipynb` — initial data exploration
- `supply_chain_KNN.ipynb` — KNN classification model on supply chain data
- `spacex_data.ipynb` — SpaceX launch data analysis

Model output charts are saved in `images/`.

---

## Documents

- `project_documents/Logistics_Intelligence_Platform.docx` — full data strategy and pipeline architecture
- `project_documents/Landed_Cost_Pipeline_Spec.docx` — landed cost calculator spec (schema, ML features, API contract)

---

## Next Steps

- [ ] Build landed cost calculation engine (Express.js service)
- [ ] Train LandedCostPredictor XGBoost model
- [ ] Integrate Equasis fleet data into vessel dimension table
- [ ] Parse and load US BTS port throughput data (7z files in datasets)
- [ ] Build Electron + React frontend