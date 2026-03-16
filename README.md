# Logistics Intelligence Platform

A data pipeline and intelligence system for supply chain analytics. The platform ingests free public datasets, stores them in a local SQLite database, and provides a desktop application for predicting shipping costs, monitoring commodity and fuel price exposure, flagging tariff opportunities, and surfacing port weather risk.

Built as part of a Data-Driven Decision Making project at BINUS University.

---

## Table of Contents

1. [What This Does](#1-what-this-does)
2. [How the Landed Cost Calculator Works](#2-how-the-landed-cost-calculator-works)
3. [How the ML Model Works](#3-how-the-ml-model-works)
4. [Database Architecture](#4-database-architecture)
5. [Data Sources and Methodology](#5-data-sources-and-methodology)
6. [Static Reference Data — Sources and Justification](#6-static-reference-data--sources-and-justification)
7. [What is Real vs Estimated](#7-what-is-real-vs-estimated)
8. [Project Structure](#8-project-structure)
9. [Quick Start](#9-quick-start)
10. [Running the Pipeline](#10-running-the-pipeline)
11. [Scheduled Runs](#11-scheduled-runs)

---

## 1. What This Does

The platform has five core capabilities:

**Landed Cost Calculator** — Estimates the total cost of shipping cargo from an origin port to a destination port, broken down into freight, import duty, insurance, port handling, and customs brokerage. Runs two methods in parallel: a deterministic rule-based formula and a trained XGBoost ML model, so you can compare both and understand where they diverge.

**Commodity Price Monitor** — Tracks 71 commodities monthly from 2000 to present (World Bank CMO). Computes month-over-month and year-over-year change, flags anomalies when prices move beyond statistical thresholds, and stores 3-year price history charts per commodity.

**Fuel Price Tracker** — Weekly Brent and WTI crude prices from EIA with 30-day and 180-day moving averages. Automatically flags when current price deviates more than 20% from the 180-day average, which feeds into freight cost calculations as a surcharge multiplier.

**Port Intelligence** — 15 major global ports with 7-day weather forecasts (Open-Meteo), efficiency scores derived from World Bank LPI data, congestion indices from World Bank CPPI 2023, and live freight cost impact calculations per TEU.

**Alert System** — Automatically generated alerts for commodity price spikes, fuel surcharges, tariff rate changes, and FTA (Free Trade Agreement) opportunities where a preferential tariff rate would save money vs the standard MFN rate.

---

## 2. How the Landed Cost Calculator Works

### Rule-Based Estimation (Deterministic)

The rule-based method computes landed cost using a known formula. Every component is either a database lookup or a fixed calculation. It always produces a result, even without an internet connection.

```
Total Landed Cost =
    FOB Value
  + Freight Cost
  + Import Duty
  + Insurance
  + Port Handling (THC)
  + Customs Brokerage
```

**Step 1 — FOB Value**

If the user provides a FOB (Free on Board) value, it is used directly. If not, the system estimates it by:
1. Taking the first 2 digits of the HS code to identify the commodity chapter (e.g. `720839` → chapter `72` = Iron and Steel)
2. Looking up the linked commodity in `dim_hs_codes` (e.g. chapter 72 → `iron_ore_cfr_spot`)
3. Getting the latest price from `fact_commodity_prices`
4. Converting to USD total using cargo weight and unit conversion (per tonne, per barrel, per kg etc.)

If no commodity match is found, a fallback of $2/kg is used and flagged as a warning in the response.

**Step 2 — Freight Cost**

```
Freight = base_rate_per_nm × distance_nm × teu_fraction × fuel_surcharge_multiplier
```

- `base_rate_per_nm` comes from a distance band lookup ($0.25–0.35/nm/TEU)
- `distance_nm` comes from a hardcoded port-pair distance table (sourced from sea-distances.org)
- `teu_fraction` = cargo_weight_kg / 10,000 (one TEU ≈ 10,000 kg max cargo)
- `fuel_surcharge_multiplier` is applied when Brent crude is >20% above its 180-day average. The fuel cost component is ~35% of base freight, so a 20% fuel spike adds roughly 7% to the freight total

**Step 3 — Import Duty**

1. Look up destination country ISO from `dim_ports`
2. Query `fact_tariff_rates` for the best available rate — tries preferential (FTA) first, falls back to MFN (Most Favoured Nation)
3. `duty = CIF_value × tariff_rate / 100`
4. If a preferential rate exists that is >3pp below MFN, an FTA Opportunity alert is generated

**Step 4 — Insurance**

```
Insurance = (FOB + freight) × 0.002
```

Standard CIF insurance rate of 0.2%. Industry convention for cargo insurance on international shipments.

**Step 5 — Port Handling (THC)**

Terminal Handling Charges looked up by destination port from published carrier THC schedules (Maersk, MSC, CMA CGM 2023). Scaled by TEU fraction.

**Step 6 — Customs Brokerage**

Flat fee per destination country from FIATA 2023 rate survey. Range $300–$800 depending on regulatory complexity.

**Confidence Interval**

The rule-based estimate comes with a confidence interval that widens under certain conditions:

| Condition | Added Uncertainty |
|-----------|------------------|
| FOB was estimated (not provided) | +4% |
| Tariff rate changed in last 90 days | +3% |
| Fuel spiking >20% above 180d average | +2% |
| No tariff data found for route | +5% |

Base confidence interval is ±5%.

---

## 3. How the ML Model Works

### What the Model Predicts

The XGBoost model predicts **ocean freight cost** for a shipment. It replaces the rule-based freight component with a learned prediction. All other components (duty, insurance, THC, brokerage) remain rule-based. The total landed cost uses the ML freight prediction when available and falls back to rule-based freight if the model is unavailable.

### Why XGBoost

XGBoost (Extreme Gradient Boosting) is a tree-based ensemble method that excels at learning non-linear relationships in tabular data. It was chosen over linear regression because freight pricing has non-linear interactions — for example, a fuel price spike matters much more on a 12,000nm route than a 500nm route, and that interaction cannot be captured by a linear model.

### Training Data Generation

We do not have historical shipment records. This is common for new logistics platforms. The standard approach is **simulation-based training**: define a data-generating process from domain knowledge, add realistic market variance, and train a model that learns the underlying relationships.

The data-generating process is based on the Stopford maritime economics pricing model (*Maritime Economics*, 3rd ed., 2009):

```
Freight = Base Rate
        + Fuel Surcharge
        + Fuel Spike Premium
        + Origin Port Cost
        + Destination Port Cost
        + Congestion Surcharge
        + Canal Toll
        × Seasonal Multiplier
        × Market Noise
```

Each component is parameterised with real data where available:

| Component | Data Source |
|-----------|------------|
| Fuel price | Real Brent crude prices from `fact_fuel_prices` (EIA, 3 years weekly) |
| Fuel deviation | Real 180-day moving average deviation from `fact_fuel_prices` |
| Port efficiency | World Bank LPI Infrastructure scores 2023 |
| Port congestion | World Bank CPPI 2023 |
| Distance | sea-distances.org port pair table |
| Canal toll | IMO published canal tariff estimates |
| Seasonal factor | 15% premium Oct–Jan (published container demand seasonality) |
| Market noise | Normal distribution σ=13%, clipped to ±50% — calibrated to published container freight rate coefficient of variation (Clarkson Research) |

3,000 training samples were generated by randomly combining port pairs, cargo weights (log-normal distribution, mean ~15 TEU), and real fuel price observations across the 3-year history.

### Features

| Feature | Source | Importance |
|---------|--------|-----------|
| cargo_teu | Derived from cargo_weight_kg / 10,000 | 36.7% |
| cargo_weight_kg | User input | 35.4% |
| distance_nm | Port pair lookup table | 10.8% |
| distance_band_enc | Encoded from distance_nm | 9.0% |
| canal_required | Route lookup (0=none, 1=Panama, 2=Suez) | 2.3% |
| is_peak_season | Month in Oct–Jan | 1.4% |
| dest_efficiency | LPI-derived port efficiency score | 1.0% |
| origin_efficiency | LPI-derived port efficiency score | 0.8% |
| month | Current month (1–12) | 0.7% |
| fuel_price_brent | Latest from fact_fuel_prices | 0.6% |
| dest_congestion | CPPI-derived congestion index | 0.5% |
| origin_congestion | CPPI-derived congestion index | 0.5% |
| fuel_deviation_pct | % deviation from 180d average | 0.5% |

Cargo size (TEU and weight) dominates at ~72% combined importance. Distance accounts for ~20%. All other features share the remaining ~8%.

### Model Performance

Trained on 3,000 samples, evaluated on 20% held-out test set:

| Metric | Value | Interpretation |
|--------|-------|----------------|
| R² (test) | 0.957 | Model explains 95.7% of freight cost variance |
| R² (train) | 0.999 | Mild overfitting — normal for XGBoost |
| MAE | $1,219 | Average absolute error per shipment |
| MAPE | 16.5% | Average percentage error |
| RMSE | $3,005 | Larger errors on outlier shipments |

The 16.5% MAPE reflects genuine market uncertainty. Container freight rates have a published coefficient of variation of 12–18% around their fundamental value (Clarkson Research Container Intelligence). A perfect model on this data would achieve ~13% MAPE — the model adds approximately 3.5% error on top of irreducible market noise.

### Rule-Based vs ML Comparison

The application shows both estimates side by side. Key interpretation:

| Gap | Meaning |
|-----|---------|
| < 10% | Models agree — typical shipment |
| 10–25% | Moderate divergence — non-linear effects present |
| > 25% | Large divergence — investigate feature contributions |

When ML predicts higher than rule-based, it is typically capturing port congestion surcharges or seasonal demand premiums that the fixed distance-band formula does not account for.

---

## 4. Database Architecture

The database is SQLite, stored locally at `./data/lip.db`. It follows a star schema with raw, validated, fact, dimension, and prediction layers.

### Pipeline Stages

```
External APIs → Raw Tables → Fact Tables → Prediction Tables → API → UI
```

**Raw tables** store data exactly as received — never modified. Serve as the audit trail.

**Fact tables** store cleaned, normalised, enriched data ready for analytics. Computed fields (moving averages, change percentages, spike flags) are added here by SQL transformations run inside the ingestion scripts.

**Dimension tables** store reference entities — ports, countries, HS codes, commodities.

**Prediction tables** store every calculation result with full inputs and outputs. `pred_landed_cost_estimates` records every API call so results can be audited and used as future training data.

### Spike Detection Algorithm

Applied to both commodity prices and fuel prices:

```sql
spike_flag = 1 IF:
    ABS(mom_change_pct) > 15%   -- month-over-month
    OR
    ABS(yoy_change_pct) > 40%   -- year-over-year
```

For fuel specifically:
```
fuel_spike = 1 IF current_price > ma_180d × 1.20
```

The thresholds (15% MoM, 40% YoY, 20% vs 180d average) are calibrated to historical commodity price volatility — moves above these thresholds are statistically unusual (beyond 1.5–2 standard deviations for most commodity series).

### Key Tables

| Table | Rows | Description |
|-------|------|-------------|
| `raw_commodity_prices` | ~22,000 | World Bank CMO raw download |
| `fact_commodity_prices` | ~22,000 | With MoM/YoY change % and spike flags |
| `raw_fuel_prices` | ~312 | EIA weekly Brent + WTI |
| `fact_fuel_prices` | ~312 | With 30d/180d moving averages |
| `raw_macro_indicators` | ~598 | ECB EUR/USD, World Bank GDP/CPI/trade |
| `raw_tariff_rates` | ~288 | WTO MFN and preferential rates |
| `fact_tariff_rates` | ~288 | With rate-change flags and FTA detection |
| `raw_weather_port` | ~105 | Open-Meteo 7-day forecasts per port |
| `raw_lpi_scores` | ~2,158 | World Bank LPI 2007–2023 all dimensions |
| `dim_countries` | ~119 | Country reference with LPI scores |
| `dim_ports` | 15 | Port coordinates, country, efficiency |
| `dim_hs_codes` | ~32 | HS chapter to commodity mappings |
| `pred_landed_cost_estimates` | grows | Every calculation request |
| `pred_rate_alerts` | ~68+ | FTA opportunities, fuel spikes, tariff changes |

---

## 5. Data Sources and Methodology

All datasets are free and publicly accessible.

| Source | What It Provides | URL | Update Schedule |
|--------|-----------------|-----|----------------|
| World Bank CMO | 71 commodities monthly 2000–2025 | thedocs.worldbank.org | Monthly |
| EIA Petroleum | Weekly Brent + WTI spot prices | api.eia.gov | Weekly |
| ECB Data Portal | Daily EUR/USD exchange rate | data-api.ecb.europa.eu | Daily |
| World Bank API | GDP growth, CPI, trade % GDP | api.worldbank.org | Annual |
| Open-Meteo | Port weather forecasts 7-day | api.open-meteo.com | Daily |
| World Bank LPI | Logistics Performance Index 2007–2023 | lpi.worldbank.org | Biennial |
| WTO TAO | Tariff rates 160+ countries HS codes | tao.wto.org | Annual |
| World Bank WDI | Container TEU traffic per country | api.worldbank.org | Annual |
| Equasis | Fleet statistics 2018–2024 | equasis.org | Annual |
| UN Comtrade | Bilateral trade flows | comtradeapi.un.org | Annual |

---

## 6. Static Reference Data — Sources and Justification

Some values are hardcoded rather than ingested from live APIs. Every value is sourced from a published reference.

### Port Congestion Index (0–1)

**Source:** World Bank Container Port Performance Index (CPPI) 2023
https://openknowledge.worldbank.org/handle/10986/39199

**Methodology:** CPPI measures median ship turnaround time per port. Lower turnaround = better performance. CPPI rank was normalised to a 0–1 congestion index where 1 = most congested.

| Port | Congestion | Basis |
|------|-----------|-------|
| PSA | 0.50 | CPPI rank 1 globally, 0.5 day median turnaround |
| PUS | 0.45 | CPPI rank 6, consistent top Asian performer |
| ANR | 0.45 | CPPI rank 10, strong European performance |
| RTM | 0.50 | CPPI rank 4, leading European port |
| HAM | 0.50 | CPPI rank 8, reliable Northern European port |
| NGB | 0.65 | High volume, moderate documented congestion |
| SHA | 0.70 | World's busiest port, documented congestion events |
| DXB | 0.55 | Middle East routing complexity |
| KUL | 0.50 | Moderate throughput relative to capacity |
| CMB | 0.55 | Transshipment hub, moderate anchor waiting |
| PIR | 0.40 | Lower volume = lower congestion |
| LAX | 0.80 | CPPI rank 78, severe documented congestion 2021–2023 |
| LGB | 0.75 | Same US West Coast bottleneck |
| MUM | 0.70 | CPPI rank 95, Indian customs delays |
| MBA | 0.60 | Limited infrastructure, long dwell times (Kenya Ports Authority) |

### Port Efficiency Score (1–5)

**Source:** World Bank LPI 2023 — Infrastructure dimension
https://lpi.worldbank.org

The LPI infrastructure score for each port's host country is used directly. This is a legitimate proxy — the LPI infrastructure dimension specifically measures quality of trade and transport infrastructure including ports. Verifiable in the `raw_lpi_scores` table.

### Port Distances (nautical miles)

**Source:** sea-distances.org and ports.com
Verifiable at: https://sea-distances.org

Canal routes use passage distance, not the alternative route distance.

### Terminal Handling Charges (THC)

**Source:** Published carrier THC schedules 2023 (Maersk, MSC, CMA CGM) and Drewry Maritime Research Container Census 2023.

### Customs Brokerage Fees

**Source:** FIATA 2023 rate survey and Flexport published rate cards.

---

## 7. What is Real vs Estimated

| Feature | Status | Notes |
|---------|--------|-------|
| Commodity prices | Real | World Bank CMO, monthly, last updated Jan 2025 |
| Fuel prices (Brent/WTI) | Real | EIA weekly, current as of last pipeline run |
| EUR/USD exchange rate | Real | ECB daily, current as of last pipeline run |
| GDP / CPI / trade data | Real | World Bank annual series |
| LPI country scores | Real | World Bank 2023 survey, in database |
| Port weather forecasts | Real | Open-Meteo 7-day, refreshed when pipeline runs |
| Tariff rates | Partial | Static MFN averages — not live WTO data |
| Port congestion index | Static | CPPI 2023 baseline — does not update automatically |
| Port distances | Static | sea-distances.org — correct for fixed routes |
| THC charges | Static | 2023 published rates |
| ML freight prediction | Trained | XGBoost trained on synthetic + real fuel data |
| FOB commodity estimate | Derived | Latest commodity price × cargo weight |

**Important:** Weather, fuel, and macro data is only as fresh as the last pipeline run. Set up scheduled runs (Section 11) to keep data current.

---

## 8. Project Structure

```
project/
├── api/                             # Express.js REST API (Node.js, localhost:3001)
│   ├── server.js                    # All endpoints
│   ├── services/
│   │   ├── landedCost.js            # Rule-based + ML calculation engine
│   │   └── alerts.js                # Alert retrieval and acknowledgement
│   └── package.json
│
├── electron-app/                    # Desktop UI (Electron + React)
│   ├── main.js                      # Electron main process
│   ├── preload.js                   # Context bridge
│   ├── src/
│   │   ├── App.js                   # All views: Dashboard, Calculator, Commodities, Ports, Alerts
│   │   ├── index.js                 # React entry point
│   │   ├── index.css                # Design system
│   │   └── services/api.js          # All API calls to localhost:3001
│   └── public/index.html
│
├── ml/                              # Machine learning
│   ├── train_freight_model.py       # Generates training data + trains XGBoost
│   ├── predict.py                   # Called by Node.js for inference
│   └── models/
│       ├── freight_model.pkl        # Trained model (not in git — regenerate)
│       └── freight_model_meta.json  # Metrics and feature importances
│
├── ingestion/                       # Data pipeline scripts
│   ├── ingest_commodity_prices.py   # World Bank CMO
│   ├── ingest_fuel_and_macro.py     # EIA + ECB + World Bank API
│   ├── ingest_tariffs.py            # WTO tariff rates
│   └── ingest_weather_and_ports.py  # Open-Meteo + LPI loader
│
├── storage/
│   └── setup_db.py                  # SQLite schema — all 25 tables
│
├── config/
│   └── sources.json                 # API URLs, port coordinates, settings
│
├── datasets/
│   └── pe_data_compile_1/
│       └── International_LPI_from_2007_to_2023_0.xlsx
│
├── data/                            # SQLite database (not in git)
├── logs/                            # Pipeline logs (not in git)
└── run_pipeline.py                  # Master orchestrator — start here
```

---

## 9. Quick Start

### Requirements

```bash
pip install requests pandas openpyxl xgboost scikit-learn numpy
# Node.js v18+ from nodejs.org
```

### First run

```bash
# 1. Set up database and ingest all data
python run_pipeline.py --backfill

# 2. Train the ML model
python ml/train_freight_model.py

# 3. Start the API (Terminal 1)
cd api && npm install && node server.js

# 4. Start the desktop app (Terminal 2)
cd electron-app && npm install && npm run dev
```

### Check status

```bash
python run_pipeline.py --status
```

### Optional API key (unlocks full EIA history)

```bash
# Get free key at eia.gov/opendata/register.php
set EIA_API_KEY=your_key_here    # Windows
export EIA_API_KEY=your_key_here # Mac/Linux
```

---

## 10. Running the Pipeline

```bash
python run_pipeline.py                    # Run all sources
python run_pipeline.py --source fuel      # Fuel prices only
python run_pipeline.py --source commodities
python run_pipeline.py --source tariffs
python run_pipeline.py --source weather
python run_pipeline.py --backfill         # Full historical load
python run_pipeline.py --status           # Show row counts
python diagnose_and_fix.py                # Diagnose and auto-fix issues
```

---

## 11. Scheduled Runs

Add to crontab on Mac/Linux (`crontab -e`):

```
0 6 * * *   cd /path/to/project && python run_pipeline.py --source weather
0 7 * * 1   cd /path/to/project && python run_pipeline.py --source fuel
0 8 2 * *   cd /path/to/project && python run_pipeline.py --source commodities
0 9 2 * *   cd /path/to/project && python run_pipeline.py --source tariffs
```

On Windows use Task Scheduler pointing to `run_pipeline.py`.
