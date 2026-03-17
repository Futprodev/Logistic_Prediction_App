<p align="center">
  <img src="electron-app/assets/icon.png" width="80" alt="Logistics Intelligence Platform" />
</p>

<h1 align="center">Logistics Intelligence Platform</h1>

<p align="center">
  A data pipeline and intelligence system for supply chain analytics.
  Built as part of a Data-Driven Decision Making project at BINUS University.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10+-blue?style=flat-square&logo=python" />
  <img src="https://img.shields.io/badge/Node.js-18+-green?style=flat-square&logo=node.js" />
  <img src="https://img.shields.io/badge/Electron-28-blue?style=flat-square&logo=electron" />
  <img src="https://img.shields.io/badge/XGBoost-ML-orange?style=flat-square" />
  <img src="https://img.shields.io/badge/SQLite-database-lightgrey?style=flat-square&logo=sqlite" />
  <img src="https://img.shields.io/badge/ARIMA-forecasting-purple?style=flat-square" />
  <img src="https://img.shields.io/badge/Prophet-forecasting-red?style=flat-square" />
</p>

---

## Table of Contents

1. [What This Does](#1-what-this-does)
2. [How the Landed Cost Calculator Works](#2-how-the-landed-cost-calculator-works)
3. [How the ML Model Works](#3-how-the-ml-model-works)
4. [Commodity Price Forecasting](#4-commodity-price-forecasting)
5. [Route Distance Calculation](#5-route-distance-calculation)
6. [Carbon Cost Estimator](#6-carbon-cost-estimator)
7. [Pipeline Scheduler](#7-pipeline-scheduler)
8. [Database Architecture](#8-database-architecture)
9. [Data Sources and Methodology](#9-data-sources-and-methodology)
10. [Static Reference Data — Sources and Justification](#10-static-reference-data--sources-and-justification)
11. [What is Real vs Estimated](#11-what-is-real-vs-estimated)
12. [Project Structure](#12-project-structure)
13. [Quick Start](#13-quick-start)
14. [Running the Pipeline](#14-running-the-pipeline)

---

## 1. What This Does

The platform has eight core capabilities:

**Landed Cost Calculator** — Estimates the total cost of shipping cargo from an origin port to a destination port. Runs two methods in parallel: a deterministic rule-based formula and a trained XGBoost ML model. The user can switch between methods and see how the cost breakdown changes, with a visual comparison of where the two approaches diverge.

**Route Comparison** — Compares landed cost across up to 5 destination ports simultaneously. Ranks routes by total cost with breakdown per route, CO₂ emissions, and FTA opportunity flags.

**Carbon Cost Estimator** — Calculates CO₂e emissions and EU ETS carbon cost per shipment using the IMO MEPC.1/Circ.684 EEOI methodology. Shows emissions in context against everyday reference points.

**Commodity Price Monitor** — Tracks 71 commodities monthly from 2000 to present. Computes MoM and YoY change, flags anomalies, and shows price history with 6-month forecasts from three models.

**Commodity Price Forecasting** — Three competing forecasting models per commodity: ARIMA (statistical baseline), Prophet (trend + seasonality), and XGBoost cross-commodity (inter-market relationships). Models are toggled on/off with MAPE displayed per model.

**Fuel Price Tracker** — Weekly Brent and WTI crude prices with 30-day and 180-day moving averages. Flags fuel spikes that affect freight cost calculations.

**Port Intelligence** — 15 major global ports with 7-day weather forecasts, LPI efficiency scores, CPPI congestion indices, TEU throughput, and freight cost impact per TEU.

**Alert System** — Automatically generated alerts for commodity price spikes, fuel surcharges, tariff changes, and FTA opportunities. Alerts can be dismissed and restored.

---

## 2. How the Landed Cost Calculator Works

### Rule-Based Estimation (Deterministic)

```
Total Landed Cost =
    FOB Value
  + Freight Cost        ← rule-based formula OR ML prediction
  + Import Duty
  + Insurance
  + Port Handling (THC)
  + Customs Brokerage
```

**Step 1 — FOB Value**

If the user provides a FOB value it is used directly. If not, the system estimates from the HS code:
1. First 2 digits of HS code identify the commodity chapter (e.g. `720839` → chapter `72` = Iron and Steel)
2. `dim_hs_codes` maps the chapter to a commodity (e.g. chapter 72 → `iron_ore_cfr_spot`)
3. Latest price from `fact_commodity_prices` × cargo weight × unit conversion

Unit conversions handled: per metric tonne, per barrel, per mmbtu, per cubic metre, per kg. Fallback of $2/kg if no match found.

Note: the FOB estimate only changes when the HS code changes (different commodity) or cargo weight changes. It is not affected by route or destination.

**Step 2 — Freight Cost (Rule-Based)**

```
Freight = base_rate_per_nm × distance_nm × teu_fraction × fuel_surcharge_multiplier
```

- `distance_nm` — Haversine great circle distance from `dim_ports` coordinates + canal corrections
- `base_rate_per_nm` — distance band lookup ($0.25–0.35/nm/TEU)
- `teu_fraction` = cargo_weight_kg / 10,000
- `fuel_surcharge_multiplier` — applied when Brent >20% above 180d average

**Step 3 — Import Duty**

Queries `fact_tariff_rates` — preferential (FTA) rate first, falls back to MFN. If preferential rate is >3pp below MFN, an FTA Opportunity alert is generated.

**Step 4 — Insurance**

`Insurance = (FOB + freight) × 0.002` — standard 0.2% CIF insurance rate.

**Step 5 — Port Handling (THC)**

Looked up by destination port from published carrier THC schedules (Maersk, MSC, CMA CGM 2023). Scaled by TEU fraction.

**Step 6 — Customs Brokerage**

Flat fee per destination country from FIATA 2023 rate survey ($300–$800).

**Confidence Interval**

| Condition | Added Uncertainty |
|-----------|-----------------|
| FOB estimated (not provided) | +4% |
| Tariff rate changed in last 90 days | +3% |
| Fuel spiking >20% above 180d average | +2% |
| No tariff data found | +5% |

Base: ±5%.

### Rule-Based vs ML Comparison

The UI shows both estimates side by side with three selectable views — Rule-Based, ML Prediction, and Compare. Switching views updates the cost breakdown chart and total. The gap between methods is colour-coded: green (<10%), amber (10–25%), red (>25%).

---

## 3. How the ML Model Works

### What the Model Predicts

The XGBoost model predicts ocean freight cost only. All other components (duty, insurance, THC, brokerage) remain rule-based. Falls back to rule-based freight if the model is unavailable.

### Why XGBoost

Chosen over linear regression because freight pricing has non-linear interactions — a fuel spike matters much more on a 12,000nm route than a 500nm route, which a linear model cannot capture.

### Training Data Generation

No historical shipment records are available. Simulation-based training was used: define a data-generating process from domain knowledge, add realistic market variance, train a model on the result.

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
        × Market Noise (σ=13%, calibrated to Clarkson Research container freight CoV)
```

Real data used in training:

| Component | Source |
|-----------|--------|
| Fuel price | Real Brent prices from `fact_fuel_prices` (EIA, 3yr weekly) |
| Fuel deviation | Real 180d moving average deviation |
| Port coordinates | Real lat/lon from `dim_ports` → Haversine distance |
| Port efficiency | World Bank LPI Infrastructure 2023 |
| Port congestion | World Bank CPPI 2023 |
| Canal corrections | IMO published canal tariff estimates |
| Seasonal factor | 15% premium Oct–Jan |

3,000 training samples generated by randomly combining port pairs from `dim_ports`, log-normal cargo weights (mean ~15 TEU), and real fuel price observations.

### Model Performance

| Metric | Value | Interpretation |
|--------|-------|----------------|
| R² (test) | 0.932 | Explains 93.2% of freight cost variance |
| MAE | $857 | Average absolute error per shipment |
| MAPE | 17.2% | Average percentage error |

The 17.2% MAPE reflects genuine market uncertainty. Container freight rates have a published coefficient of variation of 12–18% (Clarkson Research). MAE improved from $1,219 to $857 after switching from a hardcoded distance table to Haversine distances from the database.

### Inference

Node.js calls `ml/predict.py` via `spawnSync` with features as JSON on stdin. Python loads `freight_model.pkl`, runs XGBoost inference, returns prediction + confidence interval + feature contributions as JSON. Typical latency: 100–300ms.

---

## 4. Commodity Price Forecasting

Three competing models are trained per commodity and shown on the price history chart as dashed forecast lines extending 6 months forward.

### Model 1 — ARIMA

Classical statistical time series model. Uses the commodity's own autocorrelation structure — how strongly last month's price predicts this month's price. Order (p,d,q) selected by AIC across a grid of candidates.

Best for: energy commodities, metals — series dominated by autocorrelation with no strong seasonal pattern.

Typical MAPE: 4–8% on energy, 8–15% on agricultural commodities.

### Model 2 — Prophet

Facebook/Meta open-source forecasting tool. Decomposes the series into trend + yearly seasonality + noise. Handles outliers and missing values automatically.

Best for: agricultural commodities with crop cycle seasonality — palm oil, wheat, sugar, cotton.

Different from ARIMA because it fits a curve rather than modelling autocorrelation. Performs worse than ARIMA on energy and metals which have no seasonal pattern.

### Model 3 — XGBoost Cross-Commodity

Supervised regression using own price lags plus lagged prices of economically correlated commodities as features. Cross-commodity relationships are based on published economic linkages:

- Crude oil → fertilisers (energy input for ammonia production)
- Natural gas → urea, DAP (direct feedstock)
- Soybeans → soybean oil + soybean meal (crushing margin)
- Iron ore → steel proxy
- Gold → silver → platinum (safe-haven asset correlation)

Features are z-score normalised before adding to the feature matrix so scale differences between commodities don't dominate.

Best for: derived commodities with causal inter-market relationships at monthly frequency (fertilisers, oilseeds).

### Key Finding

Across 71 commodities ARIMA consistently outperforms both other models. This is consistent with the academic literature — Deaton & Laroque (1996) show primary commodity prices are well-described by a near-random walk, making classical time series methods competitive with ML on monthly data. XGBoost cross-commodity adds value only where strong causal inter-market relationships exist.

---

## 5. Route Distance Calculation

Distances are calculated dynamically using the **Haversine formula** — great circle distance between lat/lon coordinates read from `dim_ports`. This replaced a hardcoded lookup table.

```
d = 2R × arcsin(√(sin²(Δlat/2) + cos(lat1)cos(lat2)sin²(Δlon/2)))
R = 3440.065 nautical miles
```

### Canal Corrections

| Canal | Routes | Correction |
|-------|--------|-----------|
| Suez | Asia → NW Europe (RTM/HAM/ANR) | +700–800nm |
| Suez | Asia → Mediterranean (PIR) | +300–400nm |
| Suez | Indian Ocean → NW Europe | +500–600nm |
| Panama | Asia → US West Coast (LAX/LGB) | +250–300nm |

**Limitation:** Haversine gives straight-line distance, not actual shipping lane distance. Error is typically 3–8% vs actual sailing distance. A production system would integrate with Searoutes.com API for lane-routed distances.

---

## 6. Carbon Cost Estimator

### Methodology

Based on IMO EEOI — MEPC.1/Circ.684:

```
Fuel consumed (t HFO) = distance_nm × 0.03 t/nm/TEU × TEU
CO₂e (tonnes)         = fuel_consumed × 3.114 t CO₂/t HFO
Carbon cost (EUR)      = CO₂e × EU ETS price (€/tonne)
Carbon cost (USD)      = carbon_cost_EUR × EUR/USD rate
```

Note: carbon cost does not depend on the HS code — only distance and cargo weight. The HS code field is omitted from the carbon calculator.

EU ETS price is queried from `raw_macro_indicators` if available, otherwise falls back to €65/tonne (2024 average). EUR/USD uses the live ECB rate from the database.

---

## 7. Pipeline Scheduler

Runs inside the API server process (`api/services/scheduler.js`) using `node-cron`. State stored in `pipeline_schedule` table in SQLite.

| Source | Cron | Frequency |
|--------|------|-----------|
| weather | `0 */6 * * *` | Every 6 hours |
| fuel | `0 7 * * *` | Daily at 07:00 UTC |
| commodities | `0 8 * * 1` | Every Monday 08:00 UTC |
| tariffs | `0 9 1 * *` | 1st of month 09:00 UTC |

Scheduler starts automatically when `node server.js` starts. The Pipeline Schedule card on the dashboard shows live status and has "Run now" buttons per source.

API endpoints:
```
GET  /api/v1/scheduler              — get all schedule status
POST /api/v1/scheduler/:source/run  — trigger immediate run
```

---

## 8. Database Architecture

SQLite stored at `./data/lip.db`. Star schema with raw, fact, dimension, and prediction layers.

### Spike Detection

```sql
spike_flag = 1 IF ABS(mom_change_pct) > 15% OR ABS(yoy_change_pct) > 40%
fuel_spike = 1 IF current_price > ma_180d × 1.20
```

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
| `raw_lpi_scores` | ~2,158 | World Bank LPI 2007–2023 |
| `dim_countries` | ~119 | Country reference with LPI scores |
| `dim_ports` | 15 | Port coordinates, country, efficiency |
| `dim_hs_codes` | ~32 | HS chapter to commodity mappings |
| `pipeline_schedule` | 4 | Scheduler state per source |
| `pred_landed_cost_estimates` | grows | Every calculation request |
| `pred_rate_alerts` | ~68+ | FTA opportunities, fuel spikes, tariff changes |

---

## 9. Data Sources and Methodology

| Source | What It Provides | URL | Update Schedule |
|--------|-----------------|-----|----------------|
| World Bank CMO | 71 commodities monthly 2000–2025 | thedocs.worldbank.org | Monthly |
| EIA Petroleum | Weekly Brent + WTI spot prices | api.eia.gov | Weekly |
| ECB Data Portal | Daily EUR/USD exchange rate | data-api.ecb.europa.eu | Daily |
| World Bank API | GDP growth, CPI, trade % GDP | api.worldbank.org | Annual |
| Open-Meteo | Port weather forecasts 7-day | api.open-meteo.com | Daily |
| World Bank LPI | Logistics Performance Index 2007–2023 | lpi.worldbank.org | Biennial |
| WTO TAO | Tariff rates 160+ countries | tao.wto.org | Annual |

---

## 10. Static Reference Data — Sources and Justification

### Port Congestion Index (0–1)

**Source:** World Bank Container Port Performance Index (CPPI) 2023
https://openknowledge.worldbank.org/handle/10986/39199

| Port | Congestion | Basis |
|------|-----------|-------|
| PSA | 0.50 | CPPI rank 1 globally |
| PUS | 0.45 | CPPI rank 6 |
| ANR | 0.45 | CPPI rank 10 |
| RTM | 0.50 | CPPI rank 4 |
| HAM | 0.50 | CPPI rank 8 |
| NGB | 0.65 | High volume, moderate congestion |
| SHA | 0.70 | World's busiest port |
| DXB | 0.55 | Middle East routing complexity |
| KUL | 0.50 | Moderate throughput/capacity ratio |
| CMB | 0.55 | Transshipment hub |
| PIR | 0.40 | Lower volume |
| LAX | 0.80 | CPPI rank 78, severe 2021–2023 congestion |
| LGB | 0.75 | US West Coast bottleneck |
| MUM | 0.70 | CPPI rank 95, customs delays |
| MBA | 0.60 | Limited infrastructure (Kenya Ports Authority) |

### Port Efficiency Score (1–5)

**Source:** World Bank LPI 2023 — Infrastructure dimension (lpi.worldbank.org). Verifiable in `raw_lpi_scores` table.

### Port Distances

**Source:** Haversine formula applied to coordinates from `dim_ports` + canal corrections. Port coordinates from port authority published coordinates, cross-referenced with ports.com.

### Terminal Handling Charges (THC)

**Source:** Published carrier THC schedules 2023 (Maersk, MSC, CMA CGM) and Drewry Maritime Research Container Census 2023.

### Customs Brokerage Fees

**Source:** FIATA 2023 rate survey and Flexport published rate cards.

---

## 11. What is Real vs Estimated

| Feature | Status | Notes |
|---------|--------|-------|
| Commodity prices | Real | World Bank CMO, monthly |
| Fuel prices (Brent/WTI) | Real | EIA weekly, current as of last pipeline run |
| EUR/USD exchange rate | Real | ECB daily, current as of last pipeline run |
| LPI country scores | Real | World Bank 2023, in database |
| Port weather forecasts | Real | Open-Meteo 7-day, refreshed by scheduler |
| Port coordinates | Real | Port authority published coordinates |
| Tariff rates | Partial | Static MFN averages — not live |
| Port congestion index | Static | CPPI 2023 baseline |
| THC charges | Static | 2023 published carrier rates |
| EU ETS carbon price | Static fallback | €65/tonne 2024 average if not in DB |
| Route distances | Calculated | Haversine + canal corrections from dim_ports |
| ML freight prediction | Trained | XGBoost on synthetic + real fuel data |
| FOB commodity estimate | Derived | Latest commodity price × cargo weight |
| Carbon cost | Calculated | IMO EEOI formula — distance + weight only |
| Commodity forecasts | Trained | ARIMA, Prophet, XGBoost per commodity |

---

## 12. Project Structure

```
project/
├── api/                             # Express.js REST API (localhost:3001)
│   ├── server.js                    # All endpoints + scheduler startup
│   ├── services/
│   │   ├── landedCost.js            # Rule-based + ML + carbon calculation engine
│   │   ├── alerts.js                # Alert retrieval and acknowledgement
│   │   └── scheduler.js             # node-cron pipeline scheduler
│   └── package.json
│
├── electron-app/                    # Desktop UI (Electron + React)
│   ├── main.js                      # Electron main process (clean — no scheduler)
│   ├── preload.js                   # Context bridge (platform only)
│   ├── assets/
│   │   └── icon.png                 # App icon
│   ├── src/
│   │   ├── App.js                   # All views: Dashboard, Calculator, Route Compare,
│   │   │                            # Carbon, Commodities, Ports, Alerts
│   │   ├── index.js                 # React entry point
│   │   ├── index.css                # Design system
│   │   └── services/api.js          # All API calls to localhost:3001
│   └── public/index.html
│
├── ml/                              # Machine learning
│   ├── train_freight_model.py       # XGBoost freight cost model training
│   ├── predict.py                   # Freight model inference (called by Node.js)
│   ├── train_commodity_forecast.py  # ARIMA + Prophet + XGBoost forecast training
│   ├── predict_commodity.py         # Commodity forecast inference
│   └── models/
│       ├── freight_model.pkl            # Trained freight model (not in git)
│       ├── freight_model_meta.json      # Freight model metrics
│       └── commodity_forecast/
│           └── forecast_meta.json       # All commodity forecasts (not in git)
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
└── run_pipeline.py                  # Master orchestrator
```

---

## 13. Quick Start

### Requirements

```bash
pip install requests pandas openpyxl xgboost scikit-learn numpy statsmodels prophet
# Node.js v18+ from nodejs.org
```

### First Run

```bash
# 1. Set up database and ingest all data
python run_pipeline.py --backfill

# 2. Train the freight ML model
python ml/train_freight_model.py

# 3. Train commodity forecasting models
python ml/train_commodity_forecast.py

# 4. Start the API — also starts the scheduler (Terminal 1)
cd api && npm install && node server.js

# 5. Start the desktop app (Terminal 2)
cd electron-app && npm install && npm run dev
```

### Verify everything works

```bash
curl http://localhost:3001/health
curl http://localhost:3001/api/v1/scheduler
curl -s -X POST http://localhost:3001/api/v1/landed-cost \
  -H "Content-Type: application/json" \
  -d "{\"origin_port_code\":\"SHA\",\"dest_port_code\":\"RTM\",\"hs_code\":\"720839\",\"cargo_weight_kg\":24000,\"fob_value_usd\":48000}" \
  | python -m json.tool
```

### Build desktop app

```bash
cd electron-app
npm run build
# Output: electron-app/dist/Logistics Intelligence Platform Setup 1.0.0.exe
```

The built app requires the API server to be running separately on the same machine. Start it before opening the app and click "Retry Connection" if the app shows the API error screen.

### Optional API key

```bash
# Free key at eia.gov/opendata/register.php
set EIA_API_KEY=your_key_here    # Windows
export EIA_API_KEY=your_key_here # Mac/Linux
```

---

## 14. Running the Pipeline

```bash
python run_pipeline.py                     # Run all sources
python run_pipeline.py --source fuel
python run_pipeline.py --source commodities
python run_pipeline.py --source tariffs
python run_pipeline.py --source weather
python run_pipeline.py --backfill          # Full historical load
python run_pipeline.py --status            # Show row counts
python diagnose_and_fix.py                 # Diagnose and fix issues
```

Or trigger individual sources from the Pipeline Schedule card on the dashboard using "Run now" buttons.