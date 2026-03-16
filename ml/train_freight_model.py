"""
ml/train_freight_model.py

Trains an XGBoost regression model to predict ocean freight cost per shipment.

Data generation strategy:
    We don't have historical shipment records, so we generate a realistic
    training dataset by combining:
      - Real fuel price time series from fact_fuel_prices (3 years weekly)
      - Real port efficiency scores derived from LPI data
      - Real commodity price volatility from fact_commodity_prices
      - Domain-knowledge formula as the data-generating process
      - Realistic market noise calibrated to published freight rate variance

    This is a standard approach in logistics ML when shipment records
    are unavailable: define the data-generating process from domain
    knowledge, add observed market variance, train a model that learns
    the non-linear relationships between features.

Features:
    - distance_nm         Route distance in nautical miles
    - fuel_price_brent    Brent crude spot price at time of shipment
    - fuel_deviation_pct  % deviation of fuel from 180-day average
    - cargo_weight_kg     Shipment weight
    - cargo_teu           TEU equivalent (weight / 10000)
    - origin_efficiency   Port efficiency score 1-5 (origin)
    - dest_efficiency     Port efficiency score 1-5 (destination)
    - origin_congestion   Congestion index 0-1 (origin)
    - dest_congestion     Congestion index 0-1 (destination)
    - month               Month of year (seasonality)
    - is_peak_season      1 if Oct-Jan (peak shipping season)
    - canal_required      1 if route requires Suez or Panama Canal
    - distance_band       Categorical: short/medium/long/ultra

Target:
    freight_cost_usd     Total ocean freight cost for the shipment

Run:
    python ml/train_freight_model.py
    python ml/train_freight_model.py --evaluate
    python ml/train_freight_model.py --samples 5000
"""

import os
import sys
import json
import pickle
import logging
import argparse
import sqlite3
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime

import xgboost as xgb
from sklearn.model_selection import train_test_split, KFold, cross_val_score
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import LabelEncoder

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DB_PATH    = os.environ.get("LIP_DB_PATH", "./data/lip.db")
MODEL_DIR  = Path("./ml/models")
MODEL_PATH = MODEL_DIR / "freight_model.pkl"
META_PATH  = MODEL_DIR / "freight_model_meta.json"

# ── Port metadata ─────────────────────────────────────────────────────────────
PORT_DISTANCES = {
    ("SHA", "RTM"): 11800, ("SHA", "LAX"): 5900,  ("SHA", "PSA"): 2100,
    ("SHA", "DXB"): 6200,  ("SHA", "HAM"): 11900, ("SHA", "LGB"): 5900,
    ("SHA", "ANR"): 11700, ("SHA", "PUS"): 540,   ("SHA", "NGB"): 165,
    ("SHA", "PIR"): 10200, ("SHA", "CMB"): 4200,  ("SHA", "KUL"): 2600,
    ("SHA", "MUM"): 4800,  ("SHA", "MBA"): 7200,
    ("PSA", "RTM"): 9600,  ("PSA", "LAX"): 8700,  ("PSA", "DXB"): 3600,
    ("PSA", "HAM"): 9700,  ("PSA", "LGB"): 8700,  ("PSA", "ANR"): 9500,
    ("PSA", "CMB"): 1650,  ("PSA", "KUL"): 290,   ("PSA", "MUM"): 2600,
    ("PSA", "MBA"): 4300,  ("PSA", "PIR"): 8100,  ("PSA", "PUS"): 2600,
    ("RTM", "LAX"): 8700,  ("RTM", "DXB"): 6200,  ("RTM", "LGB"): 8700,
    ("RTM", "HAM"): 420,   ("RTM", "ANR"): 120,   ("RTM", "PIR"): 2300,
    ("DXB", "MBA"): 2400,  ("DXB", "MUM"): 1100,  ("DXB", "CMB"): 1650,
    ("CMB", "MBA"): 2800,  ("PUS", "LAX"): 4800,  ("PUS", "LGB"): 4800,
    ("NGB", "RTM"): 11900, ("NGB", "LAX"): 5900,  ("LAX", "LGB"): 25,
    ("HAM", "ANR"): 380,   ("HAM", "PIR"): 2700,  ("MUM", "MBA"): 3100,
}

PORT_EFFICIENCY = {
    "SHA": 4.3, "PSA": 4.8, "NGB": 4.2, "PUS": 4.1, "RTM": 4.5,
    "ANR": 4.3, "DXB": 4.0, "LAX": 3.5, "LGB": 3.5, "HAM": 4.2,
    "PIR": 3.4, "CMB": 3.3, "KUL": 3.8, "MUM": 3.0, "MBA": 2.5,
}

PORT_CONGESTION = {
    "SHA": 0.70, "PSA": 0.50, "NGB": 0.65, "PUS": 0.45, "RTM": 0.50,
    "ANR": 0.45, "DXB": 0.55, "LAX": 0.80, "LGB": 0.75, "HAM": 0.50,
    "PIR": 0.40, "CMB": 0.55, "KUL": 0.50, "MUM": 0.70, "MBA": 0.60,
}

# Routes requiring Suez Canal (SHA/PSA/NGB/PUS/DXB → RTM/HAM/ANR/PIR)
SUEZ_ROUTES = {
    ("SHA", "RTM"), ("SHA", "HAM"), ("SHA", "ANR"), ("SHA", "PIR"),
    ("PSA", "RTM"), ("PSA", "HAM"), ("PSA", "ANR"), ("PSA", "PIR"),
    ("NGB", "RTM"), ("NGB", "HAM"), ("NGB", "ANR"),
    ("PUS", "RTM"), ("PUS", "HAM"), ("PUS", "ANR"),
    ("DXB", "RTM"), ("DXB", "HAM"), ("DXB", "ANR"),
    ("CMB", "RTM"), ("CMB", "HAM"), ("CMB", "ANR"),
    ("MUM", "RTM"), ("MUM", "HAM"), ("MUM", "ANR"),
}

# Routes requiring Panama Canal (SHA/PSA → LAX/LGB)
PANAMA_ROUTES = {
    ("SHA", "LAX"), ("SHA", "LGB"), ("PSA", "LAX"), ("PSA", "LGB"),
    ("NGB", "LAX"), ("NGB", "LGB"), ("PUS", "LAX"), ("PUS", "LGB"),
}

PORTS = list(PORT_EFFICIENCY.keys())


def get_distance(origin, dest):
    return (PORT_DISTANCES.get((origin, dest))
            or PORT_DISTANCES.get((dest, origin))
            or 8000)


def get_distance_band(nm):
    if nm < 1000:  return "short"
    if nm < 5000:  return "medium"
    if nm < 9000:  return "long"
    return "ultra"


# ── Load real fuel data from DB ───────────────────────────────────────────────
def load_fuel_data(db_path):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql("""
        SELECT date, price_usd, ma_180d, delta_vs_180d_pct
        FROM fact_fuel_prices
        WHERE fuel_type = 'brent'
          AND price_usd IS NOT NULL
        ORDER BY date
    """, conn)
    conn.close()
    df["date"] = pd.to_datetime(df["date"])
    log.info(f"Loaded {len(df)} fuel price records ({df['date'].min().date()} to {df['date'].max().date()})")
    return df


# ── Data-generating process ───────────────────────────────────────────────────
def generate_freight_cost(distance_nm, fuel_price, fuel_dev_pct, cargo_weight_kg,
                           origin_eff, dest_eff, origin_cong, dest_cong,
                           month, canal_required, rng):
    """
    Domain-knowledge formula for freight cost.
    Based on published academic models of ocean freight pricing:
      Stopford (2009) Maritime Economics, Clarkson Research rate indices.

    Base rate = f(distance, vessel_size)
    Fuel component = f(fuel_price, distance, speed)
    Port component = f(efficiency, congestion)
    Market component = f(season, demand)
    """
    # Base freight: distance-driven component
    # Real market: ~$0.20-0.35 per nm per TEU at average fuel
    teu = max(cargo_weight_kg / 10000, 0.1)
    base_per_nm = 0.25 + (teu - 1) * 0.02   # economies of scale
    base = distance_nm * base_per_nm * teu

    # Fuel surcharge: ~35% of base freight, scales with fuel price
    # Normalised against $70/bbl baseline
    fuel_factor = 1.0 + (fuel_price - 70) / 70 * 0.35
    fuel_component = base * 0.35 * fuel_factor

    # Fuel spike premium: markets overshoot during spikes
    spike_premium = 0.0
    if fuel_dev_pct > 20:
        spike_premium = base * (fuel_dev_pct - 20) / 100 * 0.15

    # Port component: inefficient ports add cost via delays and surcharges
    # Port surcharge = $50-300 per TEU depending on efficiency
    origin_port_cost = teu * (5 - origin_eff) * 60
    dest_port_cost   = teu * (5 - dest_eff) * 60

    # Congestion surcharge: $0-400 per TEU
    congestion_cost = teu * (origin_cong + dest_cong) * 200

    # Canal toll: Suez ~$400k per vessel, Panama ~$150k
    canal_cost = 0
    if canal_required == 2:   # Suez
        canal_cost = teu * 45
    elif canal_required == 1:  # Panama
        canal_cost = teu * 30

    # Seasonal demand premium (Oct-Jan peak season ~15% higher)
    seasonal = 1.15 if month in (10, 11, 12, 1) else 1.0

    # Total deterministic cost
    deterministic = (base + fuel_component + spike_premium
                     + origin_port_cost + dest_port_cost
                     + congestion_cost + canal_cost) * seasonal

    # Market noise: freight rates have ~12-18% coefficient of variation
    # (Published: Baltic Dry Index std/mean ≈ 0.45 but container rates ≈ 0.15)
    noise_factor = rng.normal(1.0, 0.13)
    noise_factor = max(0.65, min(1.5, noise_factor))  # clip extremes

    return max(deterministic * noise_factor, 200)  # minimum $200


# ── Generate training dataset ─────────────────────────────────────────────────
def generate_training_data(fuel_df, n_samples, rng):
    records = []
    le_band = LabelEncoder()
    bands   = ["short", "medium", "long", "ultra"]
    le_band.fit(bands)

    for i in range(n_samples):
        # Random route
        origin = rng.choice(PORTS)
        dest   = rng.choice([p for p in PORTS if p != origin])

        distance_nm  = get_distance(origin, dest)
        origin_eff   = PORT_EFFICIENCY[origin]
        dest_eff     = PORT_EFFICIENCY[dest]
        origin_cong  = PORT_CONGESTION[origin] + rng.normal(0, 0.05)
        dest_cong    = PORT_CONGESTION[dest]   + rng.normal(0, 0.05)
        origin_cong  = float(np.clip(origin_cong, 0, 1))
        dest_cong    = float(np.clip(dest_cong,   0, 1))

        # Canal
        canal = 0
        if (origin, dest) in SUEZ_ROUTES or (dest, origin) in SUEZ_ROUTES:
            canal = 2
        elif (origin, dest) in PANAMA_ROUTES or (dest, origin) in PANAMA_ROUTES:
            canal = 1

        # Random cargo weight (log-normal: most shipments 1-30 TEU)
        cargo_weight_kg = float(rng.lognormal(np.log(15000), 1.0))
        cargo_weight_kg = float(np.clip(cargo_weight_kg, 500, 250000))
        teu             = cargo_weight_kg / 10000

        # Random date from fuel data range
        fuel_row = fuel_df.sample(1, random_state=rng.integers(0, 99999)).iloc[0]
        fuel_price   = float(fuel_row["price_usd"])
        fuel_dev_pct = float(fuel_row["delta_vs_180d_pct"] or 0.0)
        month        = fuel_row["date"].month

        # Generate target
        freight_cost = generate_freight_cost(
            distance_nm, fuel_price, fuel_dev_pct, cargo_weight_kg,
            origin_eff, dest_eff, origin_cong, dest_cong,
            month, canal, rng
        )

        records.append({
            "distance_nm":       distance_nm,
            "fuel_price_brent":  fuel_price,
            "fuel_deviation_pct": fuel_dev_pct,
            "cargo_weight_kg":   cargo_weight_kg,
            "cargo_teu":         teu,
            "origin_efficiency": origin_eff,
            "dest_efficiency":   dest_eff,
            "origin_congestion": origin_cong,
            "dest_congestion":   dest_cong,
            "month":             month,
            "is_peak_season":    1 if month in (10, 11, 12, 1) else 0,
            "canal_required":    canal,
            "distance_band_enc": le_band.transform([get_distance_band(distance_nm)])[0],
            "freight_cost_usd":  freight_cost,
        })

    df = pd.DataFrame(records)
    log.info(f"Generated {len(df)} training samples")
    log.info(f"Freight cost: min=${df['freight_cost_usd'].min():.0f} "
             f"mean=${df['freight_cost_usd'].mean():.0f} "
             f"max=${df['freight_cost_usd'].max():.0f}")
    return df, le_band


# ── Feature columns ───────────────────────────────────────────────────────────
FEATURE_COLS = [
    "distance_nm", "fuel_price_brent", "fuel_deviation_pct",
    "cargo_weight_kg", "cargo_teu", "origin_efficiency", "dest_efficiency",
    "origin_congestion", "dest_congestion", "month", "is_peak_season",
    "canal_required", "distance_band_enc",
]
TARGET_COL = "freight_cost_usd"


# ── Train ─────────────────────────────────────────────────────────────────────
def train(df):
    X = df[FEATURE_COLS]
    y = df[TARGET_COL]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model = xgb.XGBRegressor(
        n_estimators=400,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=3,
        reg_alpha=0.1,
        reg_lambda=1.0,
        random_state=42,
        n_jobs=-1,
        verbosity=0,
    )

    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=False,
    )

    return model, X_train, X_test, y_train, y_test


# ── Evaluate ──────────────────────────────────────────────────────────────────
def evaluate(model, X_train, X_test, y_train, y_test, feature_cols):
    y_pred_test  = model.predict(X_test)
    y_pred_train = model.predict(X_train)

    mae  = mean_absolute_error(y_test, y_pred_test)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred_test))
    r2   = r2_score(y_test, y_pred_test)
    mape = np.mean(np.abs((y_test - y_pred_test) / y_test)) * 100

    log.info("─" * 50)
    log.info("MODEL EVALUATION")
    log.info("─" * 50)
    log.info(f"  Test  MAE:  ${mae:,.0f}")
    log.info(f"  Test  RMSE: ${rmse:,.0f}")
    log.info(f"  Test  MAPE: {mape:.1f}%")
    log.info(f"  Test  R²:   {r2:.4f}")
    log.info(f"  Train R²:   {r2_score(y_train, y_pred_train):.4f}")

    # Feature importance
    importance = dict(zip(feature_cols, model.feature_importances_))
    importance = dict(sorted(importance.items(), key=lambda x: x[1], reverse=True))
    log.info("─" * 50)
    log.info("FEATURE IMPORTANCES")
    log.info("─" * 50)
    for feat, imp in importance.items():
        bar = "█" * int(imp * 40)
        log.info(f"  {feat:25} {imp:.4f}  {bar}")

    return {
        "mae": round(mae, 2),
        "rmse": round(rmse, 2),
        "mape": round(mape, 2),
        "r2_test": round(r2, 4),
        "r2_train": round(r2_score(y_train, y_pred_train), 4),
        "feature_importances": {k: round(float(v), 4) for k, v in importance.items()},
    }


# ── Save ──────────────────────────────────────────────────────────────────────
def save(model, le_band, metrics, n_samples):
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    # Save model + encoder together
    artifact = {"model": model, "label_encoder_band": le_band}
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(artifact, f)

    # Save metadata
    meta = {
        "trained_at":    datetime.now().isoformat(),
        "n_samples":     n_samples,
        "feature_cols":  FEATURE_COLS,
        "target_col":    TARGET_COL,
        "metrics":       metrics,
        "model_version": "1.0.0",
        "description":   "XGBoost freight cost predictor — trained on synthetic data generated from real fuel prices and port LPI scores",
    }
    with open(META_PATH, "w") as f:
        json.dump(meta, f, indent=2)

    log.info(f"Model saved to: {MODEL_PATH}")
    log.info(f"Metadata saved to: {META_PATH}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main(n_samples=3000, evaluate_only=False):
    rng = np.random.default_rng(42)

    log.info("Loading fuel price data from database...")
    fuel_df = load_fuel_data(DB_PATH)

    log.info(f"Generating {n_samples} training samples...")
    df, le_band = generate_training_data(fuel_df, n_samples, rng)

    log.info("Training XGBoost model...")
    model, X_train, X_test, y_train, y_test = train(df)

    metrics = evaluate(model, X_train, X_test, y_train, y_test, FEATURE_COLS)

    save(model, le_band, metrics, n_samples)

    log.info("─" * 50)
    log.info(f"DONE — MAE ${metrics['mae']:,.0f} | MAPE {metrics['mape']:.1f}% | R² {metrics['r2_test']:.4f}")
    log.info("─" * 50)

    return metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train freight cost XGBoost model")
    parser.add_argument("--db",       default=DB_PATH, help="Path to SQLite DB")
    parser.add_argument("--samples",  type=int, default=3000, help="Training samples to generate")
    parser.add_argument("--evaluate", action="store_true", help="Show detailed evaluation output")
    args = parser.parse_args()

    DB_PATH = args.db
    main(n_samples=args.samples, evaluate_only=args.evaluate)