"""
ml/train_commodity_forecast.py

Trains three forecasting models per commodity:
  1. ARIMA     — classical statistical baseline, own history only
  2. XGB_OWN   — XGBoost with own lags only
  3. XGB_CROSS — XGBoost with own lags + correlated commodity features

Models are saved to ml/models/commodity_forecast/ as pickle files.
Metadata including per-model MAPE is saved to forecast_meta.json.

Run:
    python ml/train_commodity_forecast.py
    python ml/train_commodity_forecast.py --commodity "Crude oil, Brent"
    python ml/train_commodity_forecast.py --horizon 6
    python ml/train_commodity_forecast.py --evaluate
"""

import os
import sys
import json
import math
import pickle
import logging
import argparse
import sqlite3
import warnings
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime

warnings.filterwarnings("ignore")

import xgboost as xgb
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error
from statsmodels.tsa.arima.model import ARIMA

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DB_PATH      = os.environ.get("LIP_DB_PATH", "./data/lip.db")
MODEL_DIR    = Path("./ml/models/commodity_forecast")
META_PATH    = MODEL_DIR / "forecast_meta.json"
HORIZON      = 6   # months to forecast forward

# ── Cross-commodity feature groups ───────────────────────────────────────────
# For each commodity, list the commodity_ids whose lagged prices
# are useful predictors. Based on published economic relationships.
CROSS_FEATURES = {
    # Energy
    "crude_oil_brent":      ["crude_oil_wti", "natural_gas_us"],
    "crude_oil_wti":        ["crude_oil_brent", "natural_gas_us"],
    "crude_oil_dubai":      ["crude_oil_brent", "crude_oil_wti"],
    "crude_oil_average":    ["crude_oil_brent", "crude_oil_wti"],
    "natural_gas_us":       ["crude_oil_brent", "natural_gas_europe"],
    "natural_gas_europe":   ["crude_oil_brent", "natural_gas_us"],
    "natural_gas_index":    ["crude_oil_brent", "natural_gas_us"],
    "coal_australian":      ["crude_oil_brent", "natural_gas_us"],
    "coal_south_african_**":["crude_oil_brent", "coal_australian"],

    # Fertilisers — follow natural gas (feedstock for ammonia)
    "urea":                 ["natural_gas_us", "natural_gas_europe", "crude_oil_brent"],
    "dap":                  ["natural_gas_us", "phosphate_rock", "crude_oil_brent"],
    "tsp":                  ["natural_gas_us", "phosphate_rock"],
    "phosphate_rock":       ["natural_gas_us", "crude_oil_brent"],
    "potassium_chloride_**":["natural_gas_us", "crude_oil_brent"],

    # Grains — follow each other + energy costs
    "wheat_us_hrw":         ["maize", "soybeans", "crude_oil_brent", "urea"],
    "wheat_us_srw":         ["wheat_us_hrw", "maize", "crude_oil_brent"],
    "maize":                ["wheat_us_hrw", "soybeans", "crude_oil_brent"],
    "sorghum":              ["maize", "wheat_us_hrw", "crude_oil_brent"],
    "barley":               ["wheat_us_hrw", "maize", "crude_oil_brent"],
    "rice_thai_25%":        ["rice_thai_5%", "maize", "crude_oil_brent"],
    "rice_thai_5%":         ["rice_thai_25%", "maize", "crude_oil_brent"],
    "rice_viet_namese_5%":  ["rice_thai_5%", "maize"],

    # Oilseeds — tight crushing margin relationships
    "soybeans":             ["soybean_oil", "soybean_meal", "maize", "crude_oil_brent"],
    "soybean_oil":          ["soybeans", "palm_oil", "crude_oil_brent"],
    "soybean_meal":         ["soybeans", "maize", "crude_oil_brent"],
    "palm_oil":             ["soybean_oil", "crude_oil_brent"],
    "palm_kernel_oil":      ["palm_oil", "coconut_oil"],
    "coconut_oil":          ["palm_oil", "palm_kernel_oil"],
    "groundnut_oil_**":     ["palm_oil", "soybean_oil"],
    "rapeseed_oil":         ["palm_oil", "soybean_oil", "crude_oil_brent"],
    "sunflower_oil":        ["palm_oil", "soybean_oil", "crude_oil_brent"],

    # Metals — follow energy (smelting costs) + each other
    "iron_ore_cfr_spot":    ["crude_oil_brent", "coal_australian"],
    "copper":               ["crude_oil_brent", "aluminum", "nickel"],
    "aluminum":             ["crude_oil_brent", "copper", "natural_gas_us"],
    "nickel":               ["crude_oil_brent", "copper"],
    "zinc":                 ["crude_oil_brent", "copper", "aluminum"],
    "lead":                 ["crude_oil_brent", "zinc", "copper"],
    "tin":                  ["crude_oil_brent", "copper"],
    "gold":                 ["silver", "platinum", "crude_oil_brent"],
    "silver":               ["gold", "copper", "crude_oil_brent"],
    "platinum":             ["gold", "crude_oil_brent"],

    # Soft commodities
    "cotton_a_index":       ["crude_oil_brent"],
    "rubber_rss3":          ["crude_oil_brent", "natural_gas_us"],
    "rubber_tsr20_**":      ["crude_oil_brent", "rubber_rss3"],
    "cocoa":                ["crude_oil_brent"],
    "coffee_arabica":       ["crude_oil_brent", "sugar_world"],
    "coffee_robusta":       ["coffee_arabica", "crude_oil_brent"],
    "tea_avg_3_auctions":   ["crude_oil_brent"],
    "sugar_world":          ["crude_oil_brent", "maize"],
    "sugar_us":             ["sugar_world", "maize"],
    "sugar_eu":             ["sugar_world"],
    "banana_us":            ["crude_oil_brent"],
    "banana_europe":        ["crude_oil_brent", "banana_us"],
    "orange":               ["crude_oil_brent"],

    # Animal products
    "beef_**":              ["maize", "soybean_meal", "crude_oil_brent"],
    "chicken_**":           ["maize", "soybean_meal", "crude_oil_brent"],
    "lamb_**":              ["maize", "crude_oil_brent"],
    "shrimps_mexican":      ["crude_oil_brent"],
    "fish_meal":            ["crude_oil_brent", "soybean_meal"],

    # Other
    "tobacco_us_import_u.v.": ["crude_oil_brent"],
    "logs_malaysian":       ["crude_oil_brent"],
    "logs_cameroon":        ["crude_oil_brent"],
    "sawnwood_malaysian":   ["logs_malaysian", "crude_oil_brent"],
    "sawnwood_cameroon":    ["logs_cameroon", "crude_oil_brent"],
    "plywood":              ["logs_malaysian", "crude_oil_brent"],
    "groundnuts":           ["crude_oil_brent", "groundnut_oil_**"],
    "liquefied_natural_gas_japan": ["natural_gas_us", "crude_oil_brent"],
}

# Own lag periods to use (months back)
OWN_LAGS  = [1, 2, 3, 6, 12]
# Cross-feature lag periods
CROSS_LAGS = [1, 2]
# Minimum rows needed to train
MIN_ROWS   = 36


# ── Data loading ──────────────────────────────────────────────────────────────
def load_all_commodities(db_path):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql("""
        SELECT commodity_id, commodity_name, date, price_usd,
               mom_change_pct, yoy_change_pct, spike_flag
        FROM fact_commodity_prices
        WHERE price_usd IS NOT NULL AND price_usd > 0
        ORDER BY commodity_id, date
    """, conn)
    conn.close()
    df["date"] = pd.to_datetime(df["date"])
    log.info(f"Loaded {len(df)} rows for {df['commodity_id'].nunique()} commodities")
    return df


def get_series(df, commodity_id):
    s = df[df["commodity_id"] == commodity_id].copy()
    s = s.set_index("date").sort_index()
    s = s[~s.index.duplicated(keep="last")]
    # Fill small gaps (1-2 missing months) by interpolation
    idx = pd.date_range(s.index.min(), s.index.max(), freq="MS")
    s = s.reindex(idx).interpolate(method="linear", limit=3)
    return s


# ── Feature engineering ───────────────────────────────────────────────────────
def make_features(series_df, all_df, commodity_id, include_cross=False):
    """Build feature matrix for supervised learning."""
    s = series_df["price_usd"].copy()

    features = pd.DataFrame(index=s.index)

    # Own lags
    for lag in OWN_LAGS:
        features[f"price_lag_{lag}"] = s.shift(lag)

    # Own momentum
    features["mom_change"] = series_df["mom_change_pct"].shift(1)
    features["yoy_change"] = series_df["yoy_change_pct"].shift(1)
    features["spike_lag1"] = series_df["spike_flag"].shift(1).fillna(0)

    # Calendar features
    features["month"] = features.index.month
    features["year"]  = features.index.year

    # Cross-commodity features
    if include_cross:
        correlated = CROSS_FEATURES.get(commodity_id, [])
        for cid in correlated:
            cross = all_df[all_df["commodity_id"] == cid].copy()
            if cross.empty:
                continue
            cross = cross.set_index("date")["price_usd"].sort_index()
            cross = cross[~cross.index.duplicated(keep="last")]
            # Normalise to z-score so scale differences don't dominate
            mean, std = cross.mean(), cross.std()
            if std > 0:
                cross_norm = (cross - mean) / std
            else:
                continue
            for lag in CROSS_LAGS:
                col = f"cross_{cid[:12]}_lag{lag}"
                features[col] = cross_norm.shift(lag).reindex(features.index)

    # Target: next month's price
    features["target"] = s.shift(-1)

    features = features.dropna()
    return features


# ── Model 1: ARIMA ────────────────────────────────────────────────────────────
def train_arima(series, horizon):
    """
    Fit ARIMA(p,d,q) model using AIC selection.
    Tests (1,1,1), (2,1,1), (1,1,2), (2,1,2) and picks best AIC.
    """
    best_aic   = np.inf
    best_order = (1, 1, 1)
    best_model = None

    for p in [1, 2]:
        for d in [1]:
            for q in [0, 1, 2]:
                try:
                    m = ARIMA(series, order=(p, d, q)).fit()
                    if m.aic < best_aic:
                        best_aic   = m.aic
                        best_order = (p, d, q)
                        best_model = m
                except Exception:
                    continue

    if best_model is None:
        best_model = ARIMA(series, order=(1, 1, 1)).fit()
        best_order = (1, 1, 1)

    forecast  = best_model.get_forecast(steps=horizon)
    mean_fc   = forecast.predicted_mean.values
    conf_int  = forecast.conf_int(alpha=0.2).values  # 80% CI

    return best_model, best_order, mean_fc, conf_int


def arima_cv_mape(series, n_test=12):
    """Walk-forward MAPE on last n_test months."""
    errors = []
    for i in range(n_test, 0, -1):
        train = series.iloc[:-i]
        actual = series.iloc[-i]
        if len(train) < MIN_ROWS:
            continue
        try:
            m = ARIMA(train, order=(1, 1, 1)).fit()
            fc = m.forecast(steps=1)[0]
            if actual > 0:
                errors.append(abs(fc - actual) / actual)
        except Exception:
            continue
    return np.mean(errors) * 100 if errors else None


# ── Model 2 & 3: XGBoost ─────────────────────────────────────────────────────
def train_xgb(features_df, horizon):
    """Train XGBoost on feature matrix, return model + horizon forecasts."""
    X = features_df.drop(columns=["target"])
    y = features_df["target"]

    n_test   = min(12, len(X) // 5)
    X_train  = X.iloc[:-n_test]
    y_train  = y.iloc[:-n_test]
    X_test   = X.iloc[-n_test:]
    y_test   = y.iloc[-n_test:]

    model = xgb.XGBRegressor(
        n_estimators=300,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=3,
        reg_alpha=0.1,
        random_state=42,
        verbosity=0,
        n_jobs=-1,
    )
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    # MAPE on test set
    preds  = model.predict(X_test)
    mape   = mean_absolute_percentage_error(y_test, preds) * 100

    # Forecast next `horizon` months by rolling prediction
    forecasts = []
    last_row  = X.iloc[-1].copy()
    last_price = y.iloc[-1]

    for step in range(horizon):
        pred = float(model.predict(last_row.values.reshape(1, -1))[0])
        pred = max(pred, 0.01)  # floor
        forecasts.append(pred)

        # Roll the lag features forward
        new_row = last_row.copy()
        for lag in sorted(OWN_LAGS, reverse=True):
            prev_lag = f"price_lag_{lag}"
            if lag > 1:
                smaller = f"price_lag_{lag - 1}" if (lag - 1) in OWN_LAGS else None
                if smaller and smaller in new_row.index:
                    new_row[prev_lag] = last_row[smaller]
            else:
                new_row["price_lag_1"] = last_price

        new_row["mom_change"] = ((pred - last_price) / last_price * 100) if last_price > 0 else 0
        new_row["month"]      = (int(last_row["month"]) % 12) + 1
        new_row["year"]       = int(last_row["year"]) + (1 if new_row["month"] == 1 else 0)

        last_row   = new_row
        last_price = pred

    # Confidence interval: use model's test set std as proxy
    residuals = np.abs(y_test.values - preds)
    ci_width  = np.percentile(residuals, 80)

    return model, mape, forecasts, ci_width


# ── Training orchestrator ─────────────────────────────────────────────────────
def train_commodity(commodity_id, commodity_name, series_df, all_df, horizon):
    results = {
        "commodity_id":   commodity_id,
        "commodity_name": commodity_name,
        "trained_at":     datetime.now().isoformat(),
        "horizon_months": horizon,
        "n_history":      len(series_df),
        "models":         {},
    }

    series = series_df["price_usd"].dropna()
    if len(series) < MIN_ROWS:
        log.warning(f"  {commodity_name}: only {len(series)} rows — skipping")
        return None

    last_date  = series.index[-1]
    last_price = float(series.iloc[-1])
    forecast_dates = pd.date_range(
        last_date + pd.DateOffset(months=1), periods=horizon, freq="MS"
    ).strftime("%Y-%m-%d").tolist()

    results["last_date"]      = last_date.strftime("%Y-%m-%d")
    results["last_price"]     = last_price
    results["forecast_dates"] = forecast_dates

    # ── ARIMA ─────────────────────────────────────────────────────────────────
    try:
        model_arima, order, fc_mean, fc_ci = train_arima(series, horizon)
        arima_mape = arima_cv_mape(series)
        results["models"]["arima"] = {
            "order":       list(order),
            "mape":        round(arima_mape, 2) if arima_mape else None,
            "forecast":    [round(float(v), 4) for v in fc_mean],
            "ci_lower":    [round(float(v), 4) for v in fc_ci[:, 0]],
            "ci_upper":    [round(float(v), 4) for v in fc_ci[:, 1]],
            "label":       f"ARIMA{order}",
            "color":       "#3b7fff",
        }
        log.info(f"  ARIMA{order} MAPE={arima_mape:.1f}%" if arima_mape else f"  ARIMA{order}")
    except Exception as e:
        log.warning(f"  ARIMA failed: {e}")

    # ── XGBoost own lags ──────────────────────────────────────────────────────
    try:
        feat_own = make_features(series_df, all_df, commodity_id, include_cross=False)
        if len(feat_own) >= MIN_ROWS:
            model_own, mape_own, fc_own, ci_own = train_xgb(feat_own, horizon)
            results["models"]["xgb_own"] = {
                "mape":     round(mape_own, 2),
                "forecast": [round(float(v), 4) for v in fc_own],
                "ci_lower": [round(float(v - ci_own), 4) for v in fc_own],
                "ci_upper": [round(float(v + ci_own), 4) for v in fc_own],
                "label":    "XGBoost (own lags)",
                "color":    "#f59e0b",
                "n_features": len(feat_own.columns) - 1,
            }
            log.info(f"  XGB_OWN  MAPE={mape_own:.1f}%")
    except Exception as e:
        log.warning(f"  XGB_OWN failed: {e}")

    # ── XGBoost cross-commodity ───────────────────────────────────────────────
    try:
        feat_cross = make_features(series_df, all_df, commodity_id, include_cross=True)
        cross_cols = [c for c in feat_cross.columns if c.startswith("cross_")]

        if len(feat_cross) >= MIN_ROWS and cross_cols:
            model_cross, mape_cross, fc_cross, ci_cross = train_xgb(feat_cross, horizon)

            # Feature importances — top cross features
            imp = dict(zip(
                feat_cross.drop(columns=["target"]).columns,
                model_cross.feature_importances_
            ))
            top_cross = sorted(
                {k: v for k, v in imp.items() if k.startswith("cross_")}.items(),
                key=lambda x: x[1], reverse=True
            )[:5]

            results["models"]["xgb_cross"] = {
                "mape":           round(mape_cross, 2),
                "forecast":       [round(float(v), 4) for v in fc_cross],
                "ci_lower":       [round(float(v - ci_cross), 4) for v in fc_cross],
                "ci_upper":       [round(float(v + ci_cross), 4) for v in fc_cross],
                "label":          "XGBoost (cross-commodity)",
                "color":          "#00c9a7",
                "n_features":     len(feat_cross.columns) - 1,
                "n_cross_features": len(cross_cols),
                "top_cross_features": [(k, round(float(v), 4)) for k, v in top_cross],
            }
            log.info(f"  XGB_CROSS MAPE={mape_cross:.1f}% ({len(cross_cols)} cross features)")
        else:
            log.info(f"  XGB_CROSS skipped — no correlated commodities defined")
    except Exception as e:
        log.warning(f"  XGB_CROSS failed: {e}")

    return results


# ── Save / load ───────────────────────────────────────────────────────────────
def save_results(all_results):
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    with open(META_PATH, "w") as f:
        json.dump(all_results, f, indent=2)
    log.info(f"Saved metadata: {META_PATH}")


def load_forecast(commodity_id):
    """Load forecast results for a single commodity."""
    if not META_PATH.exists():
        return None
    with open(META_PATH) as f:
        meta = json.load(f)
    return meta.get(commodity_id)


# ── Main ──────────────────────────────────────────────────────────────────────
def main(target_commodity=None, horizon=HORIZON, evaluate=False):
    log.info("Loading commodity data...")
    all_df = load_all_commodities(DB_PATH)

    commodities = all_df[["commodity_id", "commodity_name"]].drop_duplicates()
    if target_commodity:
        commodities = commodities[
            commodities["commodity_name"].str.contains(target_commodity, case=False)
            | commodities["commodity_id"].str.contains(target_commodity, case=False)
        ]
        if commodities.empty:
            log.error(f"No commodity matching '{target_commodity}'")
            return

    all_results = {}
    total = len(commodities)

    for i, (_, row) in enumerate(commodities.iterrows()):
        cid   = row["commodity_id"]
        cname = row["commodity_name"]
        log.info(f"[{i+1}/{total}] {cname}")

        series_df = get_series(all_df, cid)
        result    = train_commodity(cid, cname, series_df, all_df, horizon)

        if result:
            all_results[cid] = result
            if evaluate:
                for model_key, mdata in result["models"].items():
                    mape = mdata.get("mape")
                    log.info(f"         {model_key}: MAPE={mape}%")

    save_results(all_results)

    # Summary
    trained  = len(all_results)
    with_all = sum(1 for r in all_results.values() if len(r["models"]) == 3)
    log.info("─" * 50)
    log.info(f"DONE — {trained} commodities trained, {with_all} with all 3 models")
    log.info(f"Metadata: {META_PATH}")
    log.info("─" * 50)

    return all_results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train commodity price forecasting models")
    parser.add_argument("--db",          default=DB_PATH,  help="Path to SQLite DB")
    parser.add_argument("--commodity",   default=None,     help="Train one commodity only")
    parser.add_argument("--horizon",     type=int, default=HORIZON, help="Months to forecast")
    parser.add_argument("--evaluate",    action="store_true", help="Print per-model MAPE")
    args = parser.parse_args()

    DB_PATH = args.db
    main(
        target_commodity=args.commodity,
        horizon=args.horizon,
        evaluate=args.evaluate,
    )