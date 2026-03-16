"""
ml/predict.py
Called by Node.js via child_process to get freight cost prediction.
Reads features from stdin as JSON, writes prediction to stdout as JSON.

Usage (from Node.js):
    python ml/predict.py < features.json

Input JSON:
    {
        "distance_nm": 11800,
        "fuel_price_brent": 76.5,
        "fuel_deviation_pct": 5.2,
        "cargo_weight_kg": 24000,
        "origin_efficiency": 4.3,
        "dest_efficiency": 4.5,
        "origin_congestion": 0.7,
        "dest_congestion": 0.5,
        "month": 3,
        "is_peak_season": 0,
        "canal_required": 2
    }

Output JSON:
    {
        "ml_predicted_usd": 7842.50,
        "confidence_interval_pct": 13.0,
        "feature_contributions": {
            "distance_nm": 0.31,
            "fuel_price_brent": 0.28,
            ...
        },
        "model_version": "1.0.0",
        "error": null
    }
"""

import sys
import json
import pickle
import numpy as np
from pathlib import Path

MODEL_PATH = Path(__file__).parent / "models" / "freight_model.pkl"
META_PATH  = Path(__file__).parent / "models" / "freight_model_meta.json"

DISTANCE_BANDS = ["long", "medium", "short", "ultra"]  # LabelEncoder sorted order

def get_distance_band(nm):
    if nm < 1000:  return "short"
    if nm < 5000:  return "medium"
    if nm < 9000:  return "long"
    return "ultra"

def predict(features: dict) -> dict:
    # Load model
    if not MODEL_PATH.exists():
        return {
            "ml_predicted_usd": None,
            "confidence_interval_pct": None,
            "feature_contributions": {},
            "model_version": None,
            "error": "Model not found. Run: python ml/train_freight_model.py",
        }

    with open(MODEL_PATH, "rb") as f:
        artifact = pickle.load(f)

    model   = artifact["model"]
    le_band = artifact["label_encoder_band"]

    with open(META_PATH) as f:
        meta = json.load(f)

    # Encode distance band
    distance_nm  = features.get("distance_nm", 8000)
    band_str     = get_distance_band(distance_nm)
    band_encoded = int(le_band.transform([band_str])[0])

    # Build feature vector in exact training order
    feature_vector = np.array([[
        distance_nm,
        features.get("fuel_price_brent", 76.0),
        features.get("fuel_deviation_pct", 0.0),
        features.get("cargo_weight_kg", 10000),
        features.get("cargo_weight_kg", 10000) / 10000,   # cargo_teu
        features.get("origin_efficiency", 4.0),
        features.get("dest_efficiency", 4.0),
        features.get("origin_congestion", 0.5),
        features.get("dest_congestion", 0.5),
        features.get("month", 6),
        features.get("is_peak_season", 0),
        features.get("canal_required", 0),
        band_encoded,
    ]])

    # Predict
    prediction = float(model.predict(feature_vector)[0])
    prediction = max(prediction, 200)  # floor

    # Confidence interval — based on MAPE from training
    base_mape = meta["metrics"].get("mape", 13.0)

    # Widen interval for edge cases
    ci = base_mape
    if features.get("fuel_deviation_pct", 0) > 20:
        ci += 3.0
    if features.get("dest_congestion", 0.5) > 0.7:
        ci += 2.0
    if distance_nm < 500:   # very short routes have more variance
        ci += 2.0

    # Feature contributions from XGBoost feature importances
    importances = meta["metrics"].get("feature_importances", {})
    feature_names = meta.get("feature_cols", [])
    contributions = {
        name: round(float(importances.get(name, 0)), 4)
        for name in feature_names
    }

    return {
        "ml_predicted_usd":        round(prediction, 2),
        "confidence_interval_pct": round(ci, 1),
        "feature_contributions":   contributions,
        "model_version":           meta.get("model_version", "1.0.0"),
        "error":                   None,
    }


if __name__ == "__main__":
    try:
        raw = sys.stdin.read().strip()
        features = json.loads(raw)
        result   = predict(features)
        print(json.dumps(result))
    except Exception as e:
        print(json.dumps({
            "ml_predicted_usd": None,
            "confidence_interval_pct": None,
            "feature_contributions": {},
            "model_version": None,
            "error": str(e),
        }))