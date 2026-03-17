"""
ml/predict_commodity.py
Returns forecast data for a commodity from pre-trained models.
Called by the API server — reads from forecast_meta.json.

Usage:
    echo '{"commodity_id": "crude_oil_brent"}' | python ml/predict_commodity.py

Output:
    {
        "commodity_id": "crude_oil_brent",
        "commodity_name": "Crude oil, Brent",
        "last_date": "2025-01-01",
        "last_price": 76.5,
        "forecast_dates": ["2025-02-01", ...],
        "models": {
            "arima": { "label": "ARIMA(1,1,1)", "forecast": [...], "ci_lower": [...], "ci_upper": [...], "mape": 8.2, "color": "#3b7fff" },
            "xgb_own": { ... },
            "xgb_cross": { ... }
        },
        "error": null
    }
"""

import sys
import json
from pathlib import Path

META_PATH = Path(__file__).parent / "models" / "commodity_forecast" / "forecast_meta.json"


def get_forecast(commodity_id):
    if not META_PATH.exists():
        return {
            "commodity_id": commodity_id,
            "error": "Forecast models not trained. Run: python ml/train_commodity_forecast.py",
            "models": {},
        }

    with open(META_PATH) as f:
        meta = json.load(f)

    result = meta.get(commodity_id)
    if not result:
        # Try partial match
        matches = [k for k in meta if commodity_id.lower() in k.lower()]
        if matches:
            result = meta[matches[0]]
        else:
            return {
                "commodity_id": commodity_id,
                "error": f"No forecast found for '{commodity_id}'. Run training first.",
                "models": {},
                "available": list(meta.keys())[:10],
            }

    return {**result, "error": None}


if __name__ == "__main__":
    try:
        raw     = sys.stdin.read().strip()
        payload = json.loads(raw)
        cid     = payload.get("commodity_id", "")
        result  = get_forecast(cid)
        print(json.dumps(result))
    except Exception as e:
        print(json.dumps({
            "commodity_id": "",
            "error": str(e),
            "models": {},
        }))