/**
 * services/landedCost.js
 * Core calculation engine for landed cost estimation.
 * Queries SQLite directly — no ORM, fast synchronous reads via better-sqlite3.
 */

const { randomUUID } = require("crypto");
const { execSync } = require("child_process");
const path = require("path");

// ── ML model caller ───────────────────────────────────────────────────────────
function callMLModel(features) {
  try {
    const mlScript = path.join(__dirname, "../../ml/predict.py");
    const { spawnSync } = require("child_process");
    const result = spawnSync("python", [mlScript], {
      input: JSON.stringify(features),
      timeout: 8000,
      encoding: "utf8",
    });
    if (result.error) throw result.error;
    if (result.stderr) console.error("ML stderr:", result.stderr);
    return JSON.parse(result.stdout.trim());
  } catch (err) {
    console.error("ML model error:", err.message);
    return {
      ml_predicted_usd: null,
      confidence_interval_pct: null,
      feature_contributions: {},
      model_version: null,
      error: err.message,
    };
  }
}

// ── Constants ─────────────────────────────────────────────────────────────────

const INSURANCE_RATE        = 0.002;   // 0.2% of (FOB + freight)
const CUSTOMS_BROKERAGE_USD = 500;     // flat estimate, overridden by country lookup
const FUEL_SURCHARGE_THRESHOLD = 0.20; // >20% above 180d avg triggers surcharge

// Customs brokerage cost by destination country (USD flat fee estimates)
const CUSTOMS_BROKERAGE_BY_COUNTRY = {
  USA: 650, DEU: 400, CHN: 350, GBR: 420, JPN: 500,
  AUS: 380, SGP: 300, KOR: 420, NLD: 380, BEL: 380,
  FRA: 400, ITA: 420, CAN: 600, MEX: 450, BRA: 800,
  IND: 600, IDN: 550, MYS: 350, THA: 380, VNM: 400,
};

// Port handling charges (THC) per TEU in USD by port code
const THC_BY_PORT = {
  SHA: 185, PSA: 220, NGB: 175, PUS: 195, RTM: 310,
  ANR: 295, DXB: 265, LAX: 425, LGB: 410, HAM: 305,
  PIR: 245, CMB: 210, KUL: 195, MUM: 280, MBA: 320,
};

// Base freight rates per TEU by route distance band (USD)
const FREIGHT_RATE_BY_DISTANCE = [
  { maxNm: 2000,  rate: 800  },
  { maxNm: 5000,  rate: 1400 },
  { maxNm: 8000,  rate: 1900 },
  { maxNm: 12000, rate: 2600 },
  { maxNm: 99999, rate: 3400 },
];

// Approximate distances between major port pairs (nautical miles)
const PORT_DISTANCES = {
  "SHA-RTM": 11800, "SHA-LAX": 5900,  "SHA-PSA": 2100,
  "SHA-DXB": 6200,  "SHA-HAM": 11900, "SHA-LGB": 5900,
  "SHA-ANR": 11700, "SHA-PUS": 540,   "SHA-NGB": 165,
  "PSA-RTM": 9600,  "PSA-LAX": 8700,  "PSA-DXB": 3600,
  "PSA-HAM": 9700,  "PSA-LGB": 8700,  "PSA-ANR": 9500,
  "PSA-CMB": 1650,  "PSA-KUL": 290,   "PSA-MUM": 2600,
  "RTM-LAX": 8700,  "RTM-DXB": 6200,  "RTM-LGB": 8700,
  "DXB-MBA": 2400,  "DXB-MUM": 1100,  "DXB-CMB": 1650,
  "CMB-MBA": 2800,  "PUS-LAX": 4800,  "PUS-LGB": 4800,
  "NGB-RTM": 11900, "NGB-LAX": 5900,
};


// ── Helpers ───────────────────────────────────────────────────────────────────

function getRouteDistance(originPort, destPort) {
  const key1 = `${originPort}-${destPort}`;
  const key2 = `${destPort}-${originPort}`;
  return PORT_DISTANCES[key1] || PORT_DISTANCES[key2] || 8000; // default mid-range
}

function getBaseFreightRate(distanceNm) {
  const band = FREIGHT_RATE_BY_DISTANCE.find(b => distanceNm <= b.maxNm);
  return band ? band.rate : 3400;
}


// ── Port constants for ML features ───────────────────────────────────────────
const PORT_EFFICIENCY = {
  SHA: 4.3, PSA: 4.8, NGB: 4.2, PUS: 4.1, RTM: 4.5,
  ANR: 4.3, DXB: 4.0, LAX: 3.5, LGB: 3.5, HAM: 4.2,
  PIR: 3.4, CMB: 3.3, KUL: 3.8, MUM: 3.0, MBA: 2.5,
};

const PORT_CONGESTION = {
  SHA: 0.70, PSA: 0.50, NGB: 0.65, PUS: 0.45, RTM: 0.50,
  ANR: 0.45, DXB: 0.55, LAX: 0.80, LGB: 0.75, HAM: 0.50,
  PIR: 0.40, CMB: 0.55, KUL: 0.50, MUM: 0.70, MBA: 0.60,
};

// 0=none, 1=Panama, 2=Suez
const CANAL_REQUIRED = {
  "SHA-RTM":2,"SHA-HAM":2,"SHA-ANR":2,"SHA-PIR":2,
  "PSA-RTM":2,"PSA-HAM":2,"PSA-ANR":2,"PSA-PIR":2,
  "NGB-RTM":2,"NGB-HAM":2,"NGB-ANR":2,
  "DXB-RTM":2,"DXB-HAM":2,"DXB-ANR":2,
  "CMB-RTM":2,"CMB-HAM":2,"CMB-ANR":2,
  "MUM-RTM":2,"MUM-HAM":2,"MUM-ANR":2,
  "SHA-LAX":1,"SHA-LGB":1,"PSA-LAX":1,"PSA-LGB":1,
  "NGB-LAX":1,"NGB-LGB":1,"PUS-LAX":1,"PUS-LGB":1,
};

// ── Main calculation ──────────────────────────────────────────────────────────

function calculateLandedCost(db, params) {
  const {
    origin_port_code,
    dest_port_code,
    hs_code,
    cargo_weight_kg,
    fob_value_usd,      // null = estimate from commodity prices
    incoterm = "CIF",
    currency_out = "USD",
  } = params;

  const estimateId = randomUUID();
  const warnings = [];

  // ── 1. FOB value ───────────────────────────────────────────────────────────
  let cargoValueFob = fob_value_usd;
  let fobEstimated  = false;

  if (!cargoValueFob && cargo_weight_kg) {
    // Estimate from commodity price using HS chapter
    const hsChapter = String(hs_code).slice(0, 2);
    const commodityPrice = db.prepare(`
      SELECT f.price_usd, f.commodity_name, f.unit
      FROM fact_commodity_prices f
      JOIN dim_hs_codes h ON h.hs_chapter = ?
      WHERE f.commodity_id = h.related_commodity
      ORDER BY f.date DESC LIMIT 1
    `).get(hsChapter);

    if (commodityPrice) {
      // Convert commodity price to USD per kg
      let pricePerKg;
      switch (commodityPrice.unit) {
        case "per_metric_tonne": pricePerKg = commodityPrice.price_usd / 1000; break;
        case "per_barrel":       pricePerKg = commodityPrice.price_usd / 136;  break;
        case "per_mmbtu":        pricePerKg = commodityPrice.price_usd / 50;   break;
        case "per_cubic_metre":  pricePerKg = commodityPrice.price_usd / 600;  break;
        case "per_kg":           pricePerKg = commodityPrice.price_usd;        break;
        default:                 pricePerKg = commodityPrice.price_usd / 1000; break;
      }
      cargoValueFob = pricePerKg * cargo_weight_kg;
      fobEstimated  = true;
      warnings.push(`FOB value estimated from ${commodityPrice.commodity_name} price: $${cargoValueFob.toFixed(2)}`);
    } else {
      // Fallback: use $2/kg as a generic estimate
      cargoValueFob = cargo_weight_kg * 2;
      fobEstimated  = true;
      warnings.push(`FOB value estimated at $2/kg fallback — provide actual value for accuracy`);
    }
  }

  if (!cargoValueFob) {
    throw new Error("fob_value_usd is required when cargo_weight_kg is not provided");
  }

  // ── 2. Freight cost ────────────────────────────────────────────────────────
  const distanceNm     = getRouteDistance(origin_port_code, dest_port_code);
  const baseFreightTeu = getBaseFreightRate(distanceNm);

  // Fuel surcharge: check if Brent is >20% above 180d average
  let fuelSurchargeMultiplier = 1.0;
  let fuelSpikeFlag = 0;
  const fuelRow = db.prepare(`
    SELECT price_usd, ma_180d, delta_vs_180d_pct
    FROM fact_fuel_prices
    WHERE fuel_type = 'brent'
    ORDER BY date DESC LIMIT 1
  `).get();

  if (fuelRow && fuelRow.delta_vs_180d_pct > FUEL_SURCHARGE_THRESHOLD * 100) {
    fuelSurchargeMultiplier = 1 + (fuelRow.delta_vs_180d_pct / 100) * 0.3; // 30% passthrough
    fuelSpikeFlag = 1;
    warnings.push(
      `Fuel surcharge applied: Brent at $${fuelRow.price_usd.toFixed(2)} ` +
      `is ${fuelRow.delta_vs_180d_pct.toFixed(1)}% above 6-month average`
    );
  }

  // Weight-based TEU fraction (1 TEU ≈ 10,000 kg max cargo)
  const teuFraction  = cargo_weight_kg ? Math.max(cargo_weight_kg / 10000, 0.1) : 1.0;
  const freightCost  = baseFreightTeu * teuFraction * fuelSurchargeMultiplier;

  // ── 3. Tariff / duty ───────────────────────────────────────────────────────
  // Get destination country ISO from port
  const destCountry = db.prepare(`
    SELECT country_iso FROM dim_ports WHERE port_code = ?
  `).get(dest_port_code);

  const originCountry = db.prepare(`
    SELECT country_iso FROM dim_ports WHERE port_code = ?
  `).get(origin_port_code);

  const destIso   = destCountry?.country_iso || null;
  const originIso = originCountry?.country_iso || null;

  // Look up best available tariff rate (preferential first, then MFN)
  let tariffRate    = 0;
  let tariffType    = "unknown";
  let ftaName       = null;
  let tariffChangeFlag = 0;

  if (destIso && hs_code) {
    const hsPrefix = String(hs_code).slice(0, 6);

    // Try preferential rate first (origin-specific)
    const prefRate = db.prepare(`
      SELECT rate_pct, fta_name, rate_change_flag
      FROM fact_tariff_rates
      WHERE reporter_iso = ?
        AND partner_iso = ?
        AND hs_code LIKE ?
        AND tariff_type = 'preferential'
      ORDER BY rate_pct ASC LIMIT 1
    `).get(destIso, originIso, `${hsPrefix.slice(0,2)}%`);

    if (prefRate) {
      tariffRate       = prefRate.rate_pct;
      tariffType       = "preferential";
      ftaName          = prefRate.fta_name;
      tariffChangeFlag = prefRate.rate_change_flag;
    } else {
      // Fall back to MFN
      const mfnRate = db.prepare(`
        SELECT rate_pct, rate_change_flag
        FROM fact_tariff_rates
        WHERE reporter_iso = ?
          AND partner_iso IS NULL
          AND hs_code LIKE ?
          AND tariff_type = 'MFN_applied'
        ORDER BY rate_pct ASC LIMIT 1
      `).get(destIso, `${hsPrefix.slice(0,2)}%`);

      if (mfnRate) {
        tariffRate       = mfnRate.rate_pct;
        tariffType       = "MFN_applied";
        tariffChangeFlag = mfnRate.rate_change_flag;
      } else {
        warnings.push(`No tariff rate found for ${destIso} / HS ${hs_code} — duty set to 0`);
      }
    }

    // Check if a better FTA rate exists (opportunity alert)
    if (tariffType === "MFN_applied" && originIso) {
      const ftaOpp = db.prepare(`
        SELECT rate_pct, fta_name
        FROM fact_tariff_rates
        WHERE reporter_iso = ?
          AND partner_iso = ?
          AND hs_code LIKE ?
          AND tariff_type = 'preferential'
          AND rate_pct < ?
        ORDER BY rate_pct ASC LIMIT 1
      `).get(destIso, originIso, `${hsPrefix.slice(0,2)}%`, tariffRate);

      if (ftaOpp && (tariffRate - ftaOpp.rate_pct) > 3) {
        const saving = ((tariffRate - ftaOpp.rate_pct) / 100) * cargoValueFob;
        warnings.push(
          `FTA opportunity: ${ftaOpp.fta_name} rate ${ftaOpp.rate_pct}% vs MFN ${tariffRate}% ` +
          `— potential saving $${saving.toFixed(0)}`
        );
      }
    }
  }

  // CIF value = FOB + freight (used as duty base when incoterm is CIF)
  const cifBase      = incoterm === "CIF" ? cargoValueFob + freightCost : cargoValueFob;
  const importDuty   = cifBase * (tariffRate / 100);

  // ── 4. Insurance ───────────────────────────────────────────────────────────
  const insurance = (cargoValueFob + freightCost) * INSURANCE_RATE;

  // ── 5. Port handling ───────────────────────────────────────────────────────
  const thc = (THC_BY_PORT[dest_port_code] || 250) * teuFraction;

  // ── 6. Customs brokerage ───────────────────────────────────────────────────
  const brokerage = CUSTOMS_BROKERAGE_BY_COUNTRY[destIso] || CUSTOMS_BROKERAGE_USD;

  // ── 7. Total ───────────────────────────────────────────────────────────────
  const total = cargoValueFob + freightCost + importDuty + insurance + thc + brokerage;

  // Confidence interval: wider when FOB was estimated, tariff changed, or fuel spiked
  let confidenceInterval = 5.0;
  if (fobEstimated)      confidenceInterval += 4.0;
  if (tariffChangeFlag)  confidenceInterval += 3.0;
  if (fuelSpikeFlag)     confidenceInterval += 2.0;
  if (tariffType === "unknown") confidenceInterval += 5.0;

  // ── 8. Commodity spike check ───────────────────────────────────────────────
  const hsChapter = String(hs_code).slice(0, 2);
  let commodityPriceFlag = 0;
  const spikeCheck = db.prepare(`
    SELECT commodity_name, mom_change_pct
    FROM fact_commodity_prices f
    WHERE spike_flag = 1
      AND date >= date('now', '-35 days')
    ORDER BY date DESC LIMIT 1
  `).get();
  if (spikeCheck) {
    commodityPriceFlag = 1;
    warnings.push(
      `Commodity spike: ${spikeCheck.commodity_name} moved ` +
      `${spikeCheck.mom_change_pct?.toFixed(1)}% MoM in the last 35 days`
    );
  }

  // ── 9. Write to prediction table ──────────────────────────────────────────
  db.prepare(`
    INSERT INTO pred_landed_cost_estimates (
      estimate_id, origin_port_code, dest_port_code, hs_code,
      cargo_weight_kg, cargo_value_fob, freight_cost_usd, import_duty_usd,
      tariff_rate_pct, tariff_type_used, fta_name, insurance_usd,
      port_handling_usd, customs_brokerage_usd, total_landed_cost_usd,
      confidence_interval_pct, commodity_price_flag, tariff_change_flag,
      fta_opportunity_flag, fuel_spike_flag, alerts_json, model_version
    ) VALUES (
      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
    )
  `).run(
    estimateId, origin_port_code, dest_port_code, hs_code,
    cargo_weight_kg, cargoValueFob, freightCost, importDuty,
    tariffRate, tariffType, ftaName, insurance,
    thc, brokerage, total,
    confidenceInterval,
    commodityPriceFlag, tariffChangeFlag,
    warnings.some(w => w.includes("FTA opportunity")) ? 1 : 0,
    fuelSpikeFlag,
    JSON.stringify(warnings),
    "1.0.0"
  );

  // ── 10. Call ML model for freight prediction ─────────────────────────────
  const mlFeatures = {
    distance_nm:          distanceNm,
    fuel_price_brent:     fuelRow ? fuelRow.price_usd : 76.0,
    fuel_deviation_pct:   fuelRow ? (fuelRow.delta_vs_180d_pct || 0) : 0,
    cargo_weight_kg:      cargo_weight_kg || 10000,
    origin_efficiency:    PORT_EFFICIENCY[origin_port_code] || 4.0,
    dest_efficiency:      PORT_EFFICIENCY[dest_port_code]   || 4.0,
    origin_congestion:    PORT_CONGESTION[origin_port_code] || 0.5,
    dest_congestion:      PORT_CONGESTION[dest_port_code]   || 0.5,
    month:                new Date().getMonth() + 1,
    is_peak_season:       [10,11,12,1].includes(new Date().getMonth() + 1) ? 1 : 0,
    canal_required:       CANAL_REQUIRED[`${origin_port_code}-${dest_port_code}`] || 0,
  };

  const mlResult = callMLModel(mlFeatures);

  // Use ML freight if available, otherwise fall back to rule-based
  const freightFinal = mlResult.ml_predicted_usd || freightCost;
  const totalFinal   = cargoValueFob + freightFinal + importDuty + insurance + thc + brokerage;

  // ── 11. Return response ───────────────────────────────────────────────────
  return {
    estimate_id: estimateId,
    total_landed_cost_usd: Math.round(totalFinal * 100) / 100,
    confidence_interval_pct: Math.round(confidenceInterval * 10) / 10,
    breakdown: {
      cargo_value_fob:       Math.round(cargoValueFob * 100) / 100,
      freight_cost_usd:      Math.round(freightFinal * 100) / 100,
      import_duty_usd:       Math.round(importDuty * 100) / 100,
      insurance_usd:         Math.round(insurance * 100) / 100,
      port_handling_usd:     Math.round(thc * 100) / 100,
      customs_brokerage_usd: Math.round(brokerage * 100) / 100,
    },
    freight: {
      rule_based_usd:            Math.round(freightCost * 100) / 100,
      ml_predicted_usd:          mlResult.ml_predicted_usd ? Math.round(mlResult.ml_predicted_usd * 100) / 100 : null,
      ml_confidence_interval_pct: mlResult.confidence_interval_pct,
      ml_feature_contributions:   mlResult.feature_contributions,
      ml_model_version:           mlResult.model_version,
      ml_error:                   mlResult.error,
    },
    tariff_info: {
      rate_pct:    tariffRate,
      type:        tariffType,
      fta_name:    ftaName,
      change_flag: tariffChangeFlag === 1,
    },
    route_info: {
      origin_port:   origin_port_code,
      dest_port:     dest_port_code,
      distance_nm:   distanceNm,
      fob_estimated: fobEstimated,
    },
    alerts: warnings,
    model_version: "1.0.0",
  };
}


// ── Route comparison ──────────────────────────────────────────────────────────

function compareRoutes(db, params) {
  const { alternative_dest_ports, ...baseParams } = params;
  const ports = [params.dest_port_code, ...(alternative_dest_ports || [])];

  const results = ports.map(port => {
    try {
      const result = calculateLandedCost(db, { ...baseParams, dest_port_code: port });
      return { port, ...result, error: null };
    } catch (err) {
      return { port, error: err.message };
    }
  });

  // Rank by total cost
  const ranked = results
    .filter(r => !r.error)
    .sort((a, b) => a.total_landed_cost_usd - b.total_landed_cost_usd);

  return {
    cheapest_port: ranked[0]?.route_info?.dest_port || null,
    results: ranked,
    errors: results.filter(r => r.error),
  };
}


module.exports = { calculateLandedCost, compareRoutes };