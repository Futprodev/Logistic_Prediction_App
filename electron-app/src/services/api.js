const BASE = "http://localhost:3001";

async function get(path) {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) throw new Error(`API error ${res.status}: ${path}`);
  return res.json();
}

async function post(path, body) {
  const res = await fetch(`${BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "Unknown error" }));
    throw new Error(err.error || `API error ${res.status}`);
  }
  return res.json();
}

export const api = {
  health:           ()       => get("/health"),
  commodities:      ()       => get("/api/v1/commodities"),
  commodityHistory: (name, months) => get(`/api/v1/commodities/${encodeURIComponent(name)}/history?months=${months || 36}`),
  fuel:             ()       => get("/api/v1/fuel"),
  fuelHistory:      (type)   => get(`/api/v1/fuel/${type}/history?weeks=104`),
  tariffs:          (reporter, hs, partner) => get(`/api/v1/tariffs?reporter=${reporter}&hs_code=${hs}${partner ? `&partner=${partner}` : ""}`),
  ports:            ()       => get("/api/v1/ports"),
  portWeather:      (code)   => get(`/api/v1/ports/${code}/weather`),
  lpi:              (year)   => get(`/api/v1/lpi?year=${year || 2023}&limit=30`),
  alerts:           ()       => get("/api/v1/alerts"),
  macro:            ()       => get("/api/v1/macro"),
  landedCost:       (params) => post("/api/v1/landed-cost", params),
  compareRoutes:    (params) => post("/api/v1/landed-cost/compare", params),
  acknowledgeAlert: (id)     => post(`/api/v1/alerts/${id}/acknowledge`, {}),
};