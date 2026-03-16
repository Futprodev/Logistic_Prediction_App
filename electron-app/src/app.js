import React, { useState, useEffect, useCallback } from "react";
import { api } from "./services/api";
import {
  LineChart, Line, BarChart, Bar,
  XAxis, YAxis, Tooltip, ResponsiveContainer, Cell
} from "recharts";

// ── Utility ───────────────────────────────────────────────────────────────────
const fmt = (n, dec = 0) =>
  n == null ? "—" : Number(n).toLocaleString("en-US", { minimumFractionDigits: dec, maximumFractionDigits: dec });
const fmtUsd = (n) => n == null ? "—" : `$${fmt(n)}`;
const fmtPct = (n) => n == null ? "—" : `${Number(n) > 0 ? "+" : ""}${fmt(n, 1)}%`;

function PctBadge({ value }) {
  if (value == null) return null;
  const cls = value > 0 ? "tag-red" : value < 0 ? "tag-green" : "tag-blue";
  return <span className={`tag ${cls}`}>{fmtPct(value)}</span>;
}

// ── Sidebar nav ───────────────────────────────────────────────────────────────
const NAV = [
  { id: "dashboard",  icon: "▦", label: "Dashboard" },
  { id: "calculator", icon: "◈", label: "Landed Cost" },
  { id: "compare",    icon: "⇄", label: "Route Compare" },
  { id: "carbon",     icon: "◌", label: "Carbon Cost" },
  { id: "commodities",icon: "◉", label: "Commodities" },
  { id: "ports",      icon: "⬡", label: "Ports" },
  { id: "alerts",     icon: "◎", label: "Alerts" },
];

function Sidebar({ active, onChange, alertCount }) {
  return (
    <aside style={{
      width: 200, flexShrink: 0,
      background: "var(--bg-surface)",
      borderRight: "1px solid var(--border)",
      display: "flex", flexDirection: "column",
      padding: "1.5rem 0",
    }}>
      <div style={{ padding: "0 1.25rem 1.5rem", borderBottom: "1px solid var(--border)" }}>
        <div style={{ fontFamily: "var(--font-display)", fontWeight: 800, fontSize: 15, color: "var(--text-primary)", letterSpacing: "-0.02em" }}>
          LOGISTICS
        </div>
        <div style={{ fontFamily: "var(--font-mono)", fontSize: 10, color: "var(--accent-teal)", letterSpacing: "0.12em", marginTop: 2 }}>
          INTELLIGENCE
        </div>
      </div>

      <nav style={{ flex: 1, padding: "1rem 0.75rem" }}>
        {NAV.map(item => (
          <button key={item.id} onClick={() => onChange(item.id)} style={{
            width: "100%", display: "flex", alignItems: "center", gap: 10,
            padding: "9px 12px", borderRadius: "var(--radius-sm)",
            border: "none", cursor: "pointer", marginBottom: 2,
            background: active === item.id ? "var(--bg-hover)" : "transparent",
            color: active === item.id ? "var(--text-primary)" : "var(--text-secondary)",
            borderLeft: active === item.id ? "2px solid var(--accent-blue)" : "2px solid transparent",
            fontFamily: "var(--font-body)", fontSize: 13, fontWeight: active === item.id ? 500 : 400,
            transition: "all 0.15s", position: "relative",
          }}>
            <span style={{ fontSize: 14, opacity: 0.8 }}>{item.icon}</span>
            {item.label}
            {item.id === "alerts" && alertCount > 0 && (
              <span style={{
                marginLeft: "auto", background: "var(--accent-red)",
                color: "#fff", fontSize: 10, fontFamily: "var(--font-mono)",
                padding: "1px 6px", borderRadius: 10, fontWeight: 600,
              }}>{alertCount}</span>
            )}
          </button>
        ))}
      </nav>

      <div style={{ padding: "1rem 1.25rem", borderTop: "1px solid var(--border)" }}>
        <div style={{ fontFamily: "var(--font-mono)", fontSize: 10, color: "var(--text-tertiary)" }}>
          API localhost:3001
        </div>
      </div>
    </aside>
  );
}

// ── Metric card ───────────────────────────────────────────────────────────────
function MetricCard({ label, value, sub, accent, change }) {
  return (
    <div className="card" style={{ borderTop: `2px solid ${accent || "var(--border)"}` }}>
      <div className="label" style={{ marginBottom: 8 }}>{label}</div>
      <div className="value-lg" style={{ color: accent || "var(--text-primary)" }}>{value}</div>
      {(sub || change != null) && (
        <div style={{ display: "flex", alignItems: "center", gap: 8, marginTop: 6 }}>
          {sub && <span style={{ fontSize: 12, color: "var(--text-secondary)" }}>{sub}</span>}
          {change != null && <PctBadge value={change} />}
        </div>
      )}
    </div>
  );
}

// ── Sparkline ─────────────────────────────────────────────────────────────────
function Sparkline({ data, dataKey, color }) {
  if (!data?.length) return <div style={{ height: 50 }} />;
  return (
    <ResponsiveContainer width="100%" height={50}>
      <LineChart data={data}>
        <Line type="monotone" dataKey={dataKey} stroke={color} strokeWidth={1.5} dot={false} />
      </LineChart>
    </ResponsiveContainer>
  );
}

// ── Custom tooltip ────────────────────────────────────────────────────────────
function ChartTooltip({ active, payload, label, prefix = "" }) {
  if (!active || !payload?.length) return null;
  return (
    <div style={{
      background: "var(--bg-raised)", border: "1px solid var(--border-strong)",
      borderRadius: "var(--radius-sm)", padding: "8px 12px",
      fontFamily: "var(--font-mono)", fontSize: 12,
    }}>
      <div style={{ color: "var(--text-secondary)", marginBottom: 4 }}>{label}</div>
      {payload.map((p, i) => (
        <div key={i} style={{ color: p.color }}>
          {p.name}: {prefix}{fmt(p.value, 2)}
        </div>
      ))}
    </div>
  );
}

// ════════════════════════════════════════════════════════════
//  DASHBOARD VIEW
// ════════════════════════════════════════════════════════════
function DashboardView() {
  const [fuel, setFuel]         = useState([]);
  const [fuelHistory, setFuelHistory] = useState([]);
  const [commodities, setCommodities] = useState([]);
  const [alerts, setAlerts]     = useState({ summary: {}, alerts: [] });
  const [loading, setLoading]   = useState(true);

  useEffect(() => {
    Promise.all([
      api.fuel(),
      api.fuelHistory("brent"),
      api.commodities(),
      api.alerts(),
    ]).then(([f, fh, c, a]) => {
      setFuel(f.fuel_prices || []);
      setFuelHistory(fh.history || []);
      setCommodities((c.commodities || []).slice(0, 12));
      setAlerts(a);
    }).catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  if (loading) return (
    <div style={{ display: "flex", alignItems: "center", justifyContent: "center", flex: 1 }}>
      <div className="spinner" style={{ width: 32, height: 32 }} />
    </div>
  );

  const brent = fuel.find(f => f.fuel_type === "brent");
  const wti   = fuel.find(f => f.fuel_type === "wti");
  const spikeCount = commodities.filter(c => c.spike_flag).length;

  return (
    <div className="fade-in" style={{ padding: "1.5rem", overflowY: "auto", flex: 1 }}>
      <div style={{ marginBottom: "1.5rem" }}>
        <h1 style={{ fontFamily: "var(--font-display)", fontSize: 22, fontWeight: 700, letterSpacing: "-0.02em" }}>
          Market Overview
        </h1>
        <p style={{ color: "var(--text-secondary)", fontSize: 13, marginTop: 4 }}>
          Live commodity, fuel, and logistics intelligence
        </p>
      </div>

      {/* KPI row */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 12, marginBottom: "1.5rem" }}>
        <MetricCard
          label="Brent Crude"
          value={brent ? fmtUsd(brent.price_usd) : "—"}
          sub={brent ? `180d avg ${fmtUsd(brent.ma_180d)}` : null}
          change={brent?.delta_vs_180d_pct}
          accent="var(--accent-amber)"
        />
        <MetricCard
          label="WTI Crude"
          value={wti ? fmtUsd(wti.price_usd) : "—"}
          sub={wti ? `30d avg ${fmtUsd(wti.ma_30d)}` : null}
          change={wti?.delta_vs_180d_pct}
          accent="var(--accent-blue)"
        />
        <MetricCard
          label="Commodity Spikes"
          value={spikeCount}
          sub={`of ${commodities.length} tracked`}
          accent={spikeCount > 0 ? "var(--accent-red)" : "var(--accent-teal)"}
        />
        <MetricCard
          label="Active Alerts"
          value={alerts.summary?.total || 0}
          sub={`${alerts.summary?.warnings || 0} warnings`}
          accent="var(--accent-purple)"
        />
      </div>

      {/* Charts row */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginBottom: "1.5rem" }}>
        {/* Brent history */}
        <div className="card">
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
            <div>
              <div className="label">Brent Crude — 2yr history</div>
              <div style={{ fontFamily: "var(--font-mono)", fontSize: 18, fontWeight: 500, color: "var(--accent-amber)", marginTop: 4 }}>
                {brent ? fmtUsd(brent.price_usd) : "—"} / bbl
              </div>
            </div>
          </div>
          <ResponsiveContainer width="100%" height={140}>
            <LineChart data={fuelHistory}>
              <XAxis dataKey="date" tick={false} axisLine={false} tickLine={false} />
              <YAxis domain={["auto", "auto"]} tick={{ fill: "var(--text-tertiary)", fontSize: 10, fontFamily: "var(--font-mono)" }} axisLine={false} tickLine={false} width={40} />
              <Tooltip content={<ChartTooltip prefix="$" />} />
              <Line type="monotone" dataKey="price_usd" stroke="var(--accent-amber)" strokeWidth={1.5} dot={false} name="Brent" />
              <Line type="monotone" dataKey="ma_180d" stroke="rgba(245,158,11,0.3)" strokeWidth={1} dot={false} strokeDasharray="4 2" name="180d avg" />
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Commodity price bars */}
        <div className="card">
          <div className="label" style={{ marginBottom: 16 }}>Commodity MoM Change %</div>
          <ResponsiveContainer width="100%" height={170}>
            <BarChart data={commodities.slice(0, 10)} layout="vertical" margin={{ left: 0 }}>
              <XAxis type="number" tick={{ fill: "var(--text-tertiary)", fontSize: 10, fontFamily: "var(--font-mono)" }} axisLine={false} tickLine={false} />
              <YAxis type="category" dataKey="commodity_name" width={110}
                tick={{ fill: "var(--text-secondary)", fontSize: 10, fontFamily: "var(--font-mono)" }}
                axisLine={false} tickLine={false}
                tickFormatter={v => v.length > 14 ? v.slice(0, 14) + "…" : v}
              />
              <Tooltip content={<ChartTooltip />} />
              <Bar dataKey="mom_change_pct" radius={[0, 3, 3, 0]} name="MoM %">
                {commodities.slice(0, 10).map((entry, i) => (
                  <Cell key={i} fill={
                    entry.spike_flag ? "var(--accent-red)"
                    : (entry.mom_change_pct || 0) > 0 ? "var(--accent-amber)"
                    : "var(--accent-teal)"
                  } />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Recent alerts */}
      {alerts.alerts?.length > 0 && (
        <div className="card">
          <div className="label" style={{ marginBottom: 12 }}>Recent Alerts</div>
          {alerts.alerts.slice(0, 5).map(alert => (
            <div key={alert.alert_id} style={{
              display: "flex", alignItems: "flex-start", gap: 12,
              padding: "10px 0", borderBottom: "1px solid var(--border)",
            }}>
              <span className={`tag ${
                alert.severity === "ALERT" ? "tag-red"
                : alert.severity === "WARNING" ? "tag-amber"
                : "tag-blue"
              }`}>{alert.severity}</span>
              <div style={{ flex: 1 }}>
                <div style={{ fontSize: 13, color: "var(--text-primary)" }}>{alert.message}</div>
                <div style={{ fontSize: 11, color: "var(--text-tertiary)", marginTop: 2, fontFamily: "var(--font-mono)" }}>
                  {alert.alert_type} · {alert.triggered_at?.slice(0, 16)}
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ════════════════════════════════════════════════════════════
//  FREIGHT COMPARISON COMPONENT
// ════════════════════════════════════════════════════════════
function FreightComparison({ freight, activeTab, onTabChange }) {
  const active    = activeTab;
  const setActive = onTabChange;
  const rule = freight.rule_based_usd;
  const ml   = freight.ml_predicted_usd;
  const gap  = ml && rule ? ml - rule : null;
  const gapPct = gap && rule ? (gap / rule) * 100 : null;
  const contribs = freight.ml_feature_contributions || {};
  const topFeatures = Object.entries(contribs)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5);

  return (
    <div className="card" style={{ marginBottom: 12 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 14 }}>
        <div className="label">Freight Cost — Rule-Based vs ML Prediction</div>
        {gap != null && (
          <span className={`tag ${Math.abs(gapPct) < 10 ? "tag-green" : Math.abs(gapPct) < 25 ? "tag-amber" : "tag-red"}`}>
            {gapPct > 0 ? "+" : ""}{fmt(gapPct, 1)}% gap
          </span>
        )}
      </div>

      {/* Toggle tabs */}
      <div style={{ display: "flex", gap: 6, marginBottom: 14 }}>
        {["rule", "ml", "compare"].map(tab => (
          <button key={tab} onClick={() => setActive(tab)} style={{
            padding: "5px 14px", borderRadius: "var(--radius-sm)", border: "1px solid",
            borderColor: active === tab ? "var(--accent-blue)" : "var(--border-strong)",
            background: active === tab ? "rgba(59,127,255,0.12)" : "transparent",
            color: active === tab ? "var(--accent-blue)" : "var(--text-secondary)",
            fontFamily: "var(--font-mono)", fontSize: 11, cursor: "pointer",
            transition: "all 0.15s",
          }}>
            {tab === "rule" ? "Rule-Based" : tab === "ml" ? "ML Prediction" : "Compare"}
          </button>
        ))}
      </div>

      {/* Rule-based panel */}
      {active === "rule" && (
        <div className="fade-in">
          <div style={{ display: "flex", alignItems: "baseline", gap: 10, marginBottom: 10 }}>
            <div style={{ fontFamily: "var(--font-display)", fontSize: 28, fontWeight: 700, color: "var(--accent-amber)" }}>
              {fmtUsd(rule)}
            </div>
            <div style={{ fontSize: 12, color: "var(--text-secondary)" }}>deterministic formula</div>
          </div>
          <div style={{ fontSize: 12, color: "var(--text-secondary)", lineHeight: 1.8 }}>
            Calculated from: <span style={{ color: "var(--text-primary)" }}>distance band</span> ×{" "}
            <span style={{ color: "var(--text-primary)" }}>TEU fraction</span> ×{" "}
            <span style={{ color: "var(--text-primary)" }}>fuel surcharge multiplier</span>
          </div>
          <div style={{ marginTop: 10, padding: "8px 12px", background: "var(--bg-raised)", borderRadius: "var(--radius-sm)", fontSize: 12, color: "var(--text-secondary)", fontFamily: "var(--font-mono)" }}>
            Uses fixed per-nm rate from distance band lookup. Does not account for port congestion, seasonal demand, or non-linear fuel interactions.
          </div>
        </div>
      )}

      {/* ML panel */}
      {active === "ml" && (
        <div className="fade-in">
          {ml ? (
            <>
              <div style={{ display: "flex", alignItems: "baseline", gap: 10, marginBottom: 10 }}>
                <div style={{ fontFamily: "var(--font-display)", fontSize: 28, fontWeight: 700, color: "var(--accent-blue)" }}>
                  {fmtUsd(ml)}
                </div>
                <div style={{ fontSize: 12, color: "var(--text-secondary)" }}>
                  ±{freight.ml_confidence_interval_pct}% · XGBoost v{freight.ml_model_version}
                </div>
              </div>
              <div style={{ fontSize: 12, color: "var(--text-secondary)", marginBottom: 12 }}>
                Top feature contributions:
              </div>
              {topFeatures.map(([feat, imp]) => (
                <div key={feat} style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 6 }}>
                  <div style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "var(--text-secondary)", width: 160, flexShrink: 0 }}>
                    {feat.replace(/_/g, " ")}
                  </div>
                  <div style={{ flex: 1, height: 4, background: "var(--bg-hover)", borderRadius: 2, overflow: "hidden" }}>
                    <div style={{ width: `${(imp / topFeatures[0][1]) * 100}%`, height: "100%", background: "var(--accent-blue)", borderRadius: 2 }} />
                  </div>
                  <div style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "var(--text-primary)", minWidth: 40, textAlign: "right" }}>
                    {fmt(imp * 100, 1)}%
                  </div>
                </div>
              ))}
            </>
          ) : (
            <div style={{ fontSize: 12, color: "var(--accent-red)" }}>
              ML model unavailable — {freight.ml_error}
            </div>
          )}
        </div>
      )}

      {/* Compare panel */}
      {active === "compare" && (
        <div className="fade-in">
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginBottom: 14 }}>
            <div style={{ padding: "14px", background: "var(--bg-raised)", borderRadius: "var(--radius-sm)", borderLeft: "3px solid var(--accent-amber)" }}>
              <div style={{ fontSize: 11, color: "var(--text-tertiary)", fontFamily: "var(--font-mono)", marginBottom: 4 }}>RULE-BASED</div>
              <div style={{ fontFamily: "var(--font-display)", fontSize: 22, fontWeight: 700, color: "var(--accent-amber)" }}>{fmtUsd(rule)}</div>
              <div style={{ fontSize: 11, color: "var(--text-secondary)", marginTop: 4 }}>Deterministic formula</div>
            </div>
            <div style={{ padding: "14px", background: "var(--bg-raised)", borderRadius: "var(--radius-sm)", borderLeft: "3px solid var(--accent-blue)" }}>
              <div style={{ fontSize: 11, color: "var(--text-tertiary)", fontFamily: "var(--font-mono)", marginBottom: 4 }}>ML PREDICTION</div>
              <div style={{ fontFamily: "var(--font-display)", fontSize: 22, fontWeight: 700, color: "var(--accent-blue)" }}>{fmtUsd(ml)}</div>
              <div style={{ fontSize: 11, color: "var(--text-secondary)", marginTop: 4 }}>±{freight.ml_confidence_interval_pct}% confidence</div>
            </div>
          </div>
          {gap != null && (
            <div style={{ padding: "10px 14px", background: Math.abs(gapPct) < 10 ? "rgba(0,201,167,0.06)" : "rgba(245,158,11,0.06)",
              border: `1px solid ${Math.abs(gapPct) < 10 ? "rgba(0,201,167,0.2)" : "rgba(245,158,11,0.2)"}`,
              borderRadius: "var(--radius-sm)", fontSize: 12, color: "var(--text-secondary)", lineHeight: 1.7 }}>
              <strong style={{ color: "var(--text-primary)" }}>
                {Math.abs(gapPct) < 10 ? "Models agree" : Math.abs(gapPct) < 25 ? "Moderate divergence" : "Large divergence"}
              </strong>
              {" — "}
              {gap > 0
                ? `ML predicts ${fmtUsd(Math.abs(gap))} more than the formula. The model is capturing non-linear effects (congestion, seasonality, fuel-distance interaction) that the fixed-rate formula misses.`
                : `ML predicts ${fmtUsd(Math.abs(gap))} less than the formula. The formula's distance band may be overestimating for this specific route combination.`}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ════════════════════════════════════════════════════════════
//  LANDED COST CALCULATOR VIEW
// ════════════════════════════════════════════════════════════
const PORTS = ["SHA","PSA","NGB","PUS","RTM","ANR","DXB","LAX","LGB","HAM","PIR","CMB","KUL","MUM","MBA"];
const PORT_NAMES = {
  SHA:"Shanghai", PSA:"Singapore", NGB:"Ningbo", PUS:"Busan",
  RTM:"Rotterdam", ANR:"Antwerp", DXB:"Dubai", LAX:"Los Angeles",
  LGB:"Long Beach", HAM:"Hamburg", PIR:"Piraeus", CMB:"Colombo",
  KUL:"Port Klang", MUM:"Mumbai", MBA:"Mombasa",
};

const HS_QUICK_LIST = [
  { code: "270900", label: "Crude petroleum" },
  { code: "271019", label: "Fuel oils" },
  { code: "260111", label: "Iron ore" },
  { code: "720839", label: "Flat-rolled steel" },
  { code: "740300", label: "Copper cathodes" },
  { code: "760110", label: "Aluminum unwrought" },
  { code: "520100", label: "Cotton, not carded" },
  { code: "100190", label: "Wheat" },
  { code: "100590", label: "Maize / corn" },
  { code: "120100", label: "Soybeans" },
  { code: "150710", label: "Soybean oil" },
  { code: "150910", label: "Olive oil" },
  { code: "170111", label: "Raw cane sugar" },
  { code: "180100", label: "Cocoa beans" },
  { code: "090111", label: "Coffee, not roasted" },
  { code: "400110", label: "Natural rubber" },
  { code: "440320", label: "Tropical logs" },
  { code: "710812", label: "Gold, non-monetary" },
  { code: "711011", label: "Platinum" },
  { code: "847130", label: "Laptops / computers" },
  { code: "851712", label: "Smartphones" },
  { code: "870322", label: "Passenger cars" },
  { code: "890190", label: "Cargo vessels" },
  { code: "300490", label: "Pharmaceutical products" },
  { code: "610910", label: "Cotton T-shirts" },
  { code: "640299", label: "Footwear" },
  { code: "940360", label: "Wooden furniture" },
  { code: "950300", label: "Toys" },
];

function CalculatorView() {
  const [form, setForm] = useState({
    origin_port_code: "SHA",
    dest_port_code: "RTM",
    hs_code: "",
    cargo_weight_kg: 24000,
    fob_value_usd: "",
    incoterm: "CIF",
    hs_open: false,
  });

  const [activeFreightMethod, setActiveFreightMethod] = useState("ml");

  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError]   = useState(null);

  const set = (k, v) => setForm(f => ({ ...f, [k]: v }));

  const calculate = async () => {
    setLoading(true);
    setError(null);
    try {
      const params = {
        ...form,
        cargo_weight_kg: Number(form.cargo_weight_kg) || null,
        fob_value_usd: form.fob_value_usd ? Number(form.fob_value_usd) : null,
      };
      const res = await api.landedCost(params);
      setResult(res);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  const breakdown = result?.breakdown;

  const activeFreightValue = activeFreightMethod === "rule"
    ? result?.freight?.rule_based_usd
    : (result?.freight?.ml_predicted_usd || result?.freight?.rule_based_usd);

  const barData = breakdown ? [
    { name: "Cargo (FOB)",   value: breakdown.cargo_value_fob,       color: "var(--accent-blue)" },
    { name: "Freight",       value: activeFreightValue,              color: activeFreightMethod === "rule" ? "var(--accent-amber)" : "var(--accent-amber)" },
    { name: "Import Duty",   value: breakdown.import_duty_usd,       color: "var(--accent-red)" },
    { name: "Insurance",     value: breakdown.insurance_usd,         color: "var(--accent-purple)" },
    { name: "Port Handling", value: breakdown.port_handling_usd,     color: "var(--accent-teal)" },
    { name: "Brokerage",     value: breakdown.customs_brokerage_usd, color: "#64748b" },
  ] : [];

  return (
    <div className="fade-in" style={{ padding: "1.5rem", overflowY: "auto", flex: 1 }}>
      <div style={{ marginBottom: "1.5rem" }}>
        <h1 style={{ fontFamily: "var(--font-display)", fontSize: 22, fontWeight: 700, letterSpacing: "-0.02em" }}>
          Landed Cost Calculator
        </h1>
        <p style={{ color: "var(--text-secondary)", fontSize: 13, marginTop: 4 }}>
          Estimate total cost of getting cargo from origin to destination
        </p>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "360px 1fr", gap: 16 }}>
        {/* Form */}
        <div className="card" style={{ height: "fit-content" }}>
          <div className="label" style={{ marginBottom: 16 }}>Shipment Details</div>

          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10, marginBottom: 10 }}>
            <div>
              <div style={{ fontSize: 12, color: "var(--text-secondary)", marginBottom: 4 }}>Origin Port</div>
              <select className="inp" value={form.origin_port_code} onChange={e => set("origin_port_code", e.target.value)}>
                {PORTS.map(p => <option key={p} value={p}>{PORT_NAMES[p]} ({p})</option>)}
              </select>
            </div>
            <div>
              <div style={{ fontSize: 12, color: "var(--text-secondary)", marginBottom: 4 }}>Destination Port</div>
              <select className="inp" value={form.dest_port_code} onChange={e => set("dest_port_code", e.target.value)}>
                {PORTS.map(p => <option key={p} value={p}>{PORT_NAMES[p]} ({p})</option>)}
              </select>
            </div>
          </div>

          <div style={{ marginBottom: 10, position: "relative" }}>
            <div style={{ fontSize: 12, color: "var(--text-secondary)", marginBottom: 4 }}>HS Code</div>
            <input
              className="inp"
              value={form.hs_code}
              onChange={e => set("hs_code", e.target.value)}
              onFocus={() => set("hs_open", true)}
              onBlur={() => setTimeout(() => set("hs_open", false), 150)}
              placeholder="e.g. 720839 or click to browse"
            />
            {form.hs_open && (
              <div style={{
                position: "absolute", top: "100%", left: 0, right: 0, zIndex: 100,
                background: "var(--bg-raised)", border: "1px solid var(--border-strong)",
                borderRadius: "var(--radius-sm)", marginTop: 2, maxHeight: 220,
                overflowY: "auto", boxShadow: "0 8px 24px rgba(0,0,0,0.4)",
              }}>
                {HS_QUICK_LIST
                  .filter(h => !form.hs_code || h.code.startsWith(form.hs_code) || h.label.toLowerCase().includes(form.hs_code.toLowerCase()))
                  .map(h => (
                    <div key={h.code} onMouseDown={() => set("hs_code", h.code)} style={{
                      padding: "8px 12px", cursor: "pointer", display: "flex",
                      justifyContent: "space-between", alignItems: "center",
                      borderBottom: "1px solid var(--border)",
                      transition: "background 0.1s",
                    }}
                    onMouseEnter={e => e.currentTarget.style.background = "var(--bg-hover)"}
                    onMouseLeave={e => e.currentTarget.style.background = "transparent"}
                    >
                      <span style={{ fontFamily: "var(--font-mono)", fontSize: 12, color: "var(--accent-teal)" }}>{h.code}</span>
                      <span style={{ fontSize: 12, color: "var(--text-secondary)" }}>{h.label}</span>
                    </div>
                  ))}
              </div>
            )}
          </div>

          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10, marginBottom: 10 }}>
            <div>
              <div style={{ fontSize: 12, color: "var(--text-secondary)", marginBottom: 4 }}>Weight (kg)</div>
              <input className="inp" type="number" value={form.cargo_weight_kg} onChange={e => set("cargo_weight_kg", e.target.value)} placeholder="24000" />
            </div>
            <div>
              <div style={{ fontSize: 12, color: "var(--text-secondary)", marginBottom: 4 }}>FOB Value (USD)</div>
              <input className="inp" type="number" value={form.fob_value_usd} onChange={e => set("fob_value_usd", e.target.value)} placeholder="Leave blank to estimate" />
            </div>
          </div>

          <div style={{ marginBottom: 16 }}>
            <div style={{ fontSize: 12, color: "var(--text-secondary)", marginBottom: 4 }}>Incoterm</div>
            <select className="inp" value={form.incoterm} onChange={e => set("incoterm", e.target.value)}>
              <option value="CIF">CIF — Cost, Insurance, Freight</option>
              <option value="FOB">FOB — Free on Board</option>
            </select>
          </div>

          <button className="btn primary" style={{ width: "100%" }} onClick={calculate} disabled={loading}>
            {loading ? <><span className="spinner" style={{ width: 14, height: 14 }} /> Calculating…</> : "Calculate Landed Cost"}
          </button>

          {error && (
            <div style={{ marginTop: 12, padding: "8px 12px", background: "rgba(239,68,68,0.1)", border: "1px solid rgba(239,68,68,0.2)", borderRadius: "var(--radius-sm)", fontSize: 12, color: "var(--accent-red)" }}>
              {error}
            </div>
          )}
        </div>

        {/* Result */}
        {result ? (
          <div className="fade-in">
            {/* Total */}
            <div className="card" style={{ marginBottom: 12, borderTop: "2px solid var(--accent-teal)" }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
                <div> 
                  <div className="label">Total Landed Cost</div>
                    <div style={{ fontFamily: "var(--font-display)", fontSize: 36, fontWeight: 800, color: "var(--accent-teal)", marginTop: 4 }}>
                      {fmtUsd(
                        activeFreightMethod === "rule"
                          ? (breakdown?.cargo_value_fob + result?.freight?.rule_based_usd + breakdown?.import_duty_usd + breakdown?.insurance_usd + breakdown?.port_handling_usd + breakdown?.customs_brokerage_usd)
                          : result.total_landed_cost_usd
                      )}
                    </div>
                    <div style={{ fontSize: 12, color: "var(--text-secondary)", marginTop: 4 }}>
                          ±{result.confidence_interval_pct}% confidence · {result.computed_in_ms}ms ·{" "}
                          <span style={{ color: activeFreightMethod === "rule" ? "var(--accent-amber)" : "var(--accent-blue)" }}>
                            {activeFreightMethod === "rule" ? "Rule-based freight" : "ML freight"}
                          </span>
                        </div>
                  <div style={{ fontSize: 12, color: "var(--text-secondary)", marginTop: 4 }}>
                    ±{result.confidence_interval_pct}% confidence · {result.computed_in_ms}ms
                  </div>
                </div>
                <div style={{ textAlign: "right" }}>
                  <div className="label">Route</div>
                  <div style={{ fontFamily: "var(--font-mono)", fontSize: 14, color: "var(--text-primary)", marginTop: 4 }}>
                    {result.route_info.origin_port} → {result.route_info.dest_port}
                  </div>
                  <div style={{ fontSize: 12, color: "var(--text-secondary)", marginTop: 2 }}>
                    {fmt(result.route_info.distance_nm)} nm
                  </div>
                </div>
              </div>
            </div>

            {/* Freight comparison — rule-based vs ML */}
            {result.freight && (
              <FreightComparison
                freight={result.freight}
                activeTab={activeFreightMethod}
                onTabChange={setActiveFreightMethod}
              />
            )}

            {/* Breakdown chart */}
            <div className="card" style={{ marginBottom: 12 }}>
              <div className="label" style={{ marginBottom: 12 }}>Cost Breakdown</div>
              <div style={{ display: "flex", gap: 16, alignItems: "flex-start" }}>
                <ResponsiveContainer width="60%" height={180}>
                  <BarChart data={barData} layout="vertical">
                    <XAxis type="number" tick={{ fill: "var(--text-tertiary)", fontSize: 10, fontFamily: "var(--font-mono)" }} axisLine={false} tickLine={false} tickFormatter={v => `$${(v/1000).toFixed(0)}k`} />
                    <YAxis type="category" dataKey="name" width={100} tick={{ fill: "var(--text-secondary)", fontSize: 11, fontFamily: "var(--font-mono)" }} axisLine={false} tickLine={false} />
                    <Tooltip formatter={v => fmtUsd(v)} />
                    <Bar dataKey="value" radius={[0, 4, 4, 0]}>
                      {barData.map((d, i) => <Cell key={i} fill={d.color} />)}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>

                <div style={{ flex: 1 }}>
                  {barData.map((d, i) => (
                    <div key={i} style={{ display: "flex", justifyContent: "space-between", padding: "6px 0", borderBottom: "1px solid var(--border)", fontSize: 13 }}>
                      <span style={{ display: "flex", alignItems: "center", gap: 6 }}>
                        <span style={{ width: 8, height: 8, borderRadius: 2, background: d.color, flexShrink: 0 }} />
                        <span style={{ color: "var(--text-secondary)" }}>{d.name}</span>
                      </span>
                      <span style={{ fontFamily: "var(--font-mono)", color: "var(--text-primary)" }}>{fmtUsd(d.value)}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>

            {/* Tariff info */}
            <div className="card" style={{ marginBottom: 12 }}>
              <div className="label" style={{ marginBottom: 10 }}>Tariff Information</div>
              <div style={{ display: "flex", gap: 24, flexWrap: "wrap" }}>
                <div>
                  <div style={{ fontSize: 11, color: "var(--text-tertiary)", fontFamily: "var(--font-mono)" }}>RATE</div>
                  <div style={{ fontFamily: "var(--font-mono)", fontSize: 18, fontWeight: 500, color: "var(--text-primary)", marginTop: 2 }}>
                    {result.tariff_info.rate_pct}%
                  </div>
                </div>
                <div>
                  <div style={{ fontSize: 11, color: "var(--text-tertiary)", fontFamily: "var(--font-mono)" }}>TYPE</div>
                  <div style={{ marginTop: 4 }}>
                    <span className={`tag ${result.tariff_info.type === "preferential" ? "tag-green" : "tag-blue"}`}>
                      {result.tariff_info.type}
                    </span>
                  </div>
                </div>
                {result.tariff_info.fta_name && (
                  <div>
                    <div style={{ fontSize: 11, color: "var(--text-tertiary)", fontFamily: "var(--font-mono)" }}>FTA</div>
                    <div style={{ fontFamily: "var(--font-mono)", fontSize: 13, color: "var(--accent-teal)", marginTop: 4 }}>
                      {result.tariff_info.fta_name}
                    </div>
                  </div>
                )}
                {result.tariff_info.change_flag && (
                  <span className="tag tag-amber" style={{ alignSelf: "flex-end" }}>Rate changed recently</span>
                )}
              </div>
            </div>

            {/* Alerts */}
            {result.alerts?.length > 0 && (
              <div className="card">
                <div className="label" style={{ marginBottom: 10 }}>Alerts for this Shipment</div>
                {result.alerts.map((msg, i) => (
                  <div key={i} style={{ display: "flex", gap: 8, alignItems: "flex-start", padding: "6px 0", borderBottom: i < result.alerts.length - 1 ? "1px solid var(--border)" : "none" }}>
                    <span className="tag tag-amber" style={{ flexShrink: 0, marginTop: 1 }}>!</span>
                    <span style={{ fontSize: 13, color: "var(--text-secondary)" }}>{msg}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        ) : (
          <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: 300, color: "var(--text-tertiary)", flexDirection: "column", gap: 12 }}>
            <div style={{ fontSize: 32, opacity: 0.3 }}>◈</div>
            <div style={{ fontFamily: "var(--font-mono)", fontSize: 12 }}>Fill in the form and calculate</div>
          </div>
        )}
      </div>
    </div>
  );
}

// ════════════════════════════════════════════════════════════
//  COMMODITIES VIEW
// ════════════════════════════════════════════════════════════
function CommoditiesView() {
  const [commodities, setCommodities] = useState([]);
  const [selected, setSelected]       = useState(null);
  const [history, setHistory]         = useState([]);
  const [loading, setLoading]         = useState(true);

  useEffect(() => {
    api.commodities().then(r => {
      setCommodities(r.commodities || []);
      setLoading(false);
    });
  }, []);

  const selectCommodity = useCallback(async (name) => {
    setSelected(name);
    const h = await api.commodityHistory(name, 36);
    setHistory(h.history || []);
  }, []);

  if (loading) return (
    <div style={{ display: "flex", alignItems: "center", justifyContent: "center", flex: 1 }}>
      <div className="spinner" style={{ width: 32, height: 32 }} />
    </div>
  );

  return (
    <div className="fade-in" style={{ display: "flex", flex: 1, overflow: "hidden" }}>
      {/* List */}
      <div style={{ width: 280, borderRight: "1px solid var(--border)", overflowY: "auto", padding: "1rem" }}>
        <div className="label" style={{ padding: "0 4px 10px" }}>All Commodities</div>
        {commodities.map(c => (
          <div key={c.commodity_name} onClick={() => selectCommodity(c.commodity_name)} style={{
            padding: "10px 12px", borderRadius: "var(--radius-sm)", cursor: "pointer", marginBottom: 2,
            background: selected === c.commodity_name ? "var(--bg-hover)" : "transparent",
            borderLeft: selected === c.commodity_name ? "2px solid var(--accent-blue)" : "2px solid transparent",
            transition: "all 0.12s",
          }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <div style={{ fontSize: 13, color: "var(--text-primary)", fontWeight: selected === c.commodity_name ? 500 : 400 }}>
                {c.commodity_name}
              </div>
              {c.spike_flag ? <span className="tag tag-red" style={{ fontSize: 10 }}>spike</span> : null}
            </div>
            <div style={{ display: "flex", justifyContent: "space-between", marginTop: 3 }}>
              <span style={{ fontFamily: "var(--font-mono)", fontSize: 12, color: "var(--text-secondary)" }}>
                {fmtUsd(c.price_usd)}
              </span>
              <PctBadge value={c.mom_change_pct} />
            </div>
          </div>
        ))}
      </div>

      {/* Detail */}
      <div style={{ flex: 1, padding: "1.5rem", overflowY: "auto" }}>
        {selected ? (
          <div className="fade-in">
            <h2 style={{ fontFamily: "var(--font-display)", fontSize: 20, fontWeight: 700, marginBottom: "1.5rem" }}>
              {selected}
            </h2>
            <div className="card">
              <div className="label" style={{ marginBottom: 12 }}>36-Month Price History</div>
              <ResponsiveContainer width="100%" height={280}>
                <LineChart data={history}>
                  <XAxis dataKey="date" tick={{ fill: "var(--text-tertiary)", fontSize: 10, fontFamily: "var(--font-mono)" }}
                          tickFormatter={v => v?.slice(0, 7)}
                          interval={Math.max(0, Math.floor(history.length / 8) - 1)}
                          axisLine={false} tickLine={false} />
                  <YAxis tick={{ fill: "var(--text-tertiary)", fontSize: 10, fontFamily: "var(--font-mono)" }} axisLine={false} tickLine={false} width={60} />
                  <Tooltip content={<ChartTooltip prefix="$" />} />
                  <Line type="monotone" dataKey="price_usd" stroke="var(--accent-blue)" strokeWidth={2} dot={false} name="Price" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>
        ) : (
          <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "100%", color: "var(--text-tertiary)", flexDirection: "column", gap: 12 }}>
            <div style={{ fontSize: 40, opacity: 0.2 }}>◉</div>
            <div style={{ fontFamily: "var(--font-mono)", fontSize: 12 }}>Select a commodity to view history</div>
          </div>
        )}
      </div>
    </div>
  );
}

// ════════════════════════════════════════════════════════════
//  PORTS VIEW
// ════════════════════════════════════════════════════════════
const PORT_EFFICIENCY_UI = {
  SHA: 4.3, PSA: 4.8, NGB: 4.2, PUS: 4.1, RTM: 4.5,
  ANR: 4.3, DXB: 4.0, LAX: 3.5, LGB: 3.5, HAM: 4.2,
  PIR: 3.4, CMB: 3.3, KUL: 3.8, MUM: 3.0, MBA: 2.5,
};
const PORT_CONGESTION_UI = {
  SHA: 0.70, PSA: 0.50, NGB: 0.65, PUS: 0.45, RTM: 0.50,
  ANR: 0.45, DXB: 0.55, LAX: 0.80, LGB: 0.75, HAM: 0.50,
  PIR: 0.40, CMB: 0.55, KUL: 0.50, MUM: 0.70, MBA: 0.60,
};
const PORT_TEU_UI = {
  SHA: 47303000, PSA: 38702000, NGB: 33350000, PUS: 21694000,
  RTM: 14500000, ANR: 12024000, DXB: 14000000, LAX: 9921000,
  LGB: 9131000,  HAM: 8725000,  PIR: 5600000,  CMB: 6000000,
  KUL: 13700000, MUM: 5800000,  MBA: 1400000,
};

function ScoreBar({ value, max = 5, color = "var(--accent-teal)" }) {
  const pct = Math.min((value / max) * 100, 100);
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
      <div style={{ flex: 1, height: 4, background: "var(--bg-hover)", borderRadius: 2, overflow: "hidden" }}>
        <div style={{ width: `${pct}%`, height: "100%", background: color, borderRadius: 2, transition: "width 0.4s ease" }} />
      </div>
      <span style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "var(--text-secondary)", minWidth: 28 }}>
        {fmt(value, 1)}
      </span>
    </div>
  );
}

function PortsView() {
  const [ports, setPorts]       = useState([]);
  const [selected, setSelected] = useState(null);
  const [weather, setWeather]   = useState([]);

  useEffect(() => {
    api.ports().then(r => setPorts(r.ports || []));
  }, []);

  const selectPort = async (code) => {
    setSelected(code);
    const w = await api.portWeather(code);
    setWeather(w.forecast || []);
  };

  const selectedPort  = ports.find(p => p.port_code === selected);
  const windData      = weather.map(d => ({ date: d.date?.slice(5), wind: d.avg_wind_speed_kmh || 0 }));
  const waveData      = weather.map(d => ({ date: d.date?.slice(5), wave: d.max_wave_height_m || 0 }));
  const riskDays      = weather.filter(d => d.delay_risk_weather).length;
  const maxWave       = weather.reduce((m, d) => Math.max(m, d.max_wave_height_m || 0), 0);
  const maxWind       = weather.reduce((m, d) => Math.max(m, d.avg_wind_speed_kmh || 0), 0);

  return (
    <div className="fade-in" style={{ display: "flex", flex: 1, overflow: "hidden" }}>
      {/* Port list */}
      <div style={{ width: 240, borderRight: "1px solid var(--border)", overflowY: "auto", padding: "1rem" }}>
        <div className="label" style={{ padding: "0 4px 10px" }}>Major Ports</div>
        {ports.map(p => {
          const cong = PORT_CONGESTION_UI[p.port_code] || 0.5;
          const congColor = cong > 0.7 ? "var(--accent-red)" : cong > 0.55 ? "var(--accent-amber)" : "var(--accent-teal)";
          return (
            <div key={p.port_code} onClick={() => selectPort(p.port_code)} style={{
              padding: "10px 12px", borderRadius: "var(--radius-sm)", cursor: "pointer", marginBottom: 2,
              background: selected === p.port_code ? "var(--bg-hover)" : "transparent",
              borderLeft: selected === p.port_code ? "2px solid var(--accent-teal)" : "2px solid transparent",
              transition: "all 0.12s",
            }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                <span style={{ fontFamily: "var(--font-mono)", fontSize: 13, color: "var(--accent-teal)" }}>{p.port_code}</span>
                <span style={{ fontFamily: "var(--font-mono)", fontSize: 10, color: congColor }}>
                  {Math.round(cong * 100)}%
                </span>
              </div>
              <div style={{ fontSize: 12, color: "var(--text-secondary)", marginTop: 2 }}>{p.port_name}</div>
            </div>
          );
        })}
      </div>

      {/* Port detail */}
      <div style={{ flex: 1, padding: "1.5rem", overflowY: "auto" }}>
        {selected && selectedPort ? (
          <div className="fade-in">
            {/* Header */}
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: "1.25rem" }}>
              <div>
                <h2 style={{ fontFamily: "var(--font-display)", fontSize: 22, fontWeight: 700 }}>
                  {selectedPort.port_name}
                </h2>
                <div style={{ fontFamily: "var(--font-mono)", fontSize: 12, color: "var(--text-secondary)", marginTop: 4 }}>
                  {selectedPort.country_iso} · {selectedPort.lat?.toFixed(2)}°, {selectedPort.lon?.toFixed(2)}°
                </div>
              </div>
              <div style={{ display: "flex", gap: 8 }}>
                {riskDays > 0
                  ? <span className="tag tag-amber">{riskDays} risk day{riskDays > 1 ? "s" : ""} ahead</span>
                  : weather.length > 0
                  ? <span className="tag tag-green">Clear 7 days</span>
                  : null}
              </div>
            </div>

            {/* Metrics row */}
            <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 10, marginBottom: 12 }}>
              <div className="card" style={{ borderTop: "2px solid var(--accent-teal)" }}>
                <div className="label" style={{ marginBottom: 6 }}>Annual TEU</div>
                <div className="value-md">
                  {PORT_TEU_UI[selected] ? (PORT_TEU_UI[selected] / 1000000).toFixed(1) + "M" : "—"}
                </div>
              </div>
              <div className="card" style={{ borderTop: "2px solid var(--accent-blue)" }}>
                <div className="label" style={{ marginBottom: 6 }}>LPI Efficiency</div>
                <div className="value-md">{PORT_EFFICIENCY_UI[selected] || "—"} / 5</div>
                <ScoreBar value={PORT_EFFICIENCY_UI[selected] || 0} max={5} color="var(--accent-blue)" />
              </div>
              <div className="card" style={{
                borderTop: `2px solid ${(PORT_CONGESTION_UI[selected] || 0.5) > 0.7 ? "var(--accent-red)" : "var(--accent-amber)"}` }}>
                <div className="label" style={{ marginBottom: 6 }}>Congestion</div>
                <div className="value-md">{Math.round((PORT_CONGESTION_UI[selected] || 0.5) * 100)}%</div>
                <ScoreBar value={(PORT_CONGESTION_UI[selected] || 0.5) * 5} max={5}
                  color={(PORT_CONGESTION_UI[selected] || 0.5) > 0.7 ? "var(--accent-red)" : "var(--accent-amber)"} />
              </div>
              <div className="card" style={{ borderTop: "2px solid var(--accent-purple)" }}>
                <div className="label" style={{ marginBottom: 6 }}>7-Day Max Wave</div>
                <div className="value-md">{maxWave > 0 ? `${fmt(maxWave, 1)}m` : "—"}</div>
                <div style={{ fontSize: 11, color: "var(--text-tertiary)", marginTop: 4 }}>
                  Wind: {maxWind > 0 ? `${fmt(maxWind, 0)} km/h` : "—"}
                </div>
              </div>
            </div>

            {/* 7-day forecast */}
            {weather.length > 0 && (
              <div className="card" style={{ marginBottom: 12 }}>
                <div className="label" style={{ marginBottom: 12 }}>7-Day Forecast</div>
                <div style={{ display: "grid", gridTemplateColumns: "repeat(7, 1fr)", gap: 8, marginBottom: 16 }}>
                  {weather.map(day => {
                    const risk = day.delay_risk_weather;
                    const wave = day.max_wave_height_m || 0;
                    const wind = day.avg_wind_speed_kmh || 0;
                    const waveColor = wave > 4 ? "var(--accent-red)" : wave > 2 ? "var(--accent-amber)" : "var(--accent-teal)";
                    return (
                      <div key={day.date} style={{
                        textAlign: "center", padding: "12px 6px",
                        background: risk ? "rgba(239,68,68,0.06)" : "var(--bg-raised)",
                        borderRadius: "var(--radius-sm)",
                        border: `1px solid ${risk ? "rgba(239,68,68,0.2)" : "var(--border)"}`,
                      }}>
                        <div style={{ fontFamily: "var(--font-mono)", fontSize: 10, color: "var(--text-tertiary)", marginBottom: 6 }}>
                          {day.date?.slice(5)}
                        </div>
                        <div style={{ fontSize: 18, marginBottom: 6 }}>
                          {risk ? "⚠" : wave > 2 ? "〰" : "○"}
                        </div>
                        <div style={{ fontFamily: "var(--font-mono)", fontSize: 10, fontWeight: 500, marginBottom: 8,
                          color: risk ? "var(--accent-red)" : "var(--accent-teal)" }}>
                          {risk ? "RISK" : "CLEAR"}
                        </div>
                        <div style={{ borderTop: "1px solid var(--border)", paddingTop: 6 }}>
                          {wave > 0 && (
                            <div style={{ fontSize: 10, color: waveColor, fontFamily: "var(--font-mono)" }}>
                              {fmt(wave, 1)}m
                            </div>
                          )}
                          {wind > 0 && (
                            <div style={{ fontSize: 10, color: "var(--text-tertiary)", fontFamily: "var(--font-mono)", marginTop: 2 }}>
                              {fmt(wind, 0)}km/h
                            </div>
                          )}
                        </div>
                      </div>
                    );
                  })}
                </div>

                {/* Trend charts */}
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
                  <div>
                    <div style={{ fontSize: 11, color: "var(--text-tertiary)", fontFamily: "var(--font-mono)", marginBottom: 8 }}>
                      WIND SPEED (km/h)
                    </div>
                    <ResponsiveContainer width="100%" height={80}>
                      <LineChart data={windData}>
                        <XAxis dataKey="date" tick={{ fill: "var(--text-tertiary)", fontSize: 9, fontFamily: "var(--font-mono)" }} axisLine={false} tickLine={false} />
                        <YAxis hide />
                        <Tooltip formatter={v => `${fmt(v, 0)} km/h`} />
                        <Line type="monotone" dataKey="wind" stroke="var(--accent-blue)" strokeWidth={2} dot={{ r: 3, fill: "var(--accent-blue)" }} />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                  <div>
                    <div style={{ fontSize: 11, color: "var(--text-tertiary)", fontFamily: "var(--font-mono)", marginBottom: 8 }}>
                      WAVE HEIGHT (m)
                    </div>
                    <ResponsiveContainer width="100%" height={80}>
                      <LineChart data={waveData}>
                        <XAxis dataKey="date" tick={{ fill: "var(--text-tertiary)", fontSize: 9, fontFamily: "var(--font-mono)" }} axisLine={false} tickLine={false} />
                        <YAxis hide />
                        <Tooltip formatter={v => `${fmt(v, 1)}m`} />
                        <Line type="monotone" dataKey="wave" stroke="var(--accent-purple)" strokeWidth={2} dot={{ r: 3, fill: "var(--accent-purple)" }} />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              </div>
            )}

            {/* Cost impact */}
            <div className="card">
              <div className="label" style={{ marginBottom: 12 }}>Freight Cost Impact</div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
                <div>
                  <div style={{ fontSize: 12, color: "var(--text-secondary)", marginBottom: 6 }}>Efficiency surcharge</div>
                  <div style={{ fontFamily: "var(--font-mono)", fontSize: 18, fontWeight: 500, color: "var(--text-primary)" }}>
                    ~${Math.round((5 - (PORT_EFFICIENCY_UI[selected] || 4)) * 60)}/TEU
                  </div>
                  <div style={{ fontSize: 12, color: "var(--text-secondary)", marginTop: 4 }}>
                    {PORT_EFFICIENCY_UI[selected] >= 4.5
                      ? "Best-in-class efficiency — minimal surcharge"
                      : PORT_EFFICIENCY_UI[selected] >= 3.8
                      ? "Good efficiency — small surcharge"
                      : "Below-average efficiency — higher handling costs"}
                  </div>
                </div>
                <div>
                  <div style={{ fontSize: 12, color: "var(--text-secondary)", marginBottom: 6 }}>Congestion surcharge</div>
                  <div style={{ fontFamily: "var(--font-mono)", fontSize: 18, fontWeight: 500,
                    color: (PORT_CONGESTION_UI[selected] || 0.5) > 0.7 ? "var(--accent-red)" : "var(--text-primary)" }}>
                    ~${Math.round((PORT_CONGESTION_UI[selected] || 0.5) * 400)}/TEU
                  </div>
                  <div style={{ fontSize: 12, color: "var(--text-secondary)", marginTop: 4 }}>
                    {(PORT_CONGESTION_UI[selected] || 0.5) > 0.7
                      ? "High congestion — expect delays and premium surcharges"
                      : (PORT_CONGESTION_UI[selected] || 0.5) > 0.55
                      ? "Moderate congestion — monitor for seasonal spikes"
                      : "Low congestion — reliable transit times"}
                  </div>
                </div>
              </div>
            </div>

          </div>
        ) : (
          <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "100%", color: "var(--text-tertiary)", flexDirection: "column", gap: 12 }}>
            <div style={{ fontSize: 40, opacity: 0.2 }}>⬡</div>
            <div style={{ fontFamily: "var(--font-mono)", fontSize: 12 }}>Select a port to view details</div>
          </div>
        )}
      </div>
    </div>
  );
}

// ════════════════════════════════════════════════════════════
//  ALERTS VIEW
// ════════════════════════════════════════════════════════════
function AlertsView() {
  const [data, setData]     = useState({ summary: {}, alerts: [] });
  const [loading, setLoading] = useState(true);

  const load = () => {
    setLoading(true);
    api.alerts({ unacknowledged_only: false }).then(setData).finally(() => setLoading(false));
  };

  useEffect(load, []);

  const ack = async (id) => {
    await api.acknowledgeAlert(id);
    load();
  };

  const undo = async (id) => {
  await api.unacknowledgeAlert(id);
  load();
  };

  if (loading) return (
    <div style={{ display: "flex", alignItems: "center", justifyContent: "center", flex: 1 }}>
      <div className="spinner" style={{ width: 32, height: 32 }} />
    </div>
  );

  return (
    <div className="fade-in" style={{ padding: "1.5rem", overflowY: "auto", flex: 1 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "1.5rem" }}>
        <div>
          <h1 style={{ fontFamily: "var(--font-display)", fontSize: 22, fontWeight: 700, letterSpacing: "-0.02em" }}>Alerts</h1>
          <p style={{ color: "var(--text-secondary)", fontSize: 13, marginTop: 4 }}>
            {data.summary?.total || 0} active · {data.summary?.warnings || 0} warnings
          </p>
        </div>
        <button className="btn" onClick={load}>Refresh</button>
      </div>

      {data.alerts.map(alert => (
        <div key={alert.alert_id} className="card" style={{
          marginBottom: 8, display: "flex", gap: 16, alignItems: "flex-start",
          borderLeft: `3px solid ${alert.severity === "ALERT" ? "var(--accent-red)" : alert.severity === "WARNING" ? "var(--accent-amber)" : "var(--accent-blue)"}`,
          borderRadius: "0 var(--radius-lg) var(--radius-lg) 0",
          opacity: alert.acknowledged ? 0.5 : 1,
        }}>
          <div style={{ flex: 1 }}>
            <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 6 }}>
              <span className={`tag ${alert.severity === "ALERT" ? "tag-red" : alert.severity === "WARNING" ? "tag-amber" : "tag-blue"}`}>
                {alert.severity}
              </span>
              <span style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "var(--text-tertiary)" }}>
                {alert.alert_type}
              </span>
            </div>
            <div style={{ fontSize: 13, color: "var(--text-primary)" }}>{alert.message}</div>
            <div style={{ fontSize: 11, color: "var(--text-tertiary)", fontFamily: "var(--font-mono)", marginTop: 4 }}>
              {alert.triggered_at?.slice(0, 16)}
            </div>
          </div>
            <button
              className="btn"
              style={{ fontSize: 11, padding: "4px 10px", flexShrink: 0 }}
              onClick={() => alert.acknowledged ? undo(alert.alert_id) : ack(alert.alert_id)}
            >
              {alert.acknowledged ? "Restore" : "Dismiss"}
            </button>
        </div>
      ))}

      {!data.alerts.length && (
        <div style={{ textAlign: "center", padding: "3rem", color: "var(--text-tertiary)", fontFamily: "var(--font-mono)", fontSize: 12 }}>
          No alerts
        </div>
      )}
    </div>
  );
}

// ════════════════════════════════════════════════════════════
//  ROUTE COMPARE VIEW
// ════════════════════════════════════════════════════════════
function RouteCompareView() {
  const [form, setForm] = useState({
    origin_port_code: "SHA",
    hs_code: "",
    cargo_weight_kg: 24000,
    fob_value_usd: "",
  });
  const [selectedDests, setSelectedDests] = useState(["RTM", "HAM", "ANR"]);
  const [result, setResult]   = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError]     = useState(null);

  const set = (k, v) => setForm(f => ({ ...f, [k]: v }));

  const toggleDest = (port) => {
    setSelectedDests(prev =>
      prev.includes(port)
        ? prev.filter(p => p !== port)
        : prev.length < 5 ? [...prev, port] : prev
    );
  };

  const compare = async () => {
    if (selectedDests.length < 2) {
      setError("Select at least 2 destination ports to compare");
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const params = {
        ...form,
        cargo_weight_kg: Number(form.cargo_weight_kg),
        fob_value_usd:   Number(form.fob_value_usd),
        dest_port_code:  selectedDests[0],
        alternative_dest_ports: selectedDests.slice(1),
      };
      const res = await api.compareRoutes(params);
      setResult(res);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  const cheapest = result?.cheapest_port;
  const maxCost  = result?.results?.reduce((m, r) => Math.max(m, r.total_landed_cost_usd), 0) || 1;

  return (
    <div className="fade-in" style={{ padding: "1.5rem", overflowY: "auto", flex: 1 }}>
      <div style={{ marginBottom: "1.5rem" }}>
        <h1 style={{ fontFamily: "var(--font-display)", fontSize: 22, fontWeight: 700, letterSpacing: "-0.02em" }}>
          Route Comparison
        </h1>
        <p style={{ color: "var(--text-secondary)", fontSize: 13, marginTop: 4 }}>
          Compare landed cost across up to 5 destination ports simultaneously
        </p>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "320px 1fr", gap: 16 }}>
        {/* Form */}
        <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
          <div className="card">
            <div className="label" style={{ marginBottom: 14 }}>Shipment</div>
            <div style={{ marginBottom: 10 }}>
              <div style={{ fontSize: 12, color: "var(--text-secondary)", marginBottom: 4 }}>Origin Port</div>
              <select className="inp" value={form.origin_port_code} onChange={e => set("origin_port_code", e.target.value)}>
                {PORTS.map(p => <option key={p} value={p}>{PORT_NAMES[p]} ({p})</option>)}
              </select>
            </div>
            <div style={{ marginBottom: 10, position: "relative" }}>
              <div style={{ fontSize: 12, color: "var(--text-secondary)", marginBottom: 4 }}>HS Code</div>
              <input
                className="inp"
                value={form.hs_code}
                onChange={e => set("hs_code", e.target.value)}
                onFocus={() => set("hs_open", true)}
                onBlur={() => setTimeout(() => set("hs_open", false), 150)}
                placeholder="e.g. 720839 or click to browse"
              />
              {form.hs_open && (
                <div style={{
                  position: "absolute", top: "100%", left: 0, right: 0, zIndex: 100,
                  background: "var(--bg-raised)", border: "1px solid var(--border-strong)",
                  borderRadius: "var(--radius-sm)", marginTop: 2, maxHeight: 220,
                  overflowY: "auto", boxShadow: "0 8px 24px rgba(0,0,0,0.4)",
                }}>
                  {HS_QUICK_LIST
                    .filter(h => !form.hs_code || h.code.startsWith(form.hs_code) || h.label.toLowerCase().includes(form.hs_code.toLowerCase()))
                    .map(h => (
                      <div key={h.code} onMouseDown={() => set("hs_code", h.code)} style={{
                        padding: "8px 12px", cursor: "pointer", display: "flex",
                        justifyContent: "space-between", alignItems: "center",
                        borderBottom: "1px solid var(--border)",
                        transition: "background 0.1s",
                      }}
                      onMouseEnter={e => e.currentTarget.style.background = "var(--bg-hover)"}
                      onMouseLeave={e => e.currentTarget.style.background = "transparent"}
                      >
                        <span style={{ fontFamily: "var(--font-mono)", fontSize: 12, color: "var(--accent-teal)" }}>{h.code}</span>
                        <span style={{ fontSize: 12, color: "var(--text-secondary)" }}>{h.label}</span>
                      </div>
                    ))}
                </div>
              )}
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, marginBottom: 14 }}>
              <div>
                <div style={{ fontSize: 12, color: "var(--text-secondary)", marginBottom: 4 }}>Weight (kg)</div>
                <input className="inp" type="number" value={form.cargo_weight_kg} onChange={e => set("cargo_weight_kg", e.target.value)} />
              </div>
              <div>
                <div style={{ fontSize: 12, color: "var(--text-secondary)", marginBottom: 4 }}>FOB (USD)</div>
                <input className="inp" type="number" value={form.fob_value_usd} onChange={e => set("fob_value_usd", e.target.value)} placeholder="Leave blank to estimate"/>
              </div>
            </div>
            <button className="btn primary" style={{ width: "100%" }} onClick={compare} disabled={loading}>
              {loading ? <><span className="spinner" style={{ width: 14, height: 14 }} /> Comparing…</> : `Compare ${selectedDests.length} Routes`}
            </button>
            {error && (
              <div style={{ marginTop: 10, fontSize: 12, color: "var(--accent-red)", padding: "8px 10px", background: "rgba(239,68,68,0.08)", borderRadius: "var(--radius-sm)" }}>
                {error}
              </div>
            )}
          </div>

          <div className="card">
            <div className="label" style={{ marginBottom: 12 }}>Destination Ports <span style={{ color: "var(--text-tertiary)" }}>(select 2–5)</span></div>
            {PORTS.filter(p => p !== form.origin_port_code).map(p => {
              const selected = selectedDests.includes(p);
              return (
                <div key={p} onClick={() => toggleDest(p)} style={{
                  display: "flex", alignItems: "center", gap: 10,
                  padding: "8px 10px", borderRadius: "var(--radius-sm)",
                  cursor: "pointer", marginBottom: 2,
                  background: selected ? "rgba(59,127,255,0.08)" : "transparent",
                  border: `1px solid ${selected ? "rgba(59,127,255,0.3)" : "transparent"}`,
                  transition: "all 0.12s",
                }}>
                  <div style={{
                    width: 16, height: 16, borderRadius: 3,
                    border: `1.5px solid ${selected ? "var(--accent-blue)" : "var(--border-strong)"}`,
                    background: selected ? "var(--accent-blue)" : "transparent",
                    display: "flex", alignItems: "center", justifyContent: "center",
                    flexShrink: 0, fontSize: 10, color: "#fff",
                  }}>
                    {selected ? "✓" : ""}
                  </div>
                  <span style={{ fontFamily: "var(--font-mono)", fontSize: 12, color: "var(--accent-teal)" }}>{p}</span>
                  <span style={{ fontSize: 12, color: "var(--text-secondary)" }}>{PORT_NAMES[p]}</span>
                </div>
              );
            })}
          </div>
        </div>

        {/* Results */}
        <div>
          {result ? (
            <div className="fade-in">
              <div style={{ marginBottom: 12, display: "flex", alignItems: "center", gap: 10 }}>
                <div className="label">Ranked by total landed cost</div>
                {cheapest && (
                  <span className="tag tag-green">Cheapest: {PORT_NAMES[cheapest]} ({cheapest})</span>
                )}
              </div>

              {result.results.map((r, i) => {
                const isCheapest = r.route_info?.dest_port === cheapest;
                const barWidth   = (r.total_landed_cost_usd / maxCost) * 100;
                const saving     = result.results[0].total_landed_cost_usd - r.total_landed_cost_usd;

                return (
                  <div key={r.route_info?.dest_port} className="card" style={{
                    marginBottom: 10,
                    borderLeft: `3px solid ${isCheapest ? "var(--accent-teal)" : i === 1 ? "var(--accent-blue)" : "var(--border)"}`,
                    borderRadius: "0 var(--radius-lg) var(--radius-lg) 0",
                  }}>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 10 }}>
                      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                        <span style={{ fontFamily: "var(--font-display)", fontSize: 18, fontWeight: 700,
                          color: isCheapest ? "var(--accent-teal)" : "var(--text-tertiary)" }}>
                          #{i + 1}
                        </span>
                        <div>
                          <div style={{ fontFamily: "var(--font-mono)", fontSize: 14, color: "var(--text-primary)", fontWeight: 500 }}>
                            {PORT_NAMES[r.route_info?.dest_port]} ({r.route_info?.dest_port})
                          </div>
                          <div style={{ fontSize: 11, color: "var(--text-tertiary)", fontFamily: "var(--font-mono)", marginTop: 2 }}>
                            {fmt(r.route_info?.distance_nm)} nm
                            {r.carbon && ` · ${fmt(r.carbon.co2_tonnes, 1)}t CO₂`}
                          </div>
                        </div>
                      </div>
                      <div style={{ textAlign: "right" }}>
                        <div style={{ fontFamily: "var(--font-display)", fontSize: 22, fontWeight: 700,
                          color: isCheapest ? "var(--accent-teal)" : "var(--text-primary)" }}>
                          {fmtUsd(r.total_landed_cost_usd)}
                        </div>
                        {!isCheapest && saving < 0 && (
                          <div style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "var(--accent-red)", marginTop: 2 }}>
                            +{fmtUsd(Math.abs(saving))} vs cheapest
                          </div>
                        )}
                        {isCheapest && (
                          <span className="tag tag-green" style={{ marginTop: 4, display: "inline-block" }}>Best route</span>
                        )}
                      </div>
                    </div>

                    {/* Cost bar */}
                    <div style={{ height: 4, background: "var(--bg-hover)", borderRadius: 2, marginBottom: 12, overflow: "hidden" }}>
                      <div style={{ width: `${barWidth}%`, height: "100%", borderRadius: 2,
                        background: isCheapest ? "var(--accent-teal)" : "var(--accent-blue)",
                        transition: "width 0.5s ease" }} />
                    </div>

                    {/* Breakdown row */}
                    <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
                      {[
                        ["Freight",  r.breakdown?.freight_cost_usd,      "var(--accent-amber)"],
                        ["Duty",     r.breakdown?.import_duty_usd,        "var(--accent-red)"],
                        ["Insurance",r.breakdown?.insurance_usd,          "var(--accent-purple)"],
                        ["THC",      r.breakdown?.port_handling_usd,      "var(--accent-teal)"],
                        ["Brokerage",r.breakdown?.customs_brokerage_usd,  "#64748b"],
                      ].map(([label, val, color]) => (
                        <div key={label} style={{ display: "flex", alignItems: "center", gap: 5 }}>
                          <span style={{ width: 7, height: 7, borderRadius: 2, background: color, flexShrink: 0 }} />
                          <span style={{ fontSize: 11, color: "var(--text-tertiary)" }}>{label}:</span>
                          <span style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "var(--text-secondary)" }}>
                            {fmtUsd(val)}
                          </span>
                        </div>
                      ))}
                      {r.tariff_info?.type === "preferential" && (
                        <span className="tag tag-green" style={{ fontSize: 10 }}>FTA {r.tariff_info.fta_name}</span>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          ) : (
            <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: 300,
              color: "var(--text-tertiary)", flexDirection: "column", gap: 12 }}>
              <div style={{ fontSize: 40, opacity: 0.2 }}>⇄</div>
              <div style={{ fontFamily: "var(--font-mono)", fontSize: 12 }}>Select ports and compare routes</div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

// ════════════════════════════════════════════════════════════
//  CARBON COST VIEW
// ════════════════════════════════════════════════════════════
function CarbonView() {
  const [form, setForm] = useState({
    origin_port_code: "SHA",
    dest_port_code:   "RTM",
    hs_code:          "",
    cargo_weight_kg:  24000,
    fob_value_usd:    "",
  });
  const [result, setResult]   = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError]     = useState(null);

  const set = (k, v) => setForm(f => ({ ...f, [k]: v }));

  const calculate = async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await api.landedCost({
        ...form,
        cargo_weight_kg: Number(form.cargo_weight_kg),
        fob_value_usd:   Number(form.fob_value_usd),
      });
      setResult(res);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  const carbon = result?.carbon;

  // Comparisons for context
  const CONTEXT = [
    { label: "EU passenger car (1 year)", co2: 2.1 },
    { label: "London–NYC flight (economy)", co2: 0.5 },
    { label: "Average EU household (1 year)", co2: 8.0 },
    { label: "1 tonne of beef produced", co2: 60.0 },
  ];

  return (
    <div className="fade-in" style={{ padding: "1.5rem", overflowY: "auto", flex: 1 }}>
      <div style={{ marginBottom: "1.5rem" }}>
        <h1 style={{ fontFamily: "var(--font-display)", fontSize: 22, fontWeight: 700, letterSpacing: "-0.02em" }}>
          Carbon Cost Estimator
        </h1>
        <p style={{ color: "var(--text-secondary)", fontSize: 13, marginTop: 4 }}>
          CO₂e emissions and EU ETS carbon cost per shipment — IMO MEPC.1/Circ.684 methodology
        </p>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "320px 1fr", gap: 16 }}>
        {/* Form */}
        <div className="card" style={{ height: "fit-content" }}>
          <div className="label" style={{ marginBottom: 14 }}>Shipment Details</div>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, marginBottom: 10 }}>
            <div>
              <div style={{ fontSize: 12, color: "var(--text-secondary)", marginBottom: 4 }}>Origin</div>
              <select className="inp" value={form.origin_port_code} onChange={e => set("origin_port_code", e.target.value)}>
                {PORTS.map(p => <option key={p} value={p}>{p}</option>)}
              </select>
            </div>
            <div>
              <div style={{ fontSize: 12, color: "var(--text-secondary)", marginBottom: 4 }}>Destination</div>
              <select className="inp" value={form.dest_port_code} onChange={e => set("dest_port_code", e.target.value)}>
                {PORTS.map(p => <option key={p} value={p}>{p}</option>)}
              </select>
            </div>
          </div>
          <div style={{ marginBottom: 10, position: "relative" }}>
            <div style={{ fontSize: 12, color: "var(--text-secondary)", marginBottom: 4 }}>HS Code</div>
            <input
              className="inp"
              value={form.hs_code}
              onChange={e => set("hs_code", e.target.value)}
              onFocus={() => set("hs_open", true)}
              onBlur={() => setTimeout(() => set("hs_open", false), 150)}
              placeholder="e.g. 720839 or click to browse"
            />
            {form.hs_open && (
              <div style={{
                position: "absolute", top: "100%", left: 0, right: 0, zIndex: 100,
                background: "var(--bg-raised)", border: "1px solid var(--border-strong)",
                borderRadius: "var(--radius-sm)", marginTop: 2, maxHeight: 220,
                overflowY: "auto", boxShadow: "0 8px 24px rgba(0,0,0,0.4)",
              }}>
                {HS_QUICK_LIST
                  .filter(h => !form.hs_code || h.code.startsWith(form.hs_code) || h.label.toLowerCase().includes(form.hs_code.toLowerCase()))
                  .map(h => (
                    <div key={h.code} onMouseDown={() => set("hs_code", h.code)} style={{
                      padding: "8px 12px", cursor: "pointer", display: "flex",
                      justifyContent: "space-between", alignItems: "center",
                      borderBottom: "1px solid var(--border)",
                      transition: "background 0.1s",
                    }}
                    onMouseEnter={e => e.currentTarget.style.background = "var(--bg-hover)"}
                    onMouseLeave={e => e.currentTarget.style.background = "transparent"}
                    >
                      <span style={{ fontFamily: "var(--font-mono)", fontSize: 12, color: "var(--accent-teal)" }}>{h.code}</span>
                      <span style={{ fontSize: 12, color: "var(--text-secondary)" }}>{h.label}</span>
                    </div>
                  ))}
              </div>
            )}
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, marginBottom: 14 }}>
            <div>
              <div style={{ fontSize: 12, color: "var(--text-secondary)", marginBottom: 4 }}>Weight (kg)</div>
              <input className="inp" type="number" value={form.cargo_weight_kg} onChange={e => set("cargo_weight_kg", e.target.value)} />
            </div>
            <div>
              <div style={{ fontSize: 12, color: "var(--text-secondary)", marginBottom: 4 }}>FOB (USD)</div>
              <input className="inp" type="number" value={form.fob_value_usd} onChange={e => set("fob_value_usd", e.target.value)} placeholder="Leave blank to estimate"/>
            </div>
          </div>
          <button className="btn primary" style={{ width: "100%" }} onClick={calculate} disabled={loading}>
            {loading ? <><span className="spinner" style={{ width: 14, height: 14 }} /> Calculating…</> : "Calculate Carbon Cost"}
          </button>
          {error && (
            <div style={{ marginTop: 10, fontSize: 12, color: "var(--accent-red)" }}>{error}</div>
          )}

          {/* Methodology note */}
          <div style={{ marginTop: 14, padding: "10px 12px", background: "var(--bg-raised)", borderRadius: "var(--radius-sm)",
            fontSize: 11, color: "var(--text-tertiary)", lineHeight: 1.6 }}>
            <div style={{ fontFamily: "var(--font-mono)", color: "var(--text-secondary)", marginBottom: 4 }}>METHODOLOGY</div>
            IMO MEPC.1/Circ.684 EEOI<br/>
            0.03t HFO/nm/TEU × 3.114 CO₂/t HFO<br/>
            EU ETS price: €{carbon?.ets_price_eur_per_tonne || 65}/t CO₂<br/>
            Source: {carbon?.ets_price_source === "live" ? "Live macro data" : "2024 average fallback"}
          </div>
        </div>

        {/* Results */}
        {carbon ? (
          <div className="fade-in">
            {/* Main metrics */}
            <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 10, marginBottom: 12 }}>
              <div className="card" style={{ borderTop: "2px solid var(--accent-teal)" }}>
                <div className="label" style={{ marginBottom: 6 }}>Total CO₂e</div>
                <div style={{ fontFamily: "var(--font-display)", fontSize: 28, fontWeight: 800, color: "var(--accent-teal)" }}>
                  {fmt(carbon.co2_tonnes, 1)}t
                </div>
                <div style={{ fontSize: 12, color: "var(--text-secondary)", marginTop: 4 }}>
                  {fmt(carbon.co2_per_teu_tonnes, 2)}t per TEU
                </div>
              </div>
              <div className="card" style={{ borderTop: "2px solid var(--accent-amber)" }}>
                <div className="label" style={{ marginBottom: 6 }}>Carbon Cost (EUR)</div>
                <div style={{ fontFamily: "var(--font-display)", fontSize: 28, fontWeight: 800, color: "var(--accent-amber)" }}>
                  €{fmt(carbon.carbon_cost_eur)}
                </div>
                <div style={{ fontSize: 12, color: "var(--text-secondary)", marginTop: 4 }}>
                  at €{carbon.ets_price_eur_per_tonne}/t EU ETS
                </div>
              </div>
              <div className="card" style={{ borderTop: "2px solid var(--accent-blue)" }}>
                <div className="label" style={{ marginBottom: 6 }}>Carbon Cost (USD)</div>
                <div style={{ fontFamily: "var(--font-display)", fontSize: 28, fontWeight: 800, color: "var(--accent-blue)" }}>
                  {fmtUsd(carbon.carbon_cost_usd)}
                </div>
                <div style={{ fontSize: 12, color: "var(--text-secondary)", marginTop: 4 }}>
                  {fmt(carbon.carbon_pct_of_freight, 1)}% of freight cost
                </div>
              </div>
            </div>

            {/* Fuel consumption */}
            <div className="card" style={{ marginBottom: 12 }}>
              <div className="label" style={{ marginBottom: 12 }}>Fuel & Emissions Breakdown</div>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 12 }}>
                {[
                  ["Distance", `${fmt(result.route_info.distance_nm)} nm`, "var(--text-primary)"],
                  ["Fuel consumed", `${fmt(carbon.fuel_consumed_tonnes, 1)} t HFO`, "var(--accent-amber)"],
                  ["CO₂ emitted", `${fmt(carbon.co2_tonnes, 1)} t`, "var(--accent-red)"],
                  ["Intensity", `${fmt(carbon.co2_per_teu_tonnes * 1000 / result.route_info.distance_nm * 1000, 2)} gCO₂/TEU·nm`, "var(--text-secondary)"],
                ].map(([label, val, color]) => (
                  <div key={label} style={{ padding: "10px 12px", background: "var(--bg-raised)", borderRadius: "var(--radius-sm)" }}>
                    <div style={{ fontSize: 11, color: "var(--text-tertiary)", marginBottom: 4 }}>{label}</div>
                    <div style={{ fontFamily: "var(--font-mono)", fontSize: 14, fontWeight: 500, color }}>{val}</div>
                  </div>
                ))}
              </div>
            </div>

            {/* Context comparisons */}
            <div className="card">
              <div className="label" style={{ marginBottom: 12 }}>Emissions in Context</div>
              {CONTEXT.map(ctx => {
                const maxMultiple = Math.max(...CONTEXT.map(c => carbon.co2_tonnes / c.co2));
                const multiple = carbon.co2_tonnes / ctx.co2;
                const barPct = Math.min((multiple / maxMultiple) * 100, 100);
                return (
                  <div key={ctx.label} style={{ marginBottom: 14 }}>
                    <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
                      <span style={{ fontSize: 12, color: "var(--text-secondary)" }}>{ctx.label}</span>
                      <span style={{ fontFamily: "var(--font-mono)", fontSize: 12, color: "var(--text-primary)" }}>
                        {fmt(multiple, 1)}× equivalent
                      </span>
                    </div>
                    <div style={{ height: 4, background: "var(--bg-hover)", borderRadius: 2, overflow: "hidden" }}>
                      <div style={{ width: `${barPct}%`, height: "100%", background: "var(--accent-teal)", borderRadius: 2 }} />
                    </div>
                    <div style={{ fontSize: 11, color: "var(--text-tertiary)", marginTop: 3 }}>
                      This shipment = {fmt(multiple, 1)} {ctx.label.toLowerCase()}s ({ctx.co2}t CO₂)
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        ) : (
          <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: 300,
            color: "var(--text-tertiary)", flexDirection: "column", gap: 12 }}>
            <div style={{ fontSize: 40, opacity: 0.2 }}>◌</div>
            <div style={{ fontFamily: "var(--font-mono)", fontSize: 12 }}>Calculate a shipment to see carbon impact</div>
          </div>
        )}
      </div>
    </div>
  );
}

// ════════════════════════════════════════════════════════════
//  ROOT APP
// ════════════════════════════════════════════════════════════
export default function App() {
  const [view, setView]         = useState("dashboard");
  const [alertCount, setAlertCount] = useState(0);
  const [apiOk, setApiOk]       = useState(null);

  useEffect(() => {
    api.health()
      .then(() => setApiOk(true))
      .catch(() => setApiOk(false));
    api.alerts()
      .then(a => setAlertCount(a.summary?.total || 0))
      .catch(() => {});
    
  }, []);

  if (apiOk === false) return (
    <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "100vh", flexDirection: "column", gap: 16 }}>
      <div style={{ fontFamily: "var(--font-display)", fontSize: 20, fontWeight: 700 }}>Cannot connect to API</div>
      <div style={{ fontFamily: "var(--font-mono)", fontSize: 13, color: "var(--text-secondary)" }}>
        Make sure the API server is running:
      </div>
      <div style={{ fontFamily: "var(--font-mono)", fontSize: 13, color: "var(--accent-teal)", background: "var(--bg-surface)", padding: "8px 16px", borderRadius: "var(--radius-sm)", border: "1px solid var(--border)" }}>
        cd api && node server.js
      </div>
    </div>
  );

  if (apiOk === null) return (
    <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "100vh" }}>
      <div className="spinner" style={{ width: 32, height: 32 }} />
    </div>
  );

  const views = { dashboard: DashboardView, calculator: CalculatorView, compare: RouteCompareView, carbon: CarbonView, commodities: CommoditiesView, ports: PortsView, alerts: AlertsView };
  const View  = views[view] || DashboardView;

  return (
    <div style={{ display: "flex", height: "100vh", overflow: "hidden" }}>
      <Sidebar active={view} onChange={setView} alertCount={alertCount} />
      <main style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
        <View />
      </main>
    </div>
  );
}