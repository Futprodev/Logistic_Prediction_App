/**
 * services/alerts.js
 * Reads active alerts from the database and formats them for the UI.
 */

const SEVERITY_ORDER = { ALERT: 0, WARNING: 1, INFO: 2 };

function getAlerts(db, { limit = 50, unacknowledged_only = false, type = null } = {}) {
  let query = `
    SELECT alert_id, alert_type, severity, entity_id, message,
           detail_json, triggered_at, acknowledged
    FROM pred_rate_alerts
    WHERE 1=1
  `;
  const params = [];

  if (unacknowledged_only) {
    query += ` AND acknowledged = 0`;
  }
  if (type) {
    query += ` AND alert_type = ?`;
    params.push(type);
  }

  query += ` ORDER BY
    CASE severity WHEN 'ALERT' THEN 0 WHEN 'WARNING' THEN 1 ELSE 2 END,
    triggered_at DESC
    LIMIT ?`;
  params.push(limit);

  const rows = db.prepare(query).all(...params);

  return rows.map(row => ({
    ...row,
    detail: row.detail_json ? JSON.parse(row.detail_json) : null,
    detail_json: undefined,
  }));
}

function acknowledgeAlert(db, alertId) {
  const result = db.prepare(`
    UPDATE pred_rate_alerts
    SET acknowledged = 1, acknowledged_at = datetime('now')
    WHERE alert_id = ?
  `).run(alertId);
  return result.changes > 0;
}

function getAlertSummary(db) {
  const counts = db.prepare(`
    SELECT alert_type, severity, COUNT(*) as count
    FROM pred_rate_alerts
    WHERE acknowledged = 0
    GROUP BY alert_type, severity
    ORDER BY CASE severity WHEN 'ALERT' THEN 0 WHEN 'WARNING' THEN 1 ELSE 2 END
  `).all();

  const total = counts.reduce((sum, r) => sum + r.count, 0);
  const critical = counts.filter(r => r.severity === "ALERT").reduce((s, r) => s + r.count, 0);
  const warnings = counts.filter(r => r.severity === "WARNING").reduce((s, r) => s + r.count, 0);

  return { total, critical, warnings, by_type: counts };
}

module.exports = { getAlerts, acknowledgeAlert, getAlertSummary };