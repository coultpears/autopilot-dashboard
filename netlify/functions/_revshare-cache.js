// Shared loader for the repo-committed revshare cache (data/revshare.json).
// Loaded once at module init — survives across warm Lambda invocations.
//
// Data source: built by scripts/build-revshare-bundle.js from the
// subsidy-session pulls (signoff_pulls.json + revshare_FULL_pull.json).
// Per-property × per-month: Landing margin, partner net allocation, gross
// rent, occupancy, and stay count over the 11 months that have been pulled.

const fs = require('fs');
const path = require('path');

let _cache = null;

function loadJsonNear(filename) {
  // Same layered lookup pattern as _geocode-cache.js — local fs first
  // (so dev edits don't need a restart), bundled require() fallback for
  // prod Netlify where the function is esbuilt.
  const candidates = [
    path.resolve(__dirname, '..', '..', 'data', filename),
    path.resolve(process.cwd(), 'data', filename),
    path.resolve(__dirname, 'data', filename),
  ];
  for (const p of candidates) {
    try { return JSON.parse(fs.readFileSync(p, 'utf8')); }
    catch (e) { /* try next */ }
  }
  try { return require('../../data/revshare.json'); } catch (e) { /* none */ }
  return null;
}

function load() {
  if (_cache) return _cache;
  const data = loadJsonNear('revshare.json') || { _meta: {}, by_property: {} };
  _cache = data;
  const propCount = Object.keys(data.by_property || {}).length;
  const monthCount = (data._meta?.source_months || []).length;
  console.log(`[revshare-cache] loaded ${propCount} properties × ${monthCount} months`);
  return _cache;
}

// Returns an ordered array of monthly slots (oldest → newest) for the given
// property name. Each slot has shape:
//   { period_key, period, l, p, g, o, u }
// All numeric fields may be null when a month has no data for this property.
// Returns null when the property is not in the bundle at all.
function getTrend(propertyName) {
  if (!propertyName) return null;
  const c = load();
  const monthsMeta = c._meta?.source_months || [];
  const propData = c.by_property?.[propertyName];
  if (!propData) return null;
  const out = [];
  for (const m of monthsMeta) {
    const slot = propData[String(m.period_key)];
    out.push({
      period_key: m.period_key,
      period: m.period,
      l: slot?.l ?? null,
      p: slot?.p ?? null,
      g: slot?.g ?? null,
      o: slot?.o ?? null,
      u: slot?.u ?? null,
      // Fee breakdown — pass through if present in the bundle. Older bundles
      // (built before the fee fields were captured) won't have these keys.
      mf: slot?.mf ?? null,
      ff: slot?.ff ?? null,
      if_: slot?.if_ ?? null,
      wf: slot?.wf ?? null,
      pa: slot?.pa ?? null,
    });
  }
  return out;
}

// Compute the contracted management-fee rate for a property — this is the
// REAL rev share % per the partnership agreement (e.g. 30% for 2121, 25% for
// 281 Willow). Returns the rate as a 0-100 percentage and the months it was
// derived from.
//
// Math: median of (|mgmt_fee| / total_revenue) across months that have both
// fields populated and non-zero. We use median (not mean) so a single outlier
// month doesn't skew the rate; mgmt fee % is typically a flat contract rate
// that doesn't move month-to-month.
function getMgmtFeeRate(propertyName) {
  const trend = getTrend(propertyName);
  if (!trend) return null;
  const rates = [];
  for (const m of trend) {
    if (m.mf == null || m.g == null || m.g <= 0) continue;
    const r = Math.abs(m.mf) / m.g;
    if (r > 0 && r < 1) rates.push(r);   // 0 < rate < 100% sanity bound
  }
  if (!rates.length) return null;
  rates.sort((a, b) => a - b);
  const median = rates[Math.floor(rates.length / 2)];
  return {
    rate_pct: Math.round(median * 1000) / 10,   // one decimal
    months_observed: rates.length,
    min_pct: Math.round(rates[0] * 1000) / 10,
    max_pct: Math.round(rates[rates.length - 1] * 1000) / 10,
  };
}

// Coverage helper for diagnostics — returns the bundle's source-month list
// so callers can know what window the trend covers without re-reading it.
function getCoverage() {
  const c = load();
  return c._meta?.source_months || [];
}

module.exports = { getTrend, getCoverage, getMgmtFeeRate };
