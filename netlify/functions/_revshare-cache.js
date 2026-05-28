// Shared revshare data loader. Backed by Neon Postgres when
// NEON_DATABASE_URL is set, falls back to the bundled JSON
// (data/revshare.json) when not — graceful dev mode + safety net.
//
// API:
//   await revshareCache.init();          // call once per handler invocation
//   revshareCache.getTrend(propertyName)  // sync after init()
//   revshareCache.getMgmtFeeRate(name)    // sync after init()
//   revshareCache.getCoverage()           // sync after init()
//
// Why init() is async but the rest is sync:
//   grid-data.js calls these helpers inside .map() loops where Promise.all
//   gymnastics would be invasive. By doing the one DB round-trip at handler
//   start, we hydrate an in-memory cache that survives across warm Lambda
//   invocations. The full dataset is ~1.5 MB (7,700 rows × ~200 bytes) so
//   loading it all into memory is fine — much smaller than the bundle.
//
// Module-level cache:
//   The _cachePromise pattern means the first invocation in a cold container
//   pays the DB round-trip (~150ms); subsequent invocations in the same warm
//   container resolve instantly. Across cold starts, fresh data is loaded.

const fs = require('fs');
const path = require('path');

let _cachePromise = null;

// ─── Property-name alias map ─────────────────────────────────────────
// Looker/portfolio property_name vs rent-roll property_name can diverge.
// data/revshare-aliases.json is a hand-curated { lookerName: revshareName }
// override, tried only after an exact name match misses. Safe by design —
// empty map = pure exact matching (current behavior).
function loadAliases() {
  const candidates = [
    path.resolve(__dirname, '..', '..', 'data', 'revshare-aliases.json'),
    path.resolve(process.cwd(), 'data', 'revshare-aliases.json'),
    path.resolve(__dirname, 'data', 'revshare-aliases.json'),
  ];
  for (const p of candidates) {
    try {
      const j = JSON.parse(fs.readFileSync(p, 'utf8'));
      const aliases = j.aliases || {};
      const n = Object.keys(aliases).length;
      if (n) console.log(`[revshare-cache] loaded ${n} property aliases from ${p}`);
      return aliases;
    } catch (e) { /* try next */ }
  }
  return {};
}

// ─── Bundle fallback (dev mode / DB not configured) ─────────────────
function loadBundle() {
  const candidates = [
    path.resolve(__dirname, '..', '..', 'data', 'revshare.json'),
    path.resolve(process.cwd(), 'data', 'revshare.json'),
    path.resolve(__dirname, 'data', 'revshare.json'),
  ];
  for (const p of candidates) {
    try {
      const data = JSON.parse(fs.readFileSync(p, 'utf8'));
      console.log(`[revshare-cache] loaded bundle from ${p}`);
      return data;
    } catch (e) { /* try next */ }
  }
  try {
    const data = require('../../data/revshare.json');
    console.log('[revshare-cache] loaded bundle via require()');
    return data;
  } catch (e) { /* none */ }
  console.warn('[revshare-cache] no bundle found — returning empty');
  return { _meta: {}, by_property: {} };
}

// ─── Postgres backend ────────────────────────────────────────────────
// Loads ALL rows into memory once. The dataset is tiny; sub-200ms even
// over a cold pooled connection. Builds the same shape as the bundle so
// the rest of the code is identical regardless of source.
async function loadFromPostgres() {
  const pg = require('pg');
  const client = new pg.Client({ connectionString: process.env.NEON_DATABASE_URL });
  await client.connect();
  try {
    // 1) All monthly_actuals rows
    const t0 = Date.now();
    const rows = await client.query(`
      SELECT
        property_name, period_key, period,
        landing_margin, net_allocation, total_revenue, occupancy_rate, stay_count,
        mgmt_fee, ffe_fee, install_fee, wifi_fee, partner_adjustment
      FROM monthly_actuals
      ORDER BY property_name, period_key
    `);
    // 2) Distinct months in the dataset (used for the _meta.source_months
    // shape consumers expect). Order by period_key so consumers can iterate
    // chronologically without re-sorting.
    const months = await client.query(`
      SELECT DISTINCT period_key, period
      FROM monthly_actuals
      ORDER BY period_key
    `);
    console.log(`[revshare-cache] loaded ${rows.rows.length} rows × ${months.rows.length} months from Postgres in ${Date.now() - t0}ms`);

    // Reshape into the same { _meta, by_property: { name: { period_key: slot } } } structure
    const by_property = {};
    for (const r of rows.rows) {
      if (!by_property[r.property_name]) by_property[r.property_name] = {};
      by_property[r.property_name][String(r.period_key)] = {
        period: r.period,
        l: r.landing_margin != null ? Number(r.landing_margin) : null,
        p: r.net_allocation != null ? Number(r.net_allocation) : null,
        g: r.total_revenue != null ? Number(r.total_revenue) : null,
        o: r.occupancy_rate != null ? Number(r.occupancy_rate) : null,
        u: r.stay_count,
        mf: r.mgmt_fee != null ? Number(r.mgmt_fee) : null,
        ff: r.ffe_fee != null ? Number(r.ffe_fee) : null,
        if_: r.install_fee != null ? Number(r.install_fee) : null,
        wf: r.wifi_fee != null ? Number(r.wifi_fee) : null,
        pa: r.partner_adjustment != null ? Number(r.partner_adjustment) : null,
      };
    }
    return {
      _meta: {
        source: 'postgres',
        loaded_at: new Date().toISOString(),
        source_months: months.rows.map(m => ({ period: m.period, period_key: m.period_key })),
      },
      by_property,
    };
  } finally {
    await client.end();
  }
}

// ─── Public API ──────────────────────────────────────────────────────
// Memoizes a Promise so concurrent first-invocation requests share the
// load. After resolution, the cached data is sync-accessible via the
// other exported helpers.
let _cache = null;
async function init() {
  if (_cache) return _cache;
  if (!_cachePromise) {
    _cachePromise = (async () => {
      if (process.env.NEON_DATABASE_URL) {
        try {
          return await loadFromPostgres();
        } catch (e) {
          console.error('[revshare-cache] Postgres load failed, falling back to bundle:', e.message);
          return loadBundle();
        }
      }
      return loadBundle();
    })();
  }
  _cache = await _cachePromise;
  // Attach the alias map once (tiny synchronous file read; cheap to repeat
  // but we memoize it on the cache object alongside the data).
  if (_cache && !_cache._aliases) _cache._aliases = loadAliases();
  return _cache;
}

function _requireCache() {
  if (!_cache) {
    throw new Error('revshare-cache: call await init() before getTrend/getMgmtFeeRate/getCoverage');
  }
  return _cache;
}

// Returns an ordered array of monthly slots (oldest → newest) for the given
// property name. Same shape as before the Postgres backend:
//   { period_key, period, l, p, g, o, u, mf, ff, if_, wf, pa }
// Numeric fields may be null. Returns null when the property isn't in the
// dataset.
function getTrend(propertyName) {
  if (!propertyName) return null;
  const c = _requireCache();
  const monthsMeta = c._meta?.source_months || [];
  let propData = c.by_property?.[propertyName];
  // Alias fallback — the rent-roll sheet spells this property differently.
  // Only consulted on an exact-match miss; data/revshare-aliases.json.
  if (!propData) {
    const alias = c._aliases?.[propertyName];
    if (alias) propData = c.by_property?.[alias];
  }
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
      mf: slot?.mf ?? null,
      ff: slot?.ff ?? null,
      if_: slot?.if_ ?? null,
      wf: slot?.wf ?? null,
      pa: slot?.pa ?? null,
    });
  }
  return out;
}

// Median of (|mgmt_fee| / total_revenue) across months that have both
// populated — the contracted Landing take rate per the partnership.
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
    rate_pct: Math.round(median * 1000) / 10,
    months_observed: rates.length,
    min_pct: Math.round(rates[0] * 1000) / 10,
    max_pct: Math.round(rates[rates.length - 1] * 1000) / 10,
  };
}

function getCoverage() {
  const c = _requireCache();
  return c._meta?.source_months || [];
}

// Portfolio-wide effective Landing take rate, derived from actuals:
//   Σ landing_margin / Σ total_revenue across every (property × month) row
// that has both populated. Returns a decimal (e.g., 0.389 = 38.9%).
//
// Used as the fallback projection ratio for properties without per-property
// trend data, and to anchor the KPI strip's "take %" so it reflects reality
// instead of a hardcoded constant. Computed once and memoized on the cache.
//
// Returns null if no rows have both fields (shouldn't happen in practice but
// callers should fall back to a sane default like 0.31 if this returns null).
function getPortfolioTakeRate() {
  const c = _requireCache();
  if (c._portfolio_take_rate !== undefined) return c._portfolio_take_rate;
  let totalLanding = 0, totalGross = 0;
  for (const propName in (c.by_property || {})) {
    const months = c.by_property[propName];
    for (const key in months) {
      const m = months[key];
      if (m.l != null && m.g != null && m.g > 0) {
        totalLanding += m.l;
        totalGross += m.g;
      }
    }
  }
  c._portfolio_take_rate = totalGross > 0 ? totalLanding / totalGross : null;
  return c._portfolio_take_rate;
}

// Every property_name present in the revshare dataset. Used by the
// reconciliation diagnostic (scripts/list-revshare-gaps.js).
function getAllProperties() {
  const c = _requireCache();
  return Object.keys(c.by_property || {});
}

module.exports = { init, getTrend, getCoverage, getMgmtFeeRate, getPortfolioTakeRate, getAllProperties };
