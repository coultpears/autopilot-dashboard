// Netlify serverless function: Grid view data for AP dashboard
// Optimized: cached Looker auth + 3 parallel queries, aggressive CDN caching

const LOOKER_BASE = 'https://landing.cloud.looker.com';
const geocodeCache = require('./_geocode-cache.js');
const partnerAliases = require('./_partner-aliases.js');
const revshareCache = require('./_revshare-cache.js');

// Module-level token cache — survives across warm Lambda invocations (saves ~300ms)
let _cachedToken = null;
let _tokenExpiry = 0;

async function getLookerToken() {
  if (_cachedToken && Date.now() < _tokenExpiry) return _cachedToken;
  const resp = await fetch(`${LOOKER_BASE}/api/4.0/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: `client_id=${process.env.LANDING_CLIENT_ID}&client_secret=${process.env.LANDING_CLIENT_SECRET}`,
  });
  if (!resp.ok) throw new Error(`Looker auth failed: ${resp.status}`);
  const data = await resp.json();
  _cachedToken = data.access_token;
  _tokenExpiry = Date.now() + (data.expires_in ? (data.expires_in - 60) * 1000 : 3000000); // expire 1 min early
  return _cachedToken;
}

async function lookerQuery(token, view, fields, filters, sorts, limit = 5000) {
  const body = { model: 'landing', view, fields, limit: String(limit) };
  if (filters) body.filters = filters;
  if (sorts) body.sorts = sorts;
  const resp = await fetch(`${LOOKER_BASE}/api/4.0/queries/run/json`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!resp.ok) throw new Error(`Looker query failed (${view}): ${resp.status}`);
  return resp.json();
}

function cleanRow(row) {
  const out = {};
  for (const [k, v] of Object.entries(row)) out[k.includes('.') ? k.split('.').pop() : k] = v;
  // str_status normalized (kept for aggregation; rolled up to property below).
  // 'enabled' → STR active; everything else (not wanted by property, not
  // allowed, in progress, remote market, null) → not STR.
  out._unit_str_enabled = (out.str_status === 'enabled');
  delete out.str_status;
  const mgmt = (out.property_management_company || '').trim();
  const dp = (out.dp_full_name || '').trim();
  out.pmc_name = mgmt || null;
  out.dp_name = dp || null;
  // partner_name = capital owner (DP) preferred, operator (PMC) fallback.
  // Flipped 2026-05-08: reps think of "the partner" as the entity that signs
  // contracts and gets paid (Carlton Companies, Cardone Capital, Mast Capital,
  // etc.) — that's dimdirectpartner.dp_full_name. PMC is the operator (e.g.
  // American Landmark, hired by Carlton). The DP-first ordering aligns the
  // dashboard's primary "partner" column with the rep mental model and with
  // what HubSpot deal company_name is tagged. Earlier "PMC preferred" was a
  // legacy default that surfaced operators as partners (gap discovered on
  // The Heyward — pmc=American Landmark, dp=Carlton Companies).
  out.partner_name = dp || mgmt || null;
  // partner_canonical: resolves typo'd / split-named partners to a single identity
  // (e.g., "Stoltz Management Corporation" + "Stolz Properties" -> "Stoltz")
  // via the data/partner-aliases.json map. Falls back to PMC then DP.
  const resolved = partnerAliases.resolvePartner(mgmt, dp);
  out.partner_canonical = resolved.canonical;
  // partner_variants: every known spelling of this partner. Injected into the
  // client-side search hay so any spelling the user types (typo or not) finds
  // every property under this partner.
  const variants = partnerAliases.variantsFor(resolved.canonical);
  out.partner_variants = variants.length ? variants : null;
  delete out.property_management_company;
  delete out.dp_full_name;
  return out;
}

// Aggregate to one row per property_name
function aggregate(rows) {
  const groups = {};
  for (const r of rows) {
    const key = r.property_name || '';
    if (!groups[key]) {
      groups[key] = { ...r, _count: 1, _str_units: r._unit_str_enabled ? 1 : 0 };
    } else {
      const g = groups[key];
      g._count++;
      if (r._unit_str_enabled) g._str_units++;
      for (const f of ['home_count', 'home_occupied_count', 'active_property_count', 'home_daily_rent_revenue', 'home_daily_rent_cost_autopilot']) {
        if (r[f] != null) g[f] = (g[f] || 0) + r[f];
      }
      for (const f of ['all_in_revpah_net', 'markup', 'home_average_rent_cost']) {
        if (r[f] != null) {
          g[`_sum_${f}`] = (g[`_sum_${f}`] || g[f] || 0) + r[f];
          g[`_cnt_${f}`] = (g[`_cnt_${f}`] || 1) + 1;
        }
      }
    }
  }
  const results = [];
  for (const g of Object.values(groups)) {
    for (const f of ['all_in_revpah_net', 'markup', 'home_average_rent_cost']) {
      if (g[`_sum_${f}`] != null) g[f] = g[`_sum_${f}`] / g[`_cnt_${f}`];
      delete g[`_sum_${f}`]; delete g[`_cnt_${f}`];
    }
    if (g.home_count > 0) g.occupancy = g.home_occupied_count / g.home_count;
    // Property-level STR rollup: any unit with str_status='enabled' marks the
    // whole property as STR-enabled. Also expose the count for tooltips.
    g.str_property_eligible = (g._str_units > 0);
    g.str_unit_count = g._str_units;
    delete g._unit_str_enabled;
    delete g._str_units;
    delete g._count;
    results.push(g);
  }
  return results;
}

function computeStatus(r) {
  const occ = r.occupancy, units = r.home_count || 0, occupied = r.home_occupied_count || 0;
  const active = r.active_property_count || 0, futRes = r.future_reservation_count || 0;
  if (active < 1) return { status_label: 'Inactive', status_color: 'neutral' };
  if (units > 0 && occupied === 0 && futRes > 0) return { status_label: 'Pre-Launch', status_color: 'blue' };
  if (units > 0 && occupied === 0) return { status_label: 'Vacant', status_color: 'red' };
  if (occ != null && occ >= 0.85) return { status_label: 'Strong', status_color: 'green' };
  if (occ != null && occ >= 0.6) return { status_label: 'Moderate', status_color: 'amber' };
  if (occ != null && occ > 0) return { status_label: 'Low', status_color: 'red' };
  if (occ === 0) return futRes > 0 ? { status_label: 'Pre-Launch', status_color: 'blue' } : { status_label: 'Vacant', status_color: 'red' };
  return { status_label: 'Active', status_color: 'green' };
}

exports.handler = async (event) => {
  if (!process.env.LANDING_CLIENT_ID || !process.env.LANDING_CLIENT_SECRET) {
    return { statusCode: 500, body: JSON.stringify({ error: 'Credentials not configured' }) };
  }

  try {
    // Warm the revshare cache (Postgres-backed when NEON_DATABASE_URL is set,
    // falls back to data/revshare.json bundle otherwise). Parallel with the
    // Looker token fetch — both kick off independently.
    const [token, _] = await Promise.all([
      getLookerToken(),
      revshareCache.init(),
    ]);
    const date = event.queryStringParameters?.date || 'today';

    // 4 Looker queries in parallel — core + financials split to halve the critical path
    const installedFilter = { 'tbldailyhomemetrics.date_date': date, 'tbldailyhomemetrics.active_property_count': '>0', 'tbldailyhomemetrics.home_is_installed': 'Yes' };
    const [coreData, finData, resData, deinstallData] = await Promise.all([
      // Core: identity + counts (fast, ~2s)
      // STR signal: dimhome.str_status — per-unit string, values:
      //   "enabled" (STR active), "not wanted by property", "not allowed"
      //   (regulatory), "in progress", "remote market", null
      // Property-level STR = any unit on the property has str_status='enabled'.
      // We aggregate that rollup in cleanRow()/aggregate(). This captures both
      // Standard and Autopilot properties (unlike dimproperty.eligible_for_str
      // which only applies to Standard).
      lookerQuery(token, 'tbldailyhomemetrics', [
        'dimproperty.property_name', 'dimproperty.property_management_company',
        'dimdirectpartner.dp_full_name', 'dimmarket.market_name',
        'tbldailyhomemetrics.home_count', 'tbldailyhomemetrics.home_occupied_count',
        'tbldailyhomemetrics.active_property_count',
        'dimhome.str_status',
      ], installedFilter, ['dimproperty.property_name'], 5000),

      // Financials: rent/revenue/markup (slower fields isolated, ~2s)
      lookerQuery(token, 'tbldailyhomemetrics', [
        'dimproperty.property_name',
        'dimhome.home_average_rent_cost',
        'tbldailyhomemetrics.home_daily_rent_revenue', 'tbldailyhomemetrics.home_daily_rent_cost_autopilot',
        'tbldailyhomemetrics.all_in_revpah_net', 'tbldailyhomemetrics.markup',
      ], installedFilter, ['dimproperty.property_name'], 5000),

      lookerQuery(token, 'dimreservation', [
        'dimproperty.property_name',
        'dimreservation.current_reservation_count', 'dimreservation.future_reservation_count', 'dimreservation.count',
      ], { 'dimreservation.current_reservation_count': '>0' },
      ['dimproperty.property_name'], 5000),

      // Deinstalled units specifically (for DI badge)
      lookerQuery(token, 'tbldailyhomemetrics', [
        'dimproperty.property_name', 'tbldailyhomemetrics.home_count',
      ], { 'tbldailyhomemetrics.date_date': date, 'tbldailyhomemetrics.active_property_count': '>0', 'dimhome.deinstall_date': 'NOT NULL' },
      ['dimproperty.property_name'], 5000),
    ]);

    // Merge financial fields into core rows by property_name before aggregation
    const finLookup = {};
    for (const raw of finData) {
      const name = raw['dimproperty.property_name'];
      if (name) finLookup[name] = raw;
    }
    const mergedOcc = coreData.map(row => {
      const name = row['dimproperty.property_name'];
      const fin = finLookup[name] || {};
      return { ...row, ...fin };
    });

    // Aggregate occupancy
    const occAgg = aggregate(mergedOcc.map(cleanRow));

    // Reservation lookup
    const resLookup = {};
    for (const raw of resData) {
      const name = raw['dimproperty.property_name'];
      if (!name) continue;
      if (!resLookup[name]) resLookup[name] = { current_reservation_count: 0, future_reservation_count: 0, count: 0 };
      resLookup[name].current_reservation_count += raw['dimreservation.current_reservation_count'] || 0;
      resLookup[name].future_reservation_count += raw['dimreservation.future_reservation_count'] || 0;
      resLookup[name].count += raw['dimreservation.count'] || 0;
    }

    // Deinstall lookup (for DI badge display only — unit counts already correct from installed-only query)
    const deinstallLookup = {};
    for (const raw of deinstallData) {
      const name = raw['dimproperty.property_name'];
      if (name) deinstallLookup[name] = (deinstallLookup[name] || 0) + (raw['tbldailyhomemetrics.home_count'] || 1);
    }

    // Merge — home_count already reflects installed-only from the Looker query
    const records = occAgg.map(r => {
      const res = resLookup[r.property_name] || {};
      const di = deinstallLookup[r.property_name] || 0;

      // Recompute occupancy from installed counts
      r.occupancy = r.home_count > 0 ? r.home_occupied_count / r.home_count : 0;

      const status = computeStatus({ ...r, ...res });
      // Attach geocoded lat/lng (null when address never geocoded — client falls back to jitter)
      const geo = geocodeCache.lookupByPropertyName(r.property_name);

      // Per-property monthly trend from revshare bundle:
      //   - trend_l[]  : 11 monthly Landing net values (oldest → newest, nulls for missing)
      //   - last_landing / last_partner / last_gross : most-recent month's totals
      //   - projection_next3 : forward 3mo Landing net (multi-tier confidence)
      //   - projection_method : 'trend' | 'short-trend' | 'runrate-bookings'
      //   - recovery_pct : last (l+p) / monthly_base where base = units × avg_rent
      let trend_l = null, last_landing = null, last_partner = null, last_gross = null;
      let projection_next3 = null, projection_conf = null, projection_method = null;
      let recovery_pct = null;
      // Contracted mgmt fee rate — the real "rev share %" per the partnership
      // agreement (median of |mgmt_fee|/gross across reporting months). The
      // mgmt fee is the contractual Landing take; FFE/install/wifi fees are
      // setup reimbursements, not part of the rev share.
      const mgmtFeeInfo = revshareCache.getMgmtFeeRate(r.property_name);
      const contracted_mgmt_pct = mgmtFeeInfo ? mgmtFeeInfo.rate_pct : null;
      const contracted_mgmt_months = mgmtFeeInfo ? mgmtFeeInfo.months_observed : null;
      const contracted_mgmt_min = mgmtFeeInfo ? mgmtFeeInfo.min_pct : null;
      const contracted_mgmt_max = mgmtFeeInfo ? mgmtFeeInfo.max_pct : null;
      const trend = revshareCache.getTrend(r.property_name);
      const validCount = trend ? trend.filter(t => t.l != null).length : 0;
      const monthlyBase = (r.home_count || 0) * (r.home_average_rent_cost || 0);
      if (trend && trend.length) {
        trend_l = trend.map(t => t.l);
        const last = trend[trend.length - 1];
        last_landing = last?.l ?? null;
        last_partner = last?.p ?? null;
        last_gross   = last?.g ?? null;
        // Recovery: last_total / contract_base (trend-derived path)
        const lastTotal = (last_landing || 0) + (last_partner || 0);
        if (monthlyBase > 0 && lastTotal > 0) {
          recovery_pct = Math.round((lastTotal / monthlyBase) * 100);
        }
      }
      // Fallback recovery: when no trend OR trend has zero recent revenue, use
      // today's daily run-rate × 30 vs contract base. This makes "at-risk"
      // visible for the 100 properties without revshare bundle coverage so
      // they don't silently disappear from the cohort.
      if (recovery_pct == null && monthlyBase > 0 && r.home_daily_rent_revenue) {
        const monthlyRunRate = r.home_daily_rent_revenue * 30;
        recovery_pct = Math.round((monthlyRunRate / monthlyBase) * 100);
      }

      // ── Projection: tiered model ──────────────────────────────────────
      // Tier 1 · trend (≥6 valid months) — avg(last 3 non-null months) × 3,
      //          high conf when ≥9 months, med when 6–8.
      // Tier 2 · short trend (3–5 months) — same average, low conf.
      // Tier 3 · single recent month — last × 3, low conf.
      // Tier 4 · run-rate from current bookings — daily_rent_revenue × 90 ×
      //          Landing's empirical take rate (LANDING_TAKE_RATE constant),
      //          adjusted up/down by future-vs-current reservation ratio.
      //          Low conf — derived from today's snapshot, not history.
      // Tier 5 · refuse — no trend AND no daily revenue signal.
      const LANDING_TAKE_RATE = 0.31;
      if (trend && validCount >= 6) {
        const last3 = trend.slice(-3).map(t => t.l).filter(v => v != null);
        const avg = last3.reduce((s, v) => s + v, 0) / last3.length;
        projection_next3 = Math.round(avg * 3);
        projection_conf = validCount >= 9 ? 'high' : 'med';
        projection_method = 'trend';
      } else if (trend && validCount >= 3) {
        const recent = trend.slice(-Math.min(validCount, 5)).map(t => t.l).filter(v => v != null);
        const avg = recent.reduce((s, v) => s + v, 0) / recent.length;
        projection_next3 = Math.round(avg * 3);
        projection_conf = 'low';
        projection_method = 'short-trend';
      } else if (trend && validCount >= 1) {
        // Just one or two months — extrapolate from most recent valid value
        const lastValid = [...trend].reverse().find(t => t.l != null);
        if (lastValid && lastValid.l > 0) {
          projection_next3 = Math.round(lastValid.l * 3);
          projection_conf = 'low';
          projection_method = 'short-trend';
        }
      }
      if (projection_next3 == null && r.home_daily_rent_revenue) {
        // Daily revenue × 90 days = next 3mo gross. Apply Landing's take rate.
        // Bump up to +15% if forward bookings exceed current, down to -15% if light.
        const currRes = res.current_reservation_count || 0;
        const futRes = res.future_reservation_count || 0;
        let bookingFactor = 1.0;
        if (currRes > 0) {
          const ratio = futRes / currRes;
          bookingFactor = ratio >= 1.5 ? 1.15 : ratio >= 1.0 ? 1.05 : ratio >= 0.5 ? 0.95 : 0.85;
        }
        const next3Gross = r.home_daily_rent_revenue * 90 * bookingFactor;
        projection_next3 = Math.round(next3Gross * LANDING_TAKE_RATE);
        projection_conf = 'low';
        projection_method = 'runrate-bookings';
      }

      return {
        ...r,
        current_reservation_count: res.current_reservation_count || null,
        future_reservation_count: res.future_reservation_count || null,
        count: res.count || null,
        deinstall_count: di,
        lat: geo?.lat ?? null,
        lng: geo?.lng ?? null,
        trend_l,
        last_landing,
        last_partner,
        last_gross,
        projection_next3,
        projection_conf,
        projection_method,
        recovery_pct,
        contracted_mgmt_pct,
        contracted_mgmt_months,
        contracted_mgmt_min,
        contracted_mgmt_max,
        ...status,
      };
    });

    return {
      statusCode: 200,
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'public, max-age=300, stale-while-revalidate=1800',
        'Netlify-CDN-Cache-Control': 'public, max-age=1800, stale-while-revalidate=7200',
      },
      body: JSON.stringify(records),
    };
  } catch (err) {
    console.error(err);
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};
