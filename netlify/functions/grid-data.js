// Netlify serverless function: Grid view data for AP dashboard
// Optimized: cached Looker auth + 3 parallel queries, aggressive CDN caching

const LOOKER_BASE = 'https://landing.cloud.looker.com';

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
  const mgmt = (out.property_management_company || '').trim();
  const dp = (out.dp_full_name || '').trim();
  out.pmc_name = mgmt || null;
  out.dp_name = dp || null;
  // partner_name kept for backward compat: PMC preferred, DP fallback
  out.partner_name = mgmt || dp || null;
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
      groups[key] = { ...r, _count: 1 };
    } else {
      const g = groups[key];
      g._count++;
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
    const token = await getLookerToken();
    const date = event.queryStringParameters?.date || 'today';

    // 4 Looker queries in parallel — core + financials split to halve the critical path
    const installedFilter = { 'tbldailyhomemetrics.date_date': date, 'tbldailyhomemetrics.active_property_count': '>0', 'tbldailyhomemetrics.home_is_installed': 'Yes' };
    const [coreData, finData, resData, deinstallData] = await Promise.all([
      // Core: identity + counts (fast, ~2s)
      lookerQuery(token, 'tbldailyhomemetrics', [
        'dimproperty.property_name', 'dimproperty.property_management_company',
        'dimdirectpartner.dp_full_name', 'dimmarket.market_name',
        'tbldailyhomemetrics.home_count', 'tbldailyhomemetrics.home_occupied_count',
        'tbldailyhomemetrics.active_property_count',
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
      return {
        ...r,
        current_reservation_count: res.current_reservation_count || null,
        future_reservation_count: res.future_reservation_count || null,
        count: res.count || null,
        deinstall_count: di,
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
