// Netlify serverless function: Grid view data for AP dashboard
// Fetches occupancy + reservation summary from Looker, aggregates to property level

const LOOKER_BASE = 'https://landing.cloud.looker.com';
const ADMIN_BASE = 'https://admin.hellolanding.com';

async function getLookerToken() {
  const resp = await fetch(`${LOOKER_BASE}/api/4.0/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: `client_id=${process.env.LANDING_CLIENT_ID}&client_secret=${process.env.LANDING_CLIENT_SECRET}`,
  });
  if (!resp.ok) throw new Error(`Looker auth failed: ${resp.status}`);
  const data = await resp.json();
  return data.access_token;
}

async function lookerQuery(token, view, fields, filters, sorts, limit = 50000) {
  const body = { model: 'landing', view, fields, limit: String(limit) };
  if (filters) body.filters = filters;
  if (sorts) body.sorts = sorts;
  const resp = await fetch(`${LOOKER_BASE}/api/4.0/queries/run/json`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!resp.ok) throw new Error(`Looker query failed: ${resp.status} ${await resp.text().catch(() => '')}`);
  return resp.json();
}

function cleanKey(k) { return k.includes('.') ? k.split('.').pop() : k; }

function cleanRow(row) {
  const out = {};
  for (const [k, v] of Object.entries(row)) {
    out[cleanKey(k)] = v;
  }
  // Resolve partner: management company first, then direct partner
  const mgmt = (out.property_management_company || '').trim();
  const dp = (out.dp_full_name || '').trim();
  out.partner_name = mgmt || dp || null;
  delete out.property_management_company;
  delete out.dp_full_name;
  return out;
}

// Aggregate rows to one per property_name (keep first partner_name + market_name seen)
function aggregate(rows) {
  const groups = {};
  for (const r of rows) {
    const key = r.property_name || '';
    if (!groups[key]) {
      groups[key] = { ...r, _count: 1 };
    } else {
      const g = groups[key];
      g._count++;
      // Sum
      for (const f of ['home_count', 'home_occupied_count', 'active_property_count', 'home_daily_rent_revenue', 'home_daily_rent_cost_autopilot']) {
        if (r[f] != null) g[f] = (g[f] || 0) + r[f];
      }
      // Average
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
    // Finalize averages
    for (const f of ['all_in_revpah_net', 'markup', 'home_average_rent_cost']) {
      if (g[`_sum_${f}`] != null) {
        g[f] = g[`_sum_${f}`] / g[`_cnt_${f}`];
      }
      delete g[`_sum_${f}`];
      delete g[`_cnt_${f}`];
    }
    // Recalculate occupancy
    if (g.home_count > 0) {
      g.occupancy = g.home_occupied_count / g.home_count;
    }
    delete g._count;
    results.push(g);
  }
  return results;
}

function computeStatus(r) {
  const occ = r.occupancy;
  const units = r.home_count || 0;
  const occupied = r.home_occupied_count || 0;
  const active = r.active_property_count || 0;
  const futureRes = r.future_reservation_count || 0;

  if (active < 1) return { status_label: 'Inactive', status_color: 'neutral' };
  if (units > 0 && occupied === 0 && futureRes > 0) return { status_label: 'Pre-Launch', status_color: 'blue' };
  if (units > 0 && occupied === 0 && futureRes === 0) return { status_label: 'Vacant', status_color: 'red' };
  if (occ != null && occ >= 0.85) return { status_label: 'Strong', status_color: 'green' };
  if (occ != null && occ >= 0.6) return { status_label: 'Moderate', status_color: 'amber' };
  if (occ != null && occ > 0) return { status_label: 'Low', status_color: 'red' };
  if (occ != null && occ === 0) {
    return futureRes > 0
      ? { status_label: 'Pre-Launch', status_color: 'blue' }
      : { status_label: 'Vacant', status_color: 'red' };
  }
  return { status_label: 'Active', status_color: 'green' };
}

// Slug cache (lives for duration of function invocation — fine for serverless)
let slugCache = null;
async function getSlugCache() {
  if (slugCache) return slugCache;
  slugCache = {};
  try {
    const resp = await fetch(`${ADMIN_BASE}/api/v2/properties`, {
      headers: {
        'X-Client-Id': process.env.LANDING_CLIENT_ID,
        'X-Client-Secret': process.env.LANDING_CLIENT_SECRET,
      },
    });
    if (resp.ok) {
      const props = await resp.json();
      for (const p of props) {
        slugCache[p.name.trim().toLowerCase()] = p.slug;
      }
    }
  } catch (e) { console.warn('Slug cache failed:', e.message); }
  return slugCache;
}

async function resolveSlug(name) {
  const cache = await getSlugCache();
  return cache[name.trim().toLowerCase()] || name.toLowerCase().replace(/\s+/g, '-');
}

exports.handler = async (event) => {
  const clientId = process.env.LANDING_CLIENT_ID;
  const clientSecret = process.env.LANDING_CLIENT_SECRET;
  if (!clientId || !clientSecret) {
    return { statusCode: 500, body: JSON.stringify({ error: 'LANDING_CLIENT_ID/SECRET not configured' }) };
  }

  try {
    const token = await getLookerToken();

    const occFields = [
      'dimproperty.property_name',
      'dimproperty.property_management_company',
      'dimdirectpartner.dp_full_name',
      'dimmarket.market_name',
      'tbldailyhomemetrics.home_count',
      'tbldailyhomemetrics.home_occupied_count',
      'tbldailyhomemetrics.occupancy',
      'tbldailyhomemetrics.actionable_vacancy_pct',
      'tbldailyhomemetrics.active_property_count',
      'dimhome.home_average_rent_cost',
      'tbldailyhomemetrics.home_daily_rent_revenue',
      'tbldailyhomemetrics.home_daily_rent_cost_autopilot',
      'tbldailyhomemetrics.all_in_revpah_net',
      'tbldailyhomemetrics.markup',
    ];

    const resFields = [
      'dimproperty.property_name',
      'dimreservation.current_reservation_count',
      'dimreservation.future_reservation_count',
      'dimreservation.count',
    ];

    const date = event.queryStringParameters?.date || 'today';

    // Parallel fetch: occupancy (active only), reservations, and slug cache
    const [occData, resData] = await Promise.all([
      lookerQuery(token, 'tbldailyhomemetrics', occFields,
        { 'tbldailyhomemetrics.date_date': date, 'tbldailyhomemetrics.active_property_count': '>0' },
        ['dimproperty.property_name'],
        5000),
      lookerQuery(token, 'dimreservation', resFields,
        { 'dimreservation.current_reservation_count': '>0' },
        ['dimproperty.property_name'],
        5000),
      getSlugCache(),
    ]);

    // Clean and aggregate occupancy
    const occClean = occData.map(cleanRow);
    const occAgg = aggregate(occClean);

    // Clean reservation summary — build lookup by property_name
    const resLookup = {};
    for (const raw of resData) {
      const r = cleanRow(raw);
      const name = r.property_name;
      if (!name) continue;
      if (!resLookup[name]) {
        resLookup[name] = { current_reservation_count: 0, future_reservation_count: 0, count: 0 };
      }
      resLookup[name].current_reservation_count += r.current_reservation_count || 0;
      resLookup[name].future_reservation_count += r.future_reservation_count || 0;
      resLookup[name].count += r.count || 0;
    }

    // Build slug cache for admin links
    const cache = await getSlugCache();

    // Merge and enrich
    const records = occAgg.map(r => {
      const res = resLookup[r.property_name] || {};
      const slug = cache[r.property_name?.trim().toLowerCase()] || r.property_name?.toLowerCase().replace(/\s+/g, '-') || '';
      const status = computeStatus({ ...r, ...res });
      return {
        ...r,
        current_reservation_count: res.current_reservation_count || null,
        future_reservation_count: res.future_reservation_count || null,
        count: res.count || null,
        admin_url: `${ADMIN_BASE}/properties/${slug}`,
        ...status,
      };
    });

    return {
      statusCode: 200,
      headers: { 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=600, s-maxage=600' },
      body: JSON.stringify(records),
    };
  } catch (err) {
    console.error(err);
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};
