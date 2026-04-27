// Netlify serverless function: Period (3/6/8/12-month) rollups per property
// Used by the Grid view's "Period" filter to roll up reservations, occupancy,
// revenue, ADR, and RevPAU for an owner-operator lookback window.

const LOOKER_BASE = 'https://landing.cloud.looker.com';

// Module-level token cache (same pattern as grid-data.js)
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
  _tokenExpiry = Date.now() + (data.expires_in ? (data.expires_in - 60) * 1000 : 3000000);
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

// Period -> day count
const PERIOD_DAYS = { '3mo': 90, '6mo': 180, '8mo': 240, '12mo': 365 };

function dateWindowFilter(days) {
  // Looker syntax: "N days ago for N days" = [today-N, today]
  return `${days} days ago for ${days} days`;
}

exports.handler = async (event) => {
  if (!process.env.LANDING_CLIENT_ID || !process.env.LANDING_CLIENT_SECRET) {
    return { statusCode: 500, body: JSON.stringify({ error: 'Credentials not configured' }) };
  }

  const period = event.queryStringParameters?.period || '3mo';
  const days = PERIOD_DAYS[period];
  if (!days) {
    return { statusCode: 400, body: JSON.stringify({ error: `Invalid period "${period}". Valid: ${Object.keys(PERIOD_DAYS).join(', ')}` }) };
  }
  const dateFilter = dateWindowFilter(days);

  try {
    const token = await getLookerToken();

    // Two queries in parallel:
    //   1) Daily metrics rolled up across the period (installed+active only)
    //      -> occupied nights, available nights, revenue, AP cost
    //   2) Reservations with start_date in the period
    //      -> booking count, total booked nights (commitment), avg LOS

    const [dailyData, resData] = await Promise.all([
      lookerQuery(token, 'tbldailyhomemetrics', [
        'dimproperty.property_name',
        'tbldailyhomemetrics.home_count',                    // available unit-nights (sum over days)
        'tbldailyhomemetrics.home_occupied_count',           // occupied unit-nights
        'tbldailyhomemetrics.home_daily_rent_revenue',       // total revenue in period
        'tbldailyhomemetrics.home_daily_rent_cost_autopilot',// total AP cost in period
      ], {
        'tbldailyhomemetrics.date_date': dateFilter,
        'tbldailyhomemetrics.active_property_count': '>0',
        'tbldailyhomemetrics.home_is_installed': 'Yes',
      }, ['dimproperty.property_name'], 5000),

      lookerQuery(token, 'dimreservation', [
        'dimproperty.property_name',
        'dimreservation.count',                              // bookings started in period
      ], {
        'dimreservation.reservation_start_date': dateFilter,
      }, ['dimproperty.property_name'], 5000),
    ]);

    // Index by property name
    const byProp = {};

    for (const row of dailyData) {
      const name = row['dimproperty.property_name'];
      if (!name) continue;
      byProp[name] = byProp[name] || { property_name: name };
      const r = byProp[name];
      r.period_available_nights = (r.period_available_nights || 0) + (row['tbldailyhomemetrics.home_count'] || 0);
      r.period_occupied_nights = (r.period_occupied_nights || 0) + (row['tbldailyhomemetrics.home_occupied_count'] || 0);
      r.period_revenue = (r.period_revenue || 0) + (row['tbldailyhomemetrics.home_daily_rent_revenue'] || 0);
      r.period_cost = (r.period_cost || 0) + (row['tbldailyhomemetrics.home_daily_rent_cost_autopilot'] || 0);
    }

    for (const row of resData) {
      const name = row['dimproperty.property_name'];
      if (!name) continue;
      byProp[name] = byProp[name] || { property_name: name };
      const r = byProp[name];
      r.period_reservations = (r.period_reservations || 0) + (row['dimreservation.count'] || 0);
    }

    // Derive rollups
    const records = Object.values(byProp).map(r => {
      const avail = r.period_available_nights || 0;
      const occ = r.period_occupied_nights || 0;
      const rev = r.period_revenue || 0;
      const res = r.period_reservations || 0;
      return {
        property_name: r.property_name,
        period_reservations: res || 0,
        period_occupied_nights: occ || 0,
        period_available_nights: avail || 0,
        period_occupancy: avail > 0 ? occ / avail : null,
        period_revenue: rev || 0,
        period_cost: r.period_cost || 0,
        period_adr: occ > 0 ? rev / occ : null,          // revenue per occupied night
        period_revpau: avail > 0 ? rev / avail : null,   // revenue per available unit-night
        period_rev_per_res: res > 0 ? rev / res : null,  // avg revenue per new booking in window
      };
    });

    return {
      statusCode: 200,
      headers: {
        'Content-Type': 'application/json',
        // Period data changes slowly — cache longer than grid-data
        'Cache-Control': 'public, max-age=600, stale-while-revalidate=3600',
        'Netlify-CDN-Cache-Control': 'public, max-age=3600, stale-while-revalidate=14400',
      },
      body: JSON.stringify({ period, days, records }),
    };
  } catch (err) {
    console.error(err);
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};
