// Netlify serverless function: Combined property detail (units + reservations)
// Cached Looker auth, 2 parallel queries, no Admin API dependency

const LOOKER_BASE = 'https://landing.cloud.looker.com';

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
  const resp = await fetch(`${LOOKER_BASE}/api/4.0/queries/run/json`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ model: 'landing', view, fields, filters, sorts, limit: String(limit) }),
  });
  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    throw new Error(`Looker query (${view}) failed: ${resp.status} ${text.slice(0, 100)}`);
  }
  return resp.json();
}

function cleanRow(row) {
  const out = {};
  for (const [k, v] of Object.entries(row)) out[k.includes('.') ? k.split('.').pop() : k] = v;
  const mgmt = (out.property_management_company || '').trim();
  const dp = (out.dp_full_name || '').trim();
  out.pmc_name = mgmt || null;
  out.dp_name = dp || null;
  out.partner_name = mgmt || dp || null;
  delete out.property_management_company;
  delete out.dp_full_name;
  return out;
}

exports.handler = async (event) => {
  const name = event.queryStringParameters?.name;
  if (!name) return { statusCode: 400, body: JSON.stringify({ error: 'name parameter required' }) };

  try {
    const token = await getLookerToken();

    // Two Looker queries in parallel — no Admin API
    const [unitsData, resData] = await Promise.all([
      lookerQuery(token, 'tbldailyhomemetrics', [
        'dimproperty.property_name', 'dimproperty.property_management_company',
        'dimdirectpartner.dp_full_name', 'dimmarket.market_name', 'dimhome.unit_number',
        'tbldailyhomemetrics.home_is_active', 'tbldailyhomemetrics.home_is_installed',
        'tbldailyhomemetrics.home_reservation_status',
        'dimhome.rent_cost', 'dimhome.lease_rent_cost',
        'tbldailyhomemetrics.home_daily_rent_revenue', 'tbldailyhomemetrics.home_daily_rent_cost_autopilot',
        'tbldailyhomemetrics.markup',
        'dimhome.days_vacant_standard', 'dimhome.vacant_since_date',
        'dimhome.current_reservation_start_date_raw_date', 'dimhome.current_reservation_end_raw_date',
        'dimhome.revenue_share_percentage',
        'dimhome.deinstall_date', 'dimhome.deinstall_requested_date',
        'dimhome.home_deinstall_reason', 'dimhome.expected_deinstall_date',
      ], { 'dimproperty.property_name': name, 'tbldailyhomemetrics.date_date': 'today' },
      ['dimproperty.property_name', 'dimhome.unit_number']),

      lookerQuery(token, 'dimreservation', [
        'dimproperty.property_name', 'dimproperty.property_management_company',
        'dimdirectpartner.dp_full_name', 'dimmarket.market_name', 'dimhome.unit_number',
        'dimreservation.reservation_start_date', 'dimreservation.reservation_end_date',
        'dimreservation.days_in_reservation', 'dimreservation.reservation_length_raw',
        'dimreservation.reservation_length', 'dimreservation.stay_commitment_type',
        'dimreservation.current_reservation_count', 'dimreservation.future_reservation_count',
        'dimreservation.count', 'dimreservation.reservation_monthly_rent',
      ], { 'dimproperty.property_name': name },
      ['dimproperty.property_name', 'dimreservation.reservation_start_date']),
    ]);

    const reservations = resData.map(cleanRow);

    // Build unit-level Landing rent lookup from CURRENT reservations only
    const currentRentByUnit = {};
    for (const r of reservations) {
      if (r.current_reservation_count > 0 && r.unit_number != null && r.reservation_monthly_rent) {
        currentRentByUnit[String(r.unit_number)] = r.reservation_monthly_rent;
      }
    }

    // Process units — Landing rent comes from the active reservation, not estimation
    const units = unitsData.map(cleanRow);
    for (const r of units) {
      const base = r.rent_cost;
      const actualRent = currentRentByUnit[String(r.unit_number || '')];
      if (actualRent) {
        r.landing_rent = Math.round(actualRent);
        r.rent_source = 'reservation';
      }
      // No estimated fallback — if there's no active reservation, there's no Landing rent
      if (r.landing_rent && base && base > 0) {
        r.computed_markup = Math.round(((r.landing_rent - base) / base) * 10000) / 10000;
      }
    }

    return {
      statusCode: 200,
      headers: { 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=600, s-maxage=600' },
      body: JSON.stringify({ units, reservations }),
    };
  } catch (err) {
    console.error(err);
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};
