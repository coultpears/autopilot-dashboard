// Netlify serverless function: Reservation detail for property expanded row

const LOOKER_BASE = 'https://landing.cloud.looker.com';

async function getLookerToken() {
  const resp = await fetch(`${LOOKER_BASE}/api/4.0/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: `client_id=${process.env.LANDING_CLIENT_ID}&client_secret=${process.env.LANDING_CLIENT_SECRET}`,
  });
  if (!resp.ok) throw new Error(`Looker auth failed: ${resp.status}`);
  return (await resp.json()).access_token;
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
  if (!resp.ok) throw new Error(`Looker query failed: ${resp.status}`);
  return resp.json();
}

function cleanKey(k) { return k.includes('.') ? k.split('.').pop() : k; }

function cleanRow(row) {
  const out = {};
  for (const [k, v] of Object.entries(row)) out[cleanKey(k)] = v;
  const mgmt = (out.property_management_company || '').trim();
  const dp = (out.dp_full_name || '').trim();
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

    const fields = [
      'dimproperty.property_name',
      'dimproperty.property_management_company',
      'dimdirectpartner.dp_full_name',
      'dimmarket.market_name',
      'dimhome.unit_number',
      'dimreservation.reservation_start_date',
      'dimreservation.reservation_end_date',
      'dimreservation.days_in_reservation',
      'dimreservation.current_reservation_count',
      'dimreservation.future_reservation_count',
      'dimreservation.count',
    ];

    const data = await lookerQuery(token, 'dimreservation', fields,
      { 'dimproperty.property_name': name },
      ['dimproperty.property_name', 'dimreservation.reservation_start_date']);

    const records = data.map(cleanRow);

    return {
      statusCode: 200,
      headers: { 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=300' },
      body: JSON.stringify(records),
    };
  } catch (err) {
    console.error(err);
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};
