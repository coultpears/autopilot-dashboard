// Netlify serverless function: Per-unit detail for property expanded row
// Optimized: parallel Looker + Admin API calls, slug cache in parallel

const LOOKER_BASE = 'https://landing.cloud.looker.com';
const ADMIN_BASE = 'https://admin.hellolanding.com';

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
  const resp = await fetch(`${LOOKER_BASE}/api/4.0/queries/run/json`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ model: 'landing', view, fields, filters, sorts, limit: String(limit) }),
  });
  if (!resp.ok) throw new Error(`Looker query failed: ${resp.status}`);
  return resp.json();
}

function cleanRow(row) {
  const out = {};
  for (const [k, v] of Object.entries(row)) out[k.includes('.') ? k.split('.').pop() : k] = v;
  const mgmt = (out.property_management_company || '').trim();
  const dp = (out.dp_full_name || '').trim();
  out.partner_name = mgmt || dp || null;
  delete out.property_management_company;
  delete out.dp_full_name;
  return out;
}

function unwrapHomes(raw) {
  if (Array.isArray(raw)) return raw;
  if (raw && typeof raw === 'object') {
    const inner = raw.data || raw;
    if (Array.isArray(inner)) return inner;
    if (inner && typeof inner === 'object') return inner.homes || inner.data || [];
  }
  return [];
}

// Fetch Admin API homes for a property — try slug, then fallback
async function fetchAdminHomes(name) {
  const headers = {
    'X-Client-Id': process.env.LANDING_CLIENT_ID,
    'X-Client-Secret': process.env.LANDING_CLIENT_SECRET,
  };

  // Try simple slug first (fast — no slug cache needed for most properties)
  const simpleSlug = name.toLowerCase().replace(/\s+/g, '-');
  let resp = await fetch(`${ADMIN_BASE}/api/v1/properties/${simpleSlug}/homes`, { headers });
  if (resp.ok) {
    const homes = unwrapHomes(await resp.json());
    if (homes.length > 0) return homes;
  }

  // Fallback: fetch v2 properties list to find the real slug (handles UUID slugs)
  try {
    resp = await fetch(`${ADMIN_BASE}/api/v2/properties`, { headers });
    if (resp.ok) {
      const props = await resp.json();
      const match = props.find(p => p.name.trim().toLowerCase() === name.trim().toLowerCase());
      if (match && match.slug !== simpleSlug) {
        resp = await fetch(`${ADMIN_BASE}/api/v1/properties/${match.slug}/homes`, { headers });
        if (resp.ok) return unwrapHomes(await resp.json());
      }
    }
  } catch (e) { /* slug lookup failed, continue without admin data */ }

  return [];
}

exports.handler = async (event) => {
  const name = event.queryStringParameters?.name;
  if (!name) return { statusCode: 400, body: JSON.stringify({ error: 'name parameter required' }) };

  try {
    // Step 1: Looker auth
    const token = await getLookerToken();

    // Step 2: Parallel — Looker unit query + Admin API homes
    const fields = [
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
    ];

    const [lookerData, adminHomes] = await Promise.all([
      lookerQuery(token, 'tbldailyhomemetrics', fields,
        { 'dimproperty.property_name': name, 'tbldailyhomemetrics.date_date': 'today' },
        ['dimproperty.property_name', 'dimhome.unit_number']),
      fetchAdminHomes(name),
    ]);

    // Step 3: Merge
    const records = lookerData.map(cleanRow);
    const adminRent = {};
    for (const h of adminHomes) {
      if (h.unit_number != null) adminRent[String(h.unit_number)] = h.monthly_rent;
    }

    for (const r of records) {
      const unit = String(r.unit_number || '');
      const lr = adminRent[unit];
      const base = r.rent_cost;

      if (lr) {
        r.landing_rent = lr;
        r.rent_source = 'admin';
      } else if (r.home_daily_rent_cost_autopilot && r.home_daily_rent_cost_autopilot > 0) {
        r.landing_rent = Math.round(r.home_daily_rent_cost_autopilot * 30.4);
        r.rent_source = 'estimated';
      }

      if (r.landing_rent && base && base > 0) {
        r.computed_markup = Math.round(((r.landing_rent - base) / base) * 10000) / 10000;
      }
    }

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
