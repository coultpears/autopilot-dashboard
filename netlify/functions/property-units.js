// Netlify serverless function: Per-unit detail for property expanded row
// Fetches unit status from Looker + Landing Rent from Admin API

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

// Slug cache
let slugCache = null;
async function resolveSlug(name) {
  if (!slugCache) {
    slugCache = {};
    try {
      const resp = await fetch(`${ADMIN_BASE}/api/v2/properties`, {
        headers: { 'X-Client-Id': process.env.LANDING_CLIENT_ID, 'X-Client-Secret': process.env.LANDING_CLIENT_SECRET },
      });
      if (resp.ok) {
        for (const p of await resp.json()) slugCache[p.name.trim().toLowerCase()] = p.slug;
      }
    } catch (e) { console.warn('Slug cache failed:', e.message); }
  }
  return slugCache[name.trim().toLowerCase()] || name.toLowerCase().replace(/\s+/g, '-');
}

// Unwrap Admin API response
function unwrapHomes(raw) {
  if (Array.isArray(raw)) return raw;
  if (raw && typeof raw === 'object') {
    const inner = raw.data || raw;
    if (Array.isArray(inner)) return inner;
    if (inner && typeof inner === 'object') {
      return inner.homes || inner.data || [];
    }
  }
  return [];
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
      'tbldailyhomemetrics.home_is_active',
      'tbldailyhomemetrics.home_is_installed',
      'tbldailyhomemetrics.home_reservation_status',
      'dimhome.rent_cost',
      'dimhome.lease_rent_cost',
      'tbldailyhomemetrics.home_daily_rent_revenue',
      'tbldailyhomemetrics.home_daily_rent_cost_autopilot',
      'tbldailyhomemetrics.markup',
      'dimhome.days_vacant_standard',
      'dimhome.vacant_since_date',
      'dimhome.current_reservation_start_date_raw_date',
      'dimhome.current_reservation_end_raw_date',
      'dimhome.revenue_share_percentage',
      'dimhome.deinstall_date',
      'dimhome.deinstall_requested_date',
      'dimhome.home_deinstall_reason',
      'dimhome.expected_deinstall_date',
    ];

    const data = await lookerQuery(token, 'tbldailyhomemetrics', fields,
      { 'dimproperty.property_name': name, 'tbldailyhomemetrics.date_date': 'today' },
      ['dimproperty.property_name', 'dimhome.unit_number']);

    const records = data.map(cleanRow);

    // Enrich with Admin API landing rent
    try {
      const slug = await resolveSlug(name);
      const resp = await fetch(`${ADMIN_BASE}/api/v1/properties/${slug}/homes`, {
        headers: { 'X-Client-Id': process.env.LANDING_CLIENT_ID, 'X-Client-Secret': process.env.LANDING_CLIENT_SECRET },
      });
      if (resp.ok) {
        const homes = unwrapHomes(await resp.json());
        const adminRent = {};
        for (const h of homes) {
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
      }
    } catch (e) { console.warn('Admin enrichment failed:', e.message); }

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
