#!/usr/bin/env node
// Backfill geocoding for AP Supply Map.
//
// Pulls addresses from:
//   1) Looker `dimproperty` for portfolio properties (key: property_name)
//   2) HubSpot AP-pipeline deals for CoStar targets (key: SHA1 of normalized address)
//
// Geocodes missing entries via Nominatim (OpenStreetMap, 1 req/sec, free).
// Persists to data/geocodes.json after each address (so a Ctrl-C is safe).
//
// Usage (env vars auto-loaded from Netlify CLI — no prefix needed):
//   node scripts/backfill-geocoding.js [--portfolio-only|--targets-only] [--dry-run]
//
// Re-runnable: skips addresses already in cache. Re-geocodes properties whose
// address has changed since last run.

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { execSync } = require('child_process');

const REPO_ROOT = path.resolve(__dirname, '..');
const CACHE_PATH = path.join(REPO_ROOT, 'data', 'geocodes.json');

const LOOKER_BASE = 'https://landing.cloud.looker.com';
const HUBSPOT_BASE = 'https://api.hubapi.com';
const NOMINATIM_BASE = 'https://nominatim.openstreetmap.org/search';
const MAPBOX_BASE = 'https://api.mapbox.com/geocoding/v5/mapbox.places';
// ASCII-only — Node fetch rejects non-ASCII bytes in headers.
const USER_AGENT = 'autopilot-dashboard/1.0 (matt@hellolanding.com - Landing AP map)';

const AP_PIPELINE = '64402505';
const EXCLUDED_STAGES = new Set([
  '126194579', '1097165102', '129423023', '138986106', '1009548619', '126194580',
]);

const args = new Set(process.argv.slice(2));
const DRY_RUN = args.has('--dry-run');
const PORTFOLIO_ONLY = args.has('--portfolio-only');
const TARGETS_ONLY = args.has('--targets-only');

// ─── Auto-load env vars from Netlify CLI when missing ─────────────────
// Required: Looker + HubSpot. Optional: MAPBOX_TOKEN (better geocoding accuracy).
function ensureEnv() {
  const required = ['LANDING_CLIENT_ID', 'LANDING_CLIENT_SECRET', 'HUBSPOT_TOKEN'];
  const optional = ['MAPBOX_TOKEN'];
  const fetchFromNetlify = (key, isRequired) => {
    if (process.env[key]) return;
    try {
      const out = execSync(`netlify env:get ${key} --json`, { cwd: REPO_ROOT, stdio: ['ignore', 'pipe', 'pipe'] }).toString().trim();
      const m = out.match(/\{[\s\S]*\}/);
      if (!m) throw new Error(`no JSON: ${out.slice(0, 100)}`);
      const val = JSON.parse(m[0])[key];
      if (val) { process.env[key] = val; return; }
      if (isRequired) throw new Error(`'netlify env:get ${key}' returned empty`);
    } catch (e) {
      if (isRequired) throw new Error(`Could not load ${key} via 'netlify env:get': ${e.message}. Run 'netlify login' + 'netlify link', or set ${key} explicitly.`);
    }
  };
  const missing = required.filter(k => !process.env[k]).concat(optional.filter(k => !process.env[k]));
  if (missing.length) console.log(`Loading ${missing.join(', ')} from Netlify CLI...`);
  for (const k of required) fetchFromNetlify(k, true);
  for (const k of optional) fetchFromNetlify(k, false);
  if (process.env.MAPBOX_TOKEN) console.log(`Mapbox geocoder enabled (primary). Nominatim is fallback.`);
  else console.log(`No MAPBOX_TOKEN set — using Nominatim only. Set MAPBOX_TOKEN in Netlify env for ~95%+ success rate.`);
}

// ─── Cache I/O ────────────────────────────────────────────────────────
function loadCache() {
  try {
    return JSON.parse(fs.readFileSync(CACHE_PATH, 'utf8'));
  } catch {
    return { version: 1, generatedAt: null, byProperty: {}, byAddress: {} };
  }
}
function saveCache(cache) {
  if (DRY_RUN) return;
  cache.generatedAt = new Date().toISOString();
  fs.writeFileSync(CACHE_PATH, JSON.stringify(cache, null, 2) + '\n');
}

// ─── Address normalization (used as cache key for targets) ─────────────
function normalizeAddress(addr) {
  return (addr || '')
    .toLowerCase()
    .replace(/\./g, '')
    .replace(/\s+/g, ' ')
    .replace(/\bstreet\b/g, 'st')
    .replace(/\bavenue\b/g, 'ave')
    .replace(/\bboulevard\b/g, 'blvd')
    .replace(/\bdrive\b/g, 'dr')
    .replace(/\bplace\b/g, 'pl')
    .replace(/\broad\b/g, 'rd')
    .replace(/\bparkway\b/g, 'pkwy')
    .replace(/\bcourt\b/g, 'ct')
    .replace(/\bnorth\b/g, 'n')
    .replace(/\bsouth\b/g, 's')
    .replace(/\beast\b/g, 'e')
    .replace(/\bwest\b/g, 'w')
    .trim();
}
function addrHash(addr) {
  return crypto.createHash('sha1').update(normalizeAddress(addr)).digest('hex').slice(0, 16);
}

// ─── Looker ───────────────────────────────────────────────────────────
let _lookerToken;
async function lookerAuth() {
  if (_lookerToken) return _lookerToken;
  const resp = await fetch(`${LOOKER_BASE}/api/4.0/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: `client_id=${process.env.LANDING_CLIENT_ID}&client_secret=${process.env.LANDING_CLIENT_SECRET}`,
  });
  if (!resp.ok) throw new Error(`Looker auth failed: ${resp.status}`);
  _lookerToken = (await resp.json()).access_token;
  return _lookerToken;
}
async function lookerQuery(view, fields, filters, limit = 5000) {
  const token = await lookerAuth();
  const resp = await fetch(`${LOOKER_BASE}/api/4.0/queries/run/json`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ model: 'landing', view, fields, filters, limit: String(limit) }),
  });
  if (!resp.ok) throw new Error(`Looker query failed: ${resp.status} ${(await resp.text()).slice(0, 200)}`);
  return resp.json();
}

async function fetchPortfolioAddresses() {
  console.log('Fetching portfolio property addresses from Looker...');
  // Pull one row per active property — `tbldailyhomemetrics.date_date=today` gives current snapshot.
  // We only need installed+active properties since those are the pin set.
  const rows = await lookerQuery('tbldailyhomemetrics', [
    'dimproperty.property_name',
    'dimproperty.address_one',
    'dimproperty.city_name',
    'dimproperty.state',
    'dimproperty.zip',
  ], {
    'tbldailyhomemetrics.date_date': 'today',
    'tbldailyhomemetrics.active_property_count': '>0',
  }, 5000);

  // Dedupe by property_name (the query may return multiple rows per property)
  const byName = new Map();
  for (const r of rows) {
    const name = r['dimproperty.property_name'];
    if (!name || byName.has(name)) continue;
    const street = (r['dimproperty.address_one'] || '').trim();
    const city = (r['dimproperty.city_name'] || '').trim();
    const state = (r['dimproperty.state'] || '').trim();
    const zip = (r['dimproperty.zip'] || '').trim();
    if (!street || !city || !state) continue; // skip incomplete addresses
    byName.set(name, {
      property_name: name,
      address: [street, city, state, zip].filter(Boolean).join(', '),
      street, city, state, zip,
    });
  }
  console.log(`  ${byName.size} unique properties with full addresses`);
  return [...byName.values()];
}

// ─── HubSpot (CoStar targets) ─────────────────────────────────────────
async function hsApi(method, p, body) {
  const opts = { method, headers: { Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}`, 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const resp = await fetch(`${HUBSPOT_BASE}${p}`, opts);
  if (!resp.ok) {
    if (resp.status === 429) { await new Promise(r => setTimeout(r, 2000)); return hsApi(method, p, body); }
    return null;
  }
  return resp.json();
}
async function fetchTargetAddresses() {
  console.log('Fetching CoStar target addresses from HubSpot AP pipeline...');
  const props = ['property_name', 'dealname', 'property_street_address', 'property_city', 'property_state', 'property_zip', 'dealstage', 'costar_last_synced'];
  const filters = [
    { propertyName: 'pipeline', operator: 'EQ', value: AP_PIPELINE },
    { propertyName: 'costar_last_synced', operator: 'HAS_PROPERTY' },
  ];
  const out = [];
  let after = 0;
  for (let pages = 0; pages < 30; pages++) {
    const data = await hsApi('POST', '/crm/v3/objects/deals/search', {
      filterGroups: [{ filters }], properties: props, limit: 100, after,
    });
    if (!data) break;
    out.push(...(data.results || []));
    if (data.paging?.next?.after) after = data.paging.next.after;
    else break;
  }
  // Filter excluded stages, build address records
  const byAddr = new Map();
  for (const d of out) {
    const stageId = d.properties?.dealstage || '';
    if (EXCLUDED_STAGES.has(stageId)) continue;
    const street = (d.properties?.property_street_address || '').trim();
    const city = (d.properties?.property_city || '').trim();
    const state = (d.properties?.property_state || '').trim();
    const zip = (d.properties?.property_zip || '').trim();
    if (!street || !city || !state) continue;
    const fullAddr = [street, city, state, zip].filter(Boolean).join(', ');
    const key = addrHash(fullAddr);
    if (!byAddr.has(key)) byAddr.set(key, { hash: key, address: fullAddr, street, city, state, zip });
  }
  console.log(`  ${byAddr.size} unique target addresses`);
  return [...byAddr.values()];
}

// ─── Mapbox geocoder (preferred when MAPBOX_TOKEN is set, ~50ms/req, no rate limit needed) ──
async function mapboxGeocode(rec) {
  const tok = process.env.MAPBOX_TOKEN;
  if (!tok) return null;
  // Mapbox forward-geocoding wants the address as a single search string. Apartment-unit clutter
  // in `street` confuses it less than Nominatim — but we still keep it concise.
  const q = encodeURIComponent(`${rec.street}, ${rec.city}, ${rec.state} ${rec.zip || ''}`.trim());
  const url = `${MAPBOX_BASE}/${q}.json?country=us&limit=1&types=address,poi,place&access_token=${tok}`;
  try {
    const resp = await fetch(url, { headers: { 'User-Agent': USER_AGENT } });
    if (!resp.ok) return { error: `mapbox HTTP ${resp.status}` };
    const data = await resp.json();
    const feat = (data.features || [])[0];
    if (!feat || !feat.center) return { error: 'mapbox no results' };
    const [lng, lat] = feat.center;
    // Mapbox returns relevance 0-1; <0.5 is usually a town-level fallback, not an address hit.
    const accuracy = feat.properties?.accuracy || feat.place_type?.[0] || 'unknown';
    return { lat, lng, src: 'mapbox', display: feat.place_name, accuracy, relevance: feat.relevance };
  } catch (e) {
    return { error: 'mapbox: ' + e.message };
  }
}

// ─── Composite geocoder: Mapbox first (when available), Nominatim fallback ──
async function geocode(rec) {
  if (process.env.MAPBOX_TOKEN) {
    const r = await mapboxGeocode(rec);
    if (r && r.lat && r.lng && (r.relevance == null || r.relevance >= 0.7)) return r;
    // Mapbox missed or returned a low-confidence town-level hit — try Nominatim as backstop.
  }
  return nominatimGeocode(rec);
}

// ─── Nominatim geocoder (rate-limited 1 req/sec) ──────────────────────
let _lastNominatimCall = 0;
async function nominatimGeocode(rec) {
  const since = Date.now() - _lastNominatimCall;
  if (since < 1100) await new Promise(r => setTimeout(r, 1100 - since)); // be polite, 1100ms
  _lastNominatimCall = Date.now();

  const params = new URLSearchParams({
    street: `${rec.street}`,
    city: rec.city,
    state: rec.state,
    postalcode: rec.zip || '',
    country: 'us',
    format: 'json',
    limit: '1',
    addressdetails: '0',
  });
  try {
    const resp = await fetch(`${NOMINATIM_BASE}?${params}`, { headers: { 'User-Agent': USER_AGENT, 'Accept': 'application/json' } });
    if (!resp.ok) return { error: `HTTP ${resp.status}` };
    const data = await resp.json();
    if (!data.length) {
      // Retry with looser query (combined `q` instead of structured)
      await new Promise(r => setTimeout(r, 1100));
      _lastNominatimCall = Date.now();
      const fallback = await fetch(`${NOMINATIM_BASE}?` + new URLSearchParams({ q: rec.address, format: 'json', limit: '1', countrycodes: 'us' }), { headers: { 'User-Agent': USER_AGENT } });
      if (!fallback.ok) return { error: `fallback HTTP ${fallback.status}` };
      const fdata = await fallback.json();
      if (!fdata.length) return { error: 'no results' };
      return { lat: parseFloat(fdata[0].lat), lng: parseFloat(fdata[0].lon), src: 'nominatim-fallback', display: fdata[0].display_name };
    }
    return { lat: parseFloat(data[0].lat), lng: parseFloat(data[0].lon), src: 'nominatim', display: data[0].display_name };
  } catch (e) {
    return { error: e.message };
  }
}

// ─── Backfill loops ───────────────────────────────────────────────────
async function backfillPortfolio(cache, properties) {
  const queue = properties.filter(p => {
    const cached = cache.byProperty[p.property_name];
    if (!cached) return true;
    if (cached.address !== p.address) return true; // address changed since last geocode
    if (cached.lat == null || cached.lng == null) return true; // prior failure — retry
    return false;
  });
  console.log(`Portfolio: ${properties.length} total, ${queue.length} need geocoding (${properties.length - queue.length} cached)`);
  let ok = 0, fail = 0;
  for (let i = 0; i < queue.length; i++) {
    const p = queue[i];
    const r = await geocode(p);
    if (r.lat && r.lng) {
      cache.byProperty[p.property_name] = { lat: r.lat, lng: r.lng, address: p.address, src: r.src, display: r.display, geocodedAt: new Date().toISOString() };
      ok++;
    } else {
      cache.byProperty[p.property_name] = { lat: null, lng: null, address: p.address, error: r.error, attemptedAt: new Date().toISOString() };
      fail++;
    }
    if ((i + 1) % 10 === 0 || i === queue.length - 1) {
      saveCache(cache);
      console.log(`  ${(i + 1).toString().padStart(4)}/${queue.length}  ok=${ok} fail=${fail}  last="${p.property_name}" → ${r.lat ? `${r.lat.toFixed(4)},${r.lng.toFixed(4)}` : `FAIL (${r.error})`}`);
    }
  }
  return { ok, fail };
}
async function backfillTargets(cache, targets) {
  const queue = targets.filter(t => {
    const cached = cache.byAddress[t.hash];
    if (!cached) return true;
    if (cached.lat == null || cached.lng == null) return true; // retry failures
    return false;
  });
  console.log(`Targets: ${targets.length} total, ${queue.length} need geocoding (${targets.length - queue.length} cached)`);
  let ok = 0, fail = 0;
  for (let i = 0; i < queue.length; i++) {
    const t = queue[i];
    const r = await geocode(t);
    if (r.lat && r.lng) {
      cache.byAddress[t.hash] = { lat: r.lat, lng: r.lng, address: t.address, src: r.src, display: r.display, geocodedAt: new Date().toISOString() };
      ok++;
    } else {
      cache.byAddress[t.hash] = { lat: null, lng: null, address: t.address, error: r.error, attemptedAt: new Date().toISOString() };
      fail++;
    }
    if ((i + 1) % 10 === 0 || i === queue.length - 1) {
      saveCache(cache);
      console.log(`  ${(i + 1).toString().padStart(4)}/${queue.length}  ok=${ok} fail=${fail}  last="${t.address}" → ${r.lat ? `${r.lat.toFixed(4)},${r.lng.toFixed(4)}` : `FAIL (${r.error})`}`);
    }
  }
  return { ok, fail };
}

// ─── Main ─────────────────────────────────────────────────────────────
(async () => {
  ensureEnv();
  if (DRY_RUN) console.log('[DRY RUN — not writing cache]');
  const cache = loadCache();
  const startCacheSize = Object.keys(cache.byProperty).length + Object.keys(cache.byAddress).length;
  console.log(`Cache loaded: ${Object.keys(cache.byProperty).length} properties, ${Object.keys(cache.byAddress).length} target addresses`);

  let portfolioStats = { ok: 0, fail: 0 }, targetStats = { ok: 0, fail: 0 };

  if (!TARGETS_ONLY) {
    const properties = await fetchPortfolioAddresses();
    portfolioStats = await backfillPortfolio(cache, properties);
  }
  if (!PORTFOLIO_ONLY) {
    const targets = await fetchTargetAddresses();
    targetStats = await backfillTargets(cache, targets);
  }

  saveCache(cache);
  const finalSize = Object.keys(cache.byProperty).length + Object.keys(cache.byAddress).length;
  console.log('\n=== SUMMARY ===');
  console.log(`Portfolio: ${portfolioStats.ok} geocoded, ${portfolioStats.fail} failed`);
  console.log(`Targets:   ${targetStats.ok} geocoded, ${targetStats.fail} failed`);
  console.log(`Cache:     ${startCacheSize} → ${finalSize} entries`);
  console.log(`Wrote ${path.relative(REPO_ROOT, CACHE_PATH)}`);
})().catch(e => { console.error('FATAL:', e); process.exit(1); });
