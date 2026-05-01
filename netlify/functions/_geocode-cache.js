// Shared loader for the repo-committed geocoding cache (data/geocodes.json).
// Loaded once at module init — survives across warm Lambda invocations.
// Both grid-data.js (portfolio) and map-data.js (CoStar targets) use this.

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

let _cache = null;

function loadJsonNear(filename) {
  // Try fs.readFileSync first (lets local dev pick up edits without restart).
  // In production Netlify Lambdas, the function is bundled by esbuild and
  // these absolute paths usually don't exist — that's where the require()
  // fallback below comes in (esbuild statically inlines JSON requires).
  const candidates = [
    path.resolve(__dirname, '..', '..', 'data', filename),
    path.resolve(process.cwd(), 'data', filename),
    path.resolve(__dirname, 'data', filename),
  ];
  for (const p of candidates) {
    try { return { data: JSON.parse(fs.readFileSync(p, 'utf8')), path: p }; }
    catch (e) { /* try next */ }
  }
  // Bundled fallback — esbuild inlines these JSON imports at build time.
  try {
    if (filename === 'geocodes.json') return { data: require('../../data/geocodes.json'), path: 'bundled' };
    if (filename === 'geocodes-manual.json') return { data: require('../../data/geocodes-manual.json'), path: 'bundled' };
  } catch (e) { /* no bundled copy */ }
  return { data: null, path: null };
}

function load() {
  if (_cache) return _cache;
  // Layered cache:
  //   1) data/geocodes.json        — backfill output (Nominatim/Mapbox)
  //   2) data/geocodes-manual.json — hand-curated overrides (always win)
  // Both are repo-committed; readSync at module init survives across warm invocations.
  const auto = loadJsonNear('geocodes.json').data || { byProperty: {}, byAddress: {} };
  const manual = loadJsonNear('geocodes-manual.json').data || { byProperty: {}, byAddress: {} };

  // Merge — manual wins. Tag origin so console logs are debuggable.
  const byProperty = { ...auto.byProperty };
  for (const [k, v] of Object.entries(manual.byProperty || {})) byProperty[k] = { ...v, src: (v.src || 'manual') };
  const byAddress = { ...auto.byAddress };
  for (const [k, v] of Object.entries(manual.byAddress || {})) byAddress[k] = { ...v, src: (v.src || 'manual') };

  _cache = { byProperty, byAddress };
  const manualPropCount = Object.keys(manual.byProperty || {}).length;
  const manualAddrCount = Object.keys(manual.byAddress || {}).length;
  console.log(`[geocode-cache] loaded ${Object.keys(byProperty).length} properties (${manualPropCount} manual) + ${Object.keys(byAddress).length} target addresses (${manualAddrCount} manual)`);
  return _cache;
}

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

// Returns { lat, lng, src } or null when unknown / failed-to-geocode.
function lookupByPropertyName(name) {
  const c = load();
  const r = c.byProperty?.[name];
  if (r && r.lat != null && r.lng != null) return { lat: r.lat, lng: r.lng, src: r.src || 'cache' };
  return null;
}
function lookupByAddress(address) {
  const c = load();
  const r = c.byAddress?.[addrHash(address)];
  if (r && r.lat != null && r.lng != null) return { lat: r.lat, lng: r.lng, src: r.src || 'cache' };
  return null;
}

module.exports = { lookupByPropertyName, lookupByAddress, addrHash, normalizeAddress };
