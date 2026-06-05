// Shared property-detail cache warmer.
//
// Keeps the Neon `response_cache` populated with property-detail payloads so
// opening a property in the UI is a sub-second cache hit instead of a live
// Looker round-trip. Used by both the scheduled function (warm-property-cache)
// and the manual script (scripts/warm-property-detail.js).
//
// Strategy: refresh a capped batch of the STALEST (or missing) properties each
// run, at low concurrency, reusing the property-detail handler so the compute
// + save logic stays single-sourced. Failures are harmless — an un-warmed
// property just falls back to the live/read-through path on the next view.

const responseCache = require('./_response-cache.js');
const { handler: propertyDetailHandler } = require('./property-detail.js');

const DETAIL_PREFIX = 'property-detail:';

// Cache key for a property-detail entry. Mirrors the key built in
// property-detail.js: `<id>:<name>` when an id exists, else just `<name>`.
function detailKeySuffix(id, name) {
  return `${id ? id + ':' : ''}${name}`;
}

// Pull the canonical property targets from the already-cached grid-data
// payload — no extra Looker call. Returns [{ id, name }] (id may be null for
// legacy rows). [] if grid-data hasn't been cached yet (the grid must be
// loaded at least once first).
async function propertyTargets() {
  const grid = await responseCache.load('grid-data:today');
  if (!grid) return [];
  try {
    const arr = JSON.parse(grid.payloadJson);
    const seen = new Set();
    const out = [];
    for (const r of arr) {
      if (!r || !r.property_name) continue;
      const id = r.property_id != null ? String(r.property_id) : null;
      const key = detailKeySuffix(id, r.property_name);
      if (seen.has(key)) continue;
      seen.add(key);
      out.push({ id, name: r.property_name });
    }
    return out;
  } catch {
    return [];
  }
}

async function warmBatch({ limit = 20, concurrency = 3, maxAgeMs = 1440000, log = () => {} } = {}) {
  const targets = await propertyTargets();
  if (!targets.length) {
    log('no property list (grid-data:today not cached yet) — skipping');
    return { total: 0, selected: 0, warmed: 0, failed: 0, skipped: 'no-grid-cache' };
  }

  // Current cache age per property (keyed by the full `<id>:<name>` suffix)
  const ages = await responseCache.listAges(DETAIL_PREFIX);
  const ageByKey = {};
  for (const a of ages) ageByKey[a.key.slice(DETAIL_PREFIX.length)] = new Date(a.updatedAt).getTime();

  // Stalest/missing first, capped
  const now = Date.now();
  const candidates = targets
    .map((t) => {
      const k = detailKeySuffix(t.id, t.name);
      return { ...t, age: ageByKey[k] == null ? Infinity : now - ageByKey[k] };
    })
    .filter((c) => c.age >= maxAgeMs)
    .sort((a, b) => b.age - a.age)
    .slice(0, limit);

  if (!candidates.length) {
    log(`all ${targets.length} properties fresh — nothing to warm`);
    return { total: targets.length, selected: 0, warmed: 0, failed: 0 };
  }

  let warmed = 0, failed = 0, idx = 0;
  async function worker() {
    while (idx < candidates.length) {
      const c = candidates[idx++];
      try {
        // refresh=1 forces a live Looker fetch + cache update (bypasses the
        // serve-from-cache shortcut, which would otherwise just return the
        // stale copy we're trying to replace).
        const qs = { name: c.name, refresh: '1' };
        if (c.id) qs.id = c.id;
        const res = await propertyDetailHandler({ queryStringParameters: qs });
        const src = res.headers && res.headers['X-Data-Source'];
        if (res.statusCode === 200 && (src === 'live' || src === 'cache')) warmed++;
        else failed++;
      } catch {
        failed++;
      }
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, candidates.length) }, worker));

  log(`warmed ${warmed}/${candidates.length} (failed ${failed}); pool=${targets.length}`);
  return { total: targets.length, selected: candidates.length, warmed, failed };
}

module.exports = { warmBatch, propertyTargets };
