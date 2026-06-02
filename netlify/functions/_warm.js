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

// Pull the canonical property-name list from the already-cached grid-data
// payload — no extra Looker call. Returns [] if grid-data hasn't been cached
// yet (the grid must be loaded at least once first).
async function propertyNames() {
  const grid = await responseCache.load('grid-data:today');
  if (!grid) return [];
  try {
    const arr = JSON.parse(grid.payloadJson);
    return [...new Set(arr.map((r) => r && r.property_name).filter(Boolean))];
  } catch {
    return [];
  }
}

async function warmBatch({ limit = 20, concurrency = 3, maxAgeMs = 1440000, log = () => {} } = {}) {
  const names = await propertyNames();
  if (!names.length) {
    log('no property list (grid-data:today not cached yet) — skipping');
    return { total: 0, selected: 0, warmed: 0, failed: 0, skipped: 'no-grid-cache' };
  }

  // Current cache age per property
  const ages = await responseCache.listAges(DETAIL_PREFIX);
  const ageByName = {};
  for (const a of ages) ageByName[a.key.slice(DETAIL_PREFIX.length)] = new Date(a.updatedAt).getTime();

  // Stalest/missing first, capped
  const now = Date.now();
  const candidates = names
    .map((n) => ({ n, age: ageByName[n] == null ? Infinity : now - ageByName[n] }))
    .filter((c) => c.age >= maxAgeMs)
    .sort((a, b) => b.age - a.age)
    .slice(0, limit);

  if (!candidates.length) {
    log(`all ${names.length} properties fresh — nothing to warm`);
    return { total: names.length, selected: 0, warmed: 0, failed: 0 };
  }

  let warmed = 0, failed = 0, idx = 0;
  async function worker() {
    while (idx < candidates.length) {
      const c = candidates[idx++];
      try {
        // refresh=1 forces a live Looker fetch + cache update (bypasses the
        // serve-from-cache shortcut, which would otherwise just return the
        // stale copy we're trying to replace).
        const res = await propertyDetailHandler({ queryStringParameters: { name: c.n, refresh: '1' } });
        const src = res.headers && res.headers['X-Data-Source'];
        if (res.statusCode === 200 && (src === 'live' || src === 'cache')) warmed++;
        else failed++;
      } catch {
        failed++;
      }
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, candidates.length) }, worker));

  log(`warmed ${warmed}/${candidates.length} (failed ${failed}); pool=${names.length}`);
  return { total: names.length, selected: candidates.length, warmed, failed };
}

module.exports = { warmBatch, propertyNames };
