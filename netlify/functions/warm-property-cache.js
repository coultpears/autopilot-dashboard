// Scheduled function: keep the property-detail Neon cache warm.
//
// Configured in netlify.toml ([functions."warm-property-cache"] schedule).
// Each run refreshes a small batch of the stalest properties so most UI opens
// hit a warm cache. Intentionally conservative (small batch, low concurrency)
// to avoid loading the Looker Explore — un-warmed properties fall back to the
// read-through path, so a partial/failed run is harmless.

const { warmBatch } = require('./_warm.js');
const { handler: gridDataHandler } = require('./grid-data.js');
const responseCache = require('./_response-cache.js');

exports.handler = async () => {
  // Refresh grid-data (?refresh=1 forces a live Looker fetch + full
  // monthly_actuals read + re-cache) only when the cached payload is actually
  // aging out. That rebuild is the dominant driver of Neon data-transfer, and
  // its inputs (Looker daily metrics + monthly revshare) change at most daily,
  // so an hourly rebuild is plenty — between rebuilds the serve-stale cache
  // (24h) plus the property-detail warming below keep the UI fast. When fresh
  // we skip the call entirely (warmBatch loads grid-data:today for its targets
  // regardless, so the property list stays available).
  const GRID_REFRESH_MS = Number(process.env.GRID_WARM_REFRESH_MS) || 3300000; // 55 min
  let needRefresh = true;
  try {
    const ages = await responseCache.listAges('grid-data:today');
    if (ages.length) needRefresh = (Date.now() - new Date(ages[0].updatedAt).getTime()) >= GRID_REFRESH_MS;
  } catch (e) { /* age check failed — fall through and refresh */ }
  if (needRefresh) {
    try {
      const g = await gridDataHandler({ queryStringParameters: { refresh: '1' } });
      console.log('[warm-property-cache] grid-data refresh:', g.statusCode, g.headers && g.headers['X-Data-Source']);
    } catch (e) {
      console.log('[warm-property-cache] grid-data refresh failed:', e.message);
    }
  } else {
    console.log('[warm-property-cache] grid-data still fresh (<55m) — skipping rebuild');
  }

  const ttl = Number(process.env.PROPERTY_DETAIL_TTL_MS) || 1800000;
  const result = await warmBatch({
    limit: Number(process.env.WARM_BATCH_LIMIT) || 8,
    concurrency: Number(process.env.WARM_CONCURRENCY) || 3,
    // Refresh anything older than 80% of the serve-TTL so entries are renewed
    // just before they'd go stale.
    maxAgeMs: ttl * 0.8,
    log: (m) => console.log('[warm-property-cache]', m),
  });
  console.log('[warm-property-cache] result', JSON.stringify(result));
  return { statusCode: 200, body: JSON.stringify(result) };
};
