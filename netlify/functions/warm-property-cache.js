// Scheduled function: keep the property-detail Neon cache warm.
//
// Configured in netlify.toml ([functions."warm-property-cache"] schedule).
// Each run refreshes a small batch of the stalest properties so most UI opens
// hit a warm cache. Intentionally conservative (small batch, low concurrency)
// to avoid loading the Looker Explore — un-warmed properties fall back to the
// read-through path, so a partial/failed run is harmless.

const { warmBatch } = require('./_warm.js');
const { handler: gridDataHandler } = require('./grid-data.js');

exports.handler = async () => {
  // Refresh grid-data first (?refresh=1 forces a live fetch + re-cache) so the
  // main dashboard payload — and the property list the warmer reads from it —
  // stays fresh. Best-effort; a slow Looker just leaves the prior cache in place.
  try {
    const g = await gridDataHandler({ queryStringParameters: { refresh: '1' } });
    console.log('[warm-property-cache] grid-data refresh:', g.statusCode, g.headers && g.headers['X-Data-Source']);
  } catch (e) {
    console.log('[warm-property-cache] grid-data refresh failed:', e.message);
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
