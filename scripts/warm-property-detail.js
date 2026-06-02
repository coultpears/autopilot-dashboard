// Manually warm the property-detail Neon cache.
//
// Usage:
//   node scripts/warm-property-detail.js [--limit N] [--concurrency N] [--max-age-min N]
//
// Needs LANDING_CLIENT_ID, LANDING_CLIENT_SECRET, NEON_DATABASE_URL in env
// (same vars the Netlify functions use). Reads the property list from the
// cached grid-data:today payload, so load the grid at least once first.

const { warmBatch } = require('../netlify/functions/_warm.js');

const args = process.argv.slice(2);
function arg(name, def) {
  const i = args.indexOf('--' + name);
  return i >= 0 ? args[i + 1] : def;
}

(async () => {
  const result = await warmBatch({
    limit: Number(arg('limit', 50)),
    concurrency: Number(arg('concurrency', 4)),
    maxAgeMs: Number(arg('max-age-min', 25)) * 60000,
    log: (m) => console.log('[warm]', m),
  });
  console.log('[warm] done', JSON.stringify(result));
})();
