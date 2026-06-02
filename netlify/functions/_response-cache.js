// Neon-backed "last good response" cache.
//
// Lets a function serve the most recent successful payload when its upstream
// (Looker) is too slow to answer within the function's time budget — the
// dashboard degrades to slightly-stale data instead of returning a 504.
//
// Stores the already-serialized JSON string verbatim so the read path can
// return it as the response body with zero re-encoding. Best-effort by
// design: every operation swallows its own errors and a DB problem never
// breaks the live request path.

const CONNECT_TIMEOUT_MS = 8000; // never hang on a sleeping/unreachable Neon

let _tableReady = false;

async function withClient(fn) {
  if (!process.env.NEON_DATABASE_URL) return null;
  const pg = require('pg');
  const client = new pg.Client({
    connectionString: process.env.NEON_DATABASE_URL,
    connectionTimeoutMillis: CONNECT_TIMEOUT_MS,
    statement_timeout: CONNECT_TIMEOUT_MS,
  });
  await client.connect();
  try {
    return await fn(client);
  } finally {
    await client.end().catch(() => {});
  }
}

async function ensureTable(client) {
  if (_tableReady) return;
  await client.query(`
    CREATE TABLE IF NOT EXISTS response_cache (
      cache_key    TEXT PRIMARY KEY,
      payload_json TEXT NOT NULL,
      updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);
  _tableReady = true;
}

// Upsert a serialized JSON payload under a key. Best-effort; never throws.
async function save(cacheKey, payloadJson) {
  try {
    await withClient(async (client) => {
      await ensureTable(client);
      await client.query(
        `INSERT INTO response_cache (cache_key, payload_json, updated_at)
         VALUES ($1, $2, now())
         ON CONFLICT (cache_key)
         DO UPDATE SET payload_json = EXCLUDED.payload_json, updated_at = now()`,
        [cacheKey, payloadJson]
      );
    });
  } catch (e) {
    console.warn('[response-cache] save failed:', e.message);
  }
}

// Returns { payloadJson, updatedAt } for the key, or null if missing / on error.
async function load(cacheKey) {
  try {
    return await withClient(async (client) => {
      await ensureTable(client);
      const r = await client.query(
        `SELECT payload_json, updated_at FROM response_cache WHERE cache_key = $1`,
        [cacheKey]
      );
      if (!r.rows.length) return null;
      return { payloadJson: r.rows[0].payload_json, updatedAt: r.rows[0].updated_at };
    });
  } catch (e) {
    console.warn('[response-cache] load failed:', e.message);
    return null;
  }
}

// Returns [{ key, updatedAt }] for every cache entry whose key starts with
// `prefix`. Used by the warmer to find the stalest/missing property payloads.
async function listAges(prefix) {
  try {
    return await withClient(async (client) => {
      await ensureTable(client);
      const r = await client.query(
        `SELECT cache_key, updated_at FROM response_cache WHERE cache_key LIKE $1`,
        [prefix + '%']
      );
      return r.rows.map((x) => ({ key: x.cache_key, updatedAt: x.updated_at }));
    });
  } catch (e) {
    console.warn('[response-cache] listAges failed:', e.message);
    return [];
  }
}

module.exports = { save, load, listAges };
