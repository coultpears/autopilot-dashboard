// Shared fetch wrapper with an abort timeout for Looker calls.
//
// Why: the Looker `tbldailyhomemetrics` Explore can degrade (PDT rebuild /
// warehouse contention) and take minutes to answer a query. The Looker
// fetches had no timeout, so a slow Explore rode the function to its 30s
// Lambda cap and Netlify returned an opaque 504. Aborting well under the cap
// turns that into a fast, catchable error — callers can then serve stale
// data (grid-data) or return a clean 503 instead of hanging.
//
// Tunable via LOOKER_TIMEOUT_MS (default 22s — leaves ~8s headroom under the
// 30s function cap for the stale-cache read + response serialization).

const DEFAULT_TIMEOUT_MS = Number(process.env.LOOKER_TIMEOUT_MS) || 22000;

async function timedFetch(url, options = {}, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: ctrl.signal });
  } catch (e) {
    if (e.name === 'AbortError') {
      throw new Error(`Looker request timed out after ${timeoutMs}ms (Explore likely degraded)`);
    }
    throw e;
  } finally {
    clearTimeout(timer);
  }
}

module.exports = { timedFetch, DEFAULT_TIMEOUT_MS };
