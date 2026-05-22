#!/usr/bin/env node
// Reconciliation diagnostic for revshare ↔ portfolio property-name matching.
//
// Usage:
//   node scripts/list-revshare-gaps.js
//
// The grid pulls per-property P&L from the revshare dataset by exact
// property_name match (then the data/revshare-aliases.json fallback). This
// script surfaces the two failure sets so a human can curate real aliases:
//
//   1. COVERAGE GAPS  — portfolio properties with no revshare row at all.
//      Mostly legitimate (LT-only properties, or new/not-yet-reporting STR).
//   2. ORPHAN STATEMENTS — revshare property_names matching no portfolio
//      property. Could be deinstalled properties, or a name spelled
//      differently than Looker spells it.
//
// When a portfolio name in (1) and a revshare name in (2) are the SAME
// physical property, add the pair to data/revshare-aliases.json. The fuzzy
// suggestions below are ONLY hints — confirm by address before adding one,
// because a wrong alias misattributes one property's P&L to another.
//
// Data sources:
//   - revshare names  : netlify/functions/_revshare-cache.js (Postgres when
//                       NEON_DATABASE_URL is set, else the bundle)
//   - portfolio names : data/geocodes.json (geocoded portfolio snapshot)

const fs = require('fs');
const path = require('path');

const REPO = path.resolve(__dirname, '..');
const revshareCache = require(path.join(REPO, 'netlify', 'functions', '_revshare-cache.js'));

// ─── Fuzzy helpers (hints only — never auto-applied) ─────────────────
function norm(s) {
  return (s || '').toLowerCase()
    .replace(/\s*\(expansion\)\s*/g, '')
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\b(apartments?|apt?s?|residences?|living|the|at|on|of)\b/g, ' ')
    .replace(/\s+/g, ' ').trim();
}
function toks(s) { return new Set(norm(s).split(' ').filter(Boolean)); }
function jaccard(a, b) {
  const A = toks(a), B = toks(b);
  if (!A.size || !B.size) return 0;
  let i = 0; for (const x of A) if (B.has(x)) i++;
  return i / (A.size + B.size - i);
}
function lev(a, b) {
  a = norm(a); b = norm(b);
  const m = a.length, n = b.length;
  const dp = Array.from({ length: m + 1 }, (_, i) => [i, ...Array(n).fill(0)]);
  for (let j = 0; j <= n; j++) dp[0][j] = j;
  for (let i = 1; i <= m; i++)
    for (let j = 1; j <= n; j++)
      dp[i][j] = Math.min(dp[i-1][j] + 1, dp[i][j-1] + 1, dp[i-1][j-1] + (a[i-1] === b[j-1] ? 0 : 1));
  return 1 - dp[m][n] / Math.max(m, n, 1);
}
function score(a, b) { return Math.max(jaccard(a, b), lev(a, b)); }

(async () => {
  await revshareCache.init();
  const revNames = revshareCache.getAllProperties();

  // Portfolio names from the geocode cache
  let portfolioNames = [];
  try {
    const geo = JSON.parse(fs.readFileSync(path.join(REPO, 'data', 'geocodes.json'), 'utf8'));
    portfolioNames = Object.keys(geo.byProperty || {});
  } catch (e) {
    console.error('Could not read data/geocodes.json:', e.message);
    process.exit(1);
  }

  // Existing aliases
  let aliases = {};
  try {
    aliases = JSON.parse(fs.readFileSync(path.join(REPO, 'data', 'revshare-aliases.json'), 'utf8')).aliases || {};
  } catch (e) { /* none */ }

  const revSet = new Set(revNames);
  const portSet = new Set(portfolioNames);

  // Coverage gaps: portfolio props with no exact AND no alias match
  const gaps = portfolioNames.filter(n => !revSet.has(n) && !(aliases[n] && revSet.has(aliases[n])));
  // Orphan statements: revshare names with no portfolio match, and not the
  // target of an existing alias
  const aliasTargets = new Set(Object.values(aliases));
  const orphans = revNames.filter(r => !portSet.has(r) && !aliasTargets.has(r));

  console.log('═══ revshare ↔ portfolio reconciliation ═══');
  console.log(`Portfolio properties (geocodes.json) : ${portfolioNames.length}`);
  console.log(`Revshare properties                  : ${revNames.length}`);
  console.log(`Active aliases (revshare-aliases.json): ${Object.keys(aliases).length}`);
  console.log(`Coverage gaps (portfolio, no P&L)     : ${gaps.length}`);
  console.log(`Orphan statements (revshare, no prop) : ${orphans.length}`);

  // Fuzzy-pair the two unmatched sets — hints for curation only.
  const suggestions = [];
  for (const g of gaps) {
    let best = null, bs = 0;
    for (const o of orphans) {
      const s = score(g, o);
      if (s > bs) { bs = s; best = o; }
    }
    if (best) suggestions.push({ portfolio: g, revshare: best, score: +bs.toFixed(2) });
  }
  suggestions.sort((a, b) => b.score - a.score);

  const strong = suggestions.filter(s => s.score >= 0.6);
  console.log(`\n─── Candidate aliases to REVIEW (fuzzy score ≥ 0.60) ───`);
  if (!strong.length) {
    console.log('  (none — no portfolio gap closely resembles an orphan statement)');
  } else {
    console.log('  Confirm by address before adding to data/revshare-aliases.json:');
    for (const s of strong) {
      console.log(`  ${s.score}  "${s.portfolio}"  →  "${s.revshare}"`);
    }
  }

  console.log(`\n─── Coverage gaps (${gaps.length}) — portfolio properties with no revshare P&L ───`);
  for (const g of gaps.sort()) console.log(`  ${g}`);

  console.log(`\n─── Orphan statements (${orphans.length}) — revshare names matching no portfolio property ───`);
  for (const o of orphans.sort()) console.log(`  ${o}`);

  console.log('\nDone. Add confirmed pairs to data/revshare-aliases.json (key = portfolio name, value = revshare name).');
})().catch(e => { console.error('ERR', e); process.exit(1); });
