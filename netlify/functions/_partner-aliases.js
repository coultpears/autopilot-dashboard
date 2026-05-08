// Shared loader for the repo-committed partner-alias map (data/partner-aliases.json).
// Resolves Looker's split/typo'd partner names (e.g., "Stoltz Management Corporation"
// in PMC vs "Stolz Properties" in DP) to a single canonical identity so search +
// aggregation in the UI doesn't double-count or undercount the same partner.
//
// Loaded once at module init; survives across warm Lambda invocations.

const fs = require('fs');
const path = require('path');

let _byVariantLower = null;       // lowercased variant string -> canonical name
let _variantsByCanonical = null;  // canonical name -> array of original-cased variant strings
let _canonicalSet = null;

function load() {
  if (_byVariantLower) return _byVariantLower;
  const candidates = [
    path.resolve(__dirname, '..', '..', 'data', 'partner-aliases.json'),
    path.resolve(process.cwd(), 'data', 'partner-aliases.json'),
    path.resolve(__dirname, 'data', 'partner-aliases.json'),
  ];
  let data = null;
  for (const p of candidates) {
    try { data = JSON.parse(fs.readFileSync(p, 'utf8')); break; } catch (e) { /* try next */ }
  }
  // Bundled fallback — esbuild inlines this JSON require at build time.
  if (!data) {
    try { data = require('../../data/partner-aliases.json'); } catch (e) { /* no bundled copy */ }
  }
  _byVariantLower = {};
  _variantsByCanonical = {};
  _canonicalSet = new Set();
  if (data && Array.isArray(data.partners)) {
    for (const entry of data.partners) {
      const canonical = (entry.canonical || '').trim();
      if (!canonical) continue;
      _canonicalSet.add(canonical);
      const variants = Array.isArray(entry.variants) ? entry.variants : [];
      const all = [canonical, ...variants];
      _variantsByCanonical[canonical] = all;
      for (const v of all) {
        const k = (v || '').trim().toLowerCase();
        if (k) _byVariantLower[k] = canonical;
      }
    }
  }
  console.log(`[partner-aliases] ${Object.keys(_byVariantLower).length} variants -> ${_canonicalSet.size} canonical partners`);
  return _byVariantLower;
}

// All variants for a canonical name (including the canonical itself).
// Returns [] when the canonical name has no alias entry.
function variantsFor(canonical) {
  load();
  return _variantsByCanonical[canonical] || [];
}

// Returns the canonical name for a single PMC or DP name, or null if no alias matches.
function lookup(name) {
  const map = load();
  if (!name) return null;
  return map[String(name).trim().toLowerCase()] || null;
}

// Resolve a property's canonical partner from its (pmc_name, dp_name).
// Order of preference (flipped 2026-05-08 to align with rep mental model):
//   1) Alias match on DP name (capital owner — what reps think of as "the partner")
//   2) Alias match on PMC name (operator — fallback when no DP signal)
//   3) DP name itself
//   4) PMC name as final fallback
// Returns { canonical, source } where source is one of 'dp-alias' | 'pmc-alias' | 'dp' | 'pmc' | null.
//
// Earlier "PMC preferred" was a legacy default that surfaced operators as
// partners — gap discovered on The Heyward (PMC=American Landmark hired by
// DP=Carlton Companies). Reps track partnerships by capital owner / signing
// entity, not by PMC. PMC stays available via separate pmc_name field.
function resolvePartner(pmcName, dpName) {
  const dpCanon = lookup(dpName);
  if (dpCanon) return { canonical: dpCanon, source: 'dp-alias' };
  const pmcCanon = lookup(pmcName);
  if (pmcCanon) return { canonical: pmcCanon, source: 'pmc-alias' };
  const dp = (dpName || '').trim();
  if (dp) return { canonical: dp, source: 'dp' };
  const pmc = (pmcName || '').trim();
  if (pmc) return { canonical: pmc, source: 'pmc' };
  return { canonical: null, source: null };
}

module.exports = { lookup, resolvePartner, variantsFor };
