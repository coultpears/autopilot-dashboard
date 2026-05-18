#!/usr/bin/env node
// Build data/revshare.json from the subsidy-session pulls.
//
// Source files (NOT in this repo — produced by the expansion-enrichment-agent
// subsidy session, see Matt's notes for re-pull instructions):
//   - signoff_pulls.json          : per (property, month) row with
//                                   total_revenue / mgmt_fee / ffe_fee /
//                                   partner_adjustment / net_allocation / landing_margin
//   - revshare_FULL_pull.json     : per (property, month) with occ rate + stay count
//
// Output (committed to this repo, bundled into netlify functions):
//   data/revshare.json
//     {
//       "_meta": { generated_at, source_months: [...] },
//       "by_property": {
//         "<property_name>": {
//           "<YYYYMM>": {
//             period, l: landing_margin, p: partner_net,
//             g: gross, o: occ_rate, u: stay_count,
//             // Fee breakdown — exposes the CONTRACTED rev share rate.
//             // The management fee % (|mf|/g) IS the contracted Landing
//             // take rate per the partnership agreement; FFE/install/wifi
//             // are reimbursements for setup costs, not rev share.
//             mf: mgmt_fee, ff: ffe_fee, if_: install_fee, wf: wifi_fee, pa: partner_adjustment
//           }
//         }
//       }
//     }
//
// Junk entries from the source sheets (tabs like "AP Sales Sign Off",
// "Payouts", "Email Drafts", numeric-only keys with no tab_title) are
// filtered out — we keep only properties whose tab_title matches "Name (####)"
// where #### is a Landing property ID.

const fs = require('fs');
const path = require('path');

const SUBSIDY_DIR = 'C:/Users/matt/Projects/expansion-enrichment-agent/.claude/worktrees/nifty-edison-246f40/_scratch_subsidy_data';
const OUT_PATH = path.join(__dirname, '..', 'data', 'revshare.json');

function load(name) {
  return JSON.parse(fs.readFileSync(path.join(SUBSIDY_DIR, name), 'utf8'));
}

const signoff = load('signoff_pulls.json');
const fullPull = load('revshare_FULL_pull.json');

// Build (property_id, period_key) → row index from signoff for fast join.
// Some rows have no property_id (junk); skip those.
const signoffByKey = new Map();
const tabTitlePattern = /^(.+?)\s+\((\d+)\)$/;

for (const row of signoff.rows) {
  if (!row.property_id || !row.period_key) continue;
  signoffByKey.set(`${row.property_id}|${row.period_key}`, row);
}

const out = {
  _meta: {
    generated_at: new Date().toISOString(),
    source_months: signoff.sheets_meta.map(s => ({ period: s.period, period_key: s.period_key })),
    note: 'Re-pull instructions: see scripts/build-revshare-bundle.js. Source = expansion-enrichment-agent subsidy session pulls.',
  },
  by_property: {},
};

let kept = 0, droppedNoTab = 0, droppedNoId = 0;

for (const [propKey, byMonth] of Object.entries(fullPull.by_property)) {
  // Resolve a property_id from any tab_title (they should all match)
  let propId = null;
  let canonicalName = null;
  for (const monthData of Object.values(byMonth)) {
    const m = monthData.tab_title && monthData.tab_title.match(tabTitlePattern);
    if (m) {
      canonicalName = m[1];
      propId = m[2];
      break;
    }
  }
  if (!propId) { droppedNoTab++; continue; }
  if (!canonicalName) { droppedNoId++; continue; }

  // Use the canonical "Name" (stripped of the parenthesized ID) as the key.
  // This is what `dimproperty.property_name` returns from Looker, so the
  // frontend can join on the property name it already has.
  const key = canonicalName;
  out.by_property[key] = {};

  for (const [period, monthData] of Object.entries(byMonth)) {
    const periodKey = monthData.period_key;
    const signoffRow = signoffByKey.get(`${propId}|${periodKey}`);

    // Pull Landing's margin from signoff (the truth). Fall back to nulls if
    // we don't have a matching row — better to omit than mislabel.
    const l = signoffRow && signoffRow.landing_margin != null ? signoffRow.landing_margin : null;
    const p = signoffRow && signoffRow.net_allocation != null ? signoffRow.net_allocation : monthData.monthly_net_allocation;
    const g = signoffRow && signoffRow.total_revenue != null ? signoffRow.total_revenue : null;

    // Fee breakdown from signoff — exposes contracted rev share economics.
    // mgmt_fee is negative in source (deduction from partner's share); we store
    // it as-is (negative). |mgmt_fee| / total_revenue = contracted Landing
    // take rate per the partnership agreement. Other fees reimburse setup
    // costs (FFE/install/wifi) and aren't part of the rev-share split.
    const round2 = v => v != null ? Math.round(v * 100) / 100 : null;
    const mf = signoffRow ? signoffRow.mgmt_fee : null;
    const ff = signoffRow ? signoffRow.ffe_fee : null;
    const _if = signoffRow ? signoffRow.install_fee : null;
    const wf = signoffRow ? signoffRow.wifi_fee : null;
    const pa = signoffRow ? signoffRow.partner_adjustment : null;

    out.by_property[key][String(periodKey)] = {
      period,
      l: l != null ? Math.round(l * 100) / 100 : null,
      p: p != null ? Math.round(p * 100) / 100 : null,
      g: g != null ? Math.round(g * 100) / 100 : null,
      o: monthData.monthly_occ_rate != null ? Math.round(monthData.monthly_occ_rate * 10000) / 10000 : null,
      u: monthData.unit_count != null ? monthData.unit_count : null, // stay count (despite the source field name)
      mf: round2(mf),
      ff: round2(ff),
      if_: round2(_if),
      wf: round2(wf),
      pa: round2(pa),
    };
  }
  kept++;
}

fs.mkdirSync(path.dirname(OUT_PATH), { recursive: true });
fs.writeFileSync(OUT_PATH, JSON.stringify(out, null, 0));

const sizeKB = (fs.statSync(OUT_PATH).size / 1024).toFixed(1);
console.log(`Wrote ${OUT_PATH}`);
console.log(`  ${kept} properties kept, ${droppedNoTab} dropped (no parseable tab_title — junk sheet tabs)`);
console.log(`  ${out._meta.source_months.length} months: ${out._meta.source_months[0].period} → ${out._meta.source_months.slice(-1)[0].period}`);
console.log(`  size: ${sizeKB} KB`);

// Spot-check One Club Gulf Shores
const oc = out.by_property['One Club Gulf Shores'];
if (oc) {
  console.log('\nOne Club Gulf Shores sanity check:');
  for (const [k, v] of Object.entries(oc)) {
    console.log(`  ${v.period}: gross=$${v.g} landing=$${v.l} partner=$${v.p} occ=${v.o} stays=${v.u}`);
  }
}
