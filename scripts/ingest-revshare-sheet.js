#!/usr/bin/env node
// Ingest a single month's rev-share rent-roll Google Sheet into data/revshare.json.
//
// Usage:
//   node scripts/ingest-revshare-sheet.js <sheet-id-or-url> "<Period>" [--dry]
//   node scripts/ingest-revshare-sheet.js 1z2N5kNY...  "May 2026"
//   node /any/abs/path/scripts/ingest-revshare-sheet.js <sheet> "<Period>"
//
// REPO DISCOVERY — runs from any CWD. Looks for the autopilot-dashboard
// repo in this order:
//   1. $AUTOPILOT_DASHBOARD_REPO env var (explicit override)
//   2. Walking up from CWD looking for data/revshare.json (in-repo case)
//   3. Walking up from __dirname (script is in <repo>/scripts/)
//   4. Hardcoded default C:/Users/matt/Projects/autopilot-dashboard
// The Sheets pull uses Google OAuth from ~/.google-landing.env regardless
// of CWD — that file is in the user's home directory.
//
// Reads each tab whose name matches "<PropertyName> (<PropertyID>)" — that's
// the standard rev-share sheet layout. Per tab, scans for the rows containing
// the canonical totals (Total Revenue / Management Fee / Install Fee / FFE Fee
// / WiFi Fee / Partner Adjustment / Net Allocation / Landing Margin) by
// label match in column A, then takes the numeric value from the rightmost
// non-empty cell in that row.
//
// Also looks for an "Occupancy %" row (any of several common spellings) and
// counts the data rows on each tab for the "stays" approximation that the
// trend chart uses.
//
// Writes per-month per-property:
//   l: landing_margin (Landing's keep)
//   p: net_allocation (Partner's net cash)
//   g: total_revenue (gross)
//   o: occupancy rate (decimal)
//   u: stay count
//   mf: mgmt_fee (negative; |mf|/g = contracted Landing take rate)
//   ff: ffe_fee, if_: install_fee, wf: wifi_fee, pa: partner_adjustment
//
// The fee breakdown is what the dashboard's contracted_mgmt_pct chip
// depends on — without it, the "contract X%" chip won't render for the
// newly-ingested month.

const fs = require('fs');
const path = require('path');
const os = require('os');

// ─── Repo discovery — runs from any CWD ───────────────────────────────
function findRepoRoot() {
  const candidates = [];
  if (process.env.AUTOPILOT_DASHBOARD_REPO) candidates.push(process.env.AUTOPILOT_DASHBOARD_REPO);
  // Walk up from CWD
  let cur = process.cwd();
  for (let i = 0; i < 8; i++) {
    candidates.push(cur);
    const parent = path.dirname(cur);
    if (parent === cur) break;
    cur = parent;
  }
  // Walk up from this script's location (scripts/ingest-revshare-sheet.js)
  cur = __dirname;
  for (let i = 0; i < 4; i++) {
    candidates.push(cur);
    const parent = path.dirname(cur);
    if (parent === cur) break;
    cur = parent;
  }
  // Hardcoded default
  candidates.push('C:/Users/matt/Projects/autopilot-dashboard');

  for (const c of candidates) {
    if (!c) continue;
    const bundlePath = path.join(c, 'data', 'revshare.json');
    if (fs.existsSync(bundlePath)) return c;
  }
  throw new Error(
    'Could not find autopilot-dashboard repo. Set AUTOPILOT_DASHBOARD_REPO env var, ' +
    'or run from inside the repo, or ensure C:/Users/matt/Projects/autopilot-dashboard exists.'
  );
}
const REPO_ROOT = findRepoRoot();

// ─── CLI ─────────────────────────────────────────────────────────────
const args = process.argv.slice(2).filter(a => !a.startsWith('--'));
const dry  = process.argv.includes('--dry');
if (args.length < 2) {
  console.error('Usage: node scripts/ingest-revshare-sheet.js <sheet-id-or-url> "<Period label e.g. May 2026>" [--dry]');
  process.exit(1);
}
const sheetArg = args[0];
const period   = args[1];

// Period → period_key (YYYYMM). Tolerates "May 2026", "May '26", "2026-05".
function parsePeriodKey(label) {
  const monthNames = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'];
  const cleaned = label.toLowerCase().replace(/['']/g, '');
  let m;
  if ((m = cleaned.match(/(\d{4})-(\d{1,2})/))) return Number(m[1]) * 100 + Number(m[2]);
  if ((m = cleaned.match(/(\w{3,9})\s*(\d{2,4})/))) {
    const mi = monthNames.findIndex(n => m[1].startsWith(n));
    if (mi >= 0) {
      const yr = m[2].length === 2 ? 2000 + Number(m[2]) : Number(m[2]);
      return yr * 100 + (mi + 1);
    }
  }
  throw new Error(`Could not parse period "${label}". Try "May 2026" or "2026-05".`);
}
const periodKey = parsePeriodKey(period);

// Extract sheet ID from URL or use as-is
const sheetId = (sheetArg.match(/\/spreadsheets\/d\/([a-zA-Z0-9_-]+)/) || [null, sheetArg])[1];

// ─── Google OAuth ────────────────────────────────────────────────────
const envPath = path.join(os.homedir(), '.google-landing.env');
const env = {};
for (const line of fs.readFileSync(envPath, 'utf8').split(/\r?\n/)) {
  const m = line.match(/^([A-Z_]+)=(.*)$/);
  if (m) env[m[1]] = m[2];
}
async function getAccessToken() {
  const r = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id: env.GOOGLE_CLIENT_ID,
      client_secret: env.GOOGLE_CLIENT_SECRET,
      refresh_token: env.GOOGLE_REFRESH_TOKEN,
      grant_type: 'refresh_token',
    }),
  });
  const j = await r.json();
  if (!j.access_token) throw new Error('Token fetch failed: ' + JSON.stringify(j));
  return j.access_token;
}

// ─── Sheet parsing ───────────────────────────────────────────────────
const FIELD_LABELS = {
  total_revenue:      [/^total revenue/i, /^gross rev/i, /^rent revenue/i],
  mgmt_fee:           [/management fee/i, /pmc fee/i, /mgmt fee/i],
  install_fee:        [/install fee/i, /^installation/i],
  ffe_fee:            [/ffe fee/i, /^ff&e/i, /furniture/i],
  wifi_fee:           [/wifi/i, /internet fee/i],
  partner_adjustment: [/partner adjustment/i, /adjustment/i],
  net_allocation:     [/net allocation/i, /partner net/i, /^partner share/i, /^owner share/i],
  landing_margin:     [/landing margin/i, /landing net/i, /^landing share/i],
  occupancy:          [/occupancy/i, /^occ %/i],
};
function matchField(label) {
  if (!label) return null;
  const s = String(label).trim();
  for (const [field, patterns] of Object.entries(FIELD_LABELS)) {
    if (patterns.some(p => p.test(s))) return field;
  }
  return null;
}
function lastNumeric(row) {
  for (let i = row.length - 1; i >= 0; i--) {
    const v = row[i];
    if (v == null || v === '') continue;
    const n = Number(String(v).replace(/[$,\s%]/g, ''));
    if (!isNaN(n)) {
      // Percentages: if the cell had a "%", convert to decimal
      return String(v).includes('%') ? n / 100 : n;
    }
  }
  return null;
}
function parseTab(values, tabName) {
  // values = 2D array of cell strings
  const out = {
    total_revenue: null, mgmt_fee: null, install_fee: null, ffe_fee: null,
    wifi_fee: null, partner_adjustment: null, net_allocation: null,
    landing_margin: null, occupancy: null, stay_count: 0,
  };
  let dataRows = 0;
  for (const row of values) {
    if (!row || !row.length) continue;
    const labelCell = row[0];
    const field = matchField(labelCell);
    if (field) {
      out[field] = lastNumeric(row);
    }
    // Heuristic for stay count: rows whose first cell looks like a date/unit-stay row
    // (i.e. neither empty nor a known label)
    if (labelCell && !field && /^\d|^unit|^home/i.test(String(labelCell))) {
      dataRows++;
    }
  }
  if (dataRows > 0) out.stay_count = dataRows;
  return out;
}

// ─── Main ────────────────────────────────────────────────────────────
(async () => {
  console.log(`Ingest: sheet=${sheetId} period="${period}" periodKey=${periodKey} ${dry ? '(dry)' : ''}`);
  console.log(`Repo:   ${REPO_ROOT}  (override with AUTOPILOT_DASHBOARD_REPO env var)`);
  const token = await getAccessToken();
  const auth = { Authorization: `Bearer ${token}` };

  // 1) Get tab list
  const metaR = await fetch(`https://sheets.googleapis.com/v4/spreadsheets/${sheetId}?fields=sheets.properties(title,sheetId)`, { headers: auth });
  const meta = await metaR.json();
  if (!meta.sheets) throw new Error('No sheets in response: ' + JSON.stringify(meta));
  const tabPattern = /^(.+?)\s+\((\d+)\)$/;
  const propTabs = meta.sheets
    .map(s => s.properties)
    .filter(p => tabPattern.test(p.title));
  console.log(`Found ${propTabs.length} property tabs (of ${meta.sheets.length} total tabs)`);

  // 2) Batch-pull values for all property tabs in one call to avoid quota burn
  const ranges = propTabs.map(t => `'${t.title.replace(/'/g, "''")}'!A1:Z200`);
  const batchR = await fetch(
    `https://sheets.googleapis.com/v4/spreadsheets/${sheetId}/values:batchGet?${ranges.map(r => 'ranges=' + encodeURIComponent(r)).join('&')}`,
    { headers: auth }
  );
  const batch = await batchR.json();
  if (!batch.valueRanges) throw new Error('Batch get failed: ' + JSON.stringify(batch).slice(0, 300));

  // 3) Parse each tab → property record
  const newEntries = {}; // property_name → { l, p, g, o, u }
  let parsed = 0, withData = 0;
  for (let i = 0; i < propTabs.length; i++) {
    const tab = propTabs[i];
    const m = tab.title.match(tabPattern);
    const propName = m[1].trim();
    const values = batch.valueRanges[i]?.values || [];
    if (!values.length) continue;
    parsed++;
    const t = parseTab(values, tab.title);
    if (t.total_revenue == null && t.landing_margin == null && t.net_allocation == null) continue;
    withData++;
    const round2 = v => v != null ? Math.round(v * 100) / 100 : null;
    newEntries[propName] = {
      period, period_key: periodKey,
      l: round2(t.landing_margin),
      p: round2(t.net_allocation),
      g: round2(t.total_revenue),
      o: t.occupancy != null ? Math.round(t.occupancy * 10000) / 10000 : null,
      u: t.stay_count || null,
      // Fee breakdown — required for the dashboard's contracted_mgmt_pct chip.
      // Without these the "contract X%" indicator won't render for this month.
      // Stored signed (mf typically negative in source sheets).
      mf:  round2(t.mgmt_fee),
      ff:  round2(t.ffe_fee),
      if_: round2(t.install_fee),
      wf:  round2(t.wifi_fee),
      pa:  round2(t.partner_adjustment),
    };
  }
  console.log(`Parsed ${parsed} tabs, ${withData} had numeric data`);

  // 4) Spot-check 3 random entries so the human can sanity-check
  const sample = Object.entries(newEntries).slice(0, 3);
  console.log('\nSample entries:');
  for (const [name, rec] of sample) {
    console.log(`  ${name}: gross=$${rec.g} landing=$${rec.l} partner=$${rec.p} occ=${rec.o} stays=${rec.u}`);
  }

  if (dry) {
    console.log('\n--dry: not writing (set NEON_DATABASE_URL to test DB write, leave unset to test bundle write)');
    return;
  }

  // 5) Write to the active backend:
  //   - If NEON_DATABASE_URL is set → write to Postgres (production path).
  //     Data lands live; no git commit needed; Netlify functions pick it up
  //     on next request.
  //   - Otherwise → write to data/revshare.json (legacy bundle path; valid
  //     for dev mode or pre-DB-cutover). User commits + pushes; Netlify
  //     rebuilds; functions pick up the new bundle.
  if (process.env.NEON_DATABASE_URL) {
    await writeToPostgres(newEntries);
  } else {
    writeToBundle(newEntries);
  }
})().catch(e => { console.error('ERR', e); process.exit(1); });


// ─── Postgres write path ─────────────────────────────────────────────
async function writeToPostgres(newEntries) {
  let pg;
  try {
    pg = require('pg');
  } catch (e) {
    console.error('pg module not found. Install with: npm install pg');
    process.exit(1);
  }
  const client = new pg.Client({ connectionString: process.env.NEON_DATABASE_URL });
  await client.connect();
  console.log(`\nConnected to Postgres (${process.env.NEON_DATABASE_URL.replace(/:[^:@]+@/, ':****@')})`);

  const t0 = Date.now();
  let inserted = 0, updated = 0;
  for (const [propName, rec] of Object.entries(newEntries)) {
    const result = await client.query(
      `INSERT INTO monthly_actuals (
         property_name, period_key, period,
         landing_margin, net_allocation, total_revenue,
         occupancy_rate, stay_count,
         mgmt_fee, ffe_fee, install_fee, wifi_fee, partner_adjustment,
         source
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'sheet')
       ON CONFLICT (property_name, period_key) DO UPDATE SET
         period = EXCLUDED.period,
         landing_margin = EXCLUDED.landing_margin,
         net_allocation = EXCLUDED.net_allocation,
         total_revenue = EXCLUDED.total_revenue,
         occupancy_rate = EXCLUDED.occupancy_rate,
         stay_count = EXCLUDED.stay_count,
         mgmt_fee = EXCLUDED.mgmt_fee,
         ffe_fee = EXCLUDED.ffe_fee,
         install_fee = EXCLUDED.install_fee,
         wifi_fee = EXCLUDED.wifi_fee,
         partner_adjustment = EXCLUDED.partner_adjustment,
         source = 'sheet',
         ingested_at = NOW()
       RETURNING (xmax = 0) AS was_insert`,
      [propName, rec.period_key, rec.period,
       rec.l, rec.p, rec.g, rec.o, rec.u,
       rec.mf, rec.ff, rec.if_, rec.wf, rec.pa]
    );
    if (result.rows[0]?.was_insert) inserted++; else updated++;
  }
  await client.end();
  console.log(`Wrote ${inserted + updated} rows in ${((Date.now() - t0) / 1000).toFixed(1)}s (${inserted} new · ${updated} replaced)`);
  console.log(`\nDashboard will reflect the new data on next request — no rebuild needed.`);
}

// ─── Bundle write path (legacy / dev) ────────────────────────────────
function writeToBundle(newEntries) {
  const bundlePath = path.join(REPO_ROOT, 'data', 'revshare.json');
  const bundle = JSON.parse(fs.readFileSync(bundlePath, 'utf8'));
  let added = 0, replaced = 0, newProps = 0;
  for (const [propName, rec] of Object.entries(newEntries)) {
    if (!bundle.by_property[propName]) {
      bundle.by_property[propName] = {};
      newProps++;
    }
    const key = String(rec.period_key);
    if (bundle.by_property[propName][key]) replaced++; else added++;
    bundle.by_property[propName][key] = rec;
  }
  const existingPeriod = bundle._meta.source_months.find(s => s.period_key === [...Object.values(newEntries)][0]?.period_key);
  if (!existingPeriod && Object.values(newEntries)[0]) {
    const first = Object.values(newEntries)[0];
    bundle._meta.source_months.push({ period: first.period, period_key: first.period_key });
    bundle._meta.source_months.sort((a, b) => a.period_key - b.period_key);
  }
  bundle._meta.generated_at = new Date().toISOString();
  fs.writeFileSync(bundlePath, JSON.stringify(bundle, null, 0));
  const sizeKB = (fs.statSync(bundlePath).size / 1024).toFixed(1);
  console.log(`\nWrote ${bundlePath}`);
  console.log(`  ${added} new entries · ${replaced} replaced · ${newProps} brand-new properties`);
  console.log(`  bundle size: ${sizeKB} KB`);
  console.log(`  source_months now: ${bundle._meta.source_months.length}`);
  console.log(`\nNext steps:`);
  console.log(`  1. cd ${REPO_ROOT}`);
  console.log(`  2. git diff data/revshare.json | head -30`);
  console.log(`  3. git add data/revshare.json && git commit -m "rev-share: ingest" && git push`);
}
