#!/usr/bin/env node
// One-time ETL: load data/revshare.json into Neon Postgres.
//
// Usage:
//   NEON_DATABASE_URL=postgres://... node scripts/db/etl-bundle-to-pg.js
//   NEON_DATABASE_URL=postgres://... node scripts/db/etl-bundle-to-pg.js --dry
//
// Idempotent — uses INSERT ON CONFLICT DO UPDATE, so re-running is safe.
// The source column on every inserted row is set to 'bootstrap' so we can
// distinguish these from rows written by the monthly Sheet ingest later.
//
// Reads from $REPO/data/revshare.json (where REPO is auto-detected — same
// findRepoRoot() logic as ingest-revshare-sheet.js).
//
// PRE-REQ: schema.sql must have been applied to the Neon DB first. The DDL
// is idempotent (CREATE TABLE IF NOT EXISTS) so re-applying is also fine.

const fs = require('fs');
const path = require('path');

// ─── Repo discovery ──────────────────────────────────────────────────
function findRepoRoot() {
  const candidates = [];
  if (process.env.AUTOPILOT_DASHBOARD_REPO) candidates.push(process.env.AUTOPILOT_DASHBOARD_REPO);
  let cur = process.cwd();
  for (let i = 0; i < 8; i++) {
    candidates.push(cur);
    const parent = path.dirname(cur);
    if (parent === cur) break;
    cur = parent;
  }
  cur = __dirname;
  for (let i = 0; i < 4; i++) {
    candidates.push(cur);
    const parent = path.dirname(cur);
    if (parent === cur) break;
    cur = parent;
  }
  candidates.push('C:/Users/matt/Projects/autopilot-dashboard');
  for (const c of candidates) {
    if (!c) continue;
    if (fs.existsSync(path.join(c, 'data', 'revshare.json'))) return c;
  }
  throw new Error('Could not find autopilot-dashboard repo (no data/revshare.json found)');
}

// ─── CLI ─────────────────────────────────────────────────────────────
const DRY = process.argv.includes('--dry');
const DB_URL = process.env.NEON_DATABASE_URL;
if (!DB_URL) {
  console.error('NEON_DATABASE_URL env var required. Set it to your Neon connection string.');
  console.error('Example: postgresql://user:pass@ep-cool-darkness.us-east-2.aws.neon.tech/dbname?sslmode=require');
  process.exit(1);
}

// ─── Load bundle ─────────────────────────────────────────────────────
const REPO_ROOT = findRepoRoot();
const bundlePath = path.join(REPO_ROOT, 'data', 'revshare.json');
const bundle = JSON.parse(fs.readFileSync(bundlePath, 'utf8'));
const propCount = Object.keys(bundle.by_property || {}).length;
const monthCount = (bundle._meta?.source_months || []).length;

console.log(`Source: ${bundlePath}`);
console.log(`  ${propCount} properties × up to ${monthCount} months`);
console.log(`  bundle generated_at: ${bundle._meta?.generated_at || '(unknown)'}`);
console.log(`Target: ${DB_URL.replace(/:[^:@]+@/, ':****@')}  ${DRY ? '(DRY RUN)' : ''}`);

// ─── Flatten bundle into rows ────────────────────────────────────────
const rows = [];
for (const [propName, byMonth] of Object.entries(bundle.by_property || {})) {
  for (const [periodKey, slot] of Object.entries(byMonth)) {
    rows.push({
      property_name: propName,
      period_key: parseInt(periodKey, 10),
      period: slot.period,
      landing_margin: slot.l,
      net_allocation: slot.p,
      total_revenue: slot.g,
      occupancy_rate: slot.o,
      stay_count: slot.u,
      mgmt_fee: slot.mf ?? null,
      ffe_fee: slot.ff ?? null,
      install_fee: slot.if_ ?? null,
      wifi_fee: slot.wf ?? null,
      partner_adjustment: slot.pa ?? null,
    });
  }
}
console.log(`Flattened to ${rows.length} (property × month) rows`);
const withFees = rows.filter(r => r.mgmt_fee != null).length;
console.log(`  ${withFees} rows have mgmt_fee populated`);

if (DRY) {
  console.log('\nFirst 3 rows that would be inserted:');
  rows.slice(0, 3).forEach(r => console.log(' ', JSON.stringify(r)));
  console.log('\n--dry: not connecting to Postgres, exiting');
  process.exit(0);
}

// ─── Connect + insert ────────────────────────────────────────────────
// pg client is dynamically required so the script can be checked into the
// repo without forcing pg as a runtime dep for unrelated paths. Install
// it once when you actually run the ETL: `npm install pg` (or just
// `npm i --no-save pg` since it's also imported by _revshare-cache later).
let pg;
try {
  pg = require('pg');
} catch (e) {
  console.error('pg module not found. Install with: npm install pg');
  process.exit(1);
}

(async () => {
  const client = new pg.Client({ connectionString: DB_URL });
  await client.connect();
  console.log('Connected to Postgres');

  // Bulk insert in chunks of 200 to keep parameter count under PG limits
  // (max 65k parameters; we use ~14 per row, so 200 rows = 2800 params).
  const CHUNK = 200;
  let inserted = 0, updated = 0;
  const t0 = Date.now();

  for (let i = 0; i < rows.length; i += CHUNK) {
    const chunk = rows.slice(i, i + CHUNK);
    const cols = [
      'property_name', 'period_key', 'period',
      'landing_margin', 'net_allocation', 'total_revenue',
      'occupancy_rate', 'stay_count',
      'mgmt_fee', 'ffe_fee', 'install_fee', 'wifi_fee', 'partner_adjustment',
      'source',
    ];
    const placeholders = chunk.map((_, ri) => {
      const base = ri * cols.length;
      return '(' + cols.map((_, ci) => `$${base + ci + 1}`).join(', ') + ')';
    }).join(', ');
    const values = chunk.flatMap(r => [
      r.property_name, r.period_key, r.period,
      r.landing_margin, r.net_allocation, r.total_revenue,
      r.occupancy_rate, r.stay_count,
      r.mgmt_fee, r.ffe_fee, r.install_fee, r.wifi_fee, r.partner_adjustment,
      'bootstrap',
    ]);
    // ON CONFLICT DO UPDATE so reruns refresh the row in place (idempotent).
    // We update everything EXCEPT primary key + source — preserves the
    // original 'bootstrap' marker but lets data fields drift if the bundle
    // has been re-pulled.
    const sql = `
      INSERT INTO monthly_actuals (${cols.join(', ')})
      VALUES ${placeholders}
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
        ingested_at = NOW()
      RETURNING (xmax = 0) AS was_insert
    `;
    const result = await client.query(sql, values);
    for (const row of result.rows) {
      if (row.was_insert) inserted++; else updated++;
    }
    process.stdout.write(`\r  ${i + chunk.length}/${rows.length} rows processed...`);
  }
  console.log(`\nDone in ${((Date.now() - t0) / 1000).toFixed(1)}s`);
  console.log(`  ${inserted} new rows, ${updated} updated`);

  // Spot-check
  const verify = await client.query(
    `SELECT property_name, period_key, landing_margin, mgmt_fee
     FROM monthly_actuals
     WHERE property_name = ANY($1)
     ORDER BY property_name, period_key`,
    [['2121', '281 Willow', 'One Club Gulf Shores']]
  );
  console.log(`\nSanity check (${verify.rows.length} rows for 3 test properties):`);
  verify.rows.forEach(r => {
    console.log(`  ${r.property_name} ${r.period_key}: landing=$${r.landing_margin} mgmt_fee=$${r.mgmt_fee}`);
  });

  await client.end();
})().catch(e => { console.error('ETL failed:', e); process.exit(1); });
