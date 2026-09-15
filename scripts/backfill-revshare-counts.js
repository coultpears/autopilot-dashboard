#!/usr/bin/env node
// Backfill stay_count ("u") and unit_count ("n") on monthly_actuals from the original statements.
//
// WHY. Until 2026-09-15 the ingest stored "stays" as every id-looking row on a statement tab,
// which summed the Unit Level Detail AND Reservation Level Detail blocks (Flagler Crossing Aug
// 2026: 15 units + 10 reservations = 25). revshare-detail-counts.js now counts each block on
// its own; this re-reads every month's sheet with it.
//
// WHAT IT TOUCHES. Only stay_count and unit_count. Financial fields are NOT re-ingested: several
// statements were edited after their ingest (August 2026 was modified 2026-09-14), and a full
// re-run would silently move payouts.
//   - row in Neon AND a tab for it     -> set both counts from the tab
//   - row in Neon, no tab in that sheet -> both counts set NULL (the old number was wrong; unknown
//                                          beats wrong) and listed
//   - tab with no Neon row              -> listed, nothing inserted
//
//   NEON_DATABASE_URL=... node scripts/backfill-revshare-counts.js            # dry run, writes the plan
//   NEON_DATABASE_URL=... node scripts/backfill-revshare-counts.js --apply    # adds unit_count, writes
const fs = require('fs');
const path = require('path');
const os = require('os');
const { Client } = require('pg');
const { countDetail } = require('./revshare-detail-counts');

const APPLY = process.argv.includes('--apply');
const ONLY = (process.argv.find(a => a.startsWith('--month=')) || '').split('=')[1];

// period_key -> statement sheet (Drive titles "<Month> <Year> Revenue Share"; November uses "Final", not "v1")
const SHEETS = {
  202506: '1dvTGggM1Vxry9L0ERkvcAJKuk-n0Gtnd8WxebIxi6MU',
  202507: '1_CTRnQHVguFW_nuzd2WYWLK62sT3rbxNGE-iB1gdg7A',
  202508: '1WzbN_rji8FpndrQjmMSlsbQJZdFQGSxhSOZTqrLt1U0',
  202509: '156CdmE_G9ZnkrML5drRb3H0gC7USE6iLEyPUsHj0g9I',
  202510: '1-qKHIr0SOxtJAZyKUwb8cWIEQX5jMFbPqZdKy-jWV7w',
  202511: '1Iz0PCCWE3Wkz8UN9MJsapTJJyWBUNSmBCPLDsVvSfXw',
  202512: '1PHO1_HdglA9F-suAzP0nJHj8_z39mqFoB0gDcE32_P0',
  202601: '1HkyQ_dsHx9cBERoJrAhePwy3ALvHWCtYsEOfBuEJrBs',
  202602: '1GI2Glh0NMN8c_Gcn6FWvu2HiWrje39RlmEeHLUhJV4U',
  202603: '1TLiFSrG-Hm84J1eMKepeE7WgAJE0FU3HSIIiMNW4Vi0',
  202604: '1z2N5kNYsOU4C_3JdHmO1VEUnLE4kmfd8ZgIvV6yWuxE',
  202605: '1GSzb_uzdDgzWJWMhpAWgjLurERDJnqSJEreQ_QM49hw',
  202606: '15RjVX8L7ZaOFIqSCOfwdQ1-U3g_2niwQQR4uyOL0rcc',
  202607: '1u_p7Yd9LK4hufenfAuYybvKx7OF7OrTV9U3B9Pw0xhg',
  202608: '1VUd5WoqMVh98i5b2SMvvmKrt-AOMsVCEfMx-Fj97aV4',
};
const NON_PROPERTY = /^\s*(ap sales sign off|contact sheet|properties\s*--.*|.*\bpayouts?\b.*|.*\bsummary\b.*|template|instructions?|index|toc|cover|read\s*me|email|drafts?)\s*$/i;
const ROWS = 600;

const envPath = [path.join(os.homedir(), '.landing-sheets-rw.env'), path.join(os.homedir(), '.google-landing.env')].find(fs.existsSync);
const env = Object.fromEntries(fs.readFileSync(envPath, 'utf8').split(/\r?\n/).map(l => l.match(/^([A-Z_]+)=(.*)$/)).filter(Boolean).map(m => [m[1], m[2]]));

async function token() {
  const r = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ client_id: env.GOOGLE_CLIENT_ID, client_secret: env.GOOGLE_CLIENT_SECRET,
      refresh_token: env.GOOGLE_REFRESH_TOKEN, grant_type: 'refresh_token' }) });
  const j = await r.json();
  if (!j.access_token) throw new Error('token: ' + JSON.stringify(j));
  return j.access_token;
}

async function getJSON(url, auth, tries = 5) {
  for (let i = 0; i < tries; i++) {
    const r = await fetch(url, { headers: auth });
    if (r.ok) return r.json();
    if (r.status === 429 || r.status >= 500) { await new Promise(s => setTimeout(s, 2000 * 2 ** i)); continue; }
    throw new Error(`HTTP ${r.status} ${(await r.text()).slice(0, 200)}`);
  }
  throw new Error('retries exhausted: ' + url.slice(0, 120));
}

function propertyId(values, title) {
  for (const row of values) {
    for (let c = 0; c < (row || []).length; c++) {
      if (/^autopilot revenue share$/i.test(String(row[c] || '').trim())) {
        for (let j = c + 1; j < row.length; j++) {
          const v = String(row[j] || '').replace(/[^0-9]/g, '');
          if (v) return v;
        }
      }
    }
  }
  const m = title.match(/\((\d+)\)\s*$/);
  return m ? m[1] : null;
}

async function readMonth(sheetId, auth) {
  const meta = await getJSON(`https://sheets.googleapis.com/v4/spreadsheets/${sheetId}?fields=sheets.properties(title)`, auth);
  const tabs = meta.sheets.map(s => s.properties.title).filter(t => t && !NON_PROPERTY.test(t));
  const out = {}, dupes = [], clipped = [], noBlocks = [];
  for (let i = 0; i < tabs.length; i += 50) {
    const slice = tabs.slice(i, i + 50);
    const q = slice.map(t => 'ranges=' + encodeURIComponent(`'${t.replace(/'/g, "''")}'!A1:B${ROWS}`)).join('&');
    const j = await getJSON(`https://sheets.googleapis.com/v4/spreadsheets/${sheetId}/values:batchGet?${q}`, auth);
    j.valueRanges.forEach((vr, k) => {
      const values = vr.values || [];
      const pid = propertyId(values, slice[k]);
      if (!pid) return;
      const c = countDetail(values);
      if (c.units == null && c.stays == null) { noBlocks.push(slice[k]); return; }
      if (values.length >= ROWS) clipped.push(slice[k]);
      if (out[pid]) { dupes.push(`${pid}: "${out[pid].tab}" and "${slice[k]}"`); return; }
      out[pid] = { ...c, tab: slice[k] };
    });
  }
  return { byPid: out, tabs: tabs.length, dupes, clipped, noBlocks };
}

(async () => {
  if (!process.env.NEON_DATABASE_URL) throw new Error('NEON_DATABASE_URL required');
  const auth = { Authorization: `Bearer ${await token()}` };
  const db = new Client({ connectionString: process.env.NEON_DATABASE_URL });
  await db.connect();
  const hasCol = (await db.query(`SELECT 1 FROM information_schema.columns WHERE table_name='monthly_actuals' AND column_name='unit_count'`)).rowCount > 0;

  let plan = [], report = {};
  // --plan=<file>: apply a dry run's saved plan instead of re-reading 15 statements (~20 min).
  // Guarded: every row's CURRENT stay_count must still equal the plan's old_stays.
  const PLAN_FILE = (process.argv.find(a => a.startsWith('--plan=')) || '').slice(7);
  if (PLAN_FILE) {
    plan = JSON.parse(fs.readFileSync(PLAN_FILE, 'utf8')).plan;
    const cur = new Map((await db.query('SELECT property_id, period_key, stay_count FROM monthly_actuals')).rows
      .map(r => [`${r.property_id}|${r.period_key}`, r.stay_count]));
    const drift = plan.filter(p => cur.get(`${p.property_id}|${p.period_key}`) !== p.old_stays);
    if (drift.length) throw new Error(`${drift.length} rows changed since the plan was made, e.g. ${JSON.stringify(drift[0])}. Re-run the dry run.`);
    console.log(`plan ${PLAN_FILE}: ${plan.length} updates, all rows still match`);
  }
  for (const [pk, sid] of Object.entries(PLAN_FILE ? {} : SHEETS)) {
    if (ONLY && pk !== ONLY) continue;
    const m = await readMonth(sid, auth);
    const rows = (await db.query(`SELECT property_id, stay_count${hasCol ? ', unit_count' : ''} FROM monthly_actuals WHERE period_key=$1`, [Number(pk)])).rows;
    const inDb = new Set(rows.map(r => String(r.property_id)));
    let changed = 0, nulled = 0, same = 0;
    for (const r of rows) {
      const pid = String(r.property_id), t = m.byPid[pid];
      const next = t ? { stays: t.stays, units: t.units } : { stays: null, units: null };
      const prev = { stays: r.stay_count, units: hasCol ? r.unit_count : undefined };
      // before unit_count exists every row with a tab needs writing, even if its stays were right
      const unitsSettled = hasCol ? prev.units === next.units : next.units == null;
      if (prev.stays === next.stays && unitsSettled) { same++; continue; }
      plan.push({ period_key: Number(pk), property_id: pid, old_stays: prev.stays, stays: next.stays, units: next.units, tab: t ? t.tab : null });
      if (t) changed++; else nulled++;
    }
    const noRow = Object.keys(m.byPid).filter(p => !inDb.has(p));
    const stays = Object.values(m.byPid).reduce((s, x) => s + (x.stays || 0), 0);
    const units = Object.values(m.byPid).reduce((s, x) => s + (x.units || 0), 0);
    const oldSum = rows.reduce((s, r) => s + (r.stay_count || 0), 0);
    report[pk] = { tabs: m.tabs, props_parsed: Object.keys(m.byPid).length, db_rows: rows.length, changed, nulled, unchanged: same,
      tabs_without_db_row: noRow.length, duplicate_ids: m.dupes.length, clipped_at_600: m.clipped.length, no_detail_blocks: m.noBlocks.length,
      old_stays_total: oldSum, new_stays_total: stays, units_total: units };
    console.log(pk, JSON.stringify(report[pk]));
    if (m.dupes.length) console.log('   duplicate ids:', m.dupes.slice(0, 5));
    if (m.clipped.length) console.log('   clipped:', m.clipped.slice(0, 5));
  }
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const planPath = path.join(__dirname, '..', '..', 'autopilot-roster', '_backup_20260915', `revshare_counts_plan_${stamp}.json`);
  if (!PLAN_FILE) fs.writeFileSync(planPath, JSON.stringify({ report, plan }, null, 1));
  console.log(`\n${plan.length} row updates planned -> ${planPath}`);
  const fl = plan.filter(p => p.property_id === '48231');
  console.log('Flagler Crossing:', fl);

  if (!APPLY) { console.log('\nDRY RUN. Re-run with --apply to write.'); await db.end(); return; }

  await db.query('BEGIN');
  try {
    if (!hasCol) await db.query('ALTER TABLE monthly_actuals ADD COLUMN IF NOT EXISTS unit_count INT');
    for (const p of plan) {
      const r = await db.query('UPDATE monthly_actuals SET stay_count=$1, unit_count=$2 WHERE property_id=$3 AND period_key=$4',
        [p.stays, p.units, p.property_id, p.period_key]);
      if (r.rowCount !== 1) throw new Error(`expected 1 row for ${p.property_id}/${p.period_key}, got ${r.rowCount}`);
    }
    // rows that were already right on stays still need their unit_count filled
    const fill = await db.query('SELECT property_id, period_key FROM monthly_actuals WHERE unit_count IS NULL');
    await db.query('COMMIT');
    console.log(`APPLIED ${plan.length} updates. ${fill.rowCount} rows still have no unit_count (no tab, or unchanged stays on a month not re-read).`);
  } catch (e) {
    await db.query('ROLLBACK');
    throw e;
  }
  await db.end();
})().catch(e => { console.error('ERR', e.message); process.exit(1); });
