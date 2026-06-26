#!/usr/bin/env node
// Dump Neon monthly_actuals -> data/revshare.json (the fallback bundle).
//
// The ingest script (ingest-revshare-sheet.js) writes Postgres only by
// default, so the committed bundle drifts behind. Run this after an ingest
// to keep the fallback current — when Neon is unreachable/over-quota the
// functions serve this file, so a stale bundle silently drops the latest
// month. Produces the exact shape loadFromPostgres() returns.
//
//   NEON_DATABASE_URL=postgres://... node scripts/dump-revshare-bundle.js
const fs = require('fs');
const path = require('path');
const { Client } = require('pg');

(async () => {
  const url = process.env.NEON_DATABASE_URL;
  if (!url) { console.error('NEON_DATABASE_URL required'); process.exit(1); }
  const client = new Client({ connectionString: url });
  await client.connect();
  const rows = (await client.query(`
    SELECT property_id, property_name, period_key, period,
           landing_margin, net_allocation, total_revenue, occupancy_rate, stay_count,
           mgmt_fee, ffe_fee, install_fee, wifi_fee, partner_adjustment
    FROM monthly_actuals ORDER BY property_id, period_key`)).rows;
  const months = (await client.query(
    `SELECT DISTINCT period_key, period FROM monthly_actuals ORDER BY period_key`)).rows;
  await client.end();

  const by_property_id = {};
  for (const r of rows) {
    const pid = String(r.property_id);
    (by_property_id[pid] ||= {})[String(r.period_key)] = {
      property_id: pid, property_name: r.property_name, period: r.period,
      l: r.landing_margin != null ? Number(r.landing_margin) : null,
      p: r.net_allocation != null ? Number(r.net_allocation) : null,
      g: r.total_revenue != null ? Number(r.total_revenue) : null,
      o: r.occupancy_rate != null ? Number(r.occupancy_rate) : null,
      u: r.stay_count,
      mf: r.mgmt_fee != null ? Number(r.mgmt_fee) : null,
      ff: r.ffe_fee != null ? Number(r.ffe_fee) : null,
      if_: r.install_fee != null ? Number(r.install_fee) : null,
      wf: r.wifi_fee != null ? Number(r.wifi_fee) : null,
      pa: r.partner_adjustment != null ? Number(r.partner_adjustment) : null,
    };
  }
  const bundle = {
    _meta: {
      generated_at: new Date().toISOString(),
      source_months: months.map(m => ({ period: m.period, period_key: m.period_key })),
      note: 'Keyed by property_id. Regenerated from Neon monthly_actuals via scripts/dump-revshare-bundle.js.',
    },
    by_property_id,
  };
  const out = path.join(__dirname, '..', 'data', 'revshare.json');
  fs.writeFileSync(out, JSON.stringify(bundle, null, 0));
  console.log(`Wrote ${out}`);
  console.log(`  ${rows.length} rows · ${Object.keys(by_property_id).length} properties · ${months.length} months`);
  console.log(`  months: ${months.map(m=>m.period_key).join(', ')}`);
})().catch(e => { console.error(e.message); process.exit(2); });
