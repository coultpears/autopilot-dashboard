#!/usr/bin/env node
// Surfaces the highest-impact geocoding failures so we can manually resolve them.
// Joins the failure cache against grid-data.js (live) for unit counts.

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const REPO_ROOT = path.resolve(__dirname, '..');

function ensureEnv() {
  for (const k of ['LANDING_CLIENT_ID', 'LANDING_CLIENT_SECRET']) {
    if (process.env[k]) continue;
    try {
      const out = execSync(`netlify env:get ${k} --json`, { cwd: REPO_ROOT, stdio: ['ignore','pipe','pipe'] }).toString();
      process.env[k] = JSON.parse(out.match(/\{[\s\S]*\}/)[0])[k];
    } catch (e) { throw new Error(`need ${k}`); }
  }
}

async function main() {
  ensureEnv();
  const cache = JSON.parse(fs.readFileSync(path.join(REPO_ROOT, 'data', 'geocodes.json'), 'utf8'));
  const manual = (() => { try { return JSON.parse(fs.readFileSync(path.join(REPO_ROOT, 'data', 'geocodes-manual.json'), 'utf8')); } catch { return { byProperty: {}, byAddress: {} }; } })();

  // Pull live property metrics from Looker so we can rank failures by unit count
  const auth = await (await fetch('https://landing.cloud.looker.com/api/4.0/login', {
    method: 'POST', headers: {'Content-Type': 'application/x-www-form-urlencoded'},
    body: `client_id=${process.env.LANDING_CLIENT_ID}&client_secret=${process.env.LANDING_CLIENT_SECRET}`
  })).json();
  const rows = await (await fetch('https://landing.cloud.looker.com/api/4.0/queries/run/json', {
    method: 'POST',
    headers: {Authorization: `Bearer ${auth.access_token}`, 'Content-Type': 'application/json'},
    body: JSON.stringify({
      model: 'landing', view: 'tbldailyhomemetrics',
      fields: ['dimproperty.property_name', 'dimmarket.market_name', 'tbldailyhomemetrics.home_count', 'dimproperty.address_one', 'dimproperty.city_name', 'dimproperty.state', 'dimproperty.zip'],
      filters: { 'tbldailyhomemetrics.date_date': 'today', 'tbldailyhomemetrics.active_property_count': '>0', 'tbldailyhomemetrics.home_is_installed': 'Yes' },
      sorts: ['dimproperty.property_name'], limit: '5000'
    })
  })).json();

  const byProp = {};
  for (const r of rows) {
    const name = r['dimproperty.property_name'];
    if (!name) continue;
    if (!byProp[name]) {
      byProp[name] = {
        name,
        market: r['dimmarket.market_name'],
        units: 0,
        address: [r['dimproperty.address_one'], r['dimproperty.city_name'], r['dimproperty.state'], r['dimproperty.zip']].filter(Boolean).join(', '),
      };
    }
    byProp[name].units += r['tbldailyhomemetrics.home_count'] || 0;
  }

  // Failure = in cache but lat==null AND not overridden by manual
  const failures = [];
  for (const p of Object.values(byProp)) {
    if (manual.byProperty?.[p.name]) continue; // manual override exists
    const cached = cache.byProperty?.[p.name];
    if (!cached || cached.lat == null || cached.lng == null) {
      failures.push({ ...p, error: cached?.error || 'never attempted' });
    }
  }
  failures.sort((a, b) => b.units - a.units);

  console.log(`\nTotal portfolio failures: ${failures.length}`);
  console.log(`Total units affected:     ${failures.reduce((s, f) => s + f.units, 0)}\n`);
  console.log('Top failures by unit count:');
  console.log('  units  market                   property_name                    address');
  console.log('  -----  ------------------------ -------------------------------- -------------------------------------------');
  for (const f of failures.slice(0, 30)) {
    console.log(`  ${String(f.units).padStart(5)}  ${(f.market || '').padEnd(24)} ${f.name.padEnd(32).slice(0,32)} ${f.address}`);
  }
  console.log(`\n  ...${failures.length > 30 ? `${failures.length - 30} more failures below` : 'all shown'}`);
  console.log(`\nResolve the worst offenders by editing data/geocodes-manual.json:`);
  console.log(`  "byProperty": { "Property Name": { "lat": 41.8781, "lng": -87.6298, "src": "manual", "note": "..." } }`);
}
main().catch(e => { console.error('FAIL:', e.message); process.exit(1); });
