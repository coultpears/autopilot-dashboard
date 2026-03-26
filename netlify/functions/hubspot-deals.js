// Netlify serverless function: fetches AP Pipeline deals from HubSpot
// Returns: deal name→ID mapping (Closed Won) + pitch counts by week

const PIPELINE_ID = '64402505';
const CLOSED_WON_STAGE = '126194579';
const BATCH_SIZE = 200;
const MAX_PAGES = 25;

async function fetchWithRetry(url, opts, retries = 3) {
  for (let i = 0; i < retries; i++) {
    const resp = await fetch(url, opts);
    if (resp.status === 429) {
      const wait = Math.pow(2, i + 1) * 1000;
      await new Promise(r => setTimeout(r, wait));
      continue;
    }
    return resp;
  }
  return fetch(url, opts);
}

async function searchDeals(token, filters, properties, maxPages = MAX_PAGES) {
  const results = [];
  let after = 0;
  let pages = 0;

  while (pages < maxPages) {
    const resp = await fetchWithRetry('https://api.hubapi.com/crm/v3/objects/deals/search', {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        filterGroups: [{ filters }],
        properties,
        limit: BATCH_SIZE,
        after
      })
    });

    if (!resp.ok) {
      const err = await resp.text();
      throw new Error(`HubSpot ${resp.status}: ${err}`);
    }

    const data = await resp.json();
    results.push(...(data.results || []));

    if (data.paging && data.paging.next && data.paging.next.after) {
      after = data.paging.next.after;
      pages++;
    } else {
      break;
    }
  }
  return results;
}

exports.handler = async () => {
  const token = process.env.HUBSPOT_TOKEN;
  if (!token) {
    return { statusCode: 500, body: JSON.stringify({ error: 'HUBSPOT_TOKEN not configured' }) };
  }

  try {
    // 1. Closed Won deals for property linking
    const wonDeals = await searchDeals(token, [
      { propertyName: 'pipeline', operator: 'EQ', value: PIPELINE_ID },
      { propertyName: 'dealstage', operator: 'EQ', value: CLOSED_WON_STAGE }
    ], ['dealname', 'property_name']);

    const dealMap = {};
    for (const deal of wonDeals) {
      const dealName = (deal.properties.dealname || '').trim();
      const propName = (deal.properties.property_name || '').trim();
      if (dealName) dealMap[dealName.toLowerCase()] = deal.id;
      if (propName) dealMap[propName.toLowerCase()] = deal.id;
    }

    // 2. Pitches — all AP Pipeline deals with a first_pitch_date in 2025 or 2026
    const now = new Date();
    const yearStart = new Date(now.getFullYear(), 0, 1);
    const prevYearStart = new Date(now.getFullYear() - 1, 0, 1);

    const pitchDeals = await searchDeals(token, [
      { propertyName: 'pipeline', operator: 'EQ', value: PIPELINE_ID },
      { propertyName: 'first_pitch_date__ap_', operator: 'GTE', value: String(prevYearStart.getTime()) }
    ], ['first_pitch_date__ap_'], 15);

    // Aggregate pitches by week (ISO week)
    const pitchByWeek = {}; // "2026-W13" → count
    for (const deal of pitchDeals) {
      const pd = deal.properties.first_pitch_date__ap_;
      if (!pd) continue;
      const d = new Date(pd);
      if (isNaN(d)) continue;
      const year = d.getFullYear();
      const jan1 = new Date(year, 0, 1);
      const week = Math.ceil(((d - jan1) / 86400000 + jan1.getDay() + 1) / 7);
      const key = year + '-W' + String(week).padStart(2, '0');
      pitchByWeek[key] = (pitchByWeek[key] || 0) + 1;
    }

    // Also compute monthly totals
    const pitchByMonth = {};
    for (const deal of pitchDeals) {
      const pd = deal.properties.first_pitch_date__ap_;
      if (!pd) continue;
      const d = new Date(pd);
      if (isNaN(d)) continue;
      const key = d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0');
      pitchByMonth[key] = (pitchByMonth[key] || 0) + 1;
    }

    return {
      statusCode: 200,
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'public, max-age=3600'
      },
      body: JSON.stringify({
        deals: dealMap,
        count: Object.keys(dealMap).length,
        pitches: { byWeek: pitchByWeek, byMonth: pitchByMonth, total: pitchDeals.length }
      })
    };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};
