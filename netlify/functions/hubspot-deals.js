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

    // 2. Pitches — use targeted queries for accurate counts
    const now = new Date();
    const curYear = now.getFullYear();
    const prevYear = curYear - 1;
    const curMonth = now.getMonth();

    // Helper: count pitches in a date range
    async function countPitches(gte, lte) {
      const resp = await fetchWithRetry('https://api.hubapi.com/crm/v3/objects/deals/search', {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          filterGroups: [{ filters: [
            { propertyName: 'pipeline', operator: 'EQ', value: PIPELINE_ID },
            { propertyName: 'first_pitch_date__ap_', operator: 'GTE', value: String(gte.getTime()) },
            { propertyName: 'first_pitch_date__ap_', operator: 'LTE', value: String(lte.getTime()) }
          ]}],
          properties: ['first_pitch_date__ap_'],
          limit: 1
        })
      });
      if (!resp.ok) return 0;
      const data = await resp.json();
      return data.total || 0;
    }

    // Get week boundaries (Monday to Sunday, matching ops monitor)
    const dayOfWeek = now.getDay();
    const daysSinceMonday = dayOfWeek === 0 ? 6 : dayOfWeek - 1;
    const weekStart = new Date(curYear, now.getMonth(), now.getDate() - daysSinceMonday);
    const weekEnd = new Date(curYear, now.getMonth(), now.getDate(), 23, 59, 59);
    const prevWeekStart = new Date(weekStart.getTime() - 7 * 86400000);
    const prevWeekEnd = new Date(weekStart.getTime() - 1);

    // Fetch this week's pitches with rep info for per-rep breakdown
    async function fetchPitchesWithReps(gte, lte) {
      const results = await searchDeals(token, [
        { propertyName: 'pipeline', operator: 'EQ', value: PIPELINE_ID },
        { propertyName: 'first_pitch_date__ap_', operator: 'GTE', value: String(gte.getTime()) },
        { propertyName: 'first_pitch_date__ap_', operator: 'LTE', value: String(lte.getTime()) }
      ], ['first_pitch_date__ap_', 'hubspot_owner_id'], 5);
      return results;
    }

    // Fetch owners list for name mapping
    const ownersResp = await fetchWithRetry('https://api.hubapi.com/crm/v3/owners', {
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }
    });
    const ownerMap = {};
    if (ownersResp.ok) {
      const ownerData = await ownersResp.json();
      for (const o of ownerData.results || []) {
        ownerMap[o.id] = o.firstName || o.email?.split('@')[0] || 'Unknown';
      }
    }

    // Parallel: this week pitches (full), last week count
    const [thisWeekDeals, lastWeek] = await Promise.all([
      fetchPitchesWithReps(weekStart, weekEnd),
      countPitches(prevWeekStart, prevWeekEnd)
    ]);
    const thisWeek = thisWeekDeals.length;

    // Build per-rep breakdown for this week
    const pitchesByRep = {};
    for (const deal of thisWeekDeals) {
      const ownerId = deal.properties.hubspot_owner_id;
      const repName = ownerMap[ownerId] || 'Unassigned';
      pitchesByRep[repName] = (pitchesByRep[repName] || 0) + 1;
    }

    // Monthly counts — current year + prev year same months (parallel batches)
    const pitchByMonth = {};
    const monthQueries = [];
    for (let y = prevYear; y <= curYear; y++) {
      const maxM = y === curYear ? curMonth : 11;
      for (let m = 0; m <= maxM; m++) {
        const mStart = new Date(y, m, 1);
        const mEnd = new Date(y, m + 1, 0, 23, 59, 59);
        const key = y + '-' + String(m + 1).padStart(2, '0');
        monthQueries.push(countPitches(mStart, mEnd).then(c => { pitchByMonth[key] = c; }));
      }
    }
    await Promise.all(monthQueries);

    const pitchByWeek = { thisWeek, lastWeek };

    return {
      statusCode: 200,
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'public, max-age=3600'
      },
      body: JSON.stringify({
        deals: dealMap,
        count: Object.keys(dealMap).length,
        pitches: { byWeek: pitchByWeek, byMonth: pitchByMonth, byRep: pitchesByRep, total: Object.values(pitchByMonth).reduce((s,v) => s+v, 0) }
      })
    };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};
