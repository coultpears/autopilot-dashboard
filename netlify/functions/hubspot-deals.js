// Netlify serverless function: fetches AP Pipeline deals from HubSpot
// Returns: deal name→ID mapping (Closed Won) + pitch counts (matching ops monitor logic)

const PIPELINE_ID = '64402505';
const CLOSED_WON_STAGE = '126194579';
const BATCH_SIZE = 200;
const MAX_PAGES = 25;

// Active pipeline stages — matches ops monitor (New Opportunities → Contract Redline)
const ACTIVE_STAGE_IDS = [
  '126194574',  // New Opportunities
  '128203694',  // Contacted
  '185461262',  // Defining Call Schedule
  '126194575',  // Call Scheduled
  '1225117962', // IC Review
  '126194576',  // Active Opportunities
  '126194577',  // Late Stage Opportunities
  '128915635',  // Contract Discussions
  '126194578',  // Contract Redline
  '126194579',  // Closed Won
  '1321371563', // Email Campaign - Needs Assignment
];

// Central Time helper — returns YYYY-MM-DD in CT
function toCTDate(date) {
  return new Date(date.toLocaleString('en-US', { timeZone: 'America/Chicago' }));
}
function toCTDateStr(date) {
  const ct = toCTDate(date);
  return ct.getFullYear() + '-' + String(ct.getMonth() + 1).padStart(2, '0') + '-' + String(ct.getDate()).padStart(2, '0');
}

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

    // 2. Pitches — matching ops monitor logic exactly:
    //    - Active stages only (New Opps → Contract Redline)
    //    - first_pitch_date__ap_ in date range
    //    - Exclude future dates (Central Time)
    //    - Week = Monday → today (CT)
    const nowCT = toCTDate(new Date());
    const todayStr = toCTDateStr(new Date());
    const curYear = nowCT.getFullYear();
    const prevYear = curYear - 1;
    const curMonth = nowCT.getMonth();

    // Week boundaries in CT (Monday-based)
    const dayOfWeek = nowCT.getDay();
    const daysSinceMonday = dayOfWeek === 0 ? 6 : dayOfWeek - 1;
    const weekStartDate = new Date(nowCT);
    weekStartDate.setDate(nowCT.getDate() - daysSinceMonday);
    weekStartDate.setHours(0, 0, 0, 0);
    const weekStartStr = weekStartDate.getFullYear() + '-' + String(weekStartDate.getMonth() + 1).padStart(2, '0') + '-' + String(weekStartDate.getDate()).padStart(2, '0');

    // Previous week: Mon-Sun before current week
    const prevWeekStartDate = new Date(weekStartDate);
    prevWeekStartDate.setDate(prevWeekStartDate.getDate() - 7);
    const prevWeekEndDate = new Date(weekStartDate);
    prevWeekEndDate.setDate(prevWeekEndDate.getDate() - 1);
    const prevWeekStartStr = prevWeekStartDate.getFullYear() + '-' + String(prevWeekStartDate.getMonth() + 1).padStart(2, '0') + '-' + String(prevWeekStartDate.getDate()).padStart(2, '0');
    const prevWeekEndStr = prevWeekEndDate.getFullYear() + '-' + String(prevWeekEndDate.getMonth() + 1).padStart(2, '0') + '-' + String(prevWeekEndDate.getDate()).padStart(2, '0');

    // Fetch this week's pitches from active stages, then filter by date string (CT)
    // Using a broad date filter then post-filtering for CT accuracy + future date exclusion
    async function fetchPitchesInRange(gteStr, lteStr) {
      // Search with stage filter — HubSpot IN operator requires filterGroups per stage
      // Instead, fetch from all pipeline deals with pitch date in range, then filter stages client-side
      const gteMs = new Date(gteStr + 'T00:00:00-06:00').getTime(); // CT approx
      const lteMs = new Date(lteStr + 'T23:59:59-05:00').getTime();
      const results = await searchDeals(token, [
        { propertyName: 'pipeline', operator: 'EQ', value: PIPELINE_ID },
        { propertyName: 'first_pitch_date__ap_', operator: 'GTE', value: String(gteMs) },
        { propertyName: 'first_pitch_date__ap_', operator: 'LTE', value: String(lteMs) }
      ], ['first_pitch_date__ap_', 'hubspot_owner_id', 'dealstage'], 5);

      // Post-filter: active stages only + date string check (CT) + no future dates
      return results.filter(d => {
        if (!ACTIVE_STAGE_IDS.includes(d.properties.dealstage)) return false;
        const pd = (d.properties.first_pitch_date__ap_ || '').slice(0, 10);
        if (!pd || pd > todayStr) return false; // exclude future dates
        return pd >= gteStr && pd <= lteStr;
      });
    }

    // Parallel: this week + last week
    const [thisWeekDeals, lastWeekDeals] = await Promise.all([
      fetchPitchesInRange(weekStartStr, todayStr),
      fetchPitchesInRange(prevWeekStartStr, prevWeekEndStr)
    ]);
    const thisWeek = thisWeekDeals.length;
    const lastWeek = lastWeekDeals.length;

    // Fetch owners for rep name mapping
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

    // Per-rep breakdown for this week
    const pitchesByRep = {};
    for (const deal of thisWeekDeals) {
      const ownerId = deal.properties.hubspot_owner_id;
      const repName = ownerMap[ownerId] || 'Unassigned';
      pitchesByRep[repName] = (pitchesByRep[repName] || 0) + 1;
    }

    // Monthly counts — for sparklines + YoY badge
    // Use same approach: fetch + post-filter for active stages & CT dates
    // Count pitches in a month — split into 2 queries (max 5 filterGroups each)
    async function countPitchesInMonth(year, month) {
      const mStartStr = year + '-' + String(month + 1).padStart(2, '0') + '-01';
      const lastDay = new Date(year, month + 1, 0).getDate();
      const mEndStr = year + '-' + String(month + 1).padStart(2, '0') + '-' + String(lastDay).padStart(2, '0');
      const gteMs = new Date(mStartStr + 'T00:00:00-06:00').getTime();
      const lteMs = new Date(mEndStr + 'T23:59:59-05:00').getTime();

      const makeQuery = (stageIds) => fetchWithRetry('https://api.hubapi.com/crm/v3/objects/deals/search', {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          filterGroups: stageIds.map(stageId => ({
            filters: [
              { propertyName: 'pipeline', operator: 'EQ', value: PIPELINE_ID },
              { propertyName: 'dealstage', operator: 'EQ', value: stageId },
              { propertyName: 'first_pitch_date__ap_', operator: 'GTE', value: String(gteMs) },
              { propertyName: 'first_pitch_date__ap_', operator: 'LTE', value: String(lteMs) }
            ]
          })),
          properties: ['first_pitch_date__ap_'],
          limit: 1
        })
      }).then(r => r.ok ? r.json() : { total: 0 }).then(d => d.total || 0);

      // Split 11 stages into chunks of 5 (HubSpot max filterGroups = 5)
      const chunks = [];
      for (let i = 0; i < ACTIVE_STAGE_IDS.length; i += 5) {
        chunks.push(makeQuery(ACTIVE_STAGE_IDS.slice(i, i + 5)));
      }
      const counts = await Promise.all(chunks);
      return counts.reduce((s, c) => s + c, 0);
    }

    const pitchByMonth = {};
    // Run monthly queries in batches of 4 to avoid rate limits (each month = 3 API calls)
    const monthList = [];
    for (let y = prevYear; y <= curYear; y++) {
      const maxM = y === curYear ? curMonth : 11;
      for (let m = 0; m <= maxM; m++) {
        monthList.push({ year: y, month: m, key: y + '-' + String(m + 1).padStart(2, '0') });
      }
    }
    for (let i = 0; i < monthList.length; i += 4) {
      const batch = monthList.slice(i, i + 4);
      const results = await Promise.all(batch.map(({ year, month }) => countPitchesInMonth(year, month)));
      batch.forEach(({ key }, idx) => { pitchByMonth[key] = results[idx]; });
    }

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
