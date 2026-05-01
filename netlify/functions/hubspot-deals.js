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

    // 2. Pitches — count of unique deals with `first_pitch_date__ap_` set in the
    //    date range. Pitch count is a measure of REP ACTIVITY in the period; it
    //    must NOT depend on where the deal ended up afterward. We previously
    //    filtered to ACTIVE_STAGE_IDS (which excludes Lost Deal, Not Qualified,
    //    Doesn't Meet Standards, Not Reached, Existing Opp Follow Up, etc.), but
    //    that silently dropped real pitches and made rep counts ~11% too low
    //    portfolio-wide and as much as 73% too low for individual reps doing
    //    heavy CoStar outreach (where most pitches end up Lost / Not Qualified).
    //    The previous comment "matches ops monitor logic exactly" was the
    //    reason for the filter; reps reported the resulting numbers as wrong.
    //
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

    // Fetch all pipeline deals with first_pitch_date__ap_ in the date range,
    // then filter for CT accuracy and future-date exclusion. NO stage filter:
    // pitch counts reflect rep activity regardless of subsequent deal outcome.
    async function fetchPitchesInRange(gteStr, lteStr) {
      // HubSpot date-type properties are stored at 00:00 UTC, so filter against UTC midnight.
      // Using a CT offset here pushes GTE to 06:00 UTC and silently drops deals whose pitch date == gteStr.
      const gteMs = new Date(gteStr + 'T00:00:00Z').getTime();
      const lteMs = new Date(lteStr + 'T23:59:59Z').getTime();
      // 25 pages × 200 = 5000 deals/range — well above any plausible single-month pitch volume.
      const results = await searchDeals(token, [
        { propertyName: 'pipeline', operator: 'EQ', value: PIPELINE_ID },
        { propertyName: 'first_pitch_date__ap_', operator: 'GTE', value: String(gteMs) },
        { propertyName: 'first_pitch_date__ap_', operator: 'LTE', value: String(lteMs) }
      ], ['first_pitch_date__ap_', 'hubspot_owner_id', 'dealstage']);

      // Date string check (CT) + no future dates. No stage filter — see comment above.
      return results.filter(d => {
        const pd = (d.properties.first_pitch_date__ap_ || '').slice(0, 10);
        if (!pd || pd > todayStr) return false;
        return pd >= gteStr && pd <= lteStr;
      });
    }

    // Month boundaries in CT
    const monthStartStr = nowCT.getFullYear() + '-' + String(nowCT.getMonth() + 1).padStart(2, '0') + '-01';

    // Last month boundaries in CT
    const lastMonthEnd = new Date(nowCT.getFullYear(), nowCT.getMonth(), 0); // last day of prev month
    const lastMonthStart = new Date(lastMonthEnd.getFullYear(), lastMonthEnd.getMonth(), 1);
    const lastMonthStartStr = lastMonthStart.getFullYear() + '-' + String(lastMonthStart.getMonth() + 1).padStart(2, '0') + '-01';
    const lastMonthEndStr = lastMonthEnd.getFullYear() + '-' + String(lastMonthEnd.getMonth() + 1).padStart(2, '0') + '-' + String(lastMonthEnd.getDate()).padStart(2, '0');

    // Parallel: this week + last week + this month + last month
    const [thisWeekDeals, lastWeekDeals, thisMonthDeals, lastMonthDeals] = await Promise.all([
      fetchPitchesInRange(weekStartStr, todayStr),
      fetchPitchesInRange(prevWeekStartStr, prevWeekEndStr),
      fetchPitchesInRange(monthStartStr, todayStr),
      fetchPitchesInRange(lastMonthStartStr, lastMonthEndStr)
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

    // Per-rep breakdown for last week
    const lastWeekByRep = {};
    for (const deal of lastWeekDeals) {
      const ownerId = deal.properties.hubspot_owner_id;
      const repName = ownerMap[ownerId] || 'Unassigned';
      lastWeekByRep[repName] = (lastWeekByRep[repName] || 0) + 1;
    }

    // Per-rep breakdown for this month
    const monthlyPitchesByRep = {};
    for (const deal of thisMonthDeals) {
      const ownerId = deal.properties.hubspot_owner_id;
      const repName = ownerMap[ownerId] || 'Unassigned';
      monthlyPitchesByRep[repName] = (monthlyPitchesByRep[repName] || 0) + 1;
    }

    // Per-rep breakdown for last month
    const lastMonthByRep = {};
    for (const deal of lastMonthDeals) {
      const ownerId = deal.properties.hubspot_owner_id;
      const repName = ownerMap[ownerId] || 'Unassigned';
      lastMonthByRep[repName] = (lastMonthByRep[repName] || 0) + 1;
    }

    // Monthly pitch counts — simple pipeline-wide count for sparklines/YoY.
    // For the CURRENT month we cap the upper bound at today so the headline
    // doesn't include future-dated pitches (would otherwise show e.g. "9 pitches
    // in May" on May 1 when only 1 has actually happened, and the per-rep
    // breakdown — which already excludes future dates — would disagree).
    async function countPitchesInMonth(year, month) {
      const mStartStr = year + '-' + String(month + 1).padStart(2, '0') + '-01';
      const lastDay = new Date(year, month + 1, 0).getDate();
      const mEndStr = year + '-' + String(month + 1).padStart(2, '0') + '-' + String(lastDay).padStart(2, '0');
      const isCurrentMonth = year === curYear && month === curMonth;
      const effectiveEndStr = isCurrentMonth ? todayStr : mEndStr;
      const gteMs = new Date(mStartStr + 'T00:00:00Z').getTime();
      const lteMs = new Date(effectiveEndStr + 'T23:59:59Z').getTime();
      const resp = await fetchWithRetry('https://api.hubapi.com/crm/v3/objects/deals/search', {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          filterGroups: [{ filters: [
            { propertyName: 'pipeline', operator: 'EQ', value: PIPELINE_ID },
            { propertyName: 'first_pitch_date__ap_', operator: 'GTE', value: String(gteMs) },
            { propertyName: 'first_pitch_date__ap_', operator: 'LTE', value: String(lteMs) }
          ]}],
          properties: ['first_pitch_date__ap_'],
          limit: 1
        })
      });
      if (!resp.ok) return 0;
      const data = await resp.json();
      return data.total || 0;
    }

    const pitchByMonth = {};
    const monthList = [];
    for (let y = prevYear; y <= curYear; y++) {
      const maxM = y === curYear ? curMonth : 11;
      for (let m = 0; m <= maxM; m++) {
        monthList.push({ year: y, month: m, key: y + '-' + String(m + 1).padStart(2, '0') });
      }
    }
    // Batch 6 months at a time (6 API calls per batch)
    for (let i = 0; i < monthList.length; i += 6) {
      const batch = monthList.slice(i, i + 6);
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
        pitches: { byWeek: pitchByWeek, byMonth: pitchByMonth, byRep: pitchesByRep, lastWeekByRep, monthlyByRep: monthlyPitchesByRep, lastMonthByRep, total: Object.values(pitchByMonth).reduce((s,v) => s+v, 0) }
      })
    };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};
