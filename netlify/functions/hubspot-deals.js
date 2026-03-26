// Netlify serverless function: fetches AP Pipeline deals from HubSpot
// Returns a mapping of deal name → deal ID for linking in the dashboard

const PIPELINE_ID = '64402505';
const BATCH_SIZE = 100;

exports.handler = async () => {
  const token = process.env.HUBSPOT_TOKEN;
  if (!token) {
    return { statusCode: 500, body: JSON.stringify({ error: 'HUBSPOT_TOKEN not configured' }) };
  }

  try {
    const dealMap = {};
    let after = undefined;
    let pages = 0;
    const MAX_PAGES = 30; // safety cap: 3000 deals max

    while (pages < MAX_PAGES) {
      const url = new URL('https://api.hubapi.com/crm/v3/objects/deals');
      url.searchParams.set('limit', BATCH_SIZE);
      url.searchParams.set('properties', 'dealname,pipeline');
      if (after) url.searchParams.set('after', after);

      const resp = await fetch(url.toString(), {
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }
      });

      if (!resp.ok) {
        const err = await resp.text();
        return { statusCode: resp.status, body: JSON.stringify({ error: `HubSpot API error: ${resp.status}`, detail: err }) };
      }

      const data = await resp.json();
      for (const deal of data.results || []) {
        const name = (deal.properties.dealname || '').trim();
        const pipeline = deal.properties.pipeline;
        if (name && pipeline === PIPELINE_ID) {
          // Use lowercase key for fuzzy matching
          dealMap[name.toLowerCase()] = deal.id;
        }
      }

      if (data.paging && data.paging.next && data.paging.next.after) {
        after = data.paging.next.after;
        pages++;
      } else {
        break;
      }
    }

    return {
      statusCode: 200,
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'public, max-age=300' // cache 5 min
      },
      body: JSON.stringify({ deals: dealMap, count: Object.keys(dealMap).length })
    };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};
