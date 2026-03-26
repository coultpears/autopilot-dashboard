// Netlify serverless function: fetches AP Pipeline deals from HubSpot
// Uses search API to filter by pipeline, returns deal name → deal ID mapping

const PIPELINE_ID = '64402505';
const BATCH_SIZE = 200;
const MAX_PAGES = 25;

async function fetchWithRetry(url, opts, retries = 3) {
  for (let i = 0; i < retries; i++) {
    const resp = await fetch(url, opts);
    if (resp.status === 429) {
      const wait = Math.pow(2, i + 1) * 1000; // 2s, 4s, 8s
      await new Promise(r => setTimeout(r, wait));
      continue;
    }
    return resp;
  }
  return fetch(url, opts); // final attempt
}

exports.handler = async () => {
  const token = process.env.HUBSPOT_TOKEN;
  if (!token) {
    return { statusCode: 500, body: JSON.stringify({ error: 'HUBSPOT_TOKEN not configured' }) };
  }

  try {
    const dealMap = {};
    let after = 0;
    let pages = 0;

    while (pages < MAX_PAGES) {
      const resp = await fetchWithRetry('https://api.hubapi.com/crm/v3/objects/deals/search', {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          filterGroups: [{
            filters: [{
              propertyName: 'pipeline',
              operator: 'EQ',
              value: PIPELINE_ID
            }]
          }],
          properties: ['dealname'],
          limit: BATCH_SIZE,
          after
        })
      });

      if (!resp.ok) {
        const err = await resp.text();
        return { statusCode: resp.status, body: JSON.stringify({ error: `HubSpot API error: ${resp.status}`, detail: err }) };
      }

      const data = await resp.json();
      for (const deal of data.results || []) {
        const name = (deal.properties.dealname || '').trim();
        if (name) {
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
        'Cache-Control': 'public, max-age=300'
      },
      body: JSON.stringify({ deals: dealMap, count: Object.keys(dealMap).length })
    };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};
