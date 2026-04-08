// Netlify serverless function: fetches enrichment + expansion target data for AP Supply Map
// Returns: enrichment (vacancy/concessions per property) + expansion targets (net new opportunities)

const AP_PIPELINE = '64402505';
const EXPANSION_PIPELINE = '877479748';
const BATCH_SIZE = 200;
const MAX_PAGES = 30;

async function fetchWithRetry(url, opts, retries = 3) {
  for (let i = 0; i < retries; i++) {
    const resp = await fetch(url, opts);
    if (resp.status === 429) {
      await new Promise(r => setTimeout(r, Math.pow(2, i + 1) * 1000));
      continue;
    }
    return resp;
  }
  return fetch(url, opts);
}

async function searchDeals(token, filters, properties) {
  const results = [];
  let after = 0;
  let pages = 0;
  while (pages < MAX_PAGES) {
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
    if (!resp.ok) break;
    const data = await resp.json();
    results.push(...(data.results || []));
    if (data.paging?.next?.after) { after = data.paging.next.after; pages++; }
    else break;
  }
  return results;
}

exports.handler = async () => {
  const token = process.env.HUBSPOT_TOKEN;
  if (!token) {
    return { statusCode: 500, body: JSON.stringify({ error: 'HUBSPOT_TOKEN not configured' }) };
  }

  try {
    const props = [
      'dealname', 'property_name', 'property_city', 'property_state',
      'vacant_units', 'available_units', 'concession_notes', 'vacancy__',
      'total_units__expansion_', 'pipeline', 'dealstage', 'hubspot_owner_id',
      'partner_company', 'deal_category', 'lease_up_signal', 'last_enriched_date'
    ];

    const [apDeals, expDeals] = await Promise.all([
      searchDeals(token, [
        { propertyName: 'pipeline', operator: 'EQ', value: AP_PIPELINE }
      ], props),
      searchDeals(token, [
        { propertyName: 'pipeline', operator: 'EQ', value: EXPANSION_PIPELINE }
      ], props)
    ]);

    // Enrichment map: property name -> vacancy/concession data (from both pipelines)
    const enrichment = {};
    for (const deal of [...apDeals, ...expDeals]) {
      const p = deal.properties;
      const name = (p.property_name || p.dealname || '').trim();
      if (!name) continue;
      const key = name.toLowerCase();

      const vacant = parseInt(p.vacant_units) || 0;
      const available = parseInt(p.available_units) || 0;
      const vacancy = parseFloat(p.vacancy__) || 0;
      const concessions = (p.concession_notes || '').trim();

      if (!enrichment[key] || vacant > (enrichment[key].vacant || 0)) {
        enrichment[key] = {
          name,
          vacant: vacant || enrichment[key]?.vacant || 0,
          available: available || enrichment[key]?.available || 0,
          vacancy: vacancy || enrichment[key]?.vacancy || 0,
          concessions: concessions || enrichment[key]?.concessions || '',
          pipeline: p.pipeline === EXPANSION_PIPELINE ? 'expansion' : 'ap'
        };
      }
    }

    // Build set of existing AP property names (lowercase) so we can exclude from targets
    const existingProperties = new Set();
    for (const deal of apDeals) {
      const name = (deal.properties.property_name || deal.properties.dealname || '').trim().toLowerCase();
      if (name) existingProperties.add(name);
    }

    // Expansion targets: deals in expansion pipeline NOT already in AP
    // These are net-new opportunities
    const targets = [];
    for (const deal of expDeals) {
      const p = deal.properties;
      const name = (p.property_name || p.dealname || '').trim();
      if (!name) continue;
      const key = name.toLowerCase();

      // Skip if this property already exists in AP pipeline
      if (existingProperties.has(key)) continue;

      const city = (p.property_city || '').trim();
      const state = (p.property_state || '').trim();
      const market = city && state ? `${city}, ${state}` : city || state || '';

      targets.push({
        name,
        market,
        partner: (p.partner_company || '').trim() || '—',
        stage: p.dealstage || '',
        vacant: parseInt(p.vacant_units) || 0,
        available: parseInt(p.available_units) || 0,
        totalUnits: parseInt(p.total_units__expansion_) || 0,
        vacancy: parseFloat(p.vacancy__) || 0,
        concessions: (p.concession_notes || '').trim(),
        leaseUp: p.lease_up_signal === 'true',
        lastEnriched: p.last_enriched_date || '',
        dealId: deal.id
      });
    }

    return {
      statusCode: 200,
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'public, max-age=3600'
      },
      body: JSON.stringify({
        enrichment,
        targets,
        count: Object.keys(enrichment).length,
        targetCount: targets.length,
        apDeals: apDeals.length,
        expDeals: expDeals.length
      })
    };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};
