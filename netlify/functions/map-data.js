// Netlify serverless function: fetches enrichment + expansion target data for AP Supply Map
// Returns: enrichment (vacancy/concessions per property) + expansion targets (net new opportunities)
// Partner names resolved via deal company_name field + HubSpot company associations

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

async function hsApi(token, method, path, body) {
  const opts = {
    method,
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }
  };
  if (body) opts.body = JSON.stringify(body);
  const resp = await fetchWithRetry(`https://api.hubapi.com${path}`, opts);
  if (!resp.ok) return null;
  return resp.json();
}

async function searchDeals(token, filters, properties) {
  const results = [];
  let after = 0;
  let pages = 0;
  while (pages < MAX_PAGES) {
    const data = await hsApi(token, 'POST', '/crm/v3/objects/deals/search', {
      filterGroups: [{ filters }],
      properties,
      limit: BATCH_SIZE,
      after
    });
    if (!data) break;
    results.push(...(data.results || []));
    if (data.paging?.next?.after) { after = data.paging.next.after; pages++; }
    else break;
  }
  return results;
}

// Batch fetch deal→company associations
// HubSpot v4 batch: POST /crm/v4/associations/deals/companies/batch/read
async function batchGetCompanyAssociations(token, dealIds) {
  const map = {}; // dealId -> companyId
  // Process in chunks of 100
  for (let i = 0; i < dealIds.length; i += 100) {
    const chunk = dealIds.slice(i, i + 100);
    const data = await hsApi(token, 'POST', '/crm/v4/associations/deals/companies/batch/read', {
      inputs: chunk.map(id => ({ id }))
    });
    if (data?.results) {
      for (const r of data.results) {
        const dealId = r.from?.id;
        const companyId = r.to?.[0]?.toObjectId;
        if (dealId && companyId) map[dealId] = companyId;
      }
    }
    if (i + 100 < dealIds.length) await new Promise(r => setTimeout(r, 200));
  }
  return map;
}

// Batch fetch company names
async function batchGetCompanyNames(token, companyIds) {
  const map = {}; // companyId -> name
  const unique = [...new Set(companyIds)];
  for (let i = 0; i < unique.length; i += 100) {
    const chunk = unique.slice(i, i + 100);
    const data = await hsApi(token, 'POST', '/crm/v3/objects/companies/batch/read', {
      inputs: chunk.map(id => ({ id })),
      properties: ['name']
    });
    if (data?.results) {
      for (const c of data.results) {
        if (c.id && c.properties?.name) map[c.id] = c.properties.name;
      }
    }
    if (i + 100 < unique.length) await new Promise(r => setTimeout(r, 200));
  }
  return map;
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
      'partner_company', 'company_name', 'deal_category', 'lease_up_signal',
      'last_enriched_date'
    ];

    const [apDeals, expDeals] = await Promise.all([
      searchDeals(token, [
        { propertyName: 'pipeline', operator: 'EQ', value: AP_PIPELINE }
      ], props),
      searchDeals(token, [
        { propertyName: 'pipeline', operator: 'EQ', value: EXPANSION_PIPELINE }
      ], props)
    ]);

    // --- Resolve partner names for expansion deals ---
    // Step 1: Use company_name or partner_company deal property where available
    const needsAssociation = []; // deal IDs that still need partner name
    const dealPartnerMap = {}; // dealId -> partner name

    for (const deal of expDeals) {
      const p = deal.properties;
      const partner = (p.company_name || p.partner_company || '').trim();
      if (partner) {
        dealPartnerMap[deal.id] = partner;
      } else {
        needsAssociation.push(deal.id);
      }
    }

    // Step 2: Batch fetch company associations for deals missing partner name
    if (needsAssociation.length > 0) {
      const assocMap = await batchGetCompanyAssociations(token, needsAssociation);
      const companyIds = Object.values(assocMap);

      if (companyIds.length > 0) {
        const nameMap = await batchGetCompanyNames(token, companyIds);
        for (const [dealId, companyId] of Object.entries(assocMap)) {
          if (nameMap[companyId]) {
            dealPartnerMap[dealId] = nameMap[companyId];
          }
        }
      }
    }

    // Enrichment map
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

    // Existing AP property names
    const existingProperties = new Set();
    for (const deal of apDeals) {
      const name = (deal.properties.property_name || deal.properties.dealname || '').trim().toLowerCase();
      if (name) existingProperties.add(name);
    }

    // Expansion targets: not already in AP
    const targets = [];
    for (const deal of expDeals) {
      const p = deal.properties;
      const name = (p.property_name || p.dealname || '').trim();
      if (!name) continue;
      if (existingProperties.has(name.toLowerCase())) continue;

      const city = (p.property_city || '').trim();
      const state = (p.property_state || '').trim();
      const market = city && state ? `${city}, ${state}` : city || state || '';

      targets.push({
        name,
        market,
        partner: dealPartnerMap[deal.id] || '—',
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

    const withPartner = targets.filter(t => t.partner !== '—').length;

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
        targetsWithPartner: withPartner,
        apDeals: apDeals.length,
        expDeals: expDeals.length
      })
    };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};
