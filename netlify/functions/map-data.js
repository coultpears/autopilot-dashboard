// Netlify serverless function: CoStar-sourced targets for AP Supply Map
// Replaces the old expansion-pipeline target feed.
// Targets = AP pipeline deals with `costar_last_synced` set, active stages only.

const AP_PIPELINE = '64402505';
const BATCH_SIZE = 100;
const MAX_PAGES = 30;

// AP pipeline stage id → human label (fetched dynamically from HubSpot, fallback to this map)
const STAGE_LABELS = {
  '126194574': 'New Opportunities',
  '128203694': 'Contacted',
  '185461262': 'Defining Call Schedule',
  '126194575': 'Call Scheduled',
  '1225117962': 'IC Review / Proforma Request',
  '126194576': 'Active Opportunities',
  '126194577': 'Late Stage Opportunities',
  '128915635': 'Contract Discussions',
  '126194578': 'Contract Redline',
  '126194579': 'Closed Won',
  '1321371563': 'Email Campaign - Needs Assignment',
  '1343039756': 'Test Stage (CoStar)',
  '1327025670': 'Diamond Deals',
  '1083859809': 'Existing Opportunity Follow Up',
  '128917623': 'Pilot',
  '131692473': 'Request for housing / Coho',
  // Closed stages (excluded from targets)
  '1097165102': 'Lost Deal',
  '129423023': 'Not Qualified Lead',
  '138986106': "Doesn't Meet Landing Standards",
  '1009548619': 'Not Reached',
  '126194580': '(Old) Closed Lost',
};

// Dealstages that disqualify a deal as a "target" (all closed stages — won or lost)
const EXCLUDED_STAGES = new Set([
  '126194579', // Closed Won
  '1097165102', // Lost Deal
  '129423023', // Not Qualified Lead
  '138986106', // Doesn't Meet Landing Standards
  '1009548619', // Not Reached
  '126194580', // (Old) Closed Lost
]);

// Looker token cache (identical pattern to grid-data.js)
let _cachedToken = null;
let _tokenExpiry = 0;

const geocodeCache = require('./_geocode-cache.js');

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

async function batchGetCompanyAssociations(token, dealIds) {
  const map = {};
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

async function batchGetCompanyNames(token, companyIds) {
  const map = {};
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

async function fetchAllOwners(token) {
  // GET /crm/v3/owners paginated
  const map = {};
  let after = null;
  for (let pages = 0; pages < 20; pages++) {
    const path = '/crm/v3/owners' + (after ? `?after=${after}&limit=500` : '?limit=500');
    const data = await hsApi(token, 'GET', path);
    if (!data?.results) break;
    for (const o of data.results) {
      const name = [o.firstName, o.lastName].filter(Boolean).join(' ').trim() || o.email || '';
      if (o.id && name) map[o.id] = name;
    }
    if (data.paging?.next?.after) after = data.paging.next.after;
    else break;
  }
  return map;
}

// Geocoding happens client-side (Nominatim in browser + localStorage cache)
// so the function returns fast and doesn't block on Netlify's timeout.

exports.handler = async () => {
  const token = process.env.HUBSPOT_TOKEN;
  if (!token) {
    return { statusCode: 500, body: JSON.stringify({ error: 'HUBSPOT_TOKEN not configured' }) };
  }

  try {
    const props = [
      'dealname', 'property_name',
      'property_street_address', 'property_city', 'property_state', 'property_zip',
      'dealstage', 'hubspot_owner_id', 'company_name',
      'costar_total_units', 'costar_asking_rent_per_unit',
      'costar_year_built', 'costar_year_renovated', 'costar_star_rating',
      'costar_market_segment', 'costar_recorded_owner', 'costar_true_owner_contact',
      'costar_property_notes', 'costar_last_synced',
      'costar_leasing_company_website',
      'vacancy__', 'vacant_units', 'asset_class', 'lease_up_signal',
      'property_website',
    ];

    // Query: AP pipeline + costar_last_synced set
    const filters = [
      { propertyName: 'pipeline', operator: 'EQ', value: AP_PIPELINE },
      { propertyName: 'costar_last_synced', operator: 'HAS_PROPERTY' },
    ];

    const [deals, owners] = await Promise.all([
      searchDeals(token, filters, props),
      fetchAllOwners(token),
    ]);

    // Filter out closed stages
    const activeDeals = deals.filter(d => {
      const stageId = d.properties?.dealstage || '';
      const label = (STAGE_LABELS[stageId] || '').toLowerCase();
      if (EXCLUDED_STAGES.has(stageId)) return false;
      if (label.includes('closed lost') || label.includes('disqualified')) return false;
      return true;
    });

    // Resolve partner names: prefer costar_true_owner_contact / recorded_owner / company_name
    // Only fetch company associations for deals still missing partner after property-level lookup
    const dealPartnerMap = {};
    const needsAssoc = [];
    for (const deal of activeDeals) {
      const p = deal.properties;
      const partner = (p.costar_true_owner_contact || p.costar_recorded_owner || p.company_name || '').trim();
      if (partner) dealPartnerMap[deal.id] = partner;
      else needsAssoc.push(deal.id);
    }
    if (needsAssoc.length > 0) {
      const assocMap = await batchGetCompanyAssociations(token, needsAssoc);
      const companyIds = Object.values(assocMap);
      if (companyIds.length) {
        const nameMap = await batchGetCompanyNames(token, companyIds);
        for (const [dealId, companyId] of Object.entries(assocMap)) {
          if (nameMap[companyId]) dealPartnerMap[dealId] = nameMap[companyId];
        }
      }
    }

    // Build targets (pre-geocoding)
    const targets = activeDeals.map(deal => {
      const p = deal.properties;
      const name = (p.property_name || p.dealname || '').trim();
      if (!name) return null;

      const street = (p.property_street_address || '').trim();
      const city = (p.property_city || '').trim();
      const state = (p.property_state || '').trim();
      const zip = (p.property_zip || '').trim();
      const market = city && state ? `${city}, ${state}` : city || state || '';
      const addrParts = [street, city, state, zip].filter(Boolean);
      const address = addrParts.length >= 2 ? addrParts.join(', ') : '';

      const stageId = p.dealstage || '';

      const totalUnits = parseInt(p.costar_total_units) || 0;
      const vacancyPct = parseFloat(p.vacancy__) || 0;
      let vacantUnits = parseInt(p.vacant_units) || 0;
      let vacantEstimated = false;
      // Fallback: calculate vacant units from vacancy% * totalUnits when first-hand count missing
      if (!vacantUnits && vacancyPct > 0 && totalUnits > 0) {
        vacantUnits = Math.round((vacancyPct / 100) * totalUnits);
        vacantEstimated = true;
      }

      // Geocoded coords (from data/geocodes.json, populated by scripts/backfill-geocoding.js).
      // Falls back to null when address never geocoded — client jitters around market center.
      const geo = address ? geocodeCache.lookupByAddress(address) : null;
      return {
        dealId: deal.id,
        name,
        address,
        market,
        coords: geo ? { lat: geo.lat, lng: geo.lng } : null,
        coordsSource: geo ? geo.src : null,
        partner: dealPartnerMap[deal.id] || '—',
        rep: owners[p.hubspot_owner_id] || '',
        stage: STAGE_LABELS[stageId] || stageId,
        stageId,
        totalUnits: totalUnits || parseInt(p.vacant_units) || 0,
        vacancy: vacancyPct,
        vacantUnits,
        vacantEstimated,
        askingRent: parseFloat(p.costar_asking_rent_per_unit) || null,
        yearBuilt: parseInt(p.costar_year_built) || null,
        yearRenovated: parseInt(p.costar_year_renovated) || null,
        assetClass: (p.asset_class || '').trim(),
        starRating: parseFloat(p.costar_star_rating) || null,
        marketSegment: (p.costar_market_segment || '').trim(),
        notes: (p.costar_property_notes || '').trim(),
        leaseUp: p.lease_up_signal === 'true',
        lastSynced: p.costar_last_synced ? parseInt(p.costar_last_synced) : null,
        // Prefer the curated property_website; fall back to leasing company site so every target has a link
        website: (p.property_website || '').trim() || (p.costar_leasing_company_website || '').trim() || null,
        websiteSource: (p.property_website || '').trim() ? 'property' : ((p.costar_leasing_company_website || '').trim() ? 'leasing_company' : null),
      };
    }).filter(Boolean);

    // Server-side geocoding via repo-committed cache (data/geocodes.json).
    // Targets without a cached geocode get `coords: null` — client jitters around the market center.
    // Re-run scripts/backfill-geocoding.js to extend the cache for newly-added deals.

    return {
      statusCode: 200,
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'public, max-age=300, stale-while-revalidate=1800',
        'Netlify-CDN-Cache-Control': 'public, max-age=1800, stale-while-revalidate=7200',
      },
      body: JSON.stringify({
        targets,
        targetCount: targets.length,
        marketCount: new Set(targets.map(t => t.market).filter(Boolean)).size,
        // Legacy fields kept empty for backward compat with map.html
        enrichment: {},
        count: 0,
      })
    };
  } catch (err) {
    console.error(err);
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};
