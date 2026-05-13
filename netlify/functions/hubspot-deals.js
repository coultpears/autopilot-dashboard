// Netlify serverless function: fetches AP + Expansion pipeline deals from HubSpot
// Returns:
//   - dealMap: name→ID for Closed Won AP deals (property linking)
//   - pitches: company-deduped pitch counts across both pipelines, by window + category + rep

const aliases = require('./_partner-aliases.js');

const AP_PIPELINE = '64402505';
const EXPANSION_PIPELINE = '877479748';
const PITCH_PIPELINES = [AP_PIPELINE, EXPANSION_PIPELINE];
const CLOSED_WON_STAGE = '126194579';
const BATCH_SIZE = 200;
const MAX_PAGES = 50;
const CATEGORY_KEYS = ['New Logo', 'Expansion Existing', 'Expansion New', 'Other'];

// Normalize HubSpot deal_category values into the buckets the dashboard cares about.
// Real values observed in HubSpot (both pipelines combined):
//   "New Logo" · "Expansion" · "Expansion - New" · "" (untagged)
// We map:
//   "New Logo"                                  → New Logo
//   anything with both "new" and "expansion"    → Expansion New (catches "Expansion - New", "Net New Expansion")
//   anything containing "expansion" otherwise    → Expansion Existing (catches bare "Expansion", "Existing Expansion")
//   else                                         → Other (includes untagged — see memory: ~85/109 BDR-pitched deals have empty/wrong deal_category)
function normalizeCategory(raw) {
  if (!raw) return 'Other';
  const s = String(raw).toLowerCase().trim();
  if (s.includes('new logo')) return 'New Logo';
  if (/\bnew\b/.test(s) && s.includes('expansion')) return 'Expansion New';
  if (s.includes('expansion')) return 'Expansion Existing';
  return 'Other';
}

function emptyCatMap() { return Object.fromEntries(CATEGORY_KEYS.map(k => [k, 0])); }

function toCTDate(date) {
  return new Date(date.toLocaleString('en-US', { timeZone: 'America/Chicago' }));
}
function ymd(date) {
  return date.getFullYear() + '-' + String(date.getMonth() + 1).padStart(2, '0') + '-' + String(date.getDate()).padStart(2, '0');
}
function toCTDateStr(date) { return ymd(toCTDate(date)); }

// Hard 20s per-request timeout via AbortController so a hung HubSpot connection
// can't lock up the whole function. Retries up to 3x on 429 (rate limit) or
// network/timeout error; surfaces non-429 errors back to the caller.
async function fetchWithRetry(url, opts, retries = 3) {
  for (let i = 0; i <= retries; i++) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), 20000);
    try {
      const resp = await fetch(url, { ...opts, signal: ctrl.signal });
      clearTimeout(timer);
      if (resp.status === 429 && i < retries) {
        await new Promise(r => setTimeout(r, Math.pow(2, i + 1) * 1000));
        continue;
      }
      return resp;
    } catch (err) {
      clearTimeout(timer);
      if (i < retries) {
        await new Promise(r => setTimeout(r, Math.pow(2, i + 1) * 500));
        continue;
      }
      throw err;
    }
  }
}

async function hsApi(token, method, path, body) {
  const opts = { method, headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const resp = await fetchWithRetry(`https://api.hubapi.com${path}`, opts);
  if (!resp.ok) return null;
  return resp.json();
}

async function searchDeals(token, filters, properties, maxPages = MAX_PAGES) {
  const results = [];
  let after = 0;
  let pages = 0;
  while (pages < maxPages) {
    const resp = await fetchWithRetry('https://api.hubapi.com/crm/v3/objects/deals/search', {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ filterGroups: [{ filters }], properties, limit: BATCH_SIZE, after })
    });
    if (!resp.ok) {
      const err = await resp.text();
      throw new Error(`HubSpot ${resp.status}: ${err}`);
    }
    const data = await resp.json();
    results.push(...(data.results || []));
    if (data.paging?.next?.after) { after = data.paging.next.after; pages++; }
    else break;
  }
  return results;
}

// Picks the "canonical" company for a deal — the capital owner, not the PMC.
// HubSpot portal has user-defined labels Owner(75) / PMC(77) / Owner & PMC(73),
// but in practice most deals don't use them; they just flag the owner as
// "Primary" (typeId 5). Preference order:
//   1. Explicit user-defined Owner / Owner & PMC label (typeId 75 or 73)
//   2. The Primary-flagged association (typeId 5)
//   3. The first non-PMC association
//   4. First association (last resort)
function pickCanonicalCompany(toArray) {
  if (!toArray || !toArray.length) return null;
  const hasType = (assoc, id) => (assoc.associationTypes || []).some(t => t.typeId === id);
  const ownerLabeled = toArray.find(a => hasType(a, 75) || hasType(a, 73));
  if (ownerLabeled) return ownerLabeled.toObjectId;
  const primary = toArray.find(a => hasType(a, 5));
  if (primary) return primary.toObjectId;
  const nonPmc = toArray.find(a => !hasType(a, 77));
  if (nonPmc) return nonPmc.toObjectId;
  return toArray[0].toObjectId;
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
        const companyId = pickCanonicalCompany(r.to);
        if (dealId && companyId) map[dealId] = String(companyId);
      }
    }
    if (i + 100 < dealIds.length) await new Promise(r => setTimeout(r, 150));
  }
  return map;
}

async function batchGetCompanyNames(token, companyIds) {
  const map = {};
  const unique = [...new Set(companyIds)];
  for (let i = 0; i < unique.length; i += 100) {
    const chunk = unique.slice(i, i + 100);
    const data = await hsApi(token, 'POST', '/crm/v3/objects/companies/batch/read', {
      inputs: chunk.map(id => ({ id })), properties: ['name']
    });
    if (data?.results) {
      for (const c of data.results) {
        if (c.id && c.properties?.name) map[c.id] = c.properties.name;
      }
    }
    if (i + 100 < unique.length) await new Promise(r => setTimeout(r, 150));
  }
  return map;
}

// Build a canonical partner key for dedup. Pulls the company's NAME, runs it
// through the partner-aliases resolver (handles "Stoltz" / "Stolz" style variants),
// then lowercases + trims. Two HubSpot company records with the same display
// name (e.g. duplicate "RREAF Holdings" records) collapse to one partner key.
function partnerKeyFor(companyId, nameMap) {
  if (!companyId) return null;
  const raw = nameMap[companyId];
  if (!raw) return 'company:' + companyId; // fallback when name missing
  const canonical = aliases.lookup(raw) || raw;
  return canonical.toLowerCase().trim();
}

exports.handler = async () => {
  const token = process.env.HUBSPOT_TOKEN;
  if (!token) {
    return { statusCode: 500, body: JSON.stringify({ error: 'HUBSPOT_TOKEN not configured' }) };
  }

  try {
    // ─── 1. Closed Won AP deals → name/ID map for property linking (AP pipeline only) ───
    const wonDeals = await searchDeals(token, [
      { propertyName: 'pipeline', operator: 'EQ', value: AP_PIPELINE },
      { propertyName: 'dealstage', operator: 'EQ', value: CLOSED_WON_STAGE }
    ], ['dealname', 'property_name']);
    const dealMap = {};
    for (const deal of wonDeals) {
      const dealName = (deal.properties.dealname || '').trim();
      const propName = (deal.properties.property_name || '').trim();
      if (dealName) dealMap[dealName.toLowerCase()] = deal.id;
      if (propName) dealMap[propName.toLowerCase()] = deal.id;
    }

    // ─── 2. Date window setup (Central Time) ───
    const nowCT = toCTDate(new Date());
    const todayStr = ymd(nowCT);
    const curYear = nowCT.getFullYear();
    const prevYear = curYear - 1;
    const curMonth = nowCT.getMonth();

    // Week boundaries in CT (Monday-based)
    const dayOfWeek = nowCT.getDay();
    const daysSinceMonday = dayOfWeek === 0 ? 6 : dayOfWeek - 1;
    const weekStartDate = new Date(nowCT); weekStartDate.setDate(nowCT.getDate() - daysSinceMonday); weekStartDate.setHours(0, 0, 0, 0);
    const weekStartStr = ymd(weekStartDate);
    const prevWeekStartDate = new Date(weekStartDate); prevWeekStartDate.setDate(prevWeekStartDate.getDate() - 7);
    const prevWeekEndDate = new Date(weekStartDate); prevWeekEndDate.setDate(prevWeekEndDate.getDate() - 1);
    const prevWeekStartStr = ymd(prevWeekStartDate);
    const prevWeekEndStr = ymd(prevWeekEndDate);

    // Month boundaries in CT
    const monthStartStr = curYear + '-' + String(curMonth + 1).padStart(2, '0') + '-01';
    const lastMonthEnd = new Date(curYear, curMonth, 0);
    const lastMonthStart = new Date(lastMonthEnd.getFullYear(), lastMonthEnd.getMonth(), 1);
    const lastMonthStartStr = ymd(lastMonthStart);
    const lastMonthEndStr = ymd(lastMonthEnd);

    // YTD-only range: 2026-01-01 → today. Earlier history isn't needed; the dashboard
    // is forward-looking from 2026. This is the single biggest perf lever — bounds the
    // pitch search to one year of deals across both pipelines.
    const sparkStartStr = '2026-01-01';

    // ─── 3. Single fetch of all pitched deals (both pipelines) since 2026-01-01 ───
    // HubSpot date-type props are stored at 00:00 UTC, so filter on UTC midnight bounds.
    const gteMs = new Date(sparkStartStr + 'T00:00:00Z').getTime();
    const lteMs = new Date(todayStr + 'T23:59:59Z').getTime();
    const t0 = Date.now();
    const pitchedDeals = await searchDeals(token, [
      { propertyName: 'pipeline', operator: 'IN', values: PITCH_PIPELINES },
      { propertyName: 'first_pitch_date__ap_', operator: 'GTE', value: String(gteMs) },
      { propertyName: 'first_pitch_date__ap_', operator: 'LTE', value: String(lteMs) }
    ], ['first_pitch_date__ap_', 'hubspot_owner_id', 'dealstage', 'deal_category', 'pipeline']);
    console.log(`[hubspot-deals] fetched ${pitchedDeals.length} pitched deals in ${Date.now() - t0}ms`);
    // TEMP DEBUG: per-rep audit for May. Logs every pitched deal in May with owner,
    // company association, pipeline, category, dealstage. Helps verify the dedupe math.
    const _debugAll = {};
    for (const d of pitchedDeals) {
      const pd = (d.properties.first_pitch_date__ap_ || '').slice(0, 10);
      if (pd < '2026-05-01' || pd > todayStr) continue;
      const owner = d.properties.hubspot_owner_id || 'unassigned';
      if (!_debugAll[owner]) _debugAll[owner] = [];
      _debugAll[owner].push({
        id: d.id,
        pd,
        pipe: d.properties.pipeline === EXPANSION_PIPELINE ? 'EXP' : 'AP',
        cat: d.properties.deal_category || '',
        stage: d.properties.dealstage || ''
      });
    }
    console.log('[hubspot-deals] May raw-deal audit by owner_id:');
    console.log(JSON.stringify(_debugAll, null, 2));

    // ─── 4. Fetch company associations + names for every pitched deal ───
    // Two-step: (a) deal→canonical company id, (b) company id→name. The name is
    // then normalized through partner-aliases so dedup is by canonical PARTNER,
    // not by HubSpot record id. This collapses HubSpot duplicates like two
    // separate "RREAF Holdings" records pointing to the same logical partner.
    const t1 = Date.now();
    const dealIds = pitchedDeals.map(d => d.id);
    const dealToCompany = await batchGetCompanyAssociations(token, dealIds);
    const allCompanyIds = Object.values(dealToCompany);
    const companyNameMap = await batchGetCompanyNames(token, allCompanyIds);
    console.log(`[hubspot-deals] fetched ${Object.keys(dealToCompany).length} company assocs + ${Object.keys(companyNameMap).length} names in ${Date.now() - t1}ms`);

    // Annotate each deal. The `companyId` field is actually a PARTNER KEY now —
    // a normalized canonical partner name. Deals with no company association get
    // a unique `deal:<id>` key so they still count once as their own bucket.
    const annotated = pitchedDeals
      .map(d => {
        const pd = (d.properties.first_pitch_date__ap_ || '').slice(0, 10);
        if (!pd || pd > todayStr) return null;
        const cid = dealToCompany[d.id];
        const partnerKey = cid ? partnerKeyFor(cid, companyNameMap) : ('deal:' + d.id);
        return {
          id: d.id,
          pitchDate: pd,
          ownerId: d.properties.hubspot_owner_id || '',
          category: normalizeCategory(d.properties.deal_category),
          companyId: partnerKey,
        };
      })
      .filter(Boolean)
      // Sort by pitch date ASC so "earliest pitched deal in window" wins on dedup.
      .sort((a, b) => a.pitchDate.localeCompare(b.pitchDate));

    // ─── 5. Fetch owner names ───
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

    // ─── 6. Bucket by company within each window ───
    // For each window, walk pitchedDeals in chronological order. Two dedup layers:
    //   - GLOBAL dedup (for team total + team-level category): a company counts once
    //     team-wide, categorized by its earliest pitched deal across all reps.
    //   - PER-REP dedup (for per-rep counts + per-rep category): a company counts once
    //     per rep, categorized by THAT rep's earliest pitched deal of the company.
    //
    // Consequence: sum(byRep) ≥ team total when 2+ reps pitch the same company in the
    // window. Each rep gets credit for unique companies they pitched. The team-level
    // total represents unique companies team-wide (no double counting).
    function bucketWindow(deals, gteStr, lteStr) {
      const seenGlobal = new Set();
      const seenByRep = {};
      const byRep = {};
      const byCategory = emptyCatMap();
      const byRepCategory = {};
      let total = 0;
      for (const d of deals) {
        if (d.pitchDate < gteStr || d.pitchDate > lteStr) continue;
        const rep = ownerMap[d.ownerId] || 'Unassigned';
        // Team-level dedup → total + team byCategory
        if (!seenGlobal.has(d.companyId)) {
          seenGlobal.add(d.companyId);
          byCategory[d.category] = (byCategory[d.category] || 0) + 1;
          total++;
        }
        // Per-rep dedup → byRep + byRepCategory
        if (!seenByRep[rep]) seenByRep[rep] = new Set();
        if (!seenByRep[rep].has(d.companyId)) {
          seenByRep[rep].add(d.companyId);
          byRep[rep] = (byRep[rep] || 0) + 1;
          if (!byRepCategory[rep]) byRepCategory[rep] = emptyCatMap();
          byRepCategory[rep][d.category] = (byRepCategory[rep][d.category] || 0) + 1;
        }
      }
      return { total, byRep, byCategory, byRepCategory };
    }

    const thisWeekW = bucketWindow(annotated, weekStartStr, todayStr);
    const lastWeekW = bucketWindow(annotated, prevWeekStartStr, prevWeekEndStr);
    const thisMonthW = bucketWindow(annotated, monthStartStr, todayStr);
    const lastMonthW = bucketWindow(annotated, lastMonthStartStr, lastMonthEndStr);

    // ─── 7. Sparkline: unique-company count per month, 2026-01 → current month ───
    const pitchByMonth = {};
    const monthList = [];
    for (let y = 2026; y <= curYear; y++) {
      const maxM = y === curYear ? curMonth : 11;
      for (let m = 0; m <= maxM; m++) {
        monthList.push({ year: y, month: m, key: y + '-' + String(m + 1).padStart(2, '0') });
      }
    }
    for (const { year, month, key } of monthList) {
      const mStart = year + '-' + String(month + 1).padStart(2, '0') + '-01';
      const lastDay = new Date(year, month + 1, 0).getDate();
      const mEndRaw = year + '-' + String(month + 1).padStart(2, '0') + '-' + String(lastDay).padStart(2, '0');
      const isCurrentMonth = year === curYear && month === curMonth;
      const mEnd = isCurrentMonth ? todayStr : mEndRaw;
      pitchByMonth[key] = bucketWindow(annotated, mStart, mEnd).total;
    }

    // ─── 8. Response shape ───
    // - byWeek/byMonth/byRep/etc retained for backwards compatibility, now unique-company counts
    // - byCategory + byRepCategory are the new breakdowns
    const pitches = {
      byWeek: { thisWeek: thisWeekW.total, lastWeek: lastWeekW.total },
      byMonth: pitchByMonth,
      byRep: thisWeekW.byRep,
      lastWeekByRep: lastWeekW.byRep,
      monthlyByRep: thisMonthW.byRep,
      lastMonthByRep: lastMonthW.byRep,
      byCategory: {
        thisWeek: thisWeekW.byCategory,
        lastWeek: lastWeekW.byCategory,
        thisMonth: thisMonthW.byCategory,
        lastMonth: lastMonthW.byCategory,
      },
      byRepCategory: {
        thisWeek: thisWeekW.byRepCategory,
        lastWeek: lastWeekW.byRepCategory,
        thisMonth: thisMonthW.byRepCategory,
        lastMonth: lastMonthW.byRepCategory,
      },
      total: Object.values(pitchByMonth).reduce((s, v) => s + v, 0),
    };

    return {
      statusCode: 200,
      headers: { 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=3600' },
      body: JSON.stringify({ deals: dealMap, count: Object.keys(dealMap).length, pitches })
    };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};
