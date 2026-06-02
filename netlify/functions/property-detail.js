// Netlify serverless function: Combined property detail (units + reservations + trend)
// Cached Looker auth, 2 parallel queries, no Admin API dependency.
//
// New (2026-05): STR signal fields per unit, nightly rate on reservations,
// per-unit MTD revenue aggregate, and the 11-month Landing-vs-Partner trend
// from the repo-bundled revshare cache.

const fs = require('fs');
const path = require('path');
const { init: initRevshare, getTrend, getCoverage } = require('./_revshare-cache');
const { timedFetch } = require('./_looker.js');
const responseCache = require('./_response-cache.js');

const LOOKER_BASE = 'https://landing.cloud.looker.com';

let _cachedToken = null;
let _tokenExpiry = 0;

async function getLookerToken() {
  if (_cachedToken && Date.now() < _tokenExpiry) return _cachedToken;
  const resp = await timedFetch(`${LOOKER_BASE}/api/4.0/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: `client_id=${process.env.LANDING_CLIENT_ID}&client_secret=${process.env.LANDING_CLIENT_SECRET}`,
  });
  if (!resp.ok) throw new Error(`Looker auth failed: ${resp.status}`);
  const data = await resp.json();
  _cachedToken = data.access_token;
  _tokenExpiry = Date.now() + (data.expires_in ? (data.expires_in - 60) * 1000 : 3000000);
  return _cachedToken;
}

async function lookerQuery(token, view, fields, filters, sorts, limit = 5000) {
  const resp = await timedFetch(`${LOOKER_BASE}/api/4.0/queries/run/json`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ model: 'landing', view, fields, filters, sorts, limit: String(limit) }),
  });
  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    throw new Error(`Looker query (${view}) failed: ${resp.status} ${text.slice(0, 100)}`);
  }
  return resp.json();
}

function cleanRow(row) {
  const out = {};
  for (const [k, v] of Object.entries(row)) out[k.includes('.') ? k.split('.').pop() : k] = v;
  const mgmt = (out.property_management_company || '').trim();
  const dp = (out.dp_full_name || '').trim();
  out.pmc_name = mgmt || null;
  out.dp_name = dp || null;
  // partner_name = capital owner (DP) preferred, operator (PMC) fallback.
  // See grid-data.js for rationale (flipped 2026-05-08).
  out.partner_name = dp || mgmt || null;
  delete out.property_management_company;
  delete out.dp_full_name;
  return out;
}

// Today in property-local terms (Central since most properties are CST/CDT
// for billing purposes — same approach as HubSpot pitch dates).
function startOfMonthISO() {
  const now = new Date();
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1)).toISOString().slice(0, 10);
}

exports.handler = async (event) => {
  const name = event.queryStringParameters?.name;
  if (!name) return { statusCode: 400, body: JSON.stringify({ error: 'name parameter required' }) };

  const cacheKey = `property-detail:${name}`;
  // FRESH: under this age a cached payload is served as-is and labelled fresh.
  // MAX:   up to this age it's still served immediately (labelled stale) rather
  //        than making the user wait on Looker — the background warmer is
  //        responsible for refreshing entries before they get old. Only a
  //        missing or older-than-MAX entry pays a live Looker fetch.
  const SERVE_FRESH_MS = Number(process.env.PROPERTY_DETAIL_TTL_MS) || 1800000;    // 30 min
  const SERVE_MAX_MS = Number(process.env.PROPERTY_DETAIL_MAX_AGE_MS) || 86400000; // 24 h
  // The warmer calls with ?refresh=1 to force a live fetch + cache update,
  // bypassing the serve-from-cache shortcut below.
  const forceRefresh = event.queryStringParameters?.refresh === '1';

  // ── Read-through cache ───────────────────────────────────────────────
  // A cached entry returns straight from Neon (~hundreds of ms) with zero
  // Looker round-trip — the main lever against per-property lag, and it means
  // the request path never blocks on a slow/degraded Explore when we already
  // have data. Always loaded (even on forceRefresh) so it can still serve as
  // the stale fallback if the live fetch fails.
  let cachedEntry = null;
  try { cachedEntry = await responseCache.load(cacheKey); } catch (e) { /* ignore */ }
  if (!forceRefresh && cachedEntry) {
    const ageMs = Date.now() - new Date(cachedEntry.updatedAt).getTime();
    if (ageMs < SERVE_MAX_MS) {
      const fresh = ageMs < SERVE_FRESH_MS;
      return {
        statusCode: 200,
        headers: {
          'Content-Type': 'application/json',
          'Cache-Control': fresh ? 'public, max-age=600, s-maxage=600' : 'no-store',
          'X-Data-Source': fresh ? 'cache' : 'stale-cache',
          'X-Data-As-Of': new Date(cachedEntry.updatedAt).toISOString(),
        },
        body: cachedEntry.payloadJson,
      };
    }
  }

  try {
    // Warm the revshare cache in parallel with the Looker token fetch.
    // Postgres-backed when NEON_DATABASE_URL is set; falls back to bundle.
    const [token, _] = await Promise.all([
      getLookerToken(),
      initRevshare(),
    ]);
    const monthStart = startOfMonthISO();

    // Three Looker queries in parallel:
    //   1) units snapshot today (current state of each home)
    //   2) ALL reservations for this property (current/future/past) — used
    //      both for the existing reservations tab and for MTD aggregation
    //   3) (no third query yet — kept room here for future MTD-via-daily-metrics)
    const [unitsData, resData] = await Promise.all([
      lookerQuery(token, 'tbldailyhomemetrics', [
        'dimproperty.property_name', 'dimproperty.property_management_company',
        'dimdirectpartner.dp_full_name', 'dimmarket.market_name', 'dimhome.unit_number',
        'tbldailyhomemetrics.home_is_active', 'tbldailyhomemetrics.home_is_installed',
        'tbldailyhomemetrics.home_reservation_status',
        'dimhome.rent_cost', 'dimhome.lease_rent_cost',
        'tbldailyhomemetrics.home_daily_rent_revenue', 'tbldailyhomemetrics.home_daily_rent_cost_autopilot',
        'tbldailyhomemetrics.markup',
        'dimhome.days_vacant_standard', 'dimhome.vacant_since_date',
        'dimhome.current_reservation_start_date_raw_date', 'dimhome.current_reservation_end_raw_date',
        'dimhome.revenue_share_percentage',
        'dimhome.deinstall_date', 'dimhome.deinstall_requested_date',
        'dimhome.home_deinstall_reason', 'dimhome.expected_deinstall_date',
        // STR signal — dimhome.str_status is the authoritative per-unit
        // string. Values: 'enabled', 'not wanted by property', 'not allowed',
        // 'in progress', 'remote market', null. The unit is STR if and only
        // if str_status === 'enabled'. Captures Autopilot AND Standard
        // properties (dimproperty.eligible_for_str misses Autopilot).
        'dimhome.str_status',
        'dimhome.minimum_nightly_stay',
        'dimhome.property_allows_ota',
      ], { 'dimproperty.property_name': name, 'tbldailyhomemetrics.date_date': 'today' },
      ['dimproperty.property_name', 'dimhome.unit_number']),

      lookerQuery(token, 'dimreservation', [
        'dimproperty.property_name', 'dimproperty.property_management_company',
        'dimdirectpartner.dp_full_name', 'dimmarket.market_name', 'dimhome.unit_number',
        'dimreservation.reservation_start_date', 'dimreservation.reservation_end_date',
        'dimreservation.days_in_reservation', 'dimreservation.reservation_length_raw',
        'dimreservation.reservation_length', 'dimreservation.stay_commitment_type',
        'dimreservation.current_reservation_count', 'dimreservation.future_reservation_count',
        'dimreservation.count', 'dimreservation.reservation_monthly_rent',
        // Nightly-rate fields — added 2026-05 to surface STR per-night economics
        'dimreservation.admin_reservation_rent_rate',
        'dimreservation.admin_reservation_rent_unit',
      ], { 'dimproperty.property_name': name },
      ['dimproperty.property_name', 'dimreservation.reservation_start_date']),
    ]);

    const reservations = resData.map(cleanRow);

    // Build unit-level Landing rent lookup from CURRENT reservations only.
    // Two flavors:
    //   - LT (rent_unit = "month" or null): use reservation_monthly_rent
    //   - STR (rent_unit = "night"): use admin_reservation_rent_rate as the
    //     nightly rate. We DO NOT use reservation_monthly_rent for STR
    //     because it's just nightly × 30.42 — a synthetic that misleads if
    //     read as comparable to LT rent.
    const currentRentByUnit = {};
    for (const r of reservations) {
      if (r.current_reservation_count > 0 && r.unit_number != null) {
        const unit = String(r.unit_number);
        if (r.admin_reservation_rent_unit === 'night' && r.admin_reservation_rent_rate != null) {
          currentRentByUnit[unit] = {
            kind: 'nightly',
            nightly: r.admin_reservation_rent_rate,
            nights: r.reservation_length_raw,
            stay_total: r.admin_reservation_rent_rate * (r.reservation_length_raw || 0),
            mo_eq: r.reservation_monthly_rent, // the synthetic, kept for display as "≈$X/mo eq."
          };
        } else if (r.reservation_monthly_rent) {
          currentRentByUnit[unit] = {
            kind: 'monthly',
            monthly: r.reservation_monthly_rent,
          };
        }
      }
    }

    // MTD revenue per unit — sum stay revenues whose start date falls within
    // this calendar month. For nightly bookings, stay revenue = rate × nights.
    // For monthly bookings, slice to days-in-month-elapsed × (rent / 30.4).
    const mtdByUnit = {};
    const mtdNightsByUnit = {};
    const monthStartTs = Date.parse(monthStart);
    const todayTs = Date.now();
    const daysElapsed = Math.max(1, Math.floor((todayTs - monthStartTs) / 86400000) + 1);
    for (const r of reservations) {
      if (r.unit_number == null) continue;
      const start = Date.parse(r.reservation_start_date || '');
      const end = Date.parse(r.reservation_end_date || '');
      if (isNaN(start)) continue;
      // Only count stays whose start is on or before today AND whose start
      // is within or before this month (LT/mid-term started earlier still
      // accrues this month).
      if (start > todayTs) continue;
      const unit = String(r.unit_number);
      if (r.admin_reservation_rent_unit === 'night' && r.admin_reservation_rent_rate != null) {
        // STR: count this stay only if any of its nights fall in this month
        const stayStart = Math.max(start, monthStartTs);
        const stayEnd = Math.min(isNaN(end) ? todayTs : end, todayTs);
        const nightsInMonth = Math.max(0, Math.round((stayEnd - stayStart) / 86400000));
        if (nightsInMonth > 0) {
          mtdByUnit[unit] = (mtdByUnit[unit] || 0) + nightsInMonth * r.admin_reservation_rent_rate;
          mtdNightsByUnit[unit] = (mtdNightsByUnit[unit] || 0) + nightsInMonth;
        }
      } else if (r.reservation_monthly_rent && r.current_reservation_count > 0) {
        // LT/MT currently active — accrued slice this month, counting from
        // the LATER of reservation start or month start (was previously
        // always full daysElapsed, which over-counted for leases that
        // started mid-month).
        const accrualStart = Math.max(start, monthStartTs);
        const accrualDays = Math.max(0, Math.floor((todayTs - accrualStart) / 86400000) + 1);
        const slice = (r.reservation_monthly_rent / 30.42) * accrualDays;
        mtdByUnit[unit] = (mtdByUnit[unit] || 0) + slice;
        mtdNightsByUnit[unit] = (mtdNightsByUnit[unit] || 0) + accrualDays;
      }
    }

    // Process units — Landing rent comes from the active reservation, not estimation.
    // Mark STR status, surface nightly economics, attach MTD aggregate.
    const units = unitsData.map(cleanRow);
    let propertyStrEligible = false;  // any unit with str_status === 'enabled'
    let strUnitCount = 0;
    let totalNightlyRate = 0, nightlyRateCount = 0; // for property-level ADR

    for (const r of units) {
      const base = r.rent_cost;
      const unit = String(r.unit_number || '');
      const active = currentRentByUnit[unit];

      // Per-unit STR flag from authoritative str_status field
      r.str_eligible = (r.str_status === 'enabled');
      r.str_status_label = r.str_status; // keep the raw label for tooltips
      if (r.str_eligible) { propertyStrEligible = true; strUnitCount++; }
      delete r.str_status;

      if (active && active.kind === 'nightly') {
        r.rent_kind = 'nightly';
        r.nightly_rate = active.nightly;
        r.nightly_nights = active.nights;
        r.nightly_stay_total = active.stay_total;
        r.landing_rent_mo_eq = active.mo_eq;
        totalNightlyRate += active.nightly;
        nightlyRateCount++;
      } else if (active && active.kind === 'monthly') {
        r.rent_kind = 'monthly';
        r.landing_rent = Math.round(active.monthly);
        r.rent_source = 'reservation';
        if (base && base > 0) {
          r.computed_markup = Math.round(((active.monthly - base) / base) * 10000) / 10000;
        }
      }

      // MTD revenue + nights for this unit
      r.mtd_revenue = mtdByUnit[unit] != null ? Math.round(mtdByUnit[unit] * 100) / 100 : 0;
      r.mtd_nights = mtdNightsByUnit[unit] || 0;

      // Break-even nights for STR units = base / nightly (when we know both)
      if (r.rent_kind === 'nightly' && r.nightly_rate > 0 && base > 0) {
        r.str_break_even_nights = Math.round((base / r.nightly_rate) * 10) / 10;
      }
    }

    const propertyAdr = nightlyRateCount > 0 ? Math.round(totalNightlyRate / nightlyRateCount) : null;
    // Whether any current reservation is billed nightly. Kept as a separate
    // signal from str_status — 2001 East confirmed that nightly billing
    // alone doesn't make a property STR (str_status="not wanted by property").
    // STR enablement is purely whether any unit has str_status='enabled'.
    const propertyHasNightlyBookings = nightlyRateCount > 0;

    // Landing-vs-Partner monthly trend from the revshare cache (Postgres-backed).
    // Null when the property isn't in the dataset (e.g. recent additions
    // pre-onboarding); the FE renders the section conditionally.
    const trend = getTrend(name);

    // Coverage window of the revshare dataset — the FE shows this in the
    // "P&L unavailable" refusal state so the message stays accurate as new
    // months are ingested (was previously hardcoded "Jun 2025 → Apr 2026").
    const coverage = getCoverage() || [];
    const revshare_coverage = coverage.length
      ? { first: coverage[0].period, last: coverage[coverage.length - 1].period, months: coverage.length }
      : null;

    const body = JSON.stringify({
      units, reservations, trend,
      meta: {
        property_str_eligible: propertyStrEligible,  // any unit has str_status='enabled'
        str_unit_count: strUnitCount,
        property_has_nightly_bookings: propertyHasNightlyBookings,
        property_adr: propertyAdr,
        mtd_day_of_month: daysElapsed,
        trend_months: trend ? trend.length : 0,
        revshare_coverage,
      },
    });
    // Warm the read-through cache so the next viewer of this property (and the
    // stale-serve fallback) gets it without a Looker round-trip. Best-effort.
    await responseCache.save(cacheKey, body);

    return {
      statusCode: 200,
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'public, max-age=600, s-maxage=600',
        'X-Data-Source': 'live',
      },
      body,
    };
  } catch (err) {
    console.error(err);
    // Looker failed/timed out. If we have any prior payload (even older than
    // the TTL), serve it stale rather than erroring the drawer.
    if (cachedEntry) {
      return {
        statusCode: 200,
        headers: {
          'Content-Type': 'application/json',
          'Cache-Control': 'no-store',
          'X-Data-Source': 'stale-cache',
          'X-Data-As-Of': new Date(cachedEntry.updatedAt).toISOString(),
          'X-Stale-Reason': String(err.message || 'upstream error').slice(0, 200),
        },
        body: cachedEntry.payloadJson,
      };
    }
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};
