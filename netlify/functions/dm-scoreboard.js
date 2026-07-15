// Proxies the Autopilot Hub's /api/cadence into the exact shape the dashboard's
// DM scoreboard renders. The Hub is Google-admin-gated (first-party HS256
// session token signed with SESSION_SECRET). We mint a short-lived admin
// session server-side with the shared secret (HUB_SESSION_SECRET) so nothing
// sensitive ever reaches the browser. Read-only — only ever GETs the Hub.
//
// Env: HUB_SESSION_SECRET (required, = the Hub's SESSION_SECRET),
//      HUB_URL (optional, defaults to the deployed Hub),
//      HUB_ADMIN_EMAIL (optional, must be on the Hub's admin allowlist).
const crypto = require('crypto');

const HUB_URL = process.env.HUB_URL || 'https://autopilot-hub.netlify.app';
const SECRET = process.env.HUB_SESSION_SECRET || '';
const ADMIN_EMAIL = (process.env.HUB_ADMIN_EMAIL || 'matt@hellolanding.com').toLowerCase();
const GOALS = { cadWk: 80, cadMo: 350, pitWk: 10, unitsMo: 70 };

// Mint the Hub's first-party session token (mirrors autopilot-hub _auth.mintSession).
function mintToken() {
  const now = Math.floor(Date.now() / 1000);
  const b64 = (o) => Buffer.from(JSON.stringify(o)).toString('base64url');
  const h = b64({ alg: 'HS256', typ: 'JWT' });
  const p = b64({ email: ADMIN_EMAIL, isAdmin: true, orig: now, iat: now, exp: now + 600, kind: 'hubsess' });
  const sig = crypto.createHmac('sha256', SECRET).update(h + '.' + p).digest('base64url');
  return `${h}.${p}.${sig}`;
}

async function hubGet(path, token) {
  const r = await fetch(HUB_URL + path, { headers: { Authorization: 'Bearer ' + token } });
  if (!r.ok) throw new Error(`Hub ${path} -> ${r.status}`);
  return r.json();
}

const num = (v) => (v == null || v === '' ? null : Number(v));
const n0 = (v) => num(v) || 0;

exports.handler = async () => {
  if (!SECRET) {
    return { statusCode: 500, body: JSON.stringify({ error: 'HUB_SESSION_SECRET not configured' }) };
  }
  try {
    const token = mintToken();
    // Main payload carries per-DM cadence/pitch/units + meetings + funnel; the
    // overall-outbound counts are served by dedicated sub-endpoints (the main
    // call returns them null to avoid a slow fan-out).
    const [main, ov, ovp] = await Promise.all([
      hubGet('/api/cadence', token),
      hubGet('/api/cadence?only=overall', token),
      hubGet('/api/cadence?only=overallprev', token),
    ]);
    const overall = ov.overall || {};
    const overallPrev = ovp.overallPrev || {};
    const meetings = (main.meetings && main.meetings.byDm) || {};
    const funnel = (main.funnel && main.funnel.byDm) || {};

    // Trailing-4-weeks fallback: count pitched deals whose pitch date falls in
    // the last 28 days. Only used when a DM has no `net` block (see below) —
    // the Hub prefers net.t4w when present.
    const cut = Date.now() - 28 * 86400000;
    const trail4 = {};
    for (const x of (main.deals || [])) {
      if (x.state !== 'pitched' || !x.pdate) continue;
      const t = Date.parse(x.pdate);
      if (t && t >= cut && t <= Date.now()) trail4[x.dmId] = (trail4[x.dmId] || 0) + 1;
    }

    const dms = (main.dms || []).map((d) => {
      const id = d.id;
      const f = funnel[id] || {};
      const ovi = overall[id] || {};
      const mt = meetings[id] || {};
      // The Hub displays NET pitch counts when a DM carries a `net` block (it
      // dedupes multi-unit/reassigned property-deal pitches into one). Mirror
      // the Hub's own logic exactly (index.html leaderboard map ~L2573 and
      // pitchDataNet): prefer net.wk.c / net.wk.s / net.t4w, else the gross
      // pitchesWk+adhocWk / schedWk / trailing-deal-count.
      const net = d.net || null;
      return {
        id,
        name: d.name,
        pod: d.pod,
        cadWk: n0(d.cadTouchWk),
        cadWkPrev: num(d.cadTouchWkPrev),
        cadMo: n0(d.cadTouchMo),
        cadMoPrev: num(d.cadTouchMoPrev),
        ovWk: n0(ovi.wk),
        ovMo: n0(ovi.mo),
        ovMoPrev: num(overallPrev[id]),
        // "Pitches / wk" = net completed pitches this week (Hub convention).
        pitWk: (net && net.wk) ? n0(net.wk.c) : n0(d.pitchesWk) + n0(d.adhocWk),
        // "+N booked" = pitches booked for a future date this week (net).
        booked: (net && net.wk) ? n0(net.wk.s) : n0(d.schedWk),
        // Cad → Pitch = funnel pitched / cadence (company-deduped), matches the Hub.
        cadPitch: f.cadence > 0 ? Math.round((f.pitched / f.cadence) * 100) : 0,
        inPerson: n0(mt.mo),
        units: n0(d.unitsMo),
        // Trailing-4-weeks net pitches (goal 40) for the "Pitches by rep" view.
        trail4: (net && net.t4w != null) ? n0(net.t4w) : n0(trail4[id]),
      };
    });

    // Team Total row (footer) — sum the mapped per-DM values; team Cad→Pitch
    // uses the org-wide funnel (company-deduped) so it matches the Hub.
    const sum = (k) => dms.reduce((a, d) => a + (d[k] || 0), 0);
    const fo = (main.funnel && main.funnel.overall) || {};
    const totals = {
      count: dms.length,
      cadWk: sum('cadWk'), cadWkPrev: sum('cadWkPrev'),
      cadMo: sum('cadMo'), cadMoPrev: sum('cadMoPrev'),
      ovWk: sum('ovWk'), ovMo: sum('ovMo'), ovMoPrev: sum('ovMoPrev'),
      pitWk: sum('pitWk'), booked: sum('booked'),
      inPerson: sum('inPerson'), units: sum('units'),
      cadPitch: fo.cadence > 0 ? Math.round((fo.pitched / fo.cadence) * 100) : 0,
    };

    return {
      statusCode: 200,
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'public, max-age=120, stale-while-revalidate=600',
      },
      body: JSON.stringify({ dms, totals, goals: GOALS, dataSource: main.dataSource, generatedAt: main.generatedAt }),
    };
  } catch (e) {
    return { statusCode: 502, body: JSON.stringify({ error: String((e && e.message) || e) }) };
  }
};
