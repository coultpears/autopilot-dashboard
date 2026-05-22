# autopilot-dashboard Backlog

## Open

- **Revshare ↔ portfolio matching by property ID (the real fix).** Today the grid matches a property to its rev-share P&L by exact `property_name` string (with a `data/revshare-aliases.json` manual-override fallback added 2026-05-19). 582 of 679 portfolio properties match exactly; 97 are coverage gaps and 103 revshare names are orphans. Analysis (`node scripts/list-revshare-gaps.js`) showed the two unmatched sets barely overlap — the 97 gaps are overwhelmingly *genuinely* absent from statements (LT-only / not-yet-reporting STR), not misspelled — so fuzzy auto-matching is unsafe (it would misattribute one property's P&L to another). The bulletproof fix: the rev-share rent-roll sheet tabs are named `"<PropertyName> (<PropertyID>)"` — `scripts/ingest-revshare-sheet.js` already captures that numeric ID in its `tabPattern` regex but throws it away. Steps: (1) add a `property_id` column to `monthly_actuals`; (2) extend the ingest to persist the ID; (3) confirm that ID equals a Looker `dimproperty` field the grid can query; (4) match by ID, fall back to name. Backfilling historical IDs needs each month's source sheet re-pulled. Until then, real mismatches get hand-curated into `data/revshare-aliases.json`.

- **First real monthly Sheet ingest end-to-end.** First production validation of the new Postgres-backed ingest path. When the May 2026 (or first available new month) rent-roll Sheet drops, run the skill from any Claude session:
  ```
  export NEON_DATABASE_URL="postgresql://...sslmode=verify-full"   # pooled URL
  node /c/Users/matt/Projects/autopilot-dashboard/scripts/ingest-revshare-sheet.js <sheet-id> "May 2026" --dry
  # then drop --dry once the sample rows look right
  ```
  Verify in the Neon SQL editor that the new month's rows appear with `source='sheet'` (vs `source='bootstrap'` for the existing rows). Verify the dashboard renders the new month immediately, no Netlify rebuild required. This is the path that proves the whole pipeline works end-to-end; expected to be uneventful.

- **Bundle deprecation PR** (do after ~2-4 weeks of stable Postgres operation). The DB is the source of truth; `data/revshare.json` is now a 755KB frozen snapshot kept in the repo only as a rollback safety net. Once we have confidence the DB path is reliable:
  1. `git rm data/revshare.json`
  2. Remove the `loadBundle()` fallback path from `netlify/functions/_revshare-cache.js`
  3. Drop `data/**` from `included_files` in `netlify.toml` (saves bundle space — though we still need it for `_geocode-cache.js` etc, so check the other consumers first)
  4. Drop `scripts/build-revshare-bundle.js` — the subsidy-session bootstrap is no longer useful
  Net effect: smaller function bundles, less code surface area, no more "which source is authoritative" question. Pure cleanup, no behavior change.

- **DB migration tooling** (only when we need it the second time). Schema changes today require manually pasting SQL into the Neon SQL editor. Acceptable for now — we've done it once. If we hit a second schema change, set up a proper migration runner (e.g. `node-pg-migrate` or just a hand-rolled `scripts/db/migrate.js` that reads numbered files from `scripts/db/migrations/` and tracks applied versions in a `schema_versions` table). Not worth doing speculatively.

- **CoStar targets website enrichment.** Only ~39 of 1,553 CoStar targets have `property_website` or `costar_leasing_company_website`. Marketing URLs aren't in CoStar PDFs, so the ingest can't populate directly. The "Website" button only renders when a URL exists — the Google Maps link works as a workaround (uses property name + address, lands on the named place). Next steps when picked up:
  1. **Brave Search API backfill** — sign up at https://api-dashboard.search.brave.com/register (free 2k queries/month, no card required for free tier), store key as `BRAVE_API_KEY` in Netlify env, build a one-time script that runs each target name+market through Brave and writes the top non-aggregator result to `property_website` in HubSpot.
  2. **Weekly cron** to backfill any new CoStar targets from the latest ingest.
  3. OR enhance the CoStar ingest itself to do search enrichment as a post-step per deal.

- **CoStar target geocoding stragglers (~28).** Mapbox-primary backfill resolved 99.0% of target addresses (2728/2756). The remaining 28 have data-quality issues in HubSpot (typos, ambiguous street numbers, malformed FM-road designations). Resolve via `data/geocodes-manual.json` byAddress overrides — extend `scripts/list-geocode-failures.js` to also output target failures (currently portfolio-only) so the worst offenders are visible. Until then, those 28 targets fall back to market-center jitter.

- **Re-run backfill on a schedule.** New properties land in Looker and new CoStar deals land in HubSpot continuously. The cache only refreshes when `node scripts/backfill-geocoding.js` is run manually. Either:
  1. **Local cron** — Matt runs it weekly, commits the updated `data/geocodes.json`.
  2. **Netlify scheduled function** — call the geocode logic from a daily scheduled function, but the durable storage problem reappears (functions can't commit to git). Could write to Netlify Blobs and have the cache loader read from Blobs first, JSON file second.
  3. **GitHub Action** — runs the backfill weekly, opens a PR with the updated JSON. Cleanest answer if we want zero manual intervention.

- **Bulk-curate `property_website`** for high-priority CoStar targets via reps (organic growth). Map button picks it up automatically.

- **Saans / Akkurat Mono self-hosting.** Brand guide specifies these fonts but neither is freely licensed. Current design uses Instrument Sans + JetBrains Mono as free Google Fonts analogs. If Landing has licensed copies, host them in `/fonts/` and swap the `@font-face` + CSS custom properties (`--font-display`, `--font-mono`).

## Done (recent — May 2026 session)

- **Forward projection shows the booked-vs-still-to-book split per month.** The expanded trend module's "Project forward" toggle previously repeated a flat run-rate for every projected month — identical bars, identical tooltips. The honest constraint: booked revenue 2-3 months out is only a fraction of the eventual month (STR books close-in — e.g. The Hayworth had 36 forward reservations for the next month vs 4 two months out), so it can't drive the projected total. Now each projected month's TOTAL stays the recent 3-month run-rate (the honest best estimate), but `buildProjectionMonths()` also computes the confirmed-booking revenue overlapping that month (prorated from `property-detail` reservation data — nightly rate×nights or monthly rent by days). The chart splits each projected bar into a solid "booked" base and a dashed "still to book" fill — near months read mostly-solid, far months mostly-dashed — and the tooltip breaks out Total / Booked so far / Still to book. Variation comes from the real booking signal, without faking month-to-month total swings.

- **Grid v2 + Partner P&L module + portfolio KPIs + Neon Postgres migration.** Shipped across PR #1 (grid + bundle infrastructure) and PR #2 (Postgres cutover). New surfaces in the grid view: 8-tile KPI strip with click-to-drill segment filters; smart segment chips (All / STR / Mixed / LT / At-risk / Ramping / Top) with live counts; smart leaderboards (Top 5 Landing / Top 3 Ramping by absolute $ gain / At-risk 3); new columns This-month split / Last mo % of base / 12mo sparkline / Proj next 3mo with confidence dots. Expanded row gets the v4 Partner P&L module — Partner POV / Landing POV with verdict + breakdown + trajectory + narrative; vacant-fallback counterfactual at 10/15/25% (matches Landing's actual value-prop framing rather than the LT-stabilized 85% framing); realized + contracted take rates side-by-side; per-month rollup table; chart tooltip with MoM chip; methodology disclosure with per-unit base rent table; honest refusal state for the ~100 properties without bundle data. Unit-classification rewrite: "Short-term" requires actual reservation < 30 nights (not min_nightly_stay, which Looker stores as 30 even for properties hosting 2-7 night stays). Layout: detail panel capped at `min(viewport, 1680px)` so it doesn't stretch on ultra-wide screens. Backend: extended revshare bundle to capture full fee breakdown (mgmt_fee / ffe_fee / install_fee / wifi_fee / partner_adjustment) so the contracted_mgmt_pct chip can derive the real rate from the rent rolls. Migrated entire revshare store from JSON bundle to Neon Postgres — schema + ETL + dual-backend cache loader + ingest path; bundle preserved as rollback safety net; data updates now land live with no git commit needed. Skill (`/revshare-ingest`) decoupled from the subsidy session worktree so it runs from any Claude session.

## Done (recent — Apr 2026 session)

- **Address-accurate pin placement.** Portfolio 100% (679/679), targets 99.0% (2728/2756). Replaced the market-center golden-angle spiral with real geocoded coords from `data/geocodes.json`. Pipeline: `scripts/backfill-geocoding.js` reads addresses from Looker (`dimproperty.address_one/city_name/state/zip`) + HubSpot CoStar deals → geocodes via Mapbox (primary, ~50ms/req) with Nominatim fallback → writes to repo-committed JSON. `data/geocodes-manual.json` provides hand-curated overrides that always win. `_geocode-cache.js` is a shared loader for `grid-data.js` and `map-data.js`. Map render in `map.html` prefers `lat/lng` and only jitters when a property/target wasn't geocoded.
- **Period rollups in Grid view.** Added 3mo/6mo/8mo/12mo segmented control to map.html grid toolbar. Each period swaps the middle 7 columns to show period-specific metrics: Occupancy, Reservations, Cum. Nights, Rev/Res, Period Rev, ADR, RevPAU. Stats strip updates accordingly. Period selection persists in localStorage. Backed by a new `/api/grid-history` endpoint that aggregates `tbldailyhomemetrics` over the date window + counts new reservations from `dimreservation`. Sort defaults to Period Rev desc when entering period mode.
- Editorial design refresh (both Dashboard + Map):
  - Typography: Instrument Sans / DM Sans / JetBrains Mono
  - Warm off-white ground + Landing brand palette
  - Tabular numbers, mono uppercase labels with wide tracking
  - Unified 3-column grid topnav with centered segmented-control primary nav (Dashboard | Map)
  - Map/Grid view switch demoted to secondary right-side toggle
  - Responsive: timestamp hides <1200px, subtitle <1024px, live-data <880px
- Map: brighter pin palette (emerald/amber/coral/silver), detail-panel no longer overlaps search
- Selected target pin now Landing bright blue (was black, clashed with low-rent threshold border)
- CoStar Targets tab overhaul: market bubbles → drill in → multi-select property list → fits map to selected
- True Owner + Rep filters w/ debounced inputs, autocomplete, auto-zoom to filtered territory
- Per-target buttons: HubSpot deal link, Google Maps (includes property name so Maps hits the named place), Website link when curated
- Pin color by vacant-unit bucket; black border on < $1,450/u asking rent
- 138 previously-unmapped markets resolved via SUBURB_METRO + state-level fallback
- Installed-only unit counts + weighted occupancy
- Actual Landing rent from `reservation_monthly_rent` (not AP daily cost estimate)
- Deinstall row sorting + DI badges in reservation sub-table
- Hover prefetch + 10-min detail cache + targeted DOM updates
- Dropped Admin API from property-detail (Looker only)
- Module-level Looker token cache, 30-min CDN cache, split-query parallelism
- HubSpot pitch date UTC midnight fix (was undercounting Monday pitches)
- Address search via Nominatim geocoding in map search box
- Password protection via Netlify built-in

## Env vars on Netlify (for reference)

- `HUBSPOT_TOKEN` — Private app token, currently `pat-na1-4ae893d3-*` (sync'd w/ landing-ops-agents)
- `LANDING_CLIENT_ID`, `LANDING_CLIENT_SECRET` — Looker API credentials
- `MAPBOX_TOKEN` — Public token (`pk.*`) for geocoding. Free tier (100k req/mo). Used by `scripts/backfill-geocoding.js` only — runtime functions read from the repo-committed cache, not Mapbox directly.

## Deploys

- Prod: https://landing-ap-dashboard.netlify.app
- Preview alias: https://preview--landing-ap-dashboard.netlify.app
- Purge cache after deploy: `curl -X POST https://api.netlify.com/api/v1/purge -H "Authorization: Bearer $TOKEN" -d '{"site_id":"174bcd0d-df04-402a-bc6c-b4291ec6cf38"}'`
