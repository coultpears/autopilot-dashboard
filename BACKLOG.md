# autopilot-dashboard Backlog

## Open

- **CoStar targets website enrichment.** Only ~39 of 1,553 CoStar targets have `property_website` or `costar_leasing_company_website`. Marketing URLs aren't in CoStar PDFs, so the ingest can't populate directly. The "Website" button only renders when a URL exists — the Google Maps link works as a workaround (uses property name + address, lands on the named place). Next steps when picked up:
  1. **Brave Search API backfill** — sign up at https://api-dashboard.search.brave.com/register (free 2k queries/month, no card required for free tier), store key as `BRAVE_API_KEY` in Netlify env, build a one-time script that runs each target name+market through Brave and writes the top non-aggregator result to `property_website` in HubSpot.
  2. **Weekly cron** to backfill any new CoStar targets from the latest ingest.
  3. OR enhance the CoStar ingest itself to do search enrichment as a post-step per deal.

- **Server-side geocoding for target pins.** Currently all target pins in a drilled-in market jitter around the market center (city-level coords). True address-accurate pins need geocoding each deal's `property_street_address`. Client-side per-address geocoding was too slow + rate-limited; server-side scraping blocked by Google/Bing/DDG. Needs a persistent cache — either Netlify Blobs or a one-time batch script that writes lat/lng back to HubSpot custom properties.

- **Bulk-curate `property_website`** for high-priority CoStar targets via reps (organic growth). Map button picks it up automatically.

- **Saans / Akkurat Mono self-hosting.** Brand guide specifies these fonts but neither is freely licensed. Current design uses Instrument Sans + JetBrains Mono as free Google Fonts analogs. If Landing has licensed copies, host them in `/fonts/` and swap the `@font-face` + CSS custom properties (`--font-display`, `--font-mono`).

## Done (recent — Apr 2026 session)

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

## Deploys

- Prod: https://landing-ap-dashboard.netlify.app
- Preview alias: https://preview--landing-ap-dashboard.netlify.app
- Purge cache after deploy: `curl -X POST https://api.netlify.com/api/v1/purge -H "Authorization: Bearer $TOKEN" -d '{"site_id":"174bcd0d-df04-402a-bc6c-b4291ec6cf38"}'`
