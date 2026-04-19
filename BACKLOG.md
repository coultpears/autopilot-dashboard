# autopilot-dashboard Backlog

## Open

- **CoStar targets website enrichment.** Only ~39 of 1,553 CoStar targets have a `property_website` or `costar_leasing_company_website` populated. Marketing site URLs aren't in CoStar PDFs, so the ingest can't populate them directly. Options:
  1. One-time bulk backfill script that runs a search (Brave Search API free tier) for each target without a site, writes top non-aggregator result to `property_website` in HubSpot.
  2. Weekly cron that backfills any new CoStar targets from the latest ingest.
  3. Enhance the CoStar ingest itself to do search enrichment as a post-step per deal.
  For now users navigate to the property site via the Google Maps link.

- **Move geocoding to server-side cache.** Currently target pins in a market drill-in are all jittered around the market center (city-level coords). Truly address-accurate pins require geocoding each deal's `property_street_address`. Per-request client-side geocoding was too slow + rate-limited. Need a persistent cache (Netlify Blobs or a one-time script that writes lat/lng back to HubSpot custom properties).

- **Bulk curate `property_website` for high-priority CoStar targets.** Reps could spend ~30 sec per target to find + paste a URL into HubSpot. Map button picks it up automatically.

## Done (recent)

- CoStar targets tab on map (replaces Expansion pipeline feed): market bubbles → drill into market → multi-select property list → fits map to selected pins.
- True Owner + Rep filters on Targets view with debounced input, autocomplete, auto-zoom to filtered territory.
- Per-target action buttons: HubSpot deal link, Google Maps link (property name + address).
- Pin color by vacant-unit bucket (green ≥10, yellow 5-9, red 1-4, gray none).
- Black border on pins with asking rent < $1,450/u.
- Landing brand palette applied across CSS + legend.
- Installed-only unit counts + weighted occupancy in portfolio grid.
- Actual Landing rent from reservation (not AP daily cost estimation).
- Deinstall row sorting + DI badges in reservation sub-table.
- Hover prefetch + 10-min detail cache + targeted DOM updates for grid expand/collapse.
- Dropped Admin API dependency from property-detail (Looker only).
- Module-level Looker token cache, 30-min CDN cache, split-query parallelism.
- Auto-retry + real error UI on grid-data failures.
- HubSpot pitch date filter fixed (UTC midnight instead of CT offset undercounting Monday pitches).
- Address search via Nominatim geocoding in map search box.
- Password protection via Netlify built-in.
