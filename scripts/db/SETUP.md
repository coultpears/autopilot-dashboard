# Neon Postgres setup for autopilot-dashboard

One-time setup to migrate the revshare data store from `data/revshare.json` (bundled JSON, ~755 KB committed to repo) to Neon Postgres. After this is wired in, the dashboard reads from Postgres on every request and the monthly Sheet ingest writes to Postgres instead of the bundle.

## Prerequisites

- A Neon account (free tier is fine — see "Sizing" below)
- Netlify access to add an environment variable to the autopilot-dashboard site
- This branch (`feature/neon-postgres-migration`) checked out locally
- `pg` Node module — install with `npm install pg` in the repo root (only used by ETL + the cache loader)

## Step 1 — Create the Neon project

1. Go to https://console.neon.tech/ and sign in (or sign up; free tier requires no card).
2. **Create project** → name it `autopilot-dashboard`. Region: `us-east-2` (closest to Netlify's east-coast edge).
3. Postgres version: default (latest stable, currently 17).
4. After creation, Neon shows the connection details. Copy the **pooled connection string** — looks like:
   ```
   postgresql://<user>:<password>@<host>-pooler.us-east-2.aws.neon.tech/neondb?sslmode=require
   ```
   The `-pooler` host is what Netlify functions should use (handles connection pooling). The non-pooled host is for the ETL script and local development.

### Sizing
- Today's bundle: 685 properties × 11 months = ~7,500 rows × ~200 bytes = ~1.5 MB.
- Free tier: 0.5 GB storage, 1 compute unit, 100 hours/month compute. We use well under all three.
- Growth: ~700 rows/month going forward → years of runway on free tier.
- If we outgrow it (other Landing tools share the project, etc.) → Launch plan @ $19/mo handles 10 GB easily.

### Branching (optional but recommended)
Neon supports git-like branches. Useful for testing schema changes without touching prod data:
- Default branch: `main` → prod
- Optional branch: `dev` → copy of prod, safe to wipe
- Create `dev` from Neon console once schema is loaded; point `NEON_DATABASE_URL_DEV` at it for testing.

## Step 2 — Apply schema

From the repo root, with the pooled OR non-pooled connection string:

```bash
psql "<NEON_DATABASE_URL>" -f scripts/db/schema.sql
```

Verify it landed:
```bash
psql "<NEON_DATABASE_URL>" -c "\\dt"           # should show monthly_actuals
psql "<NEON_DATABASE_URL>" -c "\\dv"           # should show property_freshness, property_mgmt_fee_rate views
```

(The schema is idempotent — `CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS` — so it's safe to re-run.)

## Step 3 — Load the existing bundle into Postgres

```bash
npm install pg                                                   # if you haven't already
NEON_DATABASE_URL="<connection-string>" \
  node scripts/db/etl-bundle-to-pg.js --dry                      # dry-run first
NEON_DATABASE_URL="<connection-string>" \
  node scripts/db/etl-bundle-to-pg.js                            # actual load
```

Expected output:
```
Source: .../data/revshare.json
  685 properties × up to 11 months
Target: postgresql://<user>:****@... (us-east-2)
Flattened to ~7,500 rows
  ~6,500 rows have mgmt_fee populated
Connected to Postgres
  7500/7500 rows processed...
Done in 4.5s
  7500 new rows, 0 updated
```

(Re-running is fine; second run shows `0 new, 7500 updated` because of `ON CONFLICT DO UPDATE`.)

Sanity check:
```bash
psql "<NEON_DATABASE_URL>" -c "SELECT COUNT(*) FROM monthly_actuals;"
psql "<NEON_DATABASE_URL>" -c "SELECT property_name, period_key, landing_margin, mgmt_fee FROM monthly_actuals WHERE property_name = '2121' ORDER BY period_key;"
psql "<NEON_DATABASE_URL>" -c "SELECT * FROM property_mgmt_fee_rate WHERE property_name IN ('2121', '281 Willow', 'Trails Bend');"
```

## Step 4 — Add the env var to Netlify

1. Netlify dashboard → autopilot-dashboard site → **Site configuration** → **Environment variables**
2. **Add a variable**:
   - Key: `NEON_DATABASE_URL`
   - Value: the **pooled** connection string (the `-pooler` one)
   - Scope: all deploys
3. Trigger a deploy (or push any commit) so the functions pick up the new env var.

For local dev, add the same to your local `.env` file (or set in your shell):
```bash
export NEON_DATABASE_URL="postgresql://<pooled-connection-string>"
```

Functions will use Postgres when this env var is set, and fall back to the bundle when not set. So:
- Production (env set) → Postgres
- Local dev with env set → Postgres
- Local dev without env set → bundle (legacy path, still works)

## Step 5 — Verify the cutover

1. Hit the live grid-data endpoint: `https://<your-netlify-site>/.netlify/functions/grid-data`
2. In the response, every record should still have `trend_l`, `contracted_mgmt_pct`, etc. populated.
3. Function logs should show `[revshare-cache] loaded N rows × M months from Postgres in <X>ms` (Netlify dashboard → Functions → grid-data → Logs).
4. Open the dashboard at `/map.html`, expand a property — the trend chart + `contract X% mgmt` chip should look identical to pre-cutover.

If anything looks wrong, **unset `NEON_DATABASE_URL` in Netlify** and redeploy — the function will fall back to the bundled JSON immediately. No data loss; the bundle is still in the repo as a frozen snapshot.

## Step 6 — Test a monthly Sheet ingest end-to-end

When a new month's rent-roll Sheet drops:
```bash
NEON_DATABASE_URL="<connection-string>" \
  node /path/to/scripts/ingest-revshare-sheet.js <sheet-id> "May 2026" --dry
```

Dry-run output prints what would be inserted, no DB writes. Run live without `--dry` when satisfied. Output:
```
Wrote 685 rows in 6.2s (0 new · 685 replaced)
Dashboard will reflect the new data on next request — no rebuild needed.
```

Important:
- **No git commit needed** for the data update — it lives in Postgres now, not the repo.
- **No Netlify rebuild needed** — the cache loads fresh data on the next cold start (every few minutes when traffic is light, instant otherwise).
- The bundle (`data/revshare.json`) stays as-is in the repo as a frozen historical snapshot. We'll remove it in a follow-up PR once confidence is built.

## Step 7 — (Future) Deprecate the bundle

Once we've run a few monthly ingests and confirmed the DB path is reliable:
1. Remove `data/revshare.json` from the repo (`git rm data/revshare.json`)
2. Remove the bundle fallback path from `_revshare-cache.js` (the `loadBundle()` function)
3. Update `netlify.toml` to drop `data/**` from `included_files` (save bundle space)

This is a separate PR, deliberately not part of the cutover.

## Troubleshooting

**"NEON_DATABASE_URL env var required"** — the script isn't seeing the env var. Check your shell or the script invocation.

**"pg module not found"** — run `npm install pg` in the repo root. If you're running the ETL from a different directory, install pg there.

**"connection refused" / "SSL required"** — make sure the connection string ends with `?sslmode=require`. Neon enforces SSL.

**Slow first request after deploy** — expected. Cold start hits Postgres for the full dataset (~150ms). Warm container = instant. To eliminate even the cold start hit, configure Netlify scheduled function to ping the endpoint every 5 min.

**Bundle and DB disagree on a property** — they shouldn't after the initial ETL, but if a Sheet ingest writes only to DB and the bundle never gets updated, drift accumulates over time. The bundle should be treated as a frozen snapshot post-cutover, not maintained in parallel.

## Migration rollback

If something goes wrong:
1. In Netlify, delete or rename `NEON_DATABASE_URL` (e.g. to `NEON_DATABASE_URL_DISABLED`)
2. Redeploy
3. Functions fall back to `data/revshare.json` automatically — no code change needed

The bundle is your safety net. Keep it in the repo until the cutover is proven (a few weeks of stable operation).
