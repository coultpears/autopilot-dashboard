-- Neon Postgres schema for autopilot-dashboard revshare data.
--
-- Source of truth: monthly rev-share rent rolls (per-property × per-month).
-- Replaces the flat JSON bundle at data/revshare.json that was bundled into
-- Netlify functions. After cutover the bundle is frozen as a historical
-- snapshot until we're confident the DB is the canonical store.
--
-- Design notes:
--   - Single denormalized table. Properties are not their own entity here;
--     property_name is the natural key (matches dimproperty.property_name
--     from Looker, which is what every consumer already joins on).
--   - Fee fields stored signed (mgmt_fee is typically NEGATIVE in source).
--     Take |mgmt_fee| ÷ total_revenue for the contracted Landing take rate.
--   - period_key is YYYYMM as INT (e.g. 202506). Easy to compare/order
--     without parsing strings; period TEXT is the human-readable form.
--   - source tracks ingest provenance for audit: 'bootstrap' from the
--     one-time bundle ETL, 'sheet' from monthly skill ingests, 'manual'
--     for direct edits. ingested_at gives a recency signal per row.
--   - All numeric fields nullable. The source data has plenty of NULLs
--     (months a property didn't report, fee fields not applicable, etc.)
--     and we'd rather store NULL than fabricate zeros.

CREATE TABLE IF NOT EXISTS monthly_actuals (
  property_name        TEXT      NOT NULL,
  period_key           INT       NOT NULL,              -- YYYYMM
  period               TEXT      NOT NULL,              -- "Jun 2025"

  -- Totals (the headline numbers most queries hit)
  landing_margin       NUMERIC(14, 2),                  -- "l"  Landing's keep
  net_allocation       NUMERIC(14, 2),                  -- "p"  Partner's net cash
  total_revenue        NUMERIC(14, 2),                  -- "g"  Gross collected
  occupancy_rate       NUMERIC(6, 4),                   -- "o"  decimal 0..1
  stay_count           INT,                             -- "u"  reservations that month

  -- Fee breakdown (drives the contracted_mgmt_pct chip in the UI).
  -- mgmt_fee is the contractual Landing take; the others are setup-cost
  -- reimbursements that come out of partner's side but aren't rev share.
  mgmt_fee             NUMERIC(14, 2),
  ffe_fee              NUMERIC(14, 2),
  install_fee          NUMERIC(14, 2),
  wifi_fee             NUMERIC(14, 2),
  partner_adjustment   NUMERIC(14, 2),

  -- Provenance
  source               TEXT      NOT NULL DEFAULT 'unknown',  -- 'bootstrap' | 'sheet' | 'manual'
  ingested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (property_name, period_key)
);

-- Per-property trend lookups (getTrend) — most frequent query
CREATE INDEX IF NOT EXISTS idx_monthly_actuals_property
  ON monthly_actuals (property_name, period_key);

-- Per-period cross-property aggregates (KPI strip, leaderboards)
CREATE INDEX IF NOT EXISTS idx_monthly_actuals_period
  ON monthly_actuals (period_key);

-- Audit view: when was each property last refreshed?
CREATE OR REPLACE VIEW property_freshness AS
SELECT
  property_name,
  MAX(period_key) AS latest_period_key,
  MAX(ingested_at) AS last_ingested_at,
  COUNT(*) AS months_reporting,
  COUNT(*) FILTER (WHERE landing_margin IS NOT NULL) AS months_with_landing,
  COUNT(*) FILTER (WHERE mgmt_fee IS NOT NULL AND total_revenue > 0) AS months_with_mgmt_fee
FROM monthly_actuals
GROUP BY property_name;

-- Convenience view for the dashboard's contracted mgmt rate computation.
-- Mirrors what getMgmtFeeRate() in _revshare-cache.js was doing in JS —
-- now it's a SQL query that returns one row per property with the median,
-- min, max, and observation count.
CREATE OR REPLACE VIEW property_mgmt_fee_rate AS
SELECT
  property_name,
  ROUND((percentile_cont(0.5) WITHIN GROUP (
    ORDER BY ABS(mgmt_fee) / NULLIF(total_revenue, 0)
  ))::numeric * 1000) / 10 AS rate_pct,
  COUNT(*) FILTER (WHERE mgmt_fee IS NOT NULL AND total_revenue > 0) AS months_observed,
  ROUND((MIN(ABS(mgmt_fee) / NULLIF(total_revenue, 0)))::numeric * 1000) / 10 AS min_pct,
  ROUND((MAX(ABS(mgmt_fee) / NULLIF(total_revenue, 0)))::numeric * 1000) / 10 AS max_pct
FROM monthly_actuals
WHERE mgmt_fee IS NOT NULL AND total_revenue > 0
GROUP BY property_name;
