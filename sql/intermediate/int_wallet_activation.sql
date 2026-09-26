-- Activation proxy: wallets active on >= 2 distinct calendar days within
-- their first 7 observed days. This is a configurable BEHAVIORAL rule, not
-- a causal claim (see docs/METRICS.md, "Activation proxy" and
-- docs/EXPERIMENTATION.md for why observational uplift here is associational
-- only). The 7-day / 2-distinct-day thresholds are the defaults used across
-- this project's analysis and dashboard; eventstream.analytics.lifecycle
-- exposes a Python function to recompute this with different thresholds
-- without touching SQL.
CREATE OR REPLACE TABLE int_wallet_activation AS
SELECT
    fs.wallet_address,
    fs.first_seen_at,
    COUNT(DISTINCT d.activity_date) AS distinct_days_in_first_7d,
    (COUNT(DISTINCT d.activity_date) >= 2) AS is_activated
FROM int_wallet_first_seen fs
LEFT JOIN int_wallet_daily_activity d
    ON d.wallet_address = fs.wallet_address
    AND d.activity_date >= fs.first_seen_at
    AND d.activity_date < fs.first_seen_at + INTERVAL 7 DAY
GROUP BY fs.wallet_address, fs.first_seen_at;
