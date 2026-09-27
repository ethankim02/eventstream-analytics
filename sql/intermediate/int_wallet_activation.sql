-- Activation proxy: wallets active on >= 2 distinct calendar days within
-- their first 7 calendar days, counting the first-seen day as day 0 (so the
-- window is days 0-6). This is a configurable BEHAVIORAL rule, not
-- a causal claim (see docs/METRICS.md, "Activation proxy" and
-- docs/EXPERIMENTATION.md for why observational uplift here is associational
-- only). The 7-day / 2-distinct-day thresholds are the defaults used across
-- this project's analysis and dashboard; eventstream.analytics.lifecycle
-- exposes a Python function to recompute this with different thresholds
-- without touching SQL.
-- The window is anchored on first_seen_DATE (a day), not first_seen_at (a
-- timestamp): activity_date is truncated to midnight, so comparing it with a
-- mid-day timestamp would silently drop the first-seen day itself.
CREATE OR REPLACE TABLE int_wallet_activation AS
SELECT
    fs.wallet_address,
    fs.first_seen_at,
    COUNT(DISTINCT d.activity_date) AS distinct_days_in_first_7d,
    (COUNT(DISTINCT d.activity_date) >= 2) AS is_activated
FROM int_wallet_first_seen fs
LEFT JOIN int_wallet_daily_activity d
    ON d.wallet_address = fs.wallet_address
    AND d.activity_date >= fs.first_seen_date
    AND d.activity_date < fs.first_seen_date + INTERVAL 7 DAY
GROUP BY fs.wallet_address, fs.first_seen_at;
