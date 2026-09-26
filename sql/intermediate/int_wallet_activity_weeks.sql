-- Distinct (wallet, activity week) pairs — the grain retention cohorts are
-- measured at.
CREATE OR REPLACE TABLE int_wallet_activity_weeks AS
SELECT DISTINCT
    wallet_address,
    DATE_TRUNC('week', activity_date) AS activity_week
FROM int_wallet_daily_activity;
