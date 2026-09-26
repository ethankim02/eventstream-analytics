-- Per-wallet behavioral-funnel flags. Stage definitions (see docs/METRICS.md,
-- "Lifecycle funnel"):
--   1. first observed event               (every wallet in int_wallet_first_seen)
--   2. second observed event               (>=2 lifetime qualifying events)
--   3. active on a second distinct day     (>=2 distinct active calendar days)
--   4. active again >=7 days after first-seen
--   5. active again >=30 days after first-seen
-- Stages 4 and 5 are right-censored for recently first-seen wallets; maturity
-- filtering (has the window actually elapsed?) is applied in Python
-- (eventstream.analytics.lifecycle), not baked in here, so the same table
-- can serve any "as of" analysis date.
CREATE OR REPLACE TABLE int_wallet_funnel_flags AS
WITH events_ranked AS (
    SELECT
        wallet_address,
        block_timestamp,
        ROW_NUMBER() OVER (PARTITION BY wallet_address ORDER BY block_timestamp) AS event_rank
    FROM stg_wallet_events
    WHERE NOT is_self_transfer
),
days_ranked AS (
    SELECT
        wallet_address,
        activity_date,
        DENSE_RANK() OVER (PARTITION BY wallet_address ORDER BY activity_date) AS day_rank
    FROM int_wallet_daily_activity
)
SELECT
    fs.wallet_address,
    fs.first_seen_at,
    fs.first_seen_at + INTERVAL 7 DAY AS eligible_at_7d,
    fs.first_seen_at + INTERVAL 30 DAY AS eligible_at_30d,
    MAX(CASE WHEN er.event_rank >= 2 THEN 1 ELSE 0 END) AS has_second_event,
    MAX(CASE WHEN dr.day_rank >= 2 THEN 1 ELSE 0 END) AS active_second_distinct_day,
    MAX(CASE WHEN d.activity_date >= fs.first_seen_at + INTERVAL 7 DAY THEN 1 ELSE 0 END) AS active_after_7d,
    MAX(CASE WHEN d.activity_date >= fs.first_seen_at + INTERVAL 30 DAY THEN 1 ELSE 0 END) AS active_after_30d
FROM int_wallet_first_seen fs
LEFT JOIN events_ranked er ON er.wallet_address = fs.wallet_address
LEFT JOIN days_ranked dr ON dr.wallet_address = fs.wallet_address
LEFT JOIN int_wallet_daily_activity d ON d.wallet_address = fs.wallet_address
GROUP BY fs.wallet_address, fs.first_seen_at;
