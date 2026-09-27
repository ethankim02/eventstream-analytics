-- Per-wallet behavioral-funnel flags. Stage definitions (see docs/METRICS.md,
-- "Lifecycle funnel"):
--   1. first observed event               (every wallet in int_wallet_first_seen)
--   2. second observed event               (>=2 lifetime qualifying events)
--   3. active on a second distinct day     (>=2 distinct active calendar days)
--   4. active again >=7 days after first-seen (calendar day 7 or later)
--   5. active again >=30 days after first-seen (calendar day 30 or later)
-- Stages 4 and 5 are right-censored for recently first-seen wallets; maturity
-- filtering (has the window actually elapsed?) is applied in Python
-- (eventstream.analytics.lifecycle), not baked in here, so the same table
-- can serve any "as of" analysis date.
--
-- Scale note: every flag is a function of a wallet's (event count, distinct
-- day count, last active day), so the daily-activity table is aggregated to one
-- row per wallet FIRST and joined 1:1. (A previous version joined per-event,
-- per-day and per-day-again rows for the same wallet, which multiplies to
-- events x days x days rows for a single wallet: invisible on a small
-- fixture, billions of rows for one high-volume real-world address.)
-- Day windows are anchored on first_seen_date, not the first_seen_at
-- timestamp, for the same reason as int_wallet_activation.
CREATE OR REPLACE TABLE int_wallet_funnel_flags AS
WITH wallet_totals AS (
    SELECT
        wallet_address,
        SUM(event_count) AS total_events,
        COUNT(*) AS active_days,
        MAX(activity_date) AS last_active_date
    FROM int_wallet_daily_activity
    GROUP BY wallet_address
)
SELECT
    fs.wallet_address,
    fs.first_seen_at,
    fs.first_seen_at + INTERVAL 7 DAY AS eligible_at_7d,
    fs.first_seen_at + INTERVAL 30 DAY AS eligible_at_30d,
    CASE WHEN wt.total_events >= 2 THEN 1 ELSE 0 END AS has_second_event,
    CASE WHEN wt.active_days >= 2 THEN 1 ELSE 0 END AS active_second_distinct_day,
    CASE WHEN wt.last_active_date >= fs.first_seen_date + INTERVAL 7 DAY THEN 1 ELSE 0 END AS active_after_7d,
    CASE WHEN wt.last_active_date >= fs.first_seen_date + INTERVAL 30 DAY THEN 1 ELSE 0 END AS active_after_30d
FROM int_wallet_first_seen fs
JOIN wallet_totals wt USING (wallet_address);
