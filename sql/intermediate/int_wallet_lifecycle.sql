-- Per-wallet lifecycle summary: total observed engagement plus the average
-- gap (in days) between consecutive active days, computed with LAG so a
-- wallet's own history — not the whole population — sets its baseline gap.
CREATE OR REPLACE TABLE int_wallet_lifecycle AS
WITH ordered AS (
    SELECT
        wallet_address,
        activity_date,
        LAG(activity_date) OVER (PARTITION BY wallet_address ORDER BY activity_date) AS prev_active_date
    FROM int_wallet_daily_activity
),
gaps AS (
    SELECT
        wallet_address,
        activity_date,
        DATE_DIFF('day', prev_active_date, activity_date) AS days_since_prev
    FROM ordered
)
SELECT
    d.wallet_address,
    fs.first_seen_at,
    fs.first_seen_week,
    MIN(d.activity_date) AS first_active_date,
    MAX(d.activity_date) AS last_active_date,
    COUNT(DISTINCT d.activity_date) AS active_days,
    SUM(d.event_count) AS total_events,
    SUM(d.gross_amount) AS total_amount,
    AVG(g.days_since_prev) AS avg_days_between_active_days
FROM int_wallet_daily_activity d
JOIN int_wallet_first_seen fs USING (wallet_address)
LEFT JOIN gaps g
    ON g.wallet_address = d.wallet_address AND g.activity_date = d.activity_date
GROUP BY d.wallet_address, fs.first_seen_at, fs.first_seen_week;
