-- First-seen weekly cohort retention matrix, with explicit right-censoring
-- handling via `is_mature`: a (cohort_week, week_offset) cell is only
-- "mature" if cohort_week + week_offset weeks has fully elapsed within the
-- observed data window. Consumers (eventstream.analytics.retention) must
-- exclude immature cells before averaging W1/W4/W8-style summaries — see
-- docs/METRICS.md "Censoring" and docs/DECISIONS.md.
CREATE OR REPLACE TABLE mart_retention_cohorts AS
WITH bounds AS (
    SELECT MAX(activity_week) AS max_observed_week FROM int_wallet_activity_weeks
),
cohort AS (
    SELECT wallet_address, first_seen_week AS cohort_week
    FROM int_wallet_first_seen
),
joined AS (
    SELECT
        c.cohort_week,
        c.wallet_address,
        DATE_DIFF('week', c.cohort_week, a.activity_week) AS week_offset
    FROM cohort c
    JOIN int_wallet_activity_weeks a ON a.wallet_address = c.wallet_address
),
cohort_sizes AS (
    SELECT cohort_week, COUNT(DISTINCT wallet_address) AS cohort_size
    FROM cohort
    GROUP BY cohort_week
),
retained AS (
    SELECT cohort_week, week_offset, COUNT(DISTINCT wallet_address) AS retained_wallets
    FROM joined
    WHERE week_offset >= 0
    GROUP BY cohort_week, week_offset
)
SELECT
    r.cohort_week,
    cs.cohort_size,
    r.week_offset,
    r.retained_wallets,
    ROUND(r.retained_wallets * 1.0 / cs.cohort_size, 4) AS retention_rate,
    (r.cohort_week + INTERVAL (r.week_offset) WEEK) <= b.max_observed_week AS is_mature
FROM retained r
JOIN cohort_sizes cs USING (cohort_week)
CROSS JOIN bounds b
ORDER BY cohort_week, week_offset;
