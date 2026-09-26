-- First-seen weekly cohort retention matrix, with explicit right-censoring
-- handling via `is_mature`: a (cohort_week, week_offset) cell is only
-- "mature" if the WHOLE target week (cohort_week + week_offset) lies inside the
-- observed data window, i.e. the target week's end is at or before the end of
-- the last observed calendar day. A week the window only partly covers (say
-- data ends on a Thursday) is therefore immature: counting it as observed would
-- score wallets that simply had no chance to return yet as non-returners.
-- Consumers (eventstream.analytics.retention) must exclude immature cells
-- before averaging W1/W4/W8-style summaries — see docs/METRICS.md "Censoring"
-- and docs/DECISIONS.md.
CREATE OR REPLACE TABLE mart_retention_cohorts AS
WITH bounds AS (
    -- End of the last observed calendar day (that day is assumed complete, so
    -- end a live window on a UTC midnight; see docs/DATA_SOURCE.md).
    SELECT DATE_TRUNC('day', MAX(block_timestamp)) + INTERVAL 1 DAY AS observed_end
    FROM stg_transfers
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
    -- Offset 0 is the cohort's own week: membership is defined by activity in it, so it
    -- cannot be censored even when that week is partial.
    (
        r.week_offset = 0
        OR (r.cohort_week + INTERVAL (r.week_offset + 1) WEEK) <= b.observed_end
    ) AS is_mature
FROM retained r
JOIN cohort_sizes cs USING (cohort_week)
CROSS JOIN bounds b
ORDER BY cohort_week, week_offset;
