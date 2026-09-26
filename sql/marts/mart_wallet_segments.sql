-- Behavioral segmentation using data-derived quantile cutoffs (never
-- arbitrary hard-coded thresholds) — see docs/METRICS.md, "Behavioral
-- segments", for the exact rule and rationale:
--   one_time       : exactly 1 lifetime qualifying event
--   occasional     : >1 event, at or below the population median event count
--   repeat         : above median, at or below the 90th percentile
--   high_frequency : above the 90th percentile of events per wallet
--   high_volume    : total transferred amount at/above the 90th percentile
--                     (independent axis from frequency_segment)
CREATE OR REPLACE TABLE mart_wallet_segments AS
WITH lifecycle AS (
    SELECT wallet_address, total_events, total_amount, active_days
    FROM int_wallet_lifecycle
),
cutoffs AS (
    SELECT
        approx_quantile(total_events, 0.50) AS p50_events,
        approx_quantile(total_events, 0.90) AS p90_events,
        approx_quantile(total_amount, 0.90) AS p90_amount
    FROM lifecycle
)
SELECT
    l.wallet_address,
    l.total_events,
    l.total_amount,
    l.active_days,
    CASE
        WHEN l.total_events = 1 THEN 'one_time'
        WHEN l.total_events <= c.p50_events THEN 'occasional'
        WHEN l.total_events <= c.p90_events THEN 'repeat'
        ELSE 'high_frequency'
    END AS frequency_segment,
    CASE WHEN l.total_amount >= c.p90_amount THEN 'high_volume' ELSE 'standard_volume' END AS volume_segment
FROM lifecycle l
CROSS JOIN cutoffs c;
