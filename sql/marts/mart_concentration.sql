-- Whole-window concentration: Gini coefficient (on per-wallet total
-- transferred amount) plus top-1%/top-10% shares of volume and top-10%
-- share of transaction count, via NTILE(100) percentile bucketing (an
-- approximation under ties — documented in docs/METRICS.md, "Gini").
CREATE OR REPLACE TABLE mart_concentration AS
WITH base AS (
    SELECT wallet_address, total_amount, total_events
    FROM int_wallet_lifecycle
    WHERE total_amount > 0
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY total_amount) AS amount_rank,
        NTILE(100) OVER (ORDER BY total_amount) AS amount_percentile,
        NTILE(100) OVER (ORDER BY total_events) AS event_percentile,
        COUNT(*) OVER () AS n
    FROM base
),
totals AS (
    SELECT SUM(total_amount) AS grand_total_amount, SUM(total_events) AS grand_total_events FROM base
),
gini AS (
    SELECT
        SUM((2 * amount_rank - n - 1) * total_amount) / (n * SUM(total_amount)) AS gini_coefficient
    FROM ranked
    GROUP BY n
)
SELECT
    (SELECT gini_coefficient FROM gini) AS gini_coefficient,
    SUM(CASE WHEN r.amount_percentile >= 100 THEN r.total_amount ELSE 0 END) / t.grand_total_amount
        AS top1pct_volume_share,
    SUM(CASE WHEN r.amount_percentile > 90 THEN r.total_amount ELSE 0 END) / t.grand_total_amount
        AS top10pct_volume_share,
    SUM(CASE WHEN r.event_percentile > 90 THEN r.total_events ELSE 0 END) / t.grand_total_events
        AS top10pct_transaction_share
FROM ranked r
CROSS JOIN totals t
GROUP BY t.grand_total_amount, t.grand_total_events;
