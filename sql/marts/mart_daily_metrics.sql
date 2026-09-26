-- Daily activity mart. Self-transfers excluded throughout (they would
-- distort "active wallet" / distribution metrics — see docs/METRICS.md).
CREATE OR REPLACE TABLE mart_daily_metrics AS
WITH wallet_day AS (
    SELECT
        activity_date,
        COUNT(DISTINCT wallet_address) AS active_wallets
    FROM int_wallet_daily_activity
    GROUP BY activity_date
),
tx AS (
    SELECT
        DATE_TRUNC('day', block_timestamp) AS activity_date,
        COUNT(*) AS transfer_count,
        SUM(amount) AS total_volume,
        MEDIAN(amount) AS median_amount,
        approx_quantile(amount, 0.90) AS p90_amount,
        approx_quantile(amount, 0.99) AS p99_amount
    FROM stg_transfers
    WHERE NOT is_self_transfer
    GROUP BY 1
)
SELECT
    tx.activity_date,
    tx.transfer_count,
    wallet_day.active_wallets,
    tx.total_volume,
    tx.median_amount,
    tx.p90_amount,
    tx.p99_amount
FROM tx
JOIN wallet_day ON wallet_day.activity_date = tx.activity_date
ORDER BY 1;
