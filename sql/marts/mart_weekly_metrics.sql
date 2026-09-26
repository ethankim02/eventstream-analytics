CREATE OR REPLACE TABLE mart_weekly_metrics AS
WITH wallet_week AS (
    SELECT
        DATE_TRUNC('week', activity_date) AS activity_week,
        COUNT(DISTINCT wallet_address) AS active_wallets
    FROM int_wallet_daily_activity
    GROUP BY 1
),
tx AS (
    SELECT
        DATE_TRUNC('week', block_timestamp) AS activity_week,
        COUNT(*) AS transfer_count,
        SUM(amount) AS total_volume,
        MEDIAN(amount) AS median_amount
    FROM stg_transfers
    WHERE NOT is_self_transfer
    GROUP BY 1
)
SELECT
    tx.activity_week,
    tx.transfer_count,
    wallet_week.active_wallets,
    tx.total_volume,
    tx.median_amount
FROM tx
JOIN wallet_week ON wallet_week.activity_week = tx.activity_week
ORDER BY 1;
