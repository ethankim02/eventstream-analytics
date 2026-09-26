-- Concentration over time: top-10%-of-wallets share of that week's volume,
-- recomputed independently within each week's own wallet population.
CREATE OR REPLACE TABLE mart_concentration_by_week AS
WITH weekly AS (
    SELECT DATE_TRUNC('week', block_timestamp) AS activity_week, from_address AS wallet_address, amount
    FROM stg_transfers
    WHERE NOT is_self_transfer
    UNION ALL
    SELECT DATE_TRUNC('week', block_timestamp), to_address, amount
    FROM stg_transfers
    WHERE NOT is_self_transfer
),
wallet_week AS (
    SELECT activity_week, wallet_address, SUM(amount) AS wallet_amount
    FROM weekly
    GROUP BY 1, 2
),
ranked AS (
    SELECT *, NTILE(10) OVER (PARTITION BY activity_week ORDER BY wallet_amount) AS decile
    FROM wallet_week
),
week_totals AS (
    SELECT activity_week, SUM(wallet_amount) AS total_amount
    FROM wallet_week
    GROUP BY 1
)
SELECT
    r.activity_week,
    SUM(CASE WHEN r.decile = 10 THEN r.wallet_amount ELSE 0 END) / t.total_amount AS top10pct_volume_share
FROM ranked r
JOIN week_totals t USING (activity_week)
GROUP BY r.activity_week, t.total_amount
ORDER BY 1;
