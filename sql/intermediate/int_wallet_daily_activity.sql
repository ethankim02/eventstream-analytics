-- One row per (wallet, calendar day) the wallet had at least one qualifying
-- (non-self-transfer) interaction, with that day's event count and gross
-- amount moved (sum of SEND + RECEIVE legs — this is a behavioral activity
-- measure, not a de-duplicated token-volume measure).
CREATE OR REPLACE TABLE int_wallet_daily_activity AS
SELECT
    wallet_address,
    DATE_TRUNC('day', block_timestamp) AS activity_date,
    COUNT(*) AS event_count,
    SUM(amount) AS gross_amount
FROM stg_wallet_events
WHERE NOT is_self_transfer
GROUP BY wallet_address, DATE_TRUNC('day', block_timestamp);
