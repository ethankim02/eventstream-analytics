-- A wallet's first OBSERVED event within this dataset's window.
-- This is a first-seen (bounded-observation) cohort, not evidence of the
-- wallet's true first-ever on-chain activity. See docs/METRICS.md and
-- docs/DATA_MODEL.md ("first-seen wallet" vs. true acquisition).
-- Self-transfers are excluded: a wallet talking only to itself should not
-- define its first observed *interaction*.
CREATE OR REPLACE TABLE int_wallet_first_seen AS
SELECT
    wallet_address,
    MIN(block_timestamp) AS first_seen_at,
    DATE_TRUNC('week', MIN(block_timestamp)) AS first_seen_week
FROM stg_wallet_events
WHERE NOT is_self_transfer
GROUP BY wallet_address;
