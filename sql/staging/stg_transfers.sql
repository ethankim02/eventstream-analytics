-- Deduplicated, normalized USDC transfer events.
-- Primary key: (transaction_hash, log_index) — a log can only occupy one
-- index within one transaction, so this pair is a valid natural key.
-- Self-transfers (from = to) are retained here and flagged, never dropped;
-- downstream marts decide per-metric whether to exclude them (see
-- docs/METRICS.md, "Self-transfers").
CREATE OR REPLACE TABLE stg_transfers AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_hash, log_index
            ORDER BY ingested_at DESC
        ) AS rn
    FROM raw_transfers
)
SELECT
    transaction_hash || '-' || log_index AS event_id,
    block_number,
    block_timestamp,
    transaction_hash,
    log_index,
    lower(token_address) AS token_address,
    lower(from_address) AS from_address,
    lower(to_address) AS to_address,
    raw_amount,
    amount,
    chain_id,
    network,
    source,
    (lower(from_address) = lower(to_address)) AS is_self_transfer
FROM deduped
WHERE rn = 1;
