-- Normalized USDC transfer events WITHOUT the global dedup window.
-- Same output as stg_transfers.sql, valid only when raw_transfers is already unique on
-- (transaction_hash, log_index). That holds for a window cut by `eventstream extract-window`:
-- live ingestion writes non-overlapping block segments and de-duplicates within each, and the
-- extractor refuses windows whose segments do not tile exactly. The build asserts uniqueness
-- right after this model runs (`assert_event_ids_unique`), so the assumption is verified, not
-- trusted. Skipping the ROW_NUMBER() window matters at scale: a partitioned sort over tens of
-- millions of string keys spills to disk and dominates the whole build (see docs/DECISIONS.md).
-- Self-transfers (from = to) are retained and flagged, never dropped.
CREATE OR REPLACE TABLE stg_transfers AS
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
FROM raw_transfers;
