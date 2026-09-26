-- Explodes each transfer into two wallet-participant "interaction" rows
-- (SEND / RECEIVE) so wallet-level analytics can treat both counterparties
-- symmetrically. This intentionally is NOT used for token-volume totals
-- (mart_daily_metrics sums directly over stg_transfers) to avoid double
-- counting a single transfer's volume across two wallet rows.
CREATE OR REPLACE TABLE stg_wallet_events AS
SELECT
    event_id,
    block_timestamp,
    transaction_hash,
    from_address AS wallet_address,
    to_address AS counterparty_address,
    'SEND' AS direction,
    amount,
    is_self_transfer
FROM stg_transfers
UNION ALL
SELECT
    event_id,
    block_timestamp,
    transaction_hash,
    to_address AS wallet_address,
    from_address AS counterparty_address,
    'RECEIVE' AS direction,
    amount,
    is_self_transfer
FROM stg_transfers;
