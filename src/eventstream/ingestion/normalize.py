"""Normalization of decoded transfers into the canonical event schema.

See docs/DATA_MODEL.md for the full field-by-field data dictionary.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from decimal import Decimal

import pandas as pd

from eventstream.ingestion.usdc import RawTransfer

_ADDRESS_RE = re.compile(r"^0x[0-9a-f]{40}$")

CANONICAL_COLUMNS = [
    "event_id",
    "block_number",
    "block_timestamp",
    "transaction_hash",
    "log_index",
    "token_address",
    "from_address",
    "to_address",
    "raw_amount",
    "amount",
    "is_self_transfer",
    "chain_id",
    "network",
    "source",
    "ingested_at",
]


def normalize_address(address: str) -> str:
    """Lower-case, validate an EVM address. Raises ValueError if malformed."""
    normalized = address.lower()
    if not _ADDRESS_RE.match(normalized):
        raise ValueError(f"malformed address: {address!r}")
    return normalized


def raw_amount_to_decimal(raw_amount: int, decimals: int) -> Decimal:
    """Convert integer base units to a human-readable Decimal amount.

    Financial amounts are never represented as binary floats: base units are
    kept as Python ints end-to-end, and the human-readable amount is derived
    once, here, via Decimal.
    """
    return Decimal(raw_amount) / (Decimal(10) ** decimals)


def build_event_row(
    transfer: RawTransfer,
    block_timestamp: datetime,
    chain_id: int,
    network: str,
    source: str,
    decimals: int,
) -> dict:
    from_addr = normalize_address(transfer["from_address"])
    to_addr = normalize_address(transfer["to_address"])
    amount = raw_amount_to_decimal(transfer["raw_amount"], decimals)
    return {
        "event_id": f"{transfer['transaction_hash']}-{transfer['log_index']}",
        "block_number": transfer["block_number"],
        "block_timestamp": block_timestamp,
        "transaction_hash": transfer["transaction_hash"],
        "log_index": transfer["log_index"],
        "token_address": normalize_address(transfer["token_address"]),
        "from_address": from_addr,
        "to_address": to_addr,
        "raw_amount": transfer["raw_amount"],
        "amount": float(amount),
        "is_self_transfer": from_addr == to_addr,
        "chain_id": chain_id,
        "network": network,
        "source": source,
        "ingested_at": datetime.now(UTC),
    }


def rows_to_dataframe(rows: list[dict]) -> pd.DataFrame:
    """Build the canonical events DataFrame and deterministically deduplicate.

    Deduplication key is (transaction_hash, log_index), which is a valid
    natural primary key for an EVM event log: at most one log can occupy a
    given index within a given transaction.
    """
    if not rows:
        return pd.DataFrame(columns=CANONICAL_COLUMNS)
    df = pd.DataFrame(rows, columns=CANONICAL_COLUMNS)
    df = df.drop_duplicates(subset=["transaction_hash", "log_index"], keep="first")
    df = df.sort_values(["block_number", "log_index"]).reset_index(drop=True)
    return df
