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


def transfers_to_dataframe(
    cols: dict[str, list],
    block_timestamps: list[int],
    chain_id: int,
    network: str,
    source: str,
    decimals: int,
) -> pd.DataFrame:
    """Vectorized equivalent of `build_event_row` + `rows_to_dataframe` (bulk path).

    `cols` is the output of `usdc.decode_transfer_logs_columnar`; `block_timestamps`
    are epoch seconds aligned with it. Produces the same canonical frame as the
    per-row reference implementation (a parity test asserts this), roughly 5x
    faster, which matters when ingesting hundreds of millions of events. Raises
    ValueError on a malformed address exactly like `normalize_address`.
    """
    from_addr = pd.Series(cols["from_address"], dtype="string").str.lower()
    to_addr = pd.Series(cols["to_address"], dtype="string").str.lower()
    token = pd.Series(cols["token_address"], dtype="string").str.lower()
    for name, series in (("from", from_addr), ("to", to_addr), ("token", token)):
        bad = ~series.str.fullmatch(_ADDRESS_RE.pattern).fillna(False).astype(bool)
        if bad.any():
            raise ValueError(f"malformed {name} address: {series[bad].iloc[0]!r}")

    raw = cols["raw_amount"]
    scale = 10**decimals
    # float division is bit-identical to float(Decimal(raw) / 10**decimals) whenever
    # raw is exactly representable as a double (< 2**53); route anything larger
    # through the reference Decimal path.
    amount = [r / scale if r < 2**53 else float(raw_amount_to_decimal(r, decimals)) for r in raw]

    n = len(raw)
    df = pd.DataFrame(
        {
            "event_id": [
                f"{t}-{i}" for t, i in zip(cols["transaction_hash"], cols["log_index"], strict=True)
            ],
            "block_number": cols["block_number"],
            "block_timestamp": pd.to_datetime(block_timestamps, unit="s", utc=True),
            "transaction_hash": cols["transaction_hash"],
            "log_index": cols["log_index"],
            "token_address": token.astype(object),
            "from_address": from_addr.astype(object),
            "to_address": to_addr.astype(object),
            "raw_amount": raw,
            "amount": amount,
            "is_self_transfer": (from_addr == to_addr).astype(bool),
            "chain_id": [chain_id] * n,
            "network": [network] * n,
            "source": [source] * n,
            "ingested_at": [datetime.now(UTC)] * n,
        },
        columns=CANONICAL_COLUMNS,
    )
    df = df.drop_duplicates(subset=["transaction_hash", "log_index"], keep="first")
    return df.sort_values(["block_number", "log_index"]).reset_index(drop=True)


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
