"""Decoding of raw ERC-20 Transfer logs for the USDC contract."""

from __future__ import annotations

from typing import Any, TypedDict


class RawTransfer(TypedDict):
    block_number: int
    transaction_hash: str
    log_index: int
    token_address: str
    from_address: str
    to_address: str
    raw_amount: int


def _topic_to_address(topic: str) -> str:
    """An indexed `address` topic is a 32-byte word; the address is the last 20 bytes."""
    return "0x" + topic[-40:]


def decode_transfer_log(log: dict[str, Any]) -> RawTransfer:
    """Decode a single eth_getLogs entry for `Transfer(address,address,uint256)`.

    Raises ValueError if the log does not look like a well-formed Transfer event
    (wrong topic count), so malformed data fails loudly instead of silently.
    """
    topics = log["topics"]
    if len(topics) != 3:
        raise ValueError(f"expected 3 topics for Transfer event, got {len(topics)}")

    return RawTransfer(
        block_number=int(log["blockNumber"], 16),
        transaction_hash=log["transactionHash"],
        log_index=int(log["logIndex"], 16),
        token_address=log["address"],
        from_address=_topic_to_address(topics[1]),
        to_address=_topic_to_address(topics[2]),
        raw_amount=int(log["data"], 16),
    )


def decode_transfer_logs_columnar(logs: list[dict[str, Any]]) -> dict[str, list[Any]]:
    """Decode many logs at once into column lists (bulk-ingestion hot path).

    Applies exactly the rules of `decode_transfer_log` (a parity test asserts
    this) plus two bulk-path guards: a `removed` (reorged) log is an error, and
    `block_timestamp` carries the provider's `blockTimestamp` (epoch seconds) or
    None when the provider did not attach one.
    """
    cols: dict[str, list[Any]] = {
        "block_number": [],
        "transaction_hash": [],
        "log_index": [],
        "token_address": [],
        "from_address": [],
        "to_address": [],
        "raw_amount": [],
        "block_timestamp": [],
    }
    for log in logs:
        topics = log["topics"]
        if len(topics) != 3:
            raise ValueError(f"expected 3 topics for Transfer event, got {len(topics)}")
        if log.get("removed"):
            raise ValueError(f"provider returned a removed (reorged) log: {log}")
        cols["block_number"].append(int(log["blockNumber"], 16))
        cols["transaction_hash"].append(log["transactionHash"])
        cols["log_index"].append(int(log["logIndex"], 16))
        cols["token_address"].append(log["address"])
        cols["from_address"].append("0x" + topics[1][-40:])
        cols["to_address"].append("0x" + topics[2][-40:])
        cols["raw_amount"].append(int(log["data"], 16))
        ts = log.get("blockTimestamp")
        cols["block_timestamp"].append(int(ts, 16) if ts is not None else None)
    return cols
