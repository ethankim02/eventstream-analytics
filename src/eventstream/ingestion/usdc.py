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
