import pytest

from eventstream.ingestion.usdc import decode_transfer_log

TRANSFER_TOPIC0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
FROM_ADDR = "abc000000000000000000000000000000000dead"
TO_ADDR = "1111111111111111111111111111111111111a1a"


def _address_topic(addr: str) -> str:
    assert len(addr) == 40
    return "0x" + "0" * 24 + addr


def _log(topics=None, data="0x00000000000000000000000000000000000000000000000000000005f5e100"):
    return {
        "address": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "blockNumber": "0x64",
        "transactionHash": "0xdeadbeef",
        "logIndex": "0x2",
        "topics": topics
        or [
            TRANSFER_TOPIC0,
            _address_topic(FROM_ADDR),
            _address_topic(TO_ADDR),
        ],
        "data": data,
    }


def test_decode_transfer_log_fields():
    decoded = decode_transfer_log(_log())
    assert decoded["block_number"] == 100
    assert decoded["log_index"] == 2
    assert decoded["from_address"] == f"0x{FROM_ADDR}"
    assert decoded["to_address"] == f"0x{TO_ADDR}"
    assert decoded["raw_amount"] == 100_000_000


def test_decode_transfer_log_rejects_wrong_topic_count():
    with pytest.raises(ValueError):
        decode_transfer_log(_log(topics=["0xonly_one_topic"]))
