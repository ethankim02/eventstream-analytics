from datetime import UTC, datetime
from decimal import Decimal

import pytest

from eventstream.ingestion.normalize import (
    build_event_row,
    normalize_address,
    raw_amount_to_decimal,
    rows_to_dataframe,
)
from eventstream.ingestion.usdc import RawTransfer

ADDR = "0xAbC000000000000000000000000000000000dEaD"


def test_normalize_address_lowercases():
    assert normalize_address(ADDR) == ADDR.lower()


def test_normalize_address_rejects_malformed():
    with pytest.raises(ValueError):
        normalize_address("0x123")


def test_normalize_address_rejects_missing_prefix():
    with pytest.raises(ValueError):
        normalize_address(ADDR[2:])


def test_raw_amount_to_decimal_six_decimals():
    assert raw_amount_to_decimal(1_000_000, 6) == Decimal("1")
    assert raw_amount_to_decimal(1, 6) == Decimal("1") / Decimal(10**6)


def test_raw_amount_to_decimal_zero():
    assert raw_amount_to_decimal(0, 6) == Decimal("0")


def _transfer(tx_hash="0xaaa", log_index=0, frm=ADDR, to=ADDR, amount=1_000_000) -> RawTransfer:
    return RawTransfer(
        block_number=100,
        transaction_hash=tx_hash,
        log_index=log_index,
        token_address="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        from_address=frm,
        to_address=to,
        raw_amount=amount,
    )


def test_build_event_row_self_transfer_flagged():
    row = build_event_row(
        _transfer(frm=ADDR, to=ADDR),
        block_timestamp=datetime.now(UTC),
        chain_id=8453,
        network="base-mainnet",
        source="synthetic_fixture",
        decimals=6,
    )
    assert row["is_self_transfer"] is True
    assert row["event_id"] == "0xaaa-0"


def test_build_event_row_not_self_transfer():
    other = "0x1111111111111111111111111111111111111111"
    row = build_event_row(
        _transfer(frm=ADDR, to=other),
        block_timestamp=datetime.now(UTC),
        chain_id=8453,
        network="base-mainnet",
        source="synthetic_fixture",
        decimals=6,
    )
    assert row["is_self_transfer"] is False
    assert row["amount"] == pytest.approx(1.0)


def test_rows_to_dataframe_deduplicates_on_tx_hash_log_index():
    ts = datetime.now(UTC)
    row = build_event_row(
        _transfer(tx_hash="0xdupe", log_index=1),
        block_timestamp=ts,
        chain_id=8453,
        network="base-mainnet",
        source="synthetic_fixture",
        decimals=6,
    )
    df = rows_to_dataframe([row, dict(row)])
    assert len(df) == 1


def test_rows_to_dataframe_keeps_distinct_log_indices():
    ts = datetime.now(UTC)
    row0 = build_event_row(
        _transfer(tx_hash="0xsame", log_index=0),
        block_timestamp=ts,
        chain_id=8453,
        network="base-mainnet",
        source="synthetic_fixture",
        decimals=6,
    )
    row1 = build_event_row(
        _transfer(tx_hash="0xsame", log_index=1),
        block_timestamp=ts,
        chain_id=8453,
        network="base-mainnet",
        source="synthetic_fixture",
        decimals=6,
    )
    df = rows_to_dataframe([row0, row1])
    assert len(df) == 2


def test_rows_to_dataframe_empty():
    df = rows_to_dataframe([])
    assert len(df) == 0
