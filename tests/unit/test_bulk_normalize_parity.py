"""The vectorized bulk-ingestion path must equal the per-row reference path."""

from __future__ import annotations

from datetime import UTC, datetime

import numpy as np
import pandas as pd
import pytest

from eventstream.ingestion.normalize import (
    build_event_row,
    rows_to_dataframe,
    transfers_to_dataframe,
)
from eventstream.ingestion.usdc import decode_transfer_log, decode_transfer_logs_columnar

TOPIC0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
USDC = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
T0 = 1_790_000_000


def _topic(addr_int: int, upper: bool = False) -> str:
    h = f"{addr_int:040x}"
    return "0x" + ("0" * 24 + (h.upper() if upper else h))


def _random_logs(n: int, seed: int) -> list[dict]:
    rng = np.random.default_rng(seed)
    logs = []
    for i in range(n):
        block = 51_000_000 + int(rng.integers(0, 500))
        frm = int(rng.integers(1, 40))
        to = frm if rng.random() < 0.05 else int(rng.integers(1, 40))
        magnitude = rng.choice([0, 1, 6, 9, 12, 15, 16, 17, 20])
        amount = 0 if magnitude == 0 else int(rng.integers(1, 10)) * 10 ** int(magnitude)
        if amount:
            amount += int(rng.integers(0, 999_999))  # exercise non-round base units
        logs.append(
            {
                "address": USDC.lower(),
                "blockNumber": hex(block),
                "logIndex": hex(i % 7),
                "transactionHash": "0x" + f"{i:064x}",
                "topics": [TOPIC0, _topic(frm, upper=bool(i % 2)), _topic(to, upper=bool(i % 3))],
                "data": "0x" + f"{amount:064x}",
                "blockTimestamp": hex(T0 + 2 * block),
                "removed": False,
            }
        )
    return logs


def _reference(logs: list[dict]) -> pd.DataFrame:
    rows = [
        build_event_row(
            transfer=decode_transfer_log(log),
            block_timestamp=datetime.fromtimestamp(int(log["blockTimestamp"], 16), tz=UTC),
            chain_id=8453,
            network="base-mainnet",
            source="base_rpc_live",
            decimals=6,
        )
        for log in logs
    ]
    return rows_to_dataframe(rows)


def _bulk(logs: list[dict]) -> pd.DataFrame:
    cols = decode_transfer_logs_columnar(logs)
    return transfers_to_dataframe(
        cols,
        cols["block_timestamp"],
        chain_id=8453,
        network="base-mainnet",
        source="base_rpc_live",
        decimals=6,
    )


def test_bulk_path_matches_reference_row_for_row():
    logs = _random_logs(600, seed=7)
    assert any(int(log["data"], 16) >= 2**53 for log in logs)  # the Decimal fallback is exercised
    assert any(int(log["data"], 16) == 0 for log in logs)
    ref, bulk = _reference(logs), _bulk(logs)
    assert len(ref) == len(bulk) == 600
    cols = [c for c in ref.columns if c != "ingested_at"]
    pd.testing.assert_frame_equal(
        ref[cols].reset_index(drop=True),
        bulk[cols].reset_index(drop=True),
        check_dtype=False,
    )
    # amount must be bit-identical, not merely close
    assert ref["amount"].to_numpy().tobytes() == bulk["amount"].to_numpy().tobytes()
    assert ref["is_self_transfer"].any()


def test_columnar_decode_matches_single_log_decode():
    logs = _random_logs(50, seed=3)
    cols = decode_transfer_logs_columnar(logs)
    for i, log in enumerate(logs):
        single = decode_transfer_log(log)
        for key in (
            "block_number",
            "transaction_hash",
            "log_index",
            "token_address",
            "from_address",
            "to_address",
            "raw_amount",
        ):
            assert cols[key][i] == single[key]  # type: ignore[literal-required]


def test_columnar_decode_reports_missing_block_timestamp_as_none():
    log = _random_logs(1, seed=1)[0]
    del log["blockTimestamp"]
    assert decode_transfer_logs_columnar([log])["block_timestamp"] == [None]


@pytest.mark.parametrize(
    "mutate, message",
    [
        (lambda log: log.update(topics=log["topics"][:2]), "expected 3 topics"),
        (lambda log: log.update(removed=True), "removed"),
    ],
)
def test_columnar_decode_rejects_malformed_logs(mutate, message):
    log = _random_logs(1, seed=2)[0]
    mutate(log)
    with pytest.raises(ValueError, match=message):
        decode_transfer_logs_columnar([log])


def test_bulk_normalize_rejects_malformed_address():
    log = _random_logs(1, seed=4)[0]
    log["topics"][1] = "0x" + "zz" * 32
    with pytest.raises(ValueError, match="malformed from address"):
        _bulk([log])


def test_bulk_normalize_deduplicates_on_transaction_hash_and_log_index():
    log = _random_logs(1, seed=5)[0]
    assert len(_bulk([log, dict(log)])) == 1
