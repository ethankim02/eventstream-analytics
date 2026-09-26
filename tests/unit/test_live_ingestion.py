"""Checkpointed live ingestion, exercised against a fake RPC (no network)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from unittest.mock import patch

import duckdb
import pyarrow.parquet as pq
import pytest

from eventstream.config import USDC_CONTRACT_BASE_MAINNET, Settings
from eventstream.ingestion.base_rpc import RpcClient, RpcError
from eventstream.ingestion.live import (
    PARQUET_SCHEMA,
    IngestionIncompleteError,
    ingest_block_range,
    manifest_filename,
    plan_segments,
    verify_manifest,
)

TOPIC0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
T_START = 1_800_000_000
START, END = 1000, 1399  # 400 blocks


def _addr(n: int) -> str:
    return f"{n:040x}"


def make_log(block: int, log_index: int, frm: int, to: int, amount: int, *, with_ts: bool = True):
    log = {
        "address": USDC_CONTRACT_BASE_MAINNET.lower(),
        "blockNumber": hex(block),
        "logIndex": hex(log_index),
        "transactionHash": "0x" + f"{block * 10 + log_index:064x}",
        "topics": [TOPIC0, "0x" + _addr(frm).rjust(64, "0"), "0x" + _addr(to).rjust(64, "0")],
        "data": "0x" + f"{amount:064x}",
        "removed": False,
    }
    if with_ts:
        log["blockTimestamp"] = hex(T_START + 2 * block)
    return log


def default_logs(start: int = START, end: int = END) -> dict[int, list[dict]]:
    """Two transfers per block; every 50th block also carries a self-transfer."""
    out: dict[int, list[dict]] = {}
    for b in range(start, end + 1):
        logs = [
            make_log(b, 0, 1 + b % 7, 100 + b % 11, 5_000_000),
            make_log(b, 1, 2 + b % 5, 200 + b % 3, 1_250_000),
        ]
        if b % 50 == 0:
            logs.append(make_log(b, 2, 9, 9, 3_000_000))
        out[b] = logs
    return out


@dataclass
class FakeRpc(RpcClient):
    """Real RpcClient chunking logic over an in-memory chain that enforces a span cap."""

    logs: dict[int, list[dict]] = field(default_factory=dict)
    max_span: int = 100
    chain_id: int = 8453
    fail_blocks: frozenset[int] = frozenset()
    calls: list[tuple[int, int]] = field(default_factory=list)
    block_timestamp_lookups: list[int] = field(default_factory=list)

    def get_chain_id(self) -> int:
        return self.chain_id

    def get_logs(self, address, topics, from_block, to_block):
        self.calls.append((from_block, to_block))
        if to_block - from_block + 1 > self.max_span:
            raise RpcError(-32614, "eth_getLogs is limited to a 2,000 range")
        if any(fb in self.fail_blocks for fb in range(from_block, to_block + 1)):
            raise RpcError(-32603, "internal error")
        out: list[dict] = []
        for b in range(from_block, to_block + 1):
            out.extend(dict(log) for log in self.logs.get(b, []))
        return out

    def get_block_timestamps(self, block_numbers):
        self.block_timestamp_lookups.extend(block_numbers)
        return {b: T_START + 2 * b for b in block_numbers}


@pytest.fixture
def settings() -> Settings:
    return Settings(block_chunk_size=400)


@pytest.fixture(autouse=True)
def _no_sleep():
    with patch("eventstream.ingestion.live.time.sleep"), patch("time.sleep"):
        yield


def run(settings, out_dir, fake, **kwargs):
    kwargs.setdefault("segment_blocks", 100)
    kwargs.setdefault("workers", 3)
    kwargs.setdefault("max_segment_attempts", 1)
    return ingest_block_range(settings, START, END, out_dir, client_factory=lambda: fake, **kwargs)


def test_plan_segments_tiles_the_range_exactly():
    segs = plan_segments(10, 34, 10)
    assert segs == [(10, 19), (20, 29), (30, 34)]
    assert plan_segments(5, 5, 100) == [(5, 5)]


def test_ingest_writes_segments_manifest_and_every_event(tmp_path: Path, settings):
    logs = default_logs()
    fake = FakeRpc(rpc_url="http://fake", logs=logs, max_span=100)
    result = run(settings, tmp_path, fake)

    expected_rows = sum(len(v) for v in logs.values())
    assert result.complete and result.row_count == expected_rows
    assert result.segments_total == 4 and result.segments_fetched == 4

    manifest = json.loads((tmp_path / manifest_filename(START, END)).read_text())
    assert manifest["complete"] is True
    assert manifest["chain_id"] == 8453
    assert manifest["token_address"] == USDC_CONTRACT_BASE_MAINNET
    assert manifest["start_block_timestamp"].startswith("2027-01-15")  # T_START + 2 * 1000
    assert sum(s["row_count"] for s in manifest["segments"]) == expected_rows
    assert verify_manifest(tmp_path, manifest) == []

    files = sorted(tmp_path.glob("*.parquet"))
    assert len(files) == 4
    assert all(pq.read_schema(f).equals(PARQUET_SCHEMA, check_metadata=False) for f in files)

    con = duckdb.connect(":memory:")
    n, n_dist, n_self = con.execute(
        f"""SELECT COUNT(*), COUNT(DISTINCT transaction_hash || '-' || log_index),
                   SUM(CASE WHEN is_self_transfer THEN 1 ELSE 0 END)
            FROM read_parquet('{(tmp_path / "*.parquet").as_posix()}')"""
    ).fetchone()
    assert n == n_dist == expected_rows  # no duplicates across segments
    assert n_self == 8  # self-transfers are kept, only flagged
    assert con.execute(
        f"SELECT DISTINCT source FROM read_parquet('{(tmp_path / '*.parquet').as_posix()}')"
    ).fetchall() == [("base_rpc_live",)]


def test_span_limit_is_adapted_to_not_failed(tmp_path: Path, settings):
    fake = FakeRpc(rpc_url="http://fake", logs=default_logs(), max_span=37)
    result = run(settings, tmp_path, fake, workers=1)
    assert result.row_count == sum(len(v) for v in default_logs().values())
    assert fake.calls  # chunk size (initially 250) had to shrink below the 37-block cap


def test_uses_block_timestamp_from_logs_without_extra_lookups(tmp_path: Path, settings):
    fake = FakeRpc(rpc_url="http://fake", logs=default_logs(), max_span=100)
    run(settings, tmp_path, fake)
    # Only the two boundary blocks are looked up (for the manifest); none per event block.
    assert sorted(fake.block_timestamp_lookups) == [START, END]


def test_falls_back_to_block_lookup_when_logs_lack_timestamps(tmp_path: Path, settings):
    logs = {b: [make_log(b, 0, 1, 2, 1_000_000, with_ts=False)] for b in range(START, END + 1)}
    fake = FakeRpc(rpc_url="http://fake", logs=logs, max_span=100)
    result = run(settings, tmp_path, fake)
    assert result.row_count == 400
    con = duckdb.connect(":memory:")
    (min_ts,) = con.execute(
        f"SELECT MIN(epoch(block_timestamp)) FROM read_parquet('{(tmp_path / '*.parquet').as_posix()}')"
    ).fetchone()
    assert int(min_ts) == T_START + 2 * START


def test_unresolvable_block_timestamp_fails_loudly_never_drops_events(tmp_path: Path, settings):
    logs = {b: [make_log(b, 0, 1, 2, 1_000_000, with_ts=False)] for b in range(START, END + 1)}

    class NoBlocks(FakeRpc):
        def get_block_timestamps(self, block_numbers):
            raise RpcError(None, "provider returned no block")

    fake = NoBlocks(rpc_url="http://fake", logs=logs, max_span=100)
    with pytest.raises(IngestionIncompleteError) as excinfo:
        run(settings, tmp_path, fake)
    assert len(excinfo.value.failed) == 4
    assert not list(tmp_path.glob("*.parquet"))


def test_failed_segment_is_reported_then_only_it_is_retried_on_resume(tmp_path: Path, settings):
    logs = default_logs()
    broken = FakeRpc(rpc_url="http://fake", logs=logs, max_span=100, fail_blocks=frozenset({1250}))
    with pytest.raises(IngestionIncompleteError) as excinfo:
        run(settings, tmp_path, broken)
    assert [(s, e) for s, e, _ in excinfo.value.failed] == [(1200, 1299)]
    manifest = json.loads((tmp_path / manifest_filename(START, END)).read_text())
    assert manifest["complete"] is False
    assert len(manifest["segments"]) == 3  # the healthy segments are checkpointed

    healthy = FakeRpc(rpc_url="http://fake", logs=logs, max_span=100)
    result = run(settings, tmp_path, healthy)
    assert result.complete
    assert result.segments_reused == 3 and result.segments_fetched == 1
    assert all(lo >= 1200 and hi <= 1299 for lo, hi in healthy.calls)  # nothing else re-fetched
    assert result.row_count == sum(len(v) for v in logs.values())


def test_missing_segment_file_is_detected_and_refetched(tmp_path: Path, settings):
    logs = default_logs()
    run(settings, tmp_path, FakeRpc(rpc_url="http://fake", logs=logs, max_span=100))
    victim = tmp_path / "usdc_transfers_1100_1199.parquet"
    victim.unlink()

    manifest = json.loads((tmp_path / manifest_filename(START, END)).read_text())
    assert any("1100" in p for p in verify_manifest(tmp_path, manifest))

    healthy = FakeRpc(rpc_url="http://fake", logs=logs, max_span=100)
    result = run(settings, tmp_path, healthy)
    assert result.segments_fetched == 1 and victim.exists()


def test_rerun_after_success_fetches_nothing(tmp_path: Path, settings):
    logs = default_logs()
    run(settings, tmp_path, FakeRpc(rpc_url="http://fake", logs=logs, max_span=100))
    again = FakeRpc(rpc_url="http://fake", logs=logs, max_span=100)
    result = run(settings, tmp_path, again)
    assert result.segments_fetched == 0 and result.segments_reused == 4
    assert again.calls == []


def test_resume_with_different_segmentation_is_refused(tmp_path: Path, settings):
    logs = default_logs()
    run(settings, tmp_path, FakeRpc(rpc_url="http://fake", logs=logs, max_span=100))
    with pytest.raises(RuntimeError, match="segment_blocks"):
        run(
            settings,
            tmp_path,
            FakeRpc(rpc_url="http://fake", logs=logs, max_span=100),
            segment_blocks=50,
        )


def test_wrong_chain_id_is_refused_before_any_fetch(tmp_path: Path, settings):
    fake = FakeRpc(rpc_url="http://fake", logs=default_logs(), chain_id=1)
    with pytest.raises(RuntimeError, match="chain id"):
        run(settings, tmp_path, fake)
    assert fake.calls == []


def test_log_from_unexpected_contract_fails_the_segment(tmp_path: Path, settings):
    logs = default_logs()
    logs[1010] = [{**logs[1010][0], "address": "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca"}]
    with pytest.raises(IngestionIncompleteError) as excinfo:
        run(settings, tmp_path, FakeRpc(rpc_url="http://fake", logs=logs, max_span=100))
    assert "unexpected contract" in excinfo.value.failed[0][2]


def test_log_outside_requested_range_fails_the_segment(tmp_path: Path, settings):
    class Leaky(FakeRpc):
        def get_logs(self, address, topics, from_block, to_block):
            out = super().get_logs(address, topics, from_block, to_block)
            return [*out, make_log(to_block + 5, 0, 1, 2, 1)]

    with pytest.raises(IngestionIncompleteError) as excinfo:
        run(settings, tmp_path, Leaky(rpc_url="http://fake", logs=default_logs(), max_span=100))
    assert "outside requested" in excinfo.value.failed[0][2]
