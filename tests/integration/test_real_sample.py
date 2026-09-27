"""The committed slice of REAL Base mainnet events runs through the whole pipeline.

`data/samples/base_usdc_real_sample.parquet` is 200 contiguous blocks (about 25k events) cut from
the real extraction and independently re-verified against the RPC when it was written (see its
metadata file). It gives CI real-shaped data (real addresses, real amount distribution,
same-block multi-transfer transactions, self-transfers) with no network access.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from eventstream.analytics import concentration, lifecycle, retention, segmentation
from eventstream.analytics.anomalies import anomalies_from_warehouse
from eventstream.config import USDC_CONTRACT_BASE_MAINNET
from eventstream.validation.checks import all_passed, run_checks
from eventstream.validation.reconcile import reconcile_against_raw
from eventstream.warehouse.build import build_warehouse
from eventstream.warehouse.connection import connect_memory

ROOT = Path(__file__).resolve().parents[2]
SAMPLE = ROOT / "data" / "samples" / "base_usdc_real_sample.parquet"
META = json.loads(
    (ROOT / "data" / "samples" / "base_usdc_real_sample.metadata.json").read_text(encoding="utf-8")
)


@pytest.fixture(scope="module")
def con():
    con = connect_memory()
    build_warehouse(con, SAMPLE.as_posix(), ROOT / "sql")
    return con


def test_sample_matches_its_provenance_metadata(con):
    n, lo, hi, n_self = con.execute(
        "SELECT COUNT(*), MIN(block_number), MAX(block_number), "
        "COUNT(*) FILTER (WHERE is_self_transfer) FROM stg_transfers"
    ).fetchone()
    assert (n, lo, hi, n_self) == (
        META["row_count"],
        META["start_block"],
        META["end_block"],
        META["self_transfer_rows"],
    )


def test_sample_rows_are_real_base_usdc_events(con):
    rows = con.execute(
        "SELECT DISTINCT source, chain_id, network, token_address FROM stg_transfers"
    ).fetchall()
    assert rows == [
        (
            "base_rpc_live",
            8453,
            "base-mainnet",
            USDC_CONTRACT_BASE_MAINNET.lower(),
        )
    ]


def test_block_timestamps_follow_base_two_second_block_time(con):
    """Every event's timestamp is exactly start + 2s x (block - start_block)."""
    start_ts = con.execute(
        "SELECT epoch(CAST(? AS TIMESTAMPTZ))", [META["start_block_timestamp"]]
    ).fetchone()[0]
    bad = con.execute(
        "SELECT COUNT(*) FROM stg_transfers WHERE epoch(block_timestamp) <> ? + 2 * (block_number - ?)",
        [start_ts, META["start_block"]],
    ).fetchone()[0]
    assert bad == 0


def test_data_quality_checks_pass_on_real_events(con):
    results = run_checks(con, USDC_CONTRACT_BASE_MAINNET)
    assert all_passed(results), [r for r in results if not r.passed]


def test_independent_reconciliation_passes_on_real_events(con):
    results = reconcile_against_raw(con, SAMPLE.as_posix(), sample_wallets=200)
    assert all(r.passed for r in results), [r for r in results if not r.passed]


def test_self_transfers_are_flagged_and_excluded_from_metrics_but_never_dropped(con):
    total = con.execute("SELECT COUNT(*) FROM stg_transfers").fetchone()[0]
    metric_events = con.execute("SELECT SUM(transfer_count) FROM mart_daily_metrics").fetchone()[0]
    assert total - metric_events == META["self_transfer_rows"] > 0


def test_a_six_minute_window_has_no_mature_retention_and_reports_none_instead_of_zero(con):
    summary = retention.summary_retention(con, offsets=(1, 2, 4))
    assert (summary["mature_cohorts"] == 0).all()
    comparison = lifecycle.activation_vs_retention(con, week_offset=4)
    assert comparison.activated_return_rate is None and comparison.non_activated_return_rate is None
    funnel = lifecycle.funnel_conversion(con).set_index("stage")
    assert funnel.loc["active_after_7d", "eligible_denominator"] == 0


def test_analytics_run_on_real_events(con):
    conc = concentration.concentration_summary(con).iloc[0]
    assert 0.0 < conc["gini_coefficient"] < 1.0
    assert segmentation.segment_distribution(con)["wallets"].sum() > 0
    assert len(anomalies_from_warehouse(con)) > 0
