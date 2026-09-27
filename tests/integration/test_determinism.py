"""Published numbers must not depend on how many threads DuckDB happened to use.

`approx_quantile` (a t-digest) merges per-thread sketches in a scheduling-dependent order, so its
result moves from run to run. On the real 5.7M-event window the p90 events-per-wallet cutoff
flipped between 9 and 10 across runs, silently re-labelling every wallet with exactly 10 events,
and the high-volume wallet count moved by ~300. The segmentation cutoffs are now exact quantiles;
these tests pin that down on real events (the committed sample).
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from eventstream.analytics import segmentation
from eventstream.warehouse.build import build_warehouse
from eventstream.warehouse.connection import connect_memory

ROOT = Path(__file__).resolve().parents[2]
SAMPLE = ROOT / "data" / "samples" / "base_usdc_real_sample.parquet"


def _build(threads: int):
    con = connect_memory()
    con.execute(f"SET threads={threads}")
    build_warehouse(con, SAMPLE.as_posix(), ROOT / "sql")
    return con


@pytest.mark.parametrize("table", ["mart_wallet_segments", "mart_daily_metrics"])
def test_marts_are_identical_across_thread_counts(table):
    one, many = _build(1), _build(8)
    a = one.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchdf()
    b = many.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchdf()
    pd.testing.assert_frame_equal(a, b)


def test_segment_cutoffs_and_value_distribution_are_exact_quantiles():
    con = _build(8)
    cutoffs = segmentation.segment_cutoffs(con).iloc[0]
    wallets = con.execute("SELECT total_events, total_amount FROM int_wallet_lifecycle").fetchdf()
    assert cutoffs["p50_events"] == pytest.approx(np.quantile(wallets["total_events"], 0.5))
    assert cutoffs["p90_events"] == pytest.approx(np.quantile(wallets["total_events"], 0.9))
    assert cutoffs["p90_amount"] == pytest.approx(np.quantile(wallets["total_amount"], 0.9))

    amounts = con.execute("SELECT amount FROM stg_transfers WHERE NOT is_self_transfer").fetchdf()
    dist = segmentation.value_distribution(con).iloc[0]
    for q, col in (
        (0.1, "p10_amount"),
        (0.5, "p50_amount"),
        (0.9, "p90_amount"),
        (0.99, "p99_amount"),
    ):
        assert dist[col] == pytest.approx(np.quantile(amounts["amount"], q))
