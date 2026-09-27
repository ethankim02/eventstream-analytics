"""The raw-Parquet reconciliation must pass on a healthy warehouse and catch corrupted marts."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from eventstream import cli
from eventstream.validation.reconcile import reconcile_against_raw
from eventstream.warehouse.build import build_warehouse
from eventstream.warehouse.connection import connect_memory

SQL_ROOT = Path(__file__).resolve().parents[2] / "sql"


@pytest.fixture
def warehouse(tmp_path, small_fixture_df):
    path = tmp_path / "events.parquet"
    small_fixture_df.to_parquet(path, index=False)
    con = connect_memory()
    build_warehouse(con, str(path), SQL_ROOT)
    return con, path.as_posix()


def _failed(results) -> set[str]:
    return {r.name for r in results if not r.passed}


def test_reconciliation_passes_on_a_healthy_warehouse(warehouse):
    con, glob = warehouse
    results = reconcile_against_raw(con, glob, sample_wallets=100)
    assert len(results) == 9
    assert _failed(results) == set(), [r for r in results if not r.passed]


@pytest.mark.parametrize(
    "corruption, expected_check",
    [
        (
            "UPDATE mart_daily_metrics SET transfer_count = transfer_count + 1 "
            "WHERE activity_date = (SELECT MIN(activity_date) FROM mart_daily_metrics)",
            "reconcile_daily_metrics",
        ),
        (
            "UPDATE mart_retention_cohorts SET retained_wallets = retained_wallets + 1 "
            "WHERE week_offset = 1",
            "reconcile_retention_cells",
        ),
        (
            "UPDATE mart_retention_cohorts SET is_mature = NOT is_mature WHERE week_offset = 2",
            "reconcile_retention_cells",
        ),
        (
            "UPDATE int_wallet_lifecycle SET total_amount = total_amount * 1.01",
            "reconcile_wallet_totals",
        ),
        (
            "UPDATE mart_concentration SET gini_coefficient = gini_coefficient - 0.05",
            "reconcile_concentration",
        ),
        (
            "UPDATE mart_anomalies SET is_anomaly = NOT is_anomaly "
            "WHERE metric_name = 'transfer_count'",
            "reconcile_anomalies",
        ),
        (
            "UPDATE int_wallet_activation SET distinct_days_in_first_7d = "
            "distinct_days_in_first_7d + 1",
            "reconcile_wallet_sample_definitions",
        ),
        (
            "UPDATE int_wallet_funnel_flags SET active_after_7d = 1 - active_after_7d",
            "reconcile_wallet_sample_definitions",
        ),
    ],
)
def test_reconciliation_detects_corrupted_marts(warehouse, corruption, expected_check):
    con, glob = warehouse
    con.execute(corruption)
    assert expected_check in _failed(reconcile_against_raw(con, glob, sample_wallets=100))


def test_console_entry_point_does_not_let_click_expand_wildcards_on_windows():
    """`--source-glob "data/raw/*.parquet"` must reach the command as one literal argument."""
    with patch.object(cli, "app") as app:
        cli.main()
    app.assert_called_once_with(windows_expand_args=False)
