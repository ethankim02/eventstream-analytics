"""Fixture -> DuckDB -> SQL models -> analytics: full pipeline integration tests."""

import pandas as pd

from eventstream.analytics import concentration, lifecycle, retention, segmentation
from eventstream.analytics.anomalies import anomalies_from_warehouse


def test_warehouse_builds_expected_tables(built_con):
    tables = {row[0] for row in built_con.execute("SHOW TABLES").fetchall()}
    for expected in [
        "raw_transfers",
        "stg_transfers",
        "stg_wallet_events",
        "int_wallet_first_seen",
        "mart_daily_metrics",
        "mart_retention_cohorts",
        "mart_wallet_segments",
        "mart_concentration",
        "mart_anomalies",
    ]:
        assert expected in tables


def test_stg_wallet_events_is_exactly_double_stg_transfers(built_con):
    n_transfers = built_con.execute("SELECT COUNT(*) FROM stg_transfers").fetchone()[0]
    n_wallet_events = built_con.execute("SELECT COUNT(*) FROM stg_wallet_events").fetchone()[0]
    assert n_wallet_events == 2 * n_transfers


def test_self_transfers_retained_in_staging_but_excluded_from_daily_activity(built_con):
    n_self = built_con.execute(
        "SELECT COUNT(*) FROM stg_transfers WHERE is_self_transfer"
    ).fetchone()[0]
    assert (
        n_self >= 0
    )  # dataset may or may not contain any, but the column must exist and be queryable
    # daily activity must never include a wallet's self-transfer-only day
    orphan = built_con.execute(
        """
        SELECT COUNT(*) FROM int_wallet_daily_activity d
        WHERE NOT EXISTS (
            SELECT 1 FROM stg_wallet_events e
            WHERE e.wallet_address = d.wallet_address
              AND e.block_timestamp >= d.activity_date
              AND NOT e.is_self_transfer
        )
        """
    ).fetchone()[0]
    assert orphan == 0


def test_first_seen_is_earliest_event_per_wallet(built_con):
    mismatches = built_con.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT fs.wallet_address, fs.first_seen_at, MIN(e.block_timestamp) AS true_min
            FROM int_wallet_first_seen fs
            JOIN stg_wallet_events e ON e.wallet_address = fs.wallet_address AND NOT e.is_self_transfer
            GROUP BY fs.wallet_address, fs.first_seen_at
            HAVING fs.first_seen_at != true_min
        )
        """
    ).fetchone()[0]
    assert mismatches == 0


def test_retention_matrix_immature_cells_are_nan(built_con):
    matrix = retention.retention_matrix(built_con)
    # last cohort row's later offsets must be NaN (not enough time elapsed)
    last_cohort = matrix.index.max()
    last_row = matrix.loc[last_cohort]
    assert last_row.isna().sum() >= 1 or last_row.notna().sum() == 1


def test_summary_retention_only_uses_mature_cohorts(built_con):
    summary = retention.summary_retention(built_con, offsets=(1, 2, 4, 8))
    for _, row in summary.iterrows():
        if row["mature_cohorts"] == 0:
            assert pd.isna(row["avg_retention_rate"])  # None becomes NaN inside a DataFrame
        else:
            assert 0.0 <= row["avg_retention_rate"] <= 1.0


def test_funnel_conversion_stages_are_non_increasing_within_group(built_con):
    funnel = lifecycle.funnel_conversion(built_con)
    counts = funnel.set_index("stage")["wallets"]
    # first_event -> second_event -> active_second_distinct_day are on the SAME
    # denominator (total wallets) and each stage requires the previous one
    assert counts["first_event"] >= counts["second_event"] >= counts["active_second_distinct_day"]


def test_concentration_metrics_within_valid_bounds(built_con):
    summary = concentration.concentration_summary(built_con).iloc[0]
    assert 0.0 <= summary["gini_coefficient"] <= 1.0
    assert 0.0 <= summary["top1pct_volume_share"] <= 1.0
    assert 0.0 <= summary["top10pct_volume_share"] <= 1.0
    assert summary["top1pct_volume_share"] <= summary["top10pct_volume_share"] + 1e-9


def test_sql_gini_matches_python_reference(built_con):
    amounts = (
        built_con.execute("SELECT total_amount FROM int_wallet_lifecycle WHERE total_amount > 0")
        .fetchdf()["total_amount"]
        .to_numpy()
    )
    sql_gini = concentration.concentration_summary(built_con).iloc[0]["gini_coefficient"]
    python_gini = concentration.gini_coefficient(amounts)
    assert abs(sql_gini - python_gini) < 1e-6


def test_lorenz_curve_is_monotonic_and_bounded(built_con):
    curve = concentration.lorenz_curve(built_con)
    assert curve["cumulative_wallet_share"].is_monotonic_increasing
    assert curve["cumulative_volume_share"].is_monotonic_increasing
    assert curve["cumulative_volume_share"].max() <= 1.0 + 1e-9


def test_segment_distribution_covers_all_wallets(built_con):
    seg = segmentation.segment_distribution(built_con)
    total_lifecycle = built_con.execute("SELECT COUNT(*) FROM int_wallet_lifecycle").fetchone()[0]
    assert seg["wallets"].sum() == total_lifecycle


def test_one_time_segment_has_exactly_one_event(built_con):
    row = built_con.execute(
        "SELECT MAX(total_events) FROM mart_wallet_segments WHERE frequency_segment = 'one_time'"
    ).fetchone()[0]
    if row is not None:
        assert row == 1


def test_sql_anomaly_mart_agrees_with_python_reference(built_con):
    from eventstream.analytics.anomalies import robust_z_scores

    sql_result = anomalies_from_warehouse(built_con)
    metric = "transfer_count"
    sql_subset = sql_result[sql_result["metric_name"] == metric].sort_values("activity_date")

    py_scores = robust_z_scores(sql_subset["metric_value"].reset_index(drop=True), window=7)
    sql_scores = sql_subset["robust_z_score"].reset_index(drop=True)

    both_present = py_scores["robust_z_score"].notna() & sql_scores.notna()
    assert both_present.sum() > 0
    diffs = (py_scores["robust_z_score"][both_present] - sql_scores[both_present]).abs()
    assert diffs.max() < 1e-6


def test_activation_proxy_wallets_are_subset_of_first_seen(built_con):
    n_activation = built_con.execute("SELECT COUNT(*) FROM int_wallet_activation").fetchone()[0]
    n_first_seen = built_con.execute("SELECT COUNT(*) FROM int_wallet_first_seen").fetchone()[0]
    assert n_activation == n_first_seen


def test_activation_comparison_returns_valid_rates(built_con):
    comparison = lifecycle.activation_vs_retention(built_con, week_offset=1)
    for rate in (comparison.activated_return_rate, comparison.non_activated_return_rate):
        if rate is not None:
            assert 0.0 <= rate <= 1.0


# --- Regression tests for defects found while running on real data -----------------
# Each compares the SQL models with an independent pandas computation straight from
# the raw events, so a wrong window or a wrong join shows up as a numeric mismatch.


def _wallet_days(df: pd.DataFrame) -> pd.DataFrame:
    """One row per (wallet, UTC day) with >=1 qualifying event, both legs of each transfer."""
    ev = df[~df["is_self_transfer"]]
    legs = pd.concat(
        [
            ev[["from_address", "block_timestamp"]].set_axis(["wallet", "ts"], axis=1),
            ev[["to_address", "block_timestamp"]].set_axis(["wallet", "ts"], axis=1),
        ]
    )
    legs["day"] = legs["ts"].dt.floor("D")
    return legs


def test_activation_window_counts_the_first_seen_day(built_con, small_fixture_df):
    """Documented rule (METRICS.md): >=2 distinct days within days 0-6, first-seen day included."""
    legs = _wallet_days(small_fixture_df)
    first_day = legs.groupby("wallet")["ts"].min().dt.floor("D").rename("first_day")
    days = legs.drop_duplicates(["wallet", "day"]).join(first_day, on="wallet")
    in_window = days[
        (days["day"] >= days["first_day"])
        & (days["day"] < days["first_day"] + pd.Timedelta(days=7))
    ]
    expected = in_window.groupby("wallet")["day"].nunique()

    sql = (
        built_con.execute(
            "SELECT wallet_address, distinct_days_in_first_7d, is_activated FROM int_wallet_activation"
        )
        .fetchdf()
        .set_index("wallet_address")
    )
    assert len(sql) == len(expected)
    assert (sql["distinct_days_in_first_7d"] == expected.reindex(sql.index)).all()
    assert ((sql["distinct_days_in_first_7d"] >= 2) == sql["is_activated"]).all()


def test_funnel_flags_match_independent_reference(built_con, small_fixture_df):
    legs = _wallet_days(small_fixture_df)
    g = legs.groupby("wallet")
    ref = pd.DataFrame(
        {
            "events": g.size(),
            "active_days": g["day"].nunique(),
            "first_day": g["ts"].min().dt.floor("D"),
            "last_day": g["day"].max(),
        }
    )
    ref["has_second_event"] = ref["events"] >= 2
    ref["second_day"] = ref["active_days"] >= 2
    ref["after_7d"] = ref["last_day"] >= ref["first_day"] + pd.Timedelta(days=7)
    ref["after_30d"] = ref["last_day"] >= ref["first_day"] + pd.Timedelta(days=30)

    sql = (
        built_con.execute("SELECT * FROM int_wallet_funnel_flags")
        .fetchdf()
        .set_index("wallet_address")
    )
    assert len(sql) == len(ref)  # one row per wallet, no join fan-out
    ref = ref.reindex(sql.index)
    assert (sql["has_second_event"].astype(bool) == ref["has_second_event"]).all()
    assert (sql["active_second_distinct_day"].astype(bool) == ref["second_day"]).all()
    assert (sql["active_after_7d"].astype(bool) == ref["after_7d"]).all()
    assert (sql["active_after_30d"].astype(bool) == ref["after_30d"]).all()


def test_final_partial_week_is_not_treated_as_observed(built_con, small_fixture_df):
    last_ts = small_fixture_df["block_timestamp"].max()
    assert last_ts.dayofweek != 6  # the shared fixture ends mid-week, which is the point
    observed_end = last_ts.floor("D") + pd.Timedelta(days=1)
    cells = built_con.execute(
        "SELECT cohort_week, week_offset, is_mature FROM mart_retention_cohorts"
    ).fetchdf()
    target_end = cells["cohort_week"] + pd.to_timedelta((cells["week_offset"] + 1) * 7, unit="D")
    expected = (cells["week_offset"] == 0) | (target_end <= observed_end)  # W0 is never censored
    assert (cells["is_mature"] == expected).all()
    assert not cells[cells["is_mature"]]["cohort_week"].empty  # ...but earlier weeks still mature


def _tiny_warehouse(tmp_path, events):
    """Build a warehouse from [(iso_timestamp, from_int, to_int)] events."""
    from datetime import UTC, datetime

    from eventstream.ingestion.normalize import build_event_row, rows_to_dataframe
    from eventstream.warehouse.build import build_warehouse
    from eventstream.warehouse.connection import connect_memory

    rows = []
    for i, (ts, frm, to) in enumerate(events):
        transfer = {
            "block_number": i + 1,
            "transaction_hash": "0x" + f"{i:064x}",
            "log_index": 0,
            "token_address": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
            "from_address": "0x" + f"{frm:040x}",
            "to_address": "0x" + f"{to:040x}",
            "raw_amount": 1_000_000,
        }
        rows.append(
            build_event_row(
                transfer,  # type: ignore[arg-type]
                datetime.fromisoformat(ts).replace(tzinfo=UTC),
                8453,
                "base-mainnet",
                "base_rpc_live",
                6,
            )
        )
    path = tmp_path / "tiny.parquet"
    rows_to_dataframe(rows).to_parquet(path, index=False)
    con = connect_memory()
    build_warehouse(con, str(path), SQL_ROOT)
    return con


SQL_ROOT = __import__("pathlib").Path(__file__).resolve().parents[2] / "sql"

# 2026-06-01 is a Monday. Wallets 1&2 first appear in week 0 and return in week 2 (2026-06-15).
_BASE_EVENTS = [
    ("2026-06-01T10:00:00", 1, 2),
    ("2026-06-15T10:00:00", 1, 2),
]


def test_retention_cell_is_mature_when_window_ends_exactly_on_week_boundary(tmp_path):
    events = [*_BASE_EVENTS, ("2026-06-21T23:59:59", 8, 9)]  # Sunday, last second of week 2
    con = _tiny_warehouse(tmp_path, events)
    cell = con.execute(
        "SELECT is_mature, retained_wallets FROM mart_retention_cohorts "
        "WHERE cohort_week = TIMESTAMPTZ '2026-06-01 00:00:00+00' AND week_offset = 2"
    ).fetchone()
    assert cell == (True, 2)


def test_retention_cell_is_immature_when_window_ends_mid_week(tmp_path):
    events = [*_BASE_EVENTS, ("2026-06-18T23:59:59", 8, 9)]  # Thursday: week 2 only 4/7 observed
    con = _tiny_warehouse(tmp_path, events)
    is_mature = con.execute(
        "SELECT is_mature FROM mart_retention_cohorts "
        "WHERE cohort_week = TIMESTAMPTZ '2026-06-01 00:00:00+00' AND week_offset = 2"
    ).fetchone()[0]
    assert is_mature is False
    # ...and week 1 (fully observed) is still mature
    assert con.execute(
        "SELECT bool_and(is_mature) FROM mart_retention_cohorts WHERE week_offset <= 1"
    ).fetchone()[0]


def test_activation_comparison_can_exclude_the_opening_cohort(built_con):
    pooled = lifecycle.activation_vs_retention(built_con, week_offset=1)
    later_only = lifecycle.activation_vs_retention(
        built_con, week_offset=1, exclude_opening_cohort=True
    )
    assert later_only.opening_cohort_excluded and not pooled.opening_cohort_excluded
    n_pooled = pooled.activated_wallets + pooled.non_activated_wallets
    n_later = later_only.activated_wallets + later_only.non_activated_wallets
    assert 0 < n_later < n_pooled  # the opening cohort's wallets were removed, and only those

    opening_week = built_con.execute(
        "SELECT MIN(first_seen_week) FROM int_wallet_first_seen"
    ).fetchone()[0]
    n_opening_mature = built_con.execute(
        "SELECT COUNT(*) FROM int_wallet_first_seen WHERE first_seen_week = ?", [opening_week]
    ).fetchone()[0]
    assert n_pooled - n_later == n_opening_mature


def test_segment_cutoffs_are_ordered(built_con):
    cutoffs = segmentation.segment_cutoffs(built_con).iloc[0]
    assert 1 <= cutoffs["p50_events"] <= cutoffs["p90_events"]
    assert cutoffs["p90_amount"] > 0
