"""Fixture -> DuckDB -> SQL models -> analytics: full pipeline integration tests."""

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
            assert row["avg_retention_rate"] is None
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
