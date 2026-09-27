"""Behavioral segmentation summaries (thresholds defined in mart_wallet_segments.sql)."""

from __future__ import annotations

import duckdb
import pandas as pd


def segment_distribution(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute(
        """
        SELECT
            frequency_segment,
            COUNT(*) AS wallets,
            SUM(total_events) AS total_events,
            SUM(total_amount) AS total_amount
        FROM mart_wallet_segments
        GROUP BY frequency_segment
        ORDER BY total_events DESC
        """
    ).fetchdf()


def segment_cutoffs(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """The data-derived thresholds `mart_wallet_segments` used, recomputed the same way,
    so a reported segment table can state the cutoffs it depends on."""
    return con.execute(
        """
        SELECT
            quantile_cont(total_events, 0.50) AS p50_events,
            quantile_cont(total_events, 0.90) AS p90_events,
            quantile_cont(total_amount, 0.90) AS p90_amount
        FROM int_wallet_lifecycle
        """
    ).fetchdf()


def engagement_summary(con: duckdb.DuckDBPyConnection) -> dict:
    row = (
        con.execute(
            """
        SELECT
            AVG(total_events) AS avg_events_per_wallet,
            MEDIAN(total_events) AS median_events_per_wallet,
            AVG(active_days) AS avg_active_days,
            MEDIAN(active_days) AS median_active_days,
            SUM(CASE WHEN total_events > 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS repeat_wallet_rate
        FROM int_wallet_lifecycle
        """
        )
        .fetchdf()
        .iloc[0]
    )
    return row.to_dict()


def value_distribution(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Event-level transfer-value distribution (excludes self-transfers)."""
    return con.execute(
        """
        SELECT
            MEDIAN(amount) AS median_amount,
            AVG(amount) AS mean_amount,
            quantile_cont(amount, 0.10) AS p10_amount,
            quantile_cont(amount, 0.50) AS p50_amount,
            quantile_cont(amount, 0.90) AS p90_amount,
            quantile_cont(amount, 0.99) AS p99_amount,
            MAX(amount) AS max_amount
        FROM stg_transfers
        WHERE NOT is_self_transfer
        """
    ).fetchdf()
