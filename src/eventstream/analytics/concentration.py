"""Concentration metrics: Gini, top-decile shares, and Lorenz-curve points."""

from __future__ import annotations

import duckdb
import numpy as np
import pandas as pd


def concentration_summary(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute("SELECT * FROM mart_concentration").fetchdf()


def concentration_by_week(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute("SELECT * FROM mart_concentration_by_week ORDER BY activity_week").fetchdf()


def lorenz_curve(con: duckdb.DuckDBPyConnection, n_points: int = 100) -> pd.DataFrame:
    """Lorenz curve points (cumulative share of wallets vs. cumulative share
    of volume) for plotting alongside the line of perfect equality.
    """
    amounts = (
        con.execute(
            "SELECT total_amount FROM int_wallet_lifecycle WHERE total_amount > 0 ORDER BY total_amount"
        )
        .fetchdf()["total_amount"]
        .to_numpy()
    )

    if amounts.size == 0:
        return pd.DataFrame(columns=["cumulative_wallet_share", "cumulative_volume_share"])

    cum_amount = np.cumsum(amounts)
    cum_share = cum_amount / cum_amount[-1]
    wallet_share = np.arange(1, len(amounts) + 1) / len(amounts)

    idx = np.linspace(0, len(amounts) - 1, num=min(n_points, len(amounts))).astype(int)
    return pd.DataFrame(
        {
            "cumulative_wallet_share": wallet_share[idx],
            "cumulative_volume_share": cum_share[idx],
        }
    )


def gini_coefficient(values: np.ndarray) -> float:
    """Reference pure-Python/NumPy Gini implementation used by tests to
    cross-check the SQL mart's result independently of DuckDB.
    """
    values = np.asarray(values, dtype=float)
    values = values[values > 0]
    if values.size == 0:
        return 0.0
    sorted_vals = np.sort(values)
    n = sorted_vals.size
    ranks = np.arange(1, n + 1)
    return float(
        (2 * np.sum(ranks * sorted_vals) - (n + 1) * np.sum(sorted_vals))
        / (n * np.sum(sorted_vals))
    )
