"""First-seen cohort retention analysis, with explicit censoring handling.

Terminology note: everything here is "first-seen" / "observed return", not
true user acquisition/retention — see docs/METRICS.md and docs/DATA_MODEL.md.
"""

from __future__ import annotations

import duckdb
import pandas as pd


def retention_matrix(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Wide cohort_week x week_offset retention-rate matrix.

    Immature (censored) cells are left as NaN rather than 0 or a computed
    rate, so a dashboard/report never silently treats "not enough time has
    passed yet" as "nobody returned".
    """
    df = con.execute("SELECT * FROM mart_retention_cohorts").fetchdf()
    df = df.copy()
    df.loc[~df["is_mature"], "retention_rate"] = pd.NA
    matrix = df.pivot(index="cohort_week", columns="week_offset", values="retention_rate")
    return matrix.sort_index()


def summary_retention(
    con: duckdb.DuckDBPyConnection, offsets: tuple[int, ...] = (1, 2, 4, 8)
) -> pd.DataFrame:
    """Mature-cohort-only average observed return rate at each week offset.

    Only rows flagged `is_mature` contribute to the average: an immature
    cohort/offset pair (the window hasn't fully elapsed yet) is excluded
    from the denominator entirely rather than being counted as a non-return.
    This is the right-censoring handling required for these numbers to be
    read as real retention estimates rather than an artifact of how recently
    the data was pulled.
    """
    df = con.execute("SELECT * FROM mart_retention_cohorts WHERE is_mature").fetchdf()
    rows: list[dict[str, object]] = []
    for offset in offsets:
        subset = df[df["week_offset"] == offset]
        if subset.empty:
            rows.append(
                {
                    "week_offset": offset,
                    "mature_cohorts": 0,
                    "total_cohort_wallets": 0,
                    "avg_retention_rate": None,
                }
            )
            continue
        rows.append(
            {
                "week_offset": offset,
                "mature_cohorts": subset["cohort_week"].nunique(),
                "total_cohort_wallets": int(subset["cohort_size"].sum()),
                "avg_retention_rate": float(
                    (subset["retained_wallets"].sum()) / (subset["cohort_size"].sum())
                ),
            }
        )
    return pd.DataFrame(rows)


def cohort_sizes(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute(
        "SELECT DISTINCT cohort_week, cohort_size FROM mart_retention_cohorts ORDER BY cohort_week"
    ).fetchdf()
