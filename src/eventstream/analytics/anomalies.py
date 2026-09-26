"""Rolling-median / robust MAD z-score anomaly detection.

A pure pandas/numpy reference implementation, independent of the DuckDB SQL
in sql/marts/mart_anomalies.sql, so unit tests can validate the *statistical
method* in isolation (no warehouse needed) while an integration test checks
the SQL mart agrees with this reference on the same data.

Method and threshold rationale: see docs/METRICS.md, "Anomaly detection".
"""

from __future__ import annotations

import duckdb
import numpy as np
import pandas as pd

ROBUST_Z_SCALE = 0.6745
DEFAULT_THRESHOLD = 3.5
DEFAULT_WINDOW = 7


def robust_z_scores(series: pd.Series, window: int = DEFAULT_WINDOW) -> pd.DataFrame:
    """Trailing (excludes current point) rolling-median MAD z-score.

    Mirrors sql/marts/mart_anomalies.sql row for row:
      * rolling_median[i]  = median of the window's raw values (i-window..i-1)
      * abs_dev[i]         = |value[i] - rolling_median[i]| (i's OWN deviation)
      * rolling_mad[i]     = median of abs_dev over (i-window..i-1) — the
                              window's OWN abs_dev values, excluding abs_dev[i]
                              itself, exactly like the SQL's second window.
    Returns a DataFrame with rolling_median, rolling_mad, robust_z_score.
    The first `window` points have NaN scores (not enough trailing history).
    """
    rolling_median = series.shift(1).rolling(window=window, min_periods=window).median()
    abs_dev = (series - rolling_median).abs()
    rolling_mad = abs_dev.shift(1).rolling(window=window, min_periods=window).median()

    with np.errstate(divide="ignore", invalid="ignore"):
        z = ROBUST_Z_SCALE * (series - rolling_median) / rolling_mad
    z = z.replace([np.inf, -np.inf], np.nan)

    return pd.DataFrame(
        {
            "rolling_median": rolling_median,
            "rolling_mad": rolling_mad,
            "robust_z_score": z,
        }
    )


def detect_anomalies(
    df: pd.DataFrame,
    value_col: str,
    window: int = DEFAULT_WINDOW,
    threshold: float = DEFAULT_THRESHOLD,
) -> pd.DataFrame:
    scores = robust_z_scores(df[value_col], window=window)
    out = df.copy()
    out = out.join(scores)
    out["is_anomaly"] = out["robust_z_score"].abs() > threshold
    out.loc[out["rolling_mad"].fillna(0) == 0, "is_anomaly"] = False
    return out


def anomalies_from_warehouse(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute("SELECT * FROM mart_anomalies ORDER BY metric_name, activity_date").fetchdf()
