"""Independent reconciliation of the warehouse against the raw Parquet.

`eventstream validate` checks the shape of the staged data. This module checks the
*answers*: every headline mart is recomputed straight from the raw Parquet files, by a
different route than the SQL models (no shared intermediate tables; the epoch
arithmetic and numpy code below deliberately avoid the functions the models use), and
compared cell by cell. It is what backs the claim that the models "work on real data":
run it with `eventstream validate --reconcile-glob "data/raw/*.parquet"`.

Wallet-level definitions (activation window, funnel flags) are re-derived in pandas from
per-(wallet, day) counts for a sample that always includes the highest-volume wallets,
since those are the rows a wrong join or window would corrupt.
"""

from __future__ import annotations

import duckdb
import numpy as np
import pandas as pd

from eventstream.analytics.anomalies import robust_z_scores
from eventstream.validation.checks import CheckResult

REL_TOL = 1e-9


def _scalar(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    row = con.execute(sql).fetchone()
    assert row is not None
    return int(row[0])


def _qualifying(parquet_glob: str) -> str:
    """Raw rows that count toward metrics: self-transfers decided from the addresses."""
    return (
        f"(SELECT * FROM read_parquet('{parquet_glob}') "
        "WHERE lower(from_address) <> lower(to_address))"
    )


def _legs(parquet_glob: str) -> str:
    """One row per (wallet, transfer) for both sides of every qualifying transfer."""
    q = _qualifying(parquet_glob)
    return (
        f"(SELECT lower(from_address) AS w, block_timestamp AS t, amount AS a FROM {q} "
        f"UNION ALL SELECT lower(to_address), block_timestamp, amount FROM {q})"
    )


def _ntile_top_share(sorted_values: np.ndarray, first_bucket_exclusive: int) -> float:
    """Share of the total held by NTILE(100) buckets > first_bucket_exclusive (ascending order).

    Replicates SQL NTILE(100): with n rows, the first n % 100 buckets hold one extra row.
    """
    n = len(sorted_values)
    base, extra = divmod(n, 100)
    sizes = np.array([base + 1] * extra + [base] * (100 - extra))
    start = int(sizes[:first_bucket_exclusive].sum())
    return float(sorted_values[start:].sum() / sorted_values.sum())


def reconcile_against_raw(
    con: duckdb.DuckDBPyConnection, parquet_glob: str, sample_wallets: int = 2000
) -> list[CheckResult]:
    results: list[CheckResult] = []

    def add(name: str, mismatches: int, detail: str) -> None:
        results.append(
            CheckResult(
                name=name, passed=mismatches == 0, detail=f"{mismatches} mismatches; {detail}"
            )
        )

    raw = f"read_parquet('{parquet_glob}')"
    q = _qualifying(parquet_glob)
    legs = _legs(parquet_glob)

    # -- rows ------------------------------------------------------------------------------
    n_raw = _scalar(con, f"SELECT COUNT(*) FROM {raw}")
    n_stg = _scalar(con, "SELECT COUNT(*) FROM stg_transfers")
    n_distinct = _scalar(
        con, f"SELECT COUNT(*) FROM (SELECT DISTINCT transaction_hash, log_index FROM {raw})"
    )
    add(
        "reconcile_row_counts",
        int(n_raw != n_stg or n_raw != n_distinct),
        f"parquet {n_raw:,} rows / {n_distinct:,} distinct keys / stg_transfers {n_stg:,}",
    )
    flag_bad = _scalar(
        con,
        f"SELECT COUNT(*) FROM {raw} "
        "WHERE is_self_transfer <> (lower(from_address) = lower(to_address))",
    )
    add("reconcile_self_transfer_flag", flag_bad, "is_self_transfer agrees with the addresses")

    # -- daily and weekly activity ---------------------------------------------------------
    for grain, name, table, key in (
        ("day", "reconcile_daily_metrics", "mart_daily_metrics", "activity_date"),
        ("week", "reconcile_weekly_metrics", "mart_weekly_metrics", "activity_week"),
    ):
        bad = _scalar(
            con,
            f"""
            WITH ref AS (
                SELECT date_trunc('{grain}', block_timestamp) AS d, COUNT(*) AS n,
                       SUM(amount) AS v, MEDIAN(amount) AS med
                FROM {q} GROUP BY 1
            ),
            refw AS (
                SELECT d, COUNT(DISTINCT w) AS aw FROM (
                    SELECT date_trunc('{grain}', t) AS d, w FROM {legs}
                ) GROUP BY d
            )
            SELECT COUNT(*) FROM {table} m
            FULL JOIN ref ON ref.d = m.{key}
            FULL JOIN refw ON refw.d = ref.d
            WHERE m.{key} IS NULL OR ref.d IS NULL
               OR m.transfer_count <> ref.n
               OR m.active_wallets <> refw.aw
               OR ABS(m.total_volume - ref.v) > {REL_TOL} * GREATEST(1, ABS(ref.v))
               OR m.median_amount <> ref.med
            """,
        )
        add(
            name,
            bad,
            f"count, volume, median, active wallets per {grain}",
        )

    # -- wallet totals and first seen ------------------------------------------------------
    bad = _scalar(
        con,
        f"""
        WITH ref AS (SELECT w, COUNT(*) AS n, SUM(a) AS s, MIN(t) AS first_t FROM {legs} GROUP BY w)
        SELECT COUNT(*) FROM int_wallet_lifecycle l
        FULL JOIN ref ON ref.w = l.wallet_address
        WHERE l.wallet_address IS NULL OR ref.w IS NULL
           OR l.total_events <> ref.n
           OR ABS(l.total_amount - ref.s) > {REL_TOL} * GREATEST(1, ABS(ref.s))
           OR l.first_seen_at <> ref.first_t
        """,
    )
    add("reconcile_wallet_totals", bad, "events, gross amount and first-seen time for every wallet")

    # -- retention: every (cohort, offset) cell and its maturity flag ----------------------
    bad = _scalar(
        con,
        f"""
        WITH wk AS (
            SELECT DISTINCT w, date_trunc('week', t) AS wk FROM {legs}
        ),
        first AS (SELECT w, MIN(wk) AS c FROM wk GROUP BY w),
        sizes AS (SELECT c, COUNT(*) AS size FROM first GROUP BY c),
        cells AS (
            SELECT f.c, CAST((epoch(wk.wk) - epoch(f.c)) / 604800 AS INTEGER) AS off,
                   COUNT(*) AS retained
            FROM first f JOIN wk USING (w) GROUP BY 1, 2
        ),
        obs AS (SELECT date_trunc('day', MAX(block_timestamp)) + INTERVAL 1 DAY AS observed_end
                FROM {q}),
        ref AS (
            SELECT cells.c, cells.off, cells.retained, sizes.size,
                   (cells.off = 0 OR cells.c + INTERVAL (cells.off + 1) WEEK <= obs.observed_end)
                       AS mature
            FROM cells JOIN sizes ON sizes.c = cells.c CROSS JOIN obs
        )
        SELECT COUNT(*) FROM mart_retention_cohorts m
        FULL JOIN ref ON ref.c = m.cohort_week AND ref.off = m.week_offset
        WHERE m.cohort_week IS NULL OR ref.c IS NULL
           OR m.retained_wallets <> ref.retained
           OR m.cohort_size <> ref.size
           OR m.is_mature <> ref.mature
        """,
    )
    add("reconcile_retention_cells", bad, "retained wallets, cohort size, maturity per cell")

    # -- concentration: numpy reference for Gini and NTILE(100) shares ----------------------
    per_wallet = con.execute(
        f"SELECT SUM(a) AS s FROM {legs} GROUP BY w HAVING SUM(a) > 0"
    ).fetchnumpy()["s"]
    amounts = np.sort(np.asarray(per_wallet, dtype=float))
    n = len(amounts)
    ranks = np.arange(1, n + 1)
    gini = float((2 * (ranks * amounts).sum() - (n + 1) * amounts.sum()) / (n * amounts.sum()))
    ref_conc = {
        "gini_coefficient": gini,
        "top1pct_volume_share": _ntile_top_share(amounts, 99),
        "top10pct_volume_share": _ntile_top_share(amounts, 90),
    }
    mart = con.execute("SELECT * FROM mart_concentration").fetchdf().iloc[0]
    diffs = {k: abs(float(mart[k]) - v) for k, v in ref_conc.items()}
    add(
        "reconcile_concentration",
        sum(d > 1e-7 for d in diffs.values()),
        f"gini {gini:.6f} vs mart {float(mart['gini_coefficient']):.6f}; "
        f"top1% {ref_conc['top1pct_volume_share']:.6f}; top10% {ref_conc['top10pct_volume_share']:.6f}",
    )

    # -- anomalies: pure-pandas reference on the daily metrics -------------------------------
    daily = con.execute("SELECT * FROM mart_daily_metrics ORDER BY activity_date").fetchdf()
    mart_anom = con.execute("SELECT * FROM mart_anomalies").fetchdf()
    bad_anom = 0
    for metric in ("active_wallets", "transfer_count", "total_volume", "median_amount"):
        ref = robust_z_scores(daily[metric].astype(float).reset_index(drop=True))
        ref_flag = (ref["robust_z_score"].abs() > 3.5) & (ref["rolling_mad"].fillna(0) > 0)
        got = mart_anom[mart_anom["metric_name"] == metric].sort_values("activity_date")
        got_flag = got["is_anomaly"].reset_index(drop=True)
        bad_anom += int((ref_flag.reset_index(drop=True) != got_flag).sum())
        both = ref["robust_z_score"].notna().to_numpy() & got["robust_z_score"].notna().to_numpy()
        zdiff = np.abs(
            ref["robust_z_score"].to_numpy()[both] - got["robust_z_score"].to_numpy()[both]
        )
        bad_anom += int(
            (zdiff > 1e-6 * np.maximum(1, np.abs(ref["robust_z_score"].to_numpy()[both]))).sum()
        )
    add("reconcile_anomalies", bad_anom, "flags and robust z-scores for all four monitored metrics")

    # -- activation proxy and funnel flags, re-derived in pandas for a wallet sample ---------
    results.append(_reconcile_wallet_sample(con, legs, sample_wallets))
    return results


def _reconcile_wallet_sample(
    con: duckdb.DuckDBPyConnection, legs: str, sample_wallets: int
) -> CheckResult:
    half = max(1, sample_wallets // 2)
    # The heaviest wallets (most events) plus a deterministic pseudo-random slice.
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE _sample AS
        SELECT wallet_address AS w FROM (
            SELECT wallet_address FROM int_wallet_lifecycle ORDER BY total_events DESC LIMIT {half}
        )
        UNION
        SELECT wallet_address FROM (
            SELECT wallet_address FROM int_wallet_lifecycle
            ORDER BY hash(wallet_address) LIMIT {half}
        )
        """
    )
    days = con.execute(
        f"""
        SELECT l.w, CAST(floor(epoch(l.t) / 86400) AS BIGINT) AS day, COUNT(*) AS n
        FROM {legs} l JOIN _sample s ON s.w = l.w
        GROUP BY 1, 2
        """
    ).fetchdf()
    got = (
        con.execute(
            """
        SELECT f.wallet_address AS w, f.has_second_event, f.active_second_distinct_day,
               f.active_after_7d, f.active_after_30d, a.distinct_days_in_first_7d,
               l.active_days, l.total_events
        FROM int_wallet_funnel_flags f
        JOIN int_wallet_activation a USING (wallet_address)
        JOIN int_wallet_lifecycle l USING (wallet_address)
        JOIN _sample s ON s.w = f.wallet_address
        """
        )
        .fetchdf()
        .set_index("w")
    )
    con.execute("DROP TABLE _sample")

    # Definitions from docs/METRICS.md, applied to per-(wallet, UTC day) counts.
    g = days.groupby("w")
    ref = pd.DataFrame(
        {
            "events": g["n"].sum(),
            "active_days": g["day"].nunique(),
            "first_day": g["day"].min(),
            "last_day": g["day"].max(),
        }
    )
    in_window = days.merge(ref["first_day"], left_on="w", right_index=True)
    in_window = in_window[
        (in_window["day"] >= in_window["first_day"])
        & (in_window["day"] < in_window["first_day"] + 7)
    ]
    ref["days_in_first_7"] = in_window.groupby("w")["day"].nunique()
    ref["after_7"] = (ref["last_day"] >= ref["first_day"] + 7).astype(int)
    ref["after_30"] = (ref["last_day"] >= ref["first_day"] + 30).astype(int)

    got = got.reindex(ref.index)
    mismatch = (
        (got["has_second_event"] != (ref["events"] >= 2).astype(int))
        | (got["active_second_distinct_day"] != (ref["active_days"] >= 2).astype(int))
        | (got["active_after_7d"] != ref["after_7"])
        | (got["active_after_30d"] != ref["after_30"])
        | (got["distinct_days_in_first_7d"] != ref["days_in_first_7"])
        | (got["active_days"] != ref["active_days"])
        | (got["total_events"] != ref["events"])
    )
    n_bad = int(mismatch.sum()) + int(got["total_events"].isna().sum())
    return CheckResult(
        name="reconcile_wallet_sample_definitions",
        passed=n_bad == 0,
        detail=(
            f"{n_bad} mismatches; activation window + funnel flags re-derived in pandas for "
            f"{len(ref):,} wallets (the heaviest by events plus a hashed sample)"
        ),
    )
