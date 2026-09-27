"""Write every published real-data number to one JSON file, straight from the warehouse.

Usage:
    python scripts/build_findings.py --warehouse data/processed/base_usdc_real.duckdb \
        --window-meta data/processed/window_51493327_51579726.window.json \
        --out docs/real_data_findings.json

The figures come from the project's own analytics functions, so they equal what
`eventstream analyze` prints. Nothing is hand-typed, and volatile fields (wall-clock times) are
left out, so rerunning on the same window rewrites the same file byte for byte.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from eventstream.analytics import concentration, lifecycle, overview, retention, segmentation
from eventstream.analytics.anomalies import anomalies_from_warehouse


def clean(x: Any) -> Any:
    """JSON-safe: numpy scalars -> python, NaN/NaT -> None, timestamps -> ISO strings,
    floats -> 10 significant digits."""
    if isinstance(x, dict):
        return {str(k): clean(v) for k, v in x.items()}
    if isinstance(x, list | tuple):
        return [clean(v) for v in x]
    if x is pd.NaT:
        return None
    if isinstance(x, pd.Timestamp):
        return x.isoformat()
    if hasattr(x, "item") and not hasattr(x, "isoformat"):
        x = x.item()
    if hasattr(x, "isoformat"):
        return x.isoformat()
    if isinstance(x, float):
        if math.isnan(x):
            return None
        # A parallel SUM(double) adds in a scheduling-dependent order, so the last ~2 digits
        # differ between runs. Ten significant digits is far beyond any published precision.
        return float(f"{x:.10g}")
    return x


def build(con: duckdb.DuckDBPyConnection, meta: dict[str, Any]) -> dict[str, Any]:
    f: dict[str, Any] = {}
    ov = overview.dataset_overview(con)
    f["dataset"] = ov
    f["window"] = {
        k: meta[k]
        for k in (
            "source",
            "network",
            "chain_id",
            "token_address",
            "rpc_url",
            "start_block",
            "end_block",
            "first_event_timestamp",
            "last_event_timestamp",
            "row_count",
            "keys_unique",
            "segments_tile_window",
            "source_manifest",
            "source_manifest_complete",
        )
    }
    f["window"]["segments"] = len(meta["segments"])
    f["window"]["warehouse_rows_match_window_file"] = meta["row_count"] == ov["raw_events"]

    # ---- daily / weekly activity ---------------------------------------------------------------
    daily = con.execute("SELECT * FROM mart_daily_metrics ORDER BY activity_date").fetchdf()
    f["daily"] = [
        {
            "date": r.activity_date.strftime("%Y-%m-%d"),
            "weekday": r.activity_date.strftime("%a"),
            "transfers": int(r.transfer_count),
            "active_wallets": int(r.active_wallets),
            "volume": float(r.total_volume),
            "median_amount": float(r.median_amount),
            "p90_amount": float(r.p90_amount),
            "p99_amount": float(r.p99_amount),
        }
        for r in daily.itertuples()
    ]
    f["days_observed"] = int(len(daily))
    weekly = con.execute("SELECT * FROM mart_weekly_metrics ORDER BY activity_week").fetchdf()
    days_per_week = dict(
        con.execute(
            "SELECT date_trunc('week', activity_date), COUNT(*) FROM mart_daily_metrics GROUP BY 1"
        ).fetchall()
    )
    f["weekly"] = [
        {
            "week_start": r.activity_week.strftime("%Y-%m-%d"),
            "days_covered": int(days_per_week[r.activity_week]),
            "transfers": int(r.transfer_count),
            "active_wallets": int(r.active_wallets),
            "volume": float(r.total_volume),
            "median_amount": float(r.median_amount),
        }
        for r in weekly.itertuples()
    ]
    f["total_volume"] = float(daily["total_volume"].sum())
    f["value_distribution"] = {
        k: float(v) for k, v in segmentation.value_distribution(con).iloc[0].items()
    }

    # ---- retention: what the window can and cannot support ------------------------------------
    matrix = retention.retention_matrix(con)
    sizes = retention.cohort_sizes(con).set_index("cohort_week")["cohort_size"]
    cells = con.execute(
        "SELECT * FROM mart_retention_cohorts ORDER BY cohort_week, week_offset"
    ).fetchdf()
    opening = cells["cohort_week"].min()
    total_wallets = int(sizes.sum())
    f["cohorts"] = [
        {
            "cohort_week": cw.strftime("%Y-%m-%d"),
            "opening": bool(cw == opening),
            "size": int(sizes[cw]),
            "share_of_all_wallets": float(sizes[cw] / total_wallets),
            "rates": {int(k): (None if pd.isna(v) else float(v)) for k, v in row.items()},
        }
        for cw, row in matrix.iterrows()
    ]
    mature = cells[cells["is_mature"]]
    max_mature_offset = int(mature["week_offset"].max())
    f["retention"] = {
        "max_offset_with_any_mature_cell": max_mature_offset,
        "weekly_retention_measurable": max_mature_offset >= 1,
        "immature_cells_by_offset": {
            int(o): int((~g["is_mature"]).sum()) for o, g in cells.groupby("week_offset")
        },
        "pooled": {},
    }
    for off in (1, 2, 4):
        sub = mature[mature["week_offset"] == off]
        f["retention"]["pooled"][off] = {
            "mature_cohorts": int(sub["cohort_week"].nunique()),
            "cohort_wallets": int(sub["cohort_size"].sum()),
            "rate": (
                float(sub["retained_wallets"].sum() / sub["cohort_size"].sum())
                if len(sub)
                else None
            ),
        }

    # ---- activation proxy + funnel ---------------------------------------------------------------
    f["activation"] = {}
    for off in (2, 4):
        c = lifecycle.activation_vs_retention(con, week_offset=off, exclude_opening_cohort=False)
        f["activation"][f"W{off}"] = vars(c)
    share = con.execute(
        "SELECT AVG(CASE WHEN is_activated THEN 1.0 ELSE 0 END) FROM int_wallet_activation"
    ).fetchone()
    assert share is not None
    f["activation_share_of_all_wallets"] = float(share[0])
    f["funnel"] = [
        {
            "stage": r.stage,
            "wallets": int(r.wallets),
            "denominator": int(r.eligible_denominator),
            "rate": None if pd.isna(r.conversion_rate) else float(r.conversion_rate),
        }
        for r in lifecycle.funnel_conversion(con).itertuples()
    ]

    # ---- segments, engagement, concentration ---------------------------------------------------
    seg = segmentation.segment_distribution(con)
    f["segments"] = [
        {
            "segment": r.frequency_segment,
            "wallets": int(r.wallets),
            "wallet_share": float(r.wallets / seg["wallets"].sum()),
            "events": float(r.total_events),
            "event_share": float(r.total_events / seg["total_events"].sum()),
            "gross_volume": float(r.total_amount),
            "volume_share": float(r.total_amount / seg["total_amount"].sum()),
        }
        for r in seg.itertuples()
    ]
    f["segment_cutoffs"] = {
        k: float(v) for k, v in segmentation.segment_cutoffs(con).iloc[0].items()
    }
    f["engagement"] = {k: float(v) for k, v in segmentation.engagement_summary(con).items()}
    vol = con.execute(
        "SELECT volume_segment, COUNT(*) AS n FROM mart_wallet_segments GROUP BY 1"
    ).fetchdf()
    f["high_volume_wallets"] = int(vol.loc[vol["volume_segment"] == "high_volume", "n"].sum())
    ext = con.execute(
        """SELECT MAX(total_events), MAX(active_days),
                  COUNT(*) FILTER (WHERE total_events = 1)
           FROM int_wallet_lifecycle"""
    ).fetchone()
    assert ext is not None
    f["wallet_extremes"] = {
        "max_events": int(ext[0]),
        "max_active_days": int(ext[1]),
        "one_event_wallets": int(ext[2]),
    }
    f["concentration"] = {
        k: float(v) for k, v in concentration.concentration_summary(con).iloc[0].items()
    }
    # volume held by the very largest wallets: shows the concentration is a handful of addresses.
    # The denominator is the total over ALL wallets (a window function after WHERE would only
    # see the rows that survived the filter).
    top = con.execute(
        """WITH ranked AS (
               SELECT total_amount, ROW_NUMBER() OVER (ORDER BY total_amount DESC) AS rn
               FROM int_wallet_lifecycle
           )
           SELECT SUM(total_amount) FILTER (WHERE rn <= 1) / SUM(total_amount),
                  SUM(total_amount) FILTER (WHERE rn <= 10) / SUM(total_amount),
                  SUM(total_amount) FILTER (WHERE rn <= 100) / SUM(total_amount)
           FROM ranked"""
    ).fetchone()
    assert top is not None
    f["top_wallet_volume_share"] = {"1": float(top[0]), "10": float(top[1]), "100": float(top[2])}

    # ---- data caveats the methodology docs refer to --------------------------------------------
    q = con.execute(
        """SELECT COUNT(*), COUNT(*) FILTER (WHERE amount = 0), COUNT(*) FILTER (WHERE amount < 1),
                  COUNT(DISTINCT from_address)
           FROM stg_transfers WHERE NOT is_self_transfer"""
    ).fetchone()
    assert q is not None
    ge1 = con.execute(
        """SELECT COUNT(*) FROM (
               SELECT from_address AS w FROM stg_transfers WHERE NOT is_self_transfer AND amount >= 1
               UNION SELECT to_address FROM stg_transfers WHERE NOT is_self_transfer AND amount >= 1)"""
    ).fetchone()
    tx = con.execute(
        """WITH per_tx AS (SELECT COUNT(*) AS n FROM stg_transfers GROUP BY transaction_hash)
           SELECT COUNT(*), SUM(n) FILTER (WHERE n > 1), SUM(n), MAX(n) FROM per_tx"""
    ).fetchone()
    assert ge1 is not None and tx is not None
    f["caveats"] = {
        "qualifying_events": int(q[0]),
        "zero_amount_events": int(q[1]),
        "zero_amount_share": float(q[1] / q[0]),
        "under_1_usdc_events": int(q[2]),
        "under_1_usdc_share": float(q[2] / q[0]),
        "sender_only_active_wallets": int(q[3]),
        "active_wallets_either_side": int(ov["distinct_wallets_qualifying"]),
        "active_wallets_moving_at_least_1_usdc": int(ge1[0]),
        "transactions": int(tx[0]),
        "events_in_multi_transfer_transactions_share": float(tx[1] / tx[2]),
        "max_transfer_logs_in_one_transaction": int(tx[3]),
    }

    # ---- anomalies ---------------------------------------------------------------------------------
    an = anomalies_from_warehouse(con)
    f["anomalies"] = {
        "days_observed": int(an["activity_date"].nunique()),
        "evaluable_metric_days": int(an["rolling_mad"].notna().sum()),
        "metrics": int(an["metric_name"].nunique()),
        "flagged": int(an["is_anomaly"].sum()),
    }
    return f


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--warehouse", required=True, type=Path)
    ap.add_argument("--window-meta", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    args = ap.parse_args()

    con = duckdb.connect(str(args.warehouse), read_only=True)
    con.execute("SET TimeZone='UTC'")
    meta = json.loads(args.window_meta.read_text(encoding="utf-8"))
    findings = clean(build(con, meta))
    args.out.write_text(
        json.dumps(findings, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8"
    )
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
