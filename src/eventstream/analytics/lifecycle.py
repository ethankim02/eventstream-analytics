"""Activation proxy comparison and behavioral (lifecycle) funnel conversion.

Both analyses here are ASSOCIATIONAL, computed from observational blockchain
activity. Never phrase output as causal — see docs/EXPERIMENTATION.md for why
observational data cannot support a causal claim, and use the separate
`eventstream.experimentation` package when a causal question needs answering.
"""

from __future__ import annotations

from dataclasses import dataclass

import duckdb
import pandas as pd


@dataclass
class ActivationComparison:
    activated_wallets: int
    non_activated_wallets: int
    activated_return_rate: float | None
    non_activated_return_rate: float | None
    lift_ratio: float | None
    week_offset: int
    mature_only: bool


def activation_vs_retention(
    con: duckdb.DuckDBPyConnection, week_offset: int = 4
) -> ActivationComparison:
    """Compare mature-cohort W{week_offset} observed return between wallets
    that did/didn't satisfy the activation proxy (>=2 distinct active days
    in their first 7 observed days).
    """
    query = """
        WITH mature AS (
            SELECT wallet_address, cohort_week
            FROM (
                SELECT DISTINCT wallet_address, cohort_week, is_mature
                FROM (
                    SELECT
                        fs.wallet_address,
                        fs.first_seen_week AS cohort_week,
                        (fs.first_seen_week + INTERVAL (?) WEEK) <= (SELECT MAX(activity_week) FROM int_wallet_activity_weeks) AS is_mature
                    FROM int_wallet_first_seen fs
                )
                WHERE is_mature
            )
        ),
        returned AS (
            SELECT DISTINCT m.wallet_address
            FROM mature m
            JOIN int_wallet_activity_weeks a ON a.wallet_address = m.wallet_address
            WHERE DATE_DIFF('week', m.cohort_week, a.activity_week) = ?
        )
        SELECT
            act.is_activated,
            COUNT(DISTINCT m.wallet_address) AS cohort_wallets,
            COUNT(DISTINCT r.wallet_address) AS returned_wallets
        FROM mature m
        JOIN int_wallet_activation act ON act.wallet_address = m.wallet_address
        LEFT JOIN returned r ON r.wallet_address = m.wallet_address
        GROUP BY act.is_activated
    """
    df = con.execute(query, [week_offset, week_offset]).fetchdf()

    def _rate(is_activated: bool) -> tuple[int, float | None]:
        row = df[df["is_activated"] == is_activated]
        if row.empty or row.iloc[0]["cohort_wallets"] == 0:
            return 0, None
        wallets = int(row.iloc[0]["cohort_wallets"])
        rate = float(row.iloc[0]["returned_wallets"]) / wallets
        return wallets, rate

    activated_n, activated_rate = _rate(True)
    non_activated_n, non_activated_rate = _rate(False)
    lift = (
        activated_rate / non_activated_rate
        if activated_rate is not None and non_activated_rate not in (None, 0)
        else None
    )

    return ActivationComparison(
        activated_wallets=activated_n,
        non_activated_wallets=non_activated_n,
        activated_return_rate=activated_rate,
        non_activated_return_rate=non_activated_rate,
        lift_ratio=lift,
        week_offset=week_offset,
        mature_only=True,
    )


def funnel_conversion(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Behavioral (lifecycle) funnel conversion, maturity-aware.

    Stages 4 and 5 ("active again after 7/30 days") only count a wallet in
    their denominator once that eligibility window has actually elapsed as
    of the dataset's observed max date — otherwise a wallet first seen last
    week would count as a funnel drop-off it hasn't had time to avoid.
    """
    df = con.execute("SELECT * FROM mart_lifecycle_funnel").fetchdf()
    row = con.execute("SELECT MAX(block_timestamp) FROM stg_transfers").fetchone()
    assert row is not None
    max_observed = pd.Timestamp(row[0])

    total = len(df)
    eligible_7d = df[df["eligible_at_7d"] <= max_observed]
    eligible_30d = df[df["eligible_at_30d"] <= max_observed]

    stages = [
        {"stage": "first_event", "wallets": total, "eligible_denominator": total},
        {
            "stage": "second_event",
            "wallets": int(df["has_second_event"].sum()),
            "eligible_denominator": total,
        },
        {
            "stage": "active_second_distinct_day",
            "wallets": int(df["active_second_distinct_day"].sum()),
            "eligible_denominator": total,
        },
        {
            "stage": "active_after_7d",
            "wallets": int(eligible_7d["active_after_7d"].sum()),
            "eligible_denominator": len(eligible_7d),
        },
        {
            "stage": "active_after_30d",
            "wallets": int(eligible_30d["active_after_30d"].sum()),
            "eligible_denominator": len(eligible_30d),
        },
    ]
    out = pd.DataFrame(stages)
    out["conversion_rate"] = out["wallets"] / out["eligible_denominator"].replace(0, pd.NA)
    return out
