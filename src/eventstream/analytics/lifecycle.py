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
    opening_cohort_excluded: bool = False


def activation_vs_retention(
    con: duckdb.DuckDBPyConnection,
    week_offset: int = 4,
    exclude_opening_cohort: bool = False,
) -> ActivationComparison:
    """Compare mature-cohort W{week_offset} observed return between wallets
    that did/didn't satisfy the activation proxy (>=2 distinct active days
    within their first 7 calendar days, first-seen day included).

    `exclude_opening_cohort` drops the window's first cohort week, which is
    dominated by wallets already active before observation began (left-censored)
    and so is not a first-seen cohort in any meaningful sense.

    Use week_offset >= 2 for interpretation: the activation window spans days
    0-6, so at offset 1 it can overlap the outcome week and the association is
    partly mechanical.
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
                        -- same rule as mart_retention_cohorts: the whole target week is observed
                        (fs.first_seen_week + INTERVAL (?) WEEK) <= (
                            SELECT DATE_TRUNC('day', MAX(block_timestamp)) + INTERVAL 1 DAY
                            FROM stg_transfers
                        ) AS is_mature
                    FROM int_wallet_first_seen fs
                    WHERE NOT ? OR fs.first_seen_week > (
                        SELECT MIN(first_seen_week) FROM int_wallet_first_seen
                    )
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
    df = con.execute(query, [week_offset + 1, exclude_opening_cohort, week_offset]).fetchdf()

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
        opening_cohort_excluded=exclude_opening_cohort,
    )


def funnel_conversion(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Behavioral (lifecycle) funnel conversion, maturity-aware.

    Stages 4 and 5 ("active again after 7/30 days") only count a wallet in
    their denominator once that eligibility window has actually elapsed as
    of the dataset's observed max date — otherwise a wallet first seen last
    week would count as a funnel drop-off it hasn't had time to avoid.
    """
    # Aggregated in the warehouse: this table has one row per wallet, which is
    # millions of rows on real data, so it is never pulled into pandas.
    row = con.execute(
        """
        WITH bound AS (SELECT MAX(block_timestamp) AS max_observed FROM stg_transfers)
        SELECT
            COUNT(*),
            COALESCE(SUM(has_second_event), 0),
            COALESCE(SUM(active_second_distinct_day), 0),
            COUNT(*) FILTER (WHERE eligible_at_7d <= max_observed),
            COALESCE(SUM(active_after_7d) FILTER (WHERE eligible_at_7d <= max_observed), 0),
            COUNT(*) FILTER (WHERE eligible_at_30d <= max_observed),
            COALESCE(SUM(active_after_30d) FILTER (WHERE eligible_at_30d <= max_observed), 0)
        FROM mart_lifecycle_funnel, bound
        """
    ).fetchone()
    assert row is not None
    total, second_event, second_day, elig_7d, active_7d, elig_30d, active_30d = (
        int(v) for v in row
    )

    stages = [
        {"stage": "first_event", "wallets": total, "eligible_denominator": total},
        {"stage": "second_event", "wallets": second_event, "eligible_denominator": total},
        {
            "stage": "active_second_distinct_day",
            "wallets": second_day,
            "eligible_denominator": total,
        },
        {"stage": "active_after_7d", "wallets": active_7d, "eligible_denominator": elig_7d},
        {"stage": "active_after_30d", "wallets": active_30d, "eligible_denominator": elig_30d},
    ]
    out = pd.DataFrame(stages)
    out["conversion_rate"] = out["wallets"] / out["eligible_denominator"].replace(0, pd.NA)
    return out
