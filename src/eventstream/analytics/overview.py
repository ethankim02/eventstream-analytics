"""Dataset provenance summary: what was loaded, from where, over which window.

Printed at the top of `eventstream analyze` and shown on the dashboard so every
reported number is traceable to (and labeled with) its source: real Base RPC
extraction vs. the synthetic fixture.
"""

from __future__ import annotations

from typing import Any

import duckdb


def dataset_overview(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """Counts and bounds of the loaded dataset (self-transfers reported, never hidden)."""
    row = con.execute(
        """
        SELECT
            string_agg(DISTINCT source, ', ' ORDER BY source) AS sources,
            string_agg(DISTINCT network, ', ' ORDER BY network) AS networks,
            COUNT(*) AS raw_events,
            COUNT(*) FILTER (WHERE is_self_transfer) AS self_transfer_events,
            COUNT(*) FILTER (WHERE NOT is_self_transfer) AS qualifying_events,
            MIN(block_number) AS min_block,
            MAX(block_number) AS max_block,
            MIN(block_timestamp) AS first_event_at,
            MAX(block_timestamp) AS last_event_at
        FROM stg_transfers
        """
    ).fetchone()
    assert row is not None
    keys = [
        "sources",
        "networks",
        "raw_events",
        "self_transfer_events",
        "qualifying_events",
        "min_block",
        "max_block",
        "first_event_at",
        "last_event_at",
    ]
    out: dict[str, Any] = dict(zip(keys, row, strict=True))

    wallets_any = con.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT from_address AS w FROM stg_transfers
            UNION
            SELECT to_address FROM stg_transfers
        )
        """
    ).fetchone()
    wallets_qualifying = con.execute("SELECT COUNT(*) FROM int_wallet_first_seen").fetchone()
    assert wallets_any is not None and wallets_qualifying is not None
    # every distinct address that appears on either side of any event, self-transfers included
    out["distinct_wallets_any_event"] = wallets_any[0]
    # addresses on either side of at least one qualifying (non-self) event: the analysis population
    out["distinct_wallets_qualifying"] = wallets_qualifying[0]
    return out
