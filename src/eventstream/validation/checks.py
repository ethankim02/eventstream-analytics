"""Explicit, inspectable data-quality checks over the warehouse.

Run via `eventstream validate` (see cli.py). Every check is a plain SQL
assertion with a human-readable name — no hidden magic.
"""

from __future__ import annotations

from dataclasses import dataclass

import duckdb


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str


def _scalar(con: duckdb.DuckDBPyConnection, sql: str) -> object:
    row = con.execute(sql).fetchone()
    assert row is not None
    return row[0]


def run_checks(con: duckdb.DuckDBPyConnection, expected_token_address: str) -> list[CheckResult]:
    checks: list[CheckResult] = []

    def add(name: str, passed: bool, detail: str) -> None:
        checks.append(CheckResult(name=name, passed=passed, detail=detail))

    n_null_tx_hash = _scalar(
        con, "SELECT COUNT(*) FROM stg_transfers WHERE transaction_hash IS NULL"
    )
    add(
        "transaction_hash_not_null",
        n_null_tx_hash == 0,
        f"{n_null_tx_hash} null transaction_hash rows",
    )

    n_neg_block = _scalar(con, "SELECT COUNT(*) FROM stg_transfers WHERE block_number < 0")
    add("block_number_nonnegative", n_neg_block == 0, f"{n_neg_block} negative block_number rows")

    n_neg_log_index = _scalar(con, "SELECT COUNT(*) FROM stg_transfers WHERE log_index < 0")
    add("log_index_nonnegative", n_neg_log_index == 0, f"{n_neg_log_index} negative log_index rows")

    n_total = _scalar(con, "SELECT COUNT(*) FROM stg_transfers")
    n_distinct_ids = _scalar(con, "SELECT COUNT(DISTINCT event_id) FROM stg_transfers")
    add(
        "event_id_unique",
        n_total == n_distinct_ids,
        f"{n_total} rows vs {n_distinct_ids} distinct event_id",
    )

    n_bad_from = _scalar(
        con,
        "SELECT COUNT(*) FROM stg_transfers WHERE NOT regexp_matches(from_address, '^0x[0-9a-f]{40}$')",
    )
    add("from_address_format_valid", n_bad_from == 0, f"{n_bad_from} malformed from_address rows")

    n_bad_to = _scalar(
        con,
        "SELECT COUNT(*) FROM stg_transfers WHERE NOT regexp_matches(to_address, '^0x[0-9a-f]{40}$')",
    )
    add("to_address_format_valid", n_bad_to == 0, f"{n_bad_to} malformed to_address rows")

    n_neg_amount = _scalar(
        con, "SELECT COUNT(*) FROM stg_transfers WHERE amount < 0 OR raw_amount < 0"
    )
    add("amount_nonnegative", n_neg_amount == 0, f"{n_neg_amount} negative-amount rows")

    n_wrong_token = _scalar(
        con,
        f"SELECT COUNT(*) FROM stg_transfers WHERE token_address != lower('{expected_token_address}')",
    )
    add(
        "token_contract_matches_expected",
        n_wrong_token == 0,
        f"{n_wrong_token} rows with a token_address other than {expected_token_address}",
    )

    n_null_ts = _scalar(con, "SELECT COUNT(*) FROM stg_transfers WHERE block_timestamp IS NULL")
    add("block_timestamp_not_null", n_null_ts == 0, f"{n_null_ts} null block_timestamp rows")

    n_dupe_keys = _scalar(
        con,
        """
        SELECT COUNT(*) FROM (
            SELECT transaction_hash, log_index, COUNT(*) AS c
            FROM stg_transfers GROUP BY 1, 2 HAVING COUNT(*) > 1
        )
        """,
    )
    add(
        "no_duplicate_transaction_hash_log_index",
        n_dupe_keys == 0,
        f"{n_dupe_keys} duplicate (transaction_hash, log_index) pairs",
    )

    n_raw = _scalar(con, "SELECT COUNT(*) FROM raw_transfers")
    n_raw_distinct = _scalar(
        con, "SELECT COUNT(DISTINCT transaction_hash || '-' || log_index) FROM raw_transfers"
    )
    add(
        "normalized_row_count_matches_deduped_raw",
        n_total == n_raw_distinct,
        f"stg_transfers has {n_total} rows; raw_transfers has {n_raw} rows / {n_raw_distinct} distinct keys",
    )

    return checks


def all_passed(results: list[CheckResult]) -> bool:
    return all(r.passed for r in results)
