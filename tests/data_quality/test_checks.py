from pathlib import Path

import pandas as pd

from eventstream.validation.checks import all_passed, run_checks
from eventstream.warehouse.build import load_raw_transfers, run_sql_models
from eventstream.warehouse.connection import connect_memory

SQL_ROOT = Path(__file__).resolve().parents[2] / "sql"


def test_all_checks_pass_on_clean_fixture(built_con, settings):
    results = run_checks(built_con, settings.usdc_contract)
    failed = [r for r in results if not r.passed]
    assert not failed, f"unexpected failing checks: {failed}"
    assert all_passed(results)


def test_check_catches_wrong_token_contract(tmp_path, small_fixture_df, settings):
    tampered = small_fixture_df.copy()
    tampered.loc[0, "token_address"] = "0x" + "1" * 40

    parquet_path = tmp_path / "tampered.parquet"
    tampered.to_parquet(parquet_path, index=False)

    con = connect_memory()
    load_raw_transfers(con, str(parquet_path))
    run_sql_models(con, SQL_ROOT)

    results = run_checks(con, settings.usdc_contract)
    by_name = {r.name: r for r in results}
    assert not by_name["token_contract_matches_expected"].passed


def test_duplicate_raw_rows_collapse_without_false_duplicate_flag(
    tmp_path, small_fixture_df, settings
):
    """Raw data can contain a re-ingested duplicate of the same
    (transaction_hash, log_index) key (e.g. an overlapping re-run of a block
    range). Staging dedup must collapse it to one row, and the uniqueness
    check must pass on the DEDUPED table, not falsely fail.
    """
    duped = small_fixture_df.copy()
    dup_row = duped.iloc[[0]].copy()
    dup_row["amount"] = dup_row["amount"] + 1
    stacked = pd.concat([duped, dup_row], ignore_index=True)

    parquet_path = tmp_path / "dup.parquet"
    stacked.to_parquet(parquet_path, index=False)

    con = connect_memory()
    load_raw_transfers(con, str(parquet_path))
    run_sql_models(con, SQL_ROOT)

    results = run_checks(con, settings.usdc_contract)
    by_name = {r.name: r for r in results}
    assert by_name["no_duplicate_transaction_hash_log_index"].passed
    assert by_name["event_id_unique"].passed

    n_stg = con.execute("SELECT COUNT(*) FROM stg_transfers").fetchone()[0]
    n_raw_distinct = con.execute(
        "SELECT COUNT(DISTINCT transaction_hash || '-' || log_index) FROM raw_transfers"
    ).fetchone()[0]
    assert n_stg == n_raw_distinct == len(duped)
