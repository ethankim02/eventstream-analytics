"""Build the DuckDB warehouse: load raw Parquet, then run SQL models in order."""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path

import duckdb

from eventstream.warehouse.models import ALL_MODELS


@dataclass
class BuildReport:
    tables: dict[str, int]
    elapsed_seconds: float


def load_raw_transfers(con: duckdb.DuckDBPyConnection, parquet_glob: str) -> int:
    """(Re)build `raw_transfers` from one or more Parquet files.

    Accepts a glob so multiple ingested block-range files can be combined
    (DuckDB's read_parquet natively supports glob patterns and file lists).
    """
    con.execute(
        f"CREATE OR REPLACE TABLE raw_transfers AS SELECT * FROM read_parquet('{parquet_glob}')"
    )
    row = con.execute("SELECT COUNT(*) FROM raw_transfers").fetchone()
    assert row is not None
    return row[0]


def run_sql_models(
    con: duckdb.DuckDBPyConnection, sql_root: Path, models: list[str] | None = None
) -> dict[str, int]:
    row_counts: dict[str, int] = {}
    for rel_path in models or ALL_MODELS:
        sql_text = (sql_root / rel_path).read_text()
        con.execute(sql_text)
        table_name = Path(rel_path).stem
        row = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        assert row is not None
        row_counts[table_name] = row[0]
    return row_counts


def build_warehouse(
    con: duckdb.DuckDBPyConnection, parquet_glob: str, sql_root: Path
) -> BuildReport:
    start = time.perf_counter()
    raw_count = load_raw_transfers(con, parquet_glob)
    tables = {"raw_transfers": raw_count}
    tables.update(run_sql_models(con, sql_root))
    elapsed = time.perf_counter() - start
    return BuildReport(tables=tables, elapsed_seconds=elapsed)
