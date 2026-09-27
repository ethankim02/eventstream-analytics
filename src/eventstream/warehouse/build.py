"""Build the DuckDB warehouse: load raw Parquet, then run SQL models in order."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from eventstream.warehouse.models import ALL_MODELS, DISJOINT_MODELS

_CREATED_TABLE = re.compile(r"CREATE\s+OR\s+REPLACE\s+TABLE\s+(\w+)", re.IGNORECASE)


class DuplicateEventsError(RuntimeError):
    """Raised when a build that skipped the global dedup finds a repeated event key."""


@dataclass
class BuildReport:
    tables: dict[str, int]
    elapsed_seconds: float
    model_seconds: dict[str, float] = field(default_factory=dict)


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
    con: duckdb.DuckDBPyConnection,
    sql_root: Path,
    models: list[str] | None = None,
    timings: dict[str, float] | None = None,
) -> dict[str, int]:
    """Run models in order; returns rows per table and, if `timings` is given, seconds per model."""
    row_counts: dict[str, int] = {}
    for rel_path in models or ALL_MODELS:
        model_start = time.perf_counter()
        # Explicit UTF-8: the SQL files contain non-ASCII punctuation (em-dashes in
        # comments), and the locale default (e.g. cp949 on Korean-locale Windows)
        # cannot decode it.
        sql_text = (sql_root / rel_path).read_text(encoding="utf-8")
        con.execute(sql_text)
        # The table a model creates, not its file name: a model variant (e.g.
        # stg_transfers_disjoint.sql) can create the same table under a different file.
        code = "\n".join(ln for ln in sql_text.splitlines() if not ln.lstrip().startswith("--"))
        created = _CREATED_TABLE.search(code)
        table_name = created.group(1) if created else Path(rel_path).stem
        row = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        assert row is not None
        row_counts[table_name] = row[0]
        if timings is not None:
            timings[table_name] = time.perf_counter() - model_start
    return row_counts


def assert_event_ids_unique(con: duckdb.DuckDBPyConnection) -> None:
    """Fail loudly if `stg_transfers` holds a repeated (transaction_hash, log_index) key."""
    row = con.execute("SELECT COUNT(*), COUNT(DISTINCT event_id) FROM stg_transfers").fetchone()
    assert row is not None
    total, distinct = row
    if total != distinct:
        raise DuplicateEventsError(
            f"{total - distinct:,} repeated (transaction_hash, log_index) keys in the source; "
            "rebuild with the global dedup enabled (omit --no-global-dedup)."
        )


def build_warehouse(
    con: duckdb.DuckDBPyConnection,
    parquet_glob: str,
    sql_root: Path,
    *,
    global_dedup: bool = True,
) -> BuildReport:
    """Load raw Parquet and run every SQL model.

    `global_dedup=False` builds `stg_transfers` without the ROW_NUMBER() dedup window, for a
    source proven non-overlapping (see `eventstream.ingestion.window`). Uniqueness is then
    asserted rather than assumed, so a wrong assumption fails the build instead of skewing marts.
    """
    start = time.perf_counter()
    raw_count = load_raw_transfers(con, parquet_glob)
    tables = {"raw_transfers": raw_count}
    timings: dict[str, float] = {"raw_transfers": time.perf_counter() - start}
    if global_dedup:
        tables.update(run_sql_models(con, sql_root, timings=timings))
    else:
        # stg_transfers is the first model. Verify its keys before building anything on it.
        tables.update(run_sql_models(con, sql_root, DISJOINT_MODELS[:1], timings))
        check_start = time.perf_counter()
        assert_event_ids_unique(con)
        timings["stg_transfers_uniqueness_check"] = time.perf_counter() - check_start
        tables.update(run_sql_models(con, sql_root, DISJOINT_MODELS[1:], timings))
    elapsed = time.perf_counter() - start
    return BuildReport(tables=tables, elapsed_seconds=elapsed, model_seconds=timings)
