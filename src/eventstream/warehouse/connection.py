"""DuckDB connection helpers."""

from __future__ import annotations

import os
import re
from pathlib import Path

import duckdb

_MEMORY_LIMIT = re.compile(r"^\d+(\.\d+)?\s?(KB|MB|GB|TB|KiB|MiB|GiB|TiB)$")


def _configure(con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
    """Apply session settings shared by every connection.

    UTC: every `block_timestamp` is a TIMESTAMPTZ, and DuckDB's `DATE_TRUNC('day'/'week')`
    truncates in the *session* time zone, which defaults to the machine's local zone.
    Without this, day/week/cohort boundaries (and the censoring logic built on them)
    would silently shift with the host's time zone: on a UTC+9 machine, a window ending
    at 23:59:59 UTC on a Sunday spills into a phantom extra week. The setting is
    per-connection (it is not stored in the database file), so it is applied here.

    Resource limits (optional, for builds far larger than the machine's free RAM):
    DuckDB's default memory limit is 80% of *installed* memory, which can exceed what is
    actually free. It then leans on OS paging instead of spilling to disk deliberately, and
    large window/aggregate steps slow down by orders of magnitude. Set
    EVENTSTREAM_DUCKDB_MEMORY_LIMIT (e.g. "6GB") to a value the machine really has spare, and
    EVENTSTREAM_DUCKDB_THREADS to cap parallelism (each thread holds working memory).
    """
    con.execute("SET TimeZone='UTC'")
    limit = os.environ.get("EVENTSTREAM_DUCKDB_MEMORY_LIMIT")
    if limit:
        if not _MEMORY_LIMIT.match(limit.strip()):
            raise ValueError(f"EVENTSTREAM_DUCKDB_MEMORY_LIMIT={limit!r}: expected e.g. '6GB'")
        con.execute(f"SET memory_limit='{limit.strip()}'")
    threads = os.environ.get("EVENTSTREAM_DUCKDB_THREADS")
    if threads:
        con.execute(f"SET threads={int(threads)}")
    return con


def connect(warehouse_path: Path | str) -> duckdb.DuckDBPyConnection:
    path = Path(warehouse_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return _configure(duckdb.connect(str(path)))


def connect_memory() -> duckdb.DuckDBPyConnection:
    """An in-memory DuckDB connection, mainly for tests."""
    return _configure(duckdb.connect(":memory:"))
