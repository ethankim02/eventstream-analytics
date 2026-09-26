"""DuckDB connection helpers."""

from __future__ import annotations

from pathlib import Path

import duckdb


def _pin_utc(con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
    """Force UTC for the session.

    Every `block_timestamp` is a TIMESTAMPTZ, and DuckDB's `DATE_TRUNC('day'/'week')`
    truncates in the *session* time zone, which defaults to the machine's local zone.
    Without this, day/week/cohort boundaries (and the censoring logic built on them)
    would silently shift with the host's time zone: on a UTC+9 machine, a window ending
    at 23:59:59 UTC on a Sunday spills into a phantom extra week. The setting is
    per-connection (it is not stored in the database file), so it is applied here.
    """
    con.execute("SET TimeZone='UTC'")
    return con


def connect(warehouse_path: Path | str) -> duckdb.DuckDBPyConnection:
    path = Path(warehouse_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return _pin_utc(duckdb.connect(str(path)))


def connect_memory() -> duckdb.DuckDBPyConnection:
    """An in-memory DuckDB connection, mainly for tests."""
    return _pin_utc(duckdb.connect(":memory:"))
