"""DuckDB connection helpers."""

from __future__ import annotations

from pathlib import Path

import duckdb


def connect(warehouse_path: Path | str) -> duckdb.DuckDBPyConnection:
    path = Path(warehouse_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path))


def connect_memory() -> duckdb.DuckDBPyConnection:
    """An in-memory DuckDB connection, mainly for tests."""
    return duckdb.connect(":memory:")
