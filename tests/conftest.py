from __future__ import annotations

from pathlib import Path

import pytest

from eventstream.config import Settings
from eventstream.ingestion.fixtures import FixtureConfig, generate_synthetic_transfers
from eventstream.warehouse.build import build_warehouse
from eventstream.warehouse.connection import connect_memory

SQL_ROOT = Path(__file__).resolve().parents[1] / "sql"


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings()


@pytest.fixture(scope="session")
def small_fixture_df(settings: Settings):
    cfg = FixtureConfig(seed=123, n_wallets=200, n_days=60, events_per_day_mean=120)
    return generate_synthetic_transfers(settings, cfg)


@pytest.fixture(scope="session")
def built_con(tmp_path_factory, settings: Settings, small_fixture_df):
    tmp_dir = tmp_path_factory.mktemp("warehouse")
    parquet_path = tmp_dir / "fixture.parquet"
    small_fixture_df.to_parquet(parquet_path, index=False)

    con = connect_memory()
    build_warehouse(con, str(parquet_path), SQL_ROOT)
    return con
