"""Deterministic SYNTHETIC fixture generator.

IMPORTANT: this module never fabricates real blockchain records. It produces
clearly-labeled synthetic data (source="synthetic_fixture") shaped like the
canonical USDC-transfer event schema, so the rest of the pipeline (warehouse,
analytics, dashboard, CI) can run end-to-end with zero network dependency.

It is NOT a substitute for the real dataset produced by
`eventstream.ingestion.live`, and analytics output computed from it must
never be reported as a real-world finding. See docs/DATA_SOURCE.md.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from eventstream.config import Settings
from eventstream.ingestion.normalize import CANONICAL_COLUMNS

_HEX_CHARS = "0123456789abcdef"


def _fake_hash(rng: np.random.Generator, n_bytes: int = 32) -> str:
    return "0x" + "".join(rng.choice(list(_HEX_CHARS), size=n_bytes * 2))


def _fake_address(rng: np.random.Generator) -> str:
    return "0x" + "".join(rng.choice(list(_HEX_CHARS), size=40))


@dataclass
class FixtureConfig:
    seed: int = 42
    n_wallets: int = 1500
    n_days: int = 45
    start_date: datetime = datetime(2026, 6, 1, tzinfo=UTC)
    events_per_day_mean: int = 900
    self_transfer_rate: float = 0.01
    # Power-law-like activity skew: a small share of wallets transact far
    # more often than the rest, mirroring the heavy-tailed behavior real
    # transfer datasets exhibit (see docs/DATA_SOURCE.md / METRICS.md).
    pareto_shape: float = 1.4


def generate_synthetic_transfers(
    settings: Settings, cfg: FixtureConfig | None = None
) -> pd.DataFrame:
    cfg = cfg or FixtureConfig()
    rng = np.random.default_rng(cfg.seed)

    wallets = [_fake_address(rng) for _ in range(cfg.n_wallets)]

    # Heavy-tailed per-wallet activity weight so a minority of wallets
    # generate a disproportionate share of events/volume (used later to
    # exercise concentration/Gini analytics honestly, not to force a result).
    weights = rng.pareto(cfg.pareto_shape, size=cfg.n_wallets) + 0.1
    weights = weights / weights.sum()

    # Stagger first-seen wallets across the window so cohorts of varying
    # maturity exist (needed to exercise right-censoring logic for real).
    first_seen_day = rng.integers(0, cfg.n_days, size=cfg.n_wallets)

    rows: list[dict] = []
    block_number = 20_000_000
    for day in range(cfg.n_days):
        active_mask = first_seen_day <= day
        active_idx = np.where(active_mask)[0]
        if len(active_idx) == 0:
            continue
        day_weights = weights[active_idx]
        day_weights = day_weights / day_weights.sum()

        n_events = rng.poisson(cfg.events_per_day_mean)
        day_start = cfg.start_date + timedelta(days=day)

        for _ in range(n_events):
            sender_pos = rng.choice(active_idx, p=day_weights)
            if rng.random() < cfg.self_transfer_rate:
                receiver_pos = sender_pos
            else:
                receiver_pos = rng.choice(active_idx, p=day_weights)

            block_number += int(rng.integers(1, 4))
            seconds_into_day = int(rng.integers(0, 86_400))
            ts = day_start + timedelta(seconds=seconds_into_day)

            raw_amount = int(rng.lognormal(mean=3.5, sigma=1.8) * 10**settings.usdc_decimals)
            raw_amount = max(raw_amount, 1)

            tx_hash = _fake_hash(rng)
            rows.append(
                {
                    "event_id": f"{tx_hash}-0",
                    "block_number": block_number,
                    "block_timestamp": ts,
                    "transaction_hash": tx_hash,
                    "log_index": 0,
                    "token_address": settings.usdc_contract.lower(),
                    "from_address": wallets[sender_pos],
                    "to_address": wallets[receiver_pos],
                    "raw_amount": raw_amount,
                    "amount": raw_amount / (10**settings.usdc_decimals),
                    "is_self_transfer": sender_pos == receiver_pos,
                    "chain_id": settings.chain_id,
                    "network": settings.network,
                    "source": "synthetic_fixture",
                    "ingested_at": datetime.now(UTC),
                }
            )

    df = pd.DataFrame(rows, columns=CANONICAL_COLUMNS)
    df = df.sort_values(["block_number", "log_index"]).reset_index(drop=True)
    return df


def write_fixture(
    settings: Settings, out_path: Path, cfg: FixtureConfig | None = None
) -> pd.DataFrame:
    df = generate_synthetic_transfers(settings, cfg)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    return df
