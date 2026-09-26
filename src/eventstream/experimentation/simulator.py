"""Deterministic synthetic randomized-experiment data generator.

This module is entirely independent of the on-chain event data. It exists to
demonstrate experimentation/statistics competency on data we KNOW the ground
truth for, precisely because historical blockchain activity is observational
and must never be dressed up as a randomized experiment (see
docs/EXPERIMENTATION.md).
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from eventstream.experimentation.assignment import assign_units


@dataclass
class ExperimentConfig:
    n_units: int = 20_000
    seed: int = 7
    salt: str = "eventstream-experiment"
    allocation_ratio: float = 0.5
    baseline_conversion: float = 0.10
    treatment_conversion_lift_pp: float = 0.0
    continuous_control_mean: float = 25.0
    continuous_treatment_effect: float = 0.0
    continuous_sd: float = 8.0
    pre_period_correlation: float = 0.0
    srm_bias: float = 0.0  # nonzero => assignment ratio is skewed to simulate a bug


def generate_experiment(cfg: ExperimentConfig) -> pd.DataFrame:
    rng = np.random.default_rng(cfg.seed)
    unit_ids = [f"unit_{i:07d}" for i in range(cfg.n_units)]

    effective_ratio = cfg.allocation_ratio + cfg.srm_bias
    group = assign_units(unit_ids, ratio=effective_ratio, salt=cfg.salt)
    is_treatment = group == "treatment"

    # Pre-period covariate, correlated with the post-period continuous metric
    # via `pre_period_correlation`. Constructed BEFORE assignment is applied
    # to the outcome, so it is a valid CUPED covariate (unaffected by
    # treatment) — see cuped.py / docs/EXPERIMENTATION.md.
    pre_period = rng.normal(
        loc=cfg.continuous_control_mean, scale=cfg.continuous_sd, size=cfg.n_units
    )

    noise_sd = cfg.continuous_sd * np.sqrt(max(1 - cfg.pre_period_correlation**2, 1e-6))
    base_noise = rng.normal(loc=0.0, scale=noise_sd, size=cfg.n_units)
    continuous_metric = (
        cfg.continuous_control_mean
        + cfg.pre_period_correlation * (pre_period - cfg.continuous_control_mean)
        + base_noise
        + np.where(is_treatment, cfg.continuous_treatment_effect, 0.0)
    )

    conversion_p = np.where(
        is_treatment,
        np.clip(cfg.baseline_conversion + cfg.treatment_conversion_lift_pp, 0.0, 1.0),
        cfg.baseline_conversion,
    )
    converted = rng.binomial(1, conversion_p)

    return pd.DataFrame(
        {
            "unit_id": unit_ids,
            "group": group,
            "converted": converted,
            "continuous_metric": continuous_metric,
            "pre_period_metric": pre_period,
        }
    )


SCENARIOS: dict[str, ExperimentConfig] = {
    "null_effect": ExperimentConfig(
        seed=1, treatment_conversion_lift_pp=0.0, continuous_treatment_effect=0.0
    ),
    "positive_effect": ExperimentConfig(
        seed=2, treatment_conversion_lift_pp=0.02, continuous_treatment_effect=1.5
    ),
    "srm": ExperimentConfig(seed=3, srm_bias=0.08),
    "high_variance": ExperimentConfig(seed=4, continuous_sd=40.0, continuous_treatment_effect=1.5),
    "cuped": ExperimentConfig(
        seed=5, pre_period_correlation=0.75, continuous_treatment_effect=1.5, continuous_sd=15.0
    ),
}


def generate_scenario(name: str) -> pd.DataFrame:
    if name not in SCENARIOS:
        raise KeyError(f"unknown scenario {name!r}; choices are {sorted(SCENARIOS)}")
    return generate_experiment(SCENARIOS[name])
