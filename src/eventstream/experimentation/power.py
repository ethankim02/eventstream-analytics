"""Power / minimum-detectable-effect / sample-size calculations.

Closed-form normal-approximation formulas (standard for proportions and for
means with a known/estimated SD) — deliberately not a black-box library call,
so the formula is auditable.
"""

from __future__ import annotations

import numpy as np
from scipy import stats


def sample_size_two_proportions(
    baseline_rate: float, mde_absolute: float, alpha: float = 0.05, power: float = 0.8
) -> int:
    """Required per-group sample size to detect an absolute MDE with a
    two-sided two-proportion z-test at the given alpha/power.
    """
    p1 = baseline_rate
    p2 = baseline_rate + mde_absolute
    z_alpha = stats.norm.ppf(1 - alpha / 2)
    z_power = stats.norm.ppf(power)

    pooled = (p1 + p2) / 2
    numerator = (
        z_alpha * np.sqrt(2 * pooled * (1 - pooled))
        + z_power * np.sqrt(p1 * (1 - p1) + p2 * (1 - p2))
    ) ** 2
    n = numerator / (mde_absolute**2)
    return int(np.ceil(n))


def power_two_proportions(
    n_per_group: int, baseline_rate: float, mde_absolute: float, alpha: float = 0.05
) -> float:
    """Achieved power for a given per-group sample size and effect size."""
    p1 = baseline_rate
    p2 = baseline_rate + mde_absolute
    z_alpha = stats.norm.ppf(1 - alpha / 2)
    pooled = (p1 + p2) / 2
    se_pooled = np.sqrt(2 * pooled * (1 - pooled) / n_per_group)
    se_unpooled = np.sqrt(p1 * (1 - p1) / n_per_group + p2 * (1 - p2) / n_per_group)

    z_beta = (abs(mde_absolute) - z_alpha * se_pooled) / se_unpooled
    return float(stats.norm.cdf(z_beta))


def sample_size_two_means(
    sd: float, mde_absolute: float, alpha: float = 0.05, power: float = 0.8
) -> int:
    """Required per-group sample size for a continuous metric (two-sided,
    equal-variance approximation; Welch's test used at analysis time is
    still valid and slightly more conservative than this planning formula).
    """
    z_alpha = stats.norm.ppf(1 - alpha / 2)
    z_power = stats.norm.ppf(power)
    n = 2 * ((z_alpha + z_power) ** 2) * (sd**2) / (mde_absolute**2)
    return int(np.ceil(n))


def minimum_detectable_effect_two_proportions(
    n_per_group: int, baseline_rate: float, alpha: float = 0.05, power: float = 0.8
) -> float:
    """Smallest absolute effect detectable with the given fixed sample size."""
    z_alpha = stats.norm.ppf(1 - alpha / 2)
    z_power = stats.norm.ppf(power)
    p = baseline_rate
    se = np.sqrt(2 * p * (1 - p) / n_per_group)
    return float((z_alpha + z_power) * se)
