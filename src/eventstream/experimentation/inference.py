"""Statistical inference for the experimentation module.

Implements: two-proportion z-test, Welch's t-test, confidence intervals,
chi-square sample-ratio-mismatch (SRM) test, and a percentile bootstrap CI.
Every function returns a plain dict so results are trivial to print, log, or
render on the dashboard without a bespoke result class.
"""

from __future__ import annotations

import numpy as np
from scipy import stats


def two_proportion_ztest(
    conversions_control: int,
    n_control: int,
    conversions_treatment: int,
    n_treatment: int,
    alpha: float = 0.05,
) -> dict:
    """Two-sided two-proportion z-test on independent binomial samples."""
    p_c = conversions_control / n_control
    p_t = conversions_treatment / n_treatment
    p_pool = (conversions_control + conversions_treatment) / (n_control + n_treatment)

    se_pool = np.sqrt(p_pool * (1 - p_pool) * (1 / n_control + 1 / n_treatment))
    z = (p_t - p_c) / se_pool if se_pool > 0 else 0.0
    p_value = 2 * (1 - stats.norm.cdf(abs(z)))

    se_unpooled = np.sqrt(p_c * (1 - p_c) / n_control + p_t * (1 - p_t) / n_treatment)
    z_crit = stats.norm.ppf(1 - alpha / 2)
    diff = p_t - p_c
    ci_low = diff - z_crit * se_unpooled
    ci_high = diff + z_crit * se_unpooled

    return {
        "control_rate": p_c,
        "treatment_rate": p_t,
        "absolute_lift": diff,
        "relative_lift": (diff / p_c) if p_c > 0 else float("nan"),
        "z_statistic": z,
        "p_value": p_value,
        "ci_low": ci_low,
        "ci_high": ci_high,
        "alpha": alpha,
        "significant": bool(p_value < alpha),
    }


def welch_ttest(
    sample_control: np.ndarray, sample_treatment: np.ndarray, alpha: float = 0.05
) -> dict:
    """Welch's t-test (unequal-variance) for a continuous metric."""
    sample_control = np.asarray(sample_control, dtype=float)
    sample_treatment = np.asarray(sample_treatment, dtype=float)

    t_stat, p_value = stats.ttest_ind(sample_treatment, sample_control, equal_var=False)

    mean_c, mean_t = sample_control.mean(), sample_treatment.mean()
    var_c, var_t = sample_control.var(ddof=1), sample_treatment.var(ddof=1)
    n_c, n_t = len(sample_control), len(sample_treatment)

    se = np.sqrt(var_c / n_c + var_t / n_t)
    df = (var_c / n_c + var_t / n_t) ** 2 / (
        (var_c / n_c) ** 2 / (n_c - 1) + (var_t / n_t) ** 2 / (n_t - 1)
    )
    t_crit = stats.t.ppf(1 - alpha / 2, df)
    diff = mean_t - mean_c
    ci_low = diff - t_crit * se
    ci_high = diff + t_crit * se

    return {
        "control_mean": mean_c,
        "treatment_mean": mean_t,
        "absolute_lift": diff,
        "relative_lift": (diff / mean_c) if mean_c != 0 else float("nan"),
        "t_statistic": float(t_stat),
        "degrees_of_freedom": float(df),
        "p_value": float(p_value),
        "ci_low": ci_low,
        "ci_high": ci_high,
        "alpha": alpha,
        "significant": bool(p_value < alpha),
    }


def chi_square_srm(
    n_control: int, n_treatment: int, expected_ratio: float = 0.5, alpha: float = 0.01
) -> dict:
    """Sample-ratio-mismatch check via chi-square goodness-of-fit.

    A stricter default alpha (0.01) is standard for SRM checks: this test is
    a health check on the experiment plumbing itself, not the metric of
    interest, so false positives should be rarer than usual.
    """
    n_total = n_control + n_treatment
    expected_control = n_total * expected_ratio
    expected_treatment = n_total * (1 - expected_ratio)

    chi2, p_value = stats.chisquare(
        f_obs=[n_control, n_treatment], f_exp=[expected_control, expected_treatment]
    )
    return {
        "n_control": n_control,
        "n_treatment": n_treatment,
        "observed_ratio": n_control / n_total,
        "expected_ratio": expected_ratio,
        "chi2_statistic": float(chi2),
        "p_value": float(p_value),
        "alpha": alpha,
        "srm_detected": bool(p_value < alpha),
    }


def bootstrap_ci(
    sample_control: np.ndarray,
    sample_treatment: np.ndarray,
    n_boot: int = 2000,
    alpha: float = 0.05,
    seed: int = 0,
) -> dict:
    """Percentile bootstrap CI for the difference in means.

    Included as the "one more defensible advanced method" beyond the closed-
    form tests: makes no normality assumption, at the cost of needing many
    resamples. Useful as a sanity check against Welch's t-test, especially
    for skewed metrics (docs/EXPERIMENTATION.md).
    """
    rng = np.random.default_rng(seed)
    sample_control = np.asarray(sample_control, dtype=float)
    sample_treatment = np.asarray(sample_treatment, dtype=float)

    diffs = np.empty(n_boot)
    n_c, n_t = len(sample_control), len(sample_treatment)
    for i in range(n_boot):
        boot_c = rng.choice(sample_control, size=n_c, replace=True)
        boot_t = rng.choice(sample_treatment, size=n_t, replace=True)
        diffs[i] = boot_t.mean() - boot_c.mean()

    lo, hi = np.percentile(diffs, [100 * alpha / 2, 100 * (1 - alpha / 2)])
    point_estimate = sample_treatment.mean() - sample_control.mean()
    return {
        "point_estimate": float(point_estimate),
        "ci_low": float(lo),
        "ci_high": float(hi),
        "n_boot": n_boot,
        "alpha": alpha,
    }
