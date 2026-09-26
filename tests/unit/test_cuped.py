import numpy as np
import pytest

from eventstream.experimentation.cuped import cuped_adjust


def test_cuped_reduces_variance_with_correlated_covariate():
    rng = np.random.default_rng(0)
    x = rng.normal(0, 1, 5000)
    y = 3 * x + rng.normal(0, 0.5, 5000)
    result = cuped_adjust(y, x)
    assert result["variance_reduction_pct"] > 0.8
    assert result["variance_after"] < result["variance_before"]


def test_cuped_no_reduction_when_uncorrelated():
    rng = np.random.default_rng(1)
    x = rng.normal(0, 1, 5000)
    y = rng.normal(0, 1, 5000)
    result = cuped_adjust(y, x)
    assert abs(result["variance_reduction_pct"]) < 0.05


def test_cuped_preserves_mean():
    rng = np.random.default_rng(2)
    x = rng.normal(0, 1, 5000)
    y = 2 * x + 10 + rng.normal(0, 1, 5000)
    result = cuped_adjust(y, x)
    assert result["y_adjusted"].mean() == pytest.approx(y.mean(), rel=1e-6)


def test_cuped_theta_matches_ols_slope():
    rng = np.random.default_rng(3)
    x = rng.normal(0, 2, 3000)
    y = 1.5 * x + rng.normal(0, 1, 3000)
    result = cuped_adjust(y, x)
    slope = np.polyfit(x, y, 1)[0]
    assert result["theta"] == pytest.approx(slope, rel=0.05)
