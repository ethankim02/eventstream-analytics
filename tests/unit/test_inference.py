import numpy as np
import pytest

from eventstream.experimentation import inference


def test_two_proportion_ztest_worked_example():
    """docs/EXPERIMENTATION.md worked example: 10.0% vs 11.2% conversion."""
    n = 10_000
    result = inference.two_proportion_ztest(
        conversions_control=1000, n_control=n, conversions_treatment=1120, n_treatment=n
    )
    assert result["control_rate"] == pytest.approx(0.10)
    assert result["treatment_rate"] == pytest.approx(0.112)
    assert result["absolute_lift"] == pytest.approx(0.012, abs=1e-9)
    assert result["relative_lift"] == pytest.approx(0.12, abs=1e-9)
    assert result["ci_low"] < result["absolute_lift"] < result["ci_high"]
    assert result["significant"] is True


def test_two_proportion_ztest_no_difference_not_significant():
    result = inference.two_proportion_ztest(500, 5000, 500, 5000)
    assert result["p_value"] > 0.9
    assert result["significant"] is False


def test_two_proportion_ztest_symmetry():
    a = inference.two_proportion_ztest(100, 1000, 150, 1000)
    b = inference.two_proportion_ztest(150, 1000, 100, 1000)
    assert a["absolute_lift"] == pytest.approx(-b["absolute_lift"])
    assert a["p_value"] == pytest.approx(b["p_value"])


def test_welch_ttest_detects_shifted_mean():
    rng = np.random.default_rng(0)
    control = rng.normal(10, 2, 5000)
    treatment = rng.normal(11, 2, 5000)
    result = inference.welch_ttest(control, treatment)
    assert result["significant"] is True
    assert result["ci_low"] < result["absolute_lift"] < result["ci_high"]
    assert result["absolute_lift"] > 0


def test_welch_ttest_no_effect_not_significant():
    rng = np.random.default_rng(1)
    control = rng.normal(10, 2, 5000)
    treatment = rng.normal(10, 2, 5000)
    result = inference.welch_ttest(control, treatment)
    assert result["p_value"] > 0.05


def test_welch_ttest_handles_unequal_variance():
    rng = np.random.default_rng(2)
    control = rng.normal(10, 1, 3000)
    treatment = rng.normal(10, 20, 3000)
    result = inference.welch_ttest(control, treatment)
    assert result["degrees_of_freedom"] < 3000 + 3000 - 2


def test_chi_square_srm_flags_biased_split():
    result = inference.chi_square_srm(n_control=4000, n_treatment=6000, expected_ratio=0.5)
    assert result["srm_detected"] is True


def test_chi_square_srm_passes_balanced_split():
    result = inference.chi_square_srm(n_control=5010, n_treatment=4990, expected_ratio=0.5)
    assert result["srm_detected"] is False


def test_chi_square_srm_respects_expected_ratio():
    result = inference.chi_square_srm(n_control=2000, n_treatment=8000, expected_ratio=0.2)
    assert result["srm_detected"] is False


def test_bootstrap_ci_overlaps_parametric_ci_on_large_sample():
    rng = np.random.default_rng(3)
    control = rng.normal(20, 5, 4000)
    treatment = rng.normal(22, 5, 4000)
    t_result = inference.welch_ttest(control, treatment)
    boot = inference.bootstrap_ci(control, treatment, n_boot=1000, seed=42)
    assert boot["ci_low"] < t_result["ci_high"]
    assert boot["ci_high"] > t_result["ci_low"]


def test_bootstrap_ci_is_deterministic_given_seed():
    rng = np.random.default_rng(4)
    control = rng.normal(0, 1, 500)
    treatment = rng.normal(0.5, 1, 500)
    a = inference.bootstrap_ci(control, treatment, n_boot=500, seed=99)
    b = inference.bootstrap_ci(control, treatment, n_boot=500, seed=99)
    assert a == b
