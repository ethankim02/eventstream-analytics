from eventstream.experimentation import inference
from eventstream.experimentation.simulator import (
    ExperimentConfig,
    generate_experiment,
    generate_scenario,
)


def test_generate_experiment_is_deterministic():
    cfg = ExperimentConfig(seed=11, n_units=1000)
    a = generate_experiment(cfg)
    b = generate_experiment(cfg)
    assert a.equals(b)


def test_different_seed_changes_output():
    a = generate_experiment(ExperimentConfig(seed=1, n_units=500))
    b = generate_experiment(ExperimentConfig(seed=2, n_units=500))
    assert not a["converted"].equals(b["converted"])


def test_null_effect_scenario_rarely_significant_across_seeds():
    """Deterministic false-positive-rate check: run several FIXED seeds (not
    random each run) and require the significant fraction stay near alpha,
    so this never becomes a flaky test.
    """
    significant_count = 0
    n_seeds = 20
    for seed in range(n_seeds):
        cfg = ExperimentConfig(seed=seed, n_units=3000, treatment_conversion_lift_pp=0.0)
        df = generate_experiment(cfg)
        control = df[df["group"] == "control"]
        treatment = df[df["group"] == "treatment"]
        result = inference.two_proportion_ztest(
            int(control["converted"].sum()),
            len(control),
            int(treatment["converted"].sum()),
            len(treatment),
        )
        significant_count += int(result["significant"])
    # at alpha=0.05 we expect ~1 false positive in 20; allow generous slack
    assert significant_count <= 6


def test_positive_effect_scenario_detected():
    df = generate_scenario("positive_effect")
    control = df[df["group"] == "control"]
    treatment = df[df["group"] == "treatment"]
    result = inference.two_proportion_ztest(
        int(control["converted"].sum()),
        len(control),
        int(treatment["converted"].sum()),
        len(treatment),
    )
    assert result["significant"] is True
    assert result["absolute_lift"] > 0


def test_srm_scenario_flagged():
    df = generate_scenario("srm")
    control = df[df["group"] == "control"]
    treatment = df[df["group"] == "treatment"]
    result = inference.chi_square_srm(len(control), len(treatment))
    assert result["srm_detected"] is True


def test_non_srm_scenarios_not_flagged():
    for name in ["null_effect", "positive_effect", "high_variance", "cuped"]:
        df = generate_scenario(name)
        control = df[df["group"] == "control"]
        treatment = df[df["group"] == "treatment"]
        result = inference.chi_square_srm(len(control), len(treatment))
        assert result["srm_detected"] is False, name


def test_pre_period_metric_unaffected_by_treatment():
    """CUPED covariate must be assignment-independent (measured pre-period)."""
    df = generate_scenario("cuped")
    control_pre = df[df["group"] == "control"]["pre_period_metric"]
    treatment_pre = df[df["group"] == "treatment"]["pre_period_metric"]
    assert abs(control_pre.mean() - treatment_pre.mean()) < 0.5


def test_scenario_unknown_name_raises():
    import pytest

    with pytest.raises(KeyError):
        generate_scenario("not_a_real_scenario")


def test_generate_experiment_respects_allocation_ratio():
    cfg = ExperimentConfig(seed=5, n_units=20_000, allocation_ratio=0.3)
    df = generate_experiment(cfg)
    treatment_share = (df["group"] == "treatment").mean()
    assert 0.27 < treatment_share < 0.33
