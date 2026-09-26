from eventstream.experimentation import power


def test_sample_size_decreases_as_mde_grows():
    small_effect_n = power.sample_size_two_proportions(0.10, mde_absolute=0.01)
    large_effect_n = power.sample_size_two_proportions(0.10, mde_absolute=0.05)
    assert large_effect_n < small_effect_n


def test_sample_size_increases_with_required_power():
    n_80 = power.sample_size_two_proportions(0.10, mde_absolute=0.02, power=0.8)
    n_95 = power.sample_size_two_proportions(0.10, mde_absolute=0.02, power=0.95)
    assert n_95 > n_80


def test_achieved_power_increases_with_sample_size():
    small_n_power = power.power_two_proportions(500, baseline_rate=0.10, mde_absolute=0.02)
    large_n_power = power.power_two_proportions(20_000, baseline_rate=0.10, mde_absolute=0.02)
    assert large_n_power > small_n_power


def test_sample_size_for_means_scales_with_variance():
    low_sd_n = power.sample_size_two_means(sd=5, mde_absolute=1.0)
    high_sd_n = power.sample_size_two_means(sd=20, mde_absolute=1.0)
    assert high_sd_n > low_sd_n


def test_mde_shrinks_with_more_samples():
    small_n_mde = power.minimum_detectable_effect_two_proportions(500, baseline_rate=0.10)
    large_n_mde = power.minimum_detectable_effect_two_proportions(20_000, baseline_rate=0.10)
    assert large_n_mde < small_n_mde


def test_sample_size_at_recommended_n_achieves_target_power():
    n = power.sample_size_two_proportions(0.10, mde_absolute=0.02, power=0.8)
    achieved = power.power_two_proportions(n, baseline_rate=0.10, mde_absolute=0.02)
    assert achieved >= 0.79
