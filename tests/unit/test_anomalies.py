import numpy as np
import pandas as pd

from eventstream.analytics.anomalies import detect_anomalies, robust_z_scores


def _stable_series(n=60, level=100.0, seed=0):
    rng = np.random.default_rng(seed)
    noise = rng.normal(0, 2, n)
    return pd.Series(level + noise)


def test_stable_series_mostly_not_flagged():
    df = pd.DataFrame({"activity_date": range(60), "value": _stable_series()})
    result = detect_anomalies(df, "value", window=7)
    # allow at most a couple of borderline flags out of 60 stable points
    assert result["is_anomaly"].sum() <= 2


def test_injected_spike_is_detected():
    series = _stable_series(n=60).to_numpy().copy()
    series[45] += 500  # obvious spike
    df = pd.DataFrame({"activity_date": range(60), "value": series})
    result = detect_anomalies(df, "value", window=7)
    assert bool(result.loc[45, "is_anomaly"])


def test_injected_drop_is_detected():
    series = _stable_series(n=60).to_numpy().copy()
    series[50] -= 400
    df = pd.DataFrame({"activity_date": range(60), "value": series})
    result = detect_anomalies(df, "value", window=7)
    assert bool(result.loc[50, "is_anomaly"])


def test_first_window_points_have_no_score():
    df = pd.DataFrame({"activity_date": range(20), "value": _stable_series(n=20)})
    result = detect_anomalies(df, "value", window=7)
    assert result["robust_z_score"].iloc[:7].isna().all()


def test_zero_mad_never_flags_even_with_deviation():
    """A perfectly flat trailing window has rolling_mad == 0; the function
    must not divide-by-zero its way into a spurious flag.
    """
    values = [10.0] * 20 + [10.5]
    df = pd.DataFrame({"activity_date": range(len(values)), "value": values})
    result = detect_anomalies(df, "value", window=7)
    assert result["is_anomaly"].iloc[-1] == False  # noqa: E712


def test_robust_z_scores_current_point_never_in_its_own_baseline():
    series = pd.Series([10.0] * 10 + [1000.0])
    scores = robust_z_scores(series, window=7)
    # the huge final point shouldn't have shifted its own rolling_median
    assert scores["rolling_median"].iloc[-1] == 10.0
