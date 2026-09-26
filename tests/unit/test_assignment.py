import numpy as np

from eventstream.experimentation.assignment import assign_unit, assign_units


def test_assignment_is_deterministic_across_calls():
    ids = [f"unit_{i}" for i in range(500)]
    first = assign_units(ids, ratio=0.5, salt="s1")
    second = assign_units(ids, ratio=0.5, salt="s1")
    assert (first == second).all()


def test_assignment_independent_of_lookup_order():
    ids = [f"unit_{i}" for i in range(200)]
    whole = {uid: assign_unit(uid, ratio=0.5, salt="s2") for uid in ids}
    subset_ids = ids[::3]
    subset = {uid: assign_unit(uid, ratio=0.5, salt="s2") for uid in subset_ids}
    for uid in subset_ids:
        assert subset[uid] == whole[uid]


def test_assignment_roughly_matches_ratio():
    ids = [f"unit_{i}" for i in range(20_000)]
    groups = assign_units(ids, ratio=0.5, salt="ratio-check")
    treatment_share = (groups == "treatment").mean()
    assert 0.47 < treatment_share < 0.53


def test_assignment_respects_configurable_ratio():
    ids = [f"unit_{i}" for i in range(20_000)]
    groups = assign_units(ids, ratio=0.2, salt="ratio-20")
    treatment_share = (groups == "treatment").mean()
    assert 0.17 < treatment_share < 0.23


def test_different_salt_changes_assignment():
    ids = [f"unit_{i}" for i in range(500)]
    a = assign_units(ids, ratio=0.5, salt="salt-a")
    b = assign_units(ids, ratio=0.5, salt="salt-b")
    assert not np.array_equal(a, b)
