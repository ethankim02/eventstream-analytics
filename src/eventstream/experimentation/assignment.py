"""Deterministic hash-based random assignment.

Uses a stable hash of (salt, unit_id) rather than a stateful RNG draw per
unit, so assignment is reproducible given the same salt/ratio regardless of
call order, batch size, or which units are looked up — the same property
production experimentation systems rely on (a unit's bucket doesn't change
just because you queried a different subset first).
"""

from __future__ import annotations

import hashlib

import numpy as np
import pandas as pd

_MAX_HASH = 2**32


def _stable_unit_score(unit_id: str, salt: str) -> float:
    digest = hashlib.sha256(f"{salt}:{unit_id}".encode()).hexdigest()
    return int(digest[:8], 16) / _MAX_HASH


def assign_unit(unit_id: str, ratio: float = 0.5, salt: str = "experiment") -> str:
    """Deterministically bucket a single unit into 'treatment' or 'control'."""
    return "treatment" if _stable_unit_score(unit_id, salt) < ratio else "control"


def assign_units(
    unit_ids: pd.Series | list[str], ratio: float = 0.5, salt: str = "experiment"
) -> np.ndarray:
    """Vectorized deterministic assignment for many units at once."""
    return np.array([assign_unit(uid, ratio=ratio, salt=salt) for uid in unit_ids])
