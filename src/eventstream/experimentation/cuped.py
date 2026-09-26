"""CUPED (Controlled-experiment Using Pre-Experiment Data) variance reduction.

Assumptions (see docs/EXPERIMENTATION.md for the full explanation):
  1. The covariate X is measured strictly BEFORE assignment/treatment, so it
     cannot itself be affected by the treatment.
  2. Theta is estimated on the pooled (control+treatment) sample so the
     adjustment doesn't leak treatment-group information into itself.
  3. The linear adjustment `Y_adj = Y - theta*(X - mean(X))` is unbiased for
     the group means (it shifts every unit by the same function of X), so it
     only removes variance explained by X — it never manufactures an effect.
"""

from __future__ import annotations

import numpy as np


def cuped_adjust(y: np.ndarray, x: np.ndarray) -> dict:
    y = np.asarray(y, dtype=float)
    x = np.asarray(x, dtype=float)

    cov_xy = np.cov(x, y, ddof=1)[0, 1]
    var_x = np.var(x, ddof=1)
    theta = cov_xy / var_x if var_x > 0 else 0.0

    y_adjusted = y - theta * (x - x.mean())

    var_before = np.var(y, ddof=1)
    var_after = np.var(y_adjusted, ddof=1)
    variance_reduction_pct = 1 - (var_after / var_before) if var_before > 0 else 0.0

    return {
        "theta": float(theta),
        "y_adjusted": y_adjusted,
        "variance_before": float(var_before),
        "variance_after": float(var_after),
        "variance_reduction_pct": float(variance_reduction_pct),
    }
