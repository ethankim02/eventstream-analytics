# Experimentation Methodology

## Why this is a separate module, on separate data

Everything under `eventstream.analytics` (retention, segmentation,
concentration, anomalies) runs on **observational** data — historical USDC
transfer activity that nobody randomized. Observational data can show strong
associations (e.g. "activated wallets have a higher observed return rate"),
but it **cannot** support a causal claim: wallets that self-select into more
active early behavior may differ from other wallets in every way, not just
the one being measured (confounding).

A real randomized experiment is the only way to isolate a causal effect,
because random assignment makes control and treatment groups
statistically equivalent *in expectation* on every variable, observed or not
— confounding included.

This project has no real randomized experiment to analyze (historical
on-chain activity obviously wasn't run as one). So `eventstream.experimentation`
demonstrates the *methodology* on a **deterministic synthetic data
generator** (`eventstream.experimentation.simulator`) instead — data whose
ground truth (the true injected effect, or lack of one) is known in advance,
which is precisely what lets the statistical tests below be checked for
correctness rather than taken on faith.

**Never** are the words "causes," "leads to," or "results in" used about the
observational analytics output in this project. Where an association is
reported (e.g. the activation proxy comparison), it is phrased as
"associational" / "observed," explicitly.

## Randomization and assignment

`eventstream.experimentation.assignment.assign_unit` deterministically
buckets a unit into `treatment`/`control` via
`sha256(f"{salt}:{unit_id}")`, thresholded against the configured allocation
ratio. This is deterministic (same unit + salt always yields the same
bucket, independent of query order or batch size) and configurable
(any control:treatment split, not just 50/50).

## Null hypothesis, p-values, confidence intervals

For a binary metric (e.g. conversion), the null hypothesis
_H₀_ is that the true control and treatment conversion rates are equal;
the two-proportion z-test (`eventstream.experimentation.inference.two_proportion_ztest`)
tests this against a two-sided alternative. The **p-value** is the
probability of observing a difference at least as extreme as the one
measured, *if H₀ were true*. It is **not** the probability that H₀ is true,
and it says nothing about the size of the effect on its own — hence always
reporting a **confidence interval** alongside it (the range of effect sizes
consistent with the data at a given confidence level, e.g. 95%).

For a continuous metric, Welch's t-test
(`eventstream.experimentation.inference.welch_ttest`) is used instead of
Student's t-test specifically because it does **not** assume equal variance
between groups — a safer default, since real experiment arms frequently do
have different variance even under the null.

## Practical vs. statistical significance

A result can be statistically significant (a very small p-value) while being
practically irrelevant (e.g. a 0.01 percentage-point lift, detectable only
because the sample size is enormous), and vice versa: an underpowered test
can fail to reach significance despite a practically meaningful true effect.
Always look at the **absolute lift**, **relative lift**, and the **width**
of the confidence interval — a p-value alone is not a decision.

## Type I / Type II error, power, MDE

- **Type I error (α)**: rejecting H₀ when it's actually true — a false
  positive. This project defaults α = 0.05 for standard tests, but α = 0.01
  for the SRM check specifically (see below) — an SRM check is a health
  check on the experiment's own plumbing, not the metric of interest, so
  false alarms should be rarer.
- **Type II error (β)**: failing to reject H₀ when the alternative is
  actually true — a false negative. **Power** is `1 - β`: the probability of
  detecting a true effect of a given size.
- **Minimum detectable effect (MDE)**: the smallest true effect size a test
  is powered to reliably detect, given a fixed sample size.
  `eventstream.experimentation.power` implements sample-size-for-a-given-MDE,
  achieved-power-for-a-given-n, and MDE-for-a-given-n, all via closed-form
  normal-approximation formulas (auditable, not a black-box library call).

## Sample Ratio Mismatch (SRM)

SRM is a **data-quality check on the experiment itself**: if the observed
control/treatment split diverges from the configured allocation ratio by
more than chance, something is broken in the assignment or logging pipeline
— and the experiment's results should not be trusted until that's fixed,
regardless of what the metric results say.
`eventstream.experimentation.inference.chi_square_srm` runs a chi-square
goodness-of-fit test against the expected ratio, at the stricter α = 0.01.

## CUPED (variance reduction)

CUPED (Controlled-experiment Using Pre-Experiment Data) reduces a metric's
variance — and therefore the sample size needed to detect a given effect —
by regressing out a **pre-experiment** covariate that's correlated with the
outcome but *could not itself have been affected by treatment* (it was
measured before assignment). `eventstream.experimentation.cuped.cuped_adjust`
computes `θ = Cov(Y, X) / Var(X)` and returns
`Y_adj = Y - θ * (X - mean(X))`.

**Assumptions**, made explicit and testable:
1. `X` (the covariate) is measured strictly before assignment — the
   synthetic simulator's `pre_period_metric` is generated before treatment
   is applied to the outcome, and a unit test
   (`test_pre_period_metric_unaffected_by_treatment`) checks its mean is
   statistically indistinguishable between the two arms.
2. `θ` is estimated on the pooled sample (not per-arm), so the adjustment
   doesn't leak treatment-arm information into itself.
3. The adjustment is a per-unit linear shift — it does not change either
   arm's mean (verified in `test_cuped_preserves_mean`), only variance.

The `cuped` scenario (`pre_period_correlation=0.75`) demonstrates a large,
quantified variance reduction — see the Experiments tab of the dashboard for
a live number on that scenario.

## Bootstrap confidence interval

`eventstream.experimentation.inference.bootstrap_ci` computes a percentile
bootstrap CI for the difference in means — resampling both arms with
replacement many times and taking the 2.5th/97.5th percentile of the
resulting distribution of differences. Included as a second, assumption-light
method (no normality assumption, unlike Welch's t-test) specifically to
sanity-check the parametric CI, which matters most for skewed metrics.

## Worked example

Control conversion: 10.0% (1,000 / 10,000)
Treatment conversion: 11.2% (1,120 / 10,000)

```
Absolute lift:  +1.20 percentage points
Relative lift:  +12.0%
z-statistic:    2.756
95% CI:         [+0.35pp, +2.05pp]
p-value:        0.00584
```

These are the exact values `eventstream.experimentation.inference.two_proportion_ztest(1000, 10000, 1120, 10000)`
returns — not hand-typed guesses — and are pinned by
`tests/unit/test_inference.py::test_two_proportion_ztest_worked_example`.

**What this result supports**: under the (here, synthetic) randomized
assignment, we can reject the null of no difference at conventional
significance, and the true effect is estimated to fall in the reported CI
with 95% confidence.

**What it does not support**: nothing about *why* the treatment worked,
whether the effect generalizes beyond this population/time period, or (per
the standard multiple-testing caveat) a strong conclusion if this is one of
many metrics/segments tested without a correction for multiple comparisons —
this project does not implement a multiple-testing correction, and a
real analysis with many simultaneous tests should apply one (e.g.
Bonferroni or Benjamini-Hochberg) before drawing conclusions across them.
