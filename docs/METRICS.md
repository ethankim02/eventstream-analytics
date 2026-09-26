# Metrics

Every metric below states its exact formula, unit, grain, assumptions, and
caveats. See `docs/DATA_MODEL.md` for schema/terminology and
`docs/EXPERIMENTATION.md` for anything involving statistical inference.

## Self-transfers

A self-transfer is a `Transfer` event where `from_address == to_address`.
These are **retained** in `stg_transfers` (never dropped) but **excluded**
from every metric below unless stated otherwise, because counting a wallet
sending funds to itself as an "interaction" would inflate activity without
representing real behavior.

## Activity

**Active wallets** (daily/weekly)
Number of distinct wallet addresses (from either side of a transfer) with at
least one non-self-transfer event in the period. Grain: wallet × day (or
week). Source: `int_wallet_daily_activity` / `mart_daily_metrics`,
`mart_weekly_metrics`.

**Transfer count** (daily/weekly)
Count of qualifying (non-self) `Transfer` events in the period. Grain: event
× day/week.

**USDC volume** (daily/weekly)
Sum of `amount` over qualifying transfers in the period. Unit: USDC (human
units, i.e. already decimal-adjusted). This is summed once over
`stg_transfers`, never over the exploded `stg_wallet_events` table (which
would double-count).

**Median / distribution of transfer values**
`MEDIAN(amount)`, plus `p10`/`p50`/`p90`/`p99` via `approx_quantile`
(DuckDB's t-digest approximate quantile — appropriate given transfer-value
distributions are heavily right-skewed; see `docs/DECISIONS.md`). Reported
alongside the mean where relevant, since mean is pulled hard by the tail.

## First-seen cohorts and retention

**First-seen wallet / first-seen cohort**
A wallet's earliest qualifying event timestamp within this dataset's
observation window (`int_wallet_first_seen.first_seen_at`), truncated to the
week for cohorting (`first_seen_week`). **Not** the wallet's true first-ever
on-chain activity — see `docs/DATA_MODEL.md`.

**Observed return rate at week offset _k_**
Among wallets first-seen in cohort week _C_, the share with at least one
qualifying event during week `C + k`. Grain: cohort × week offset.

**Censoring (mature cohorts only)**
A `(cohort_week, week_offset)` cell is **mature** only if
`cohort_week + week_offset weeks <= max_observed_week` in the dataset — i.e.
enough time has actually elapsed to observe that offset at all. Immature
cells are excluded from every summary retention number
(`eventstream.analytics.retention.summary_retention` filters on
`mart_retention_cohorts.is_mature`) rather than being silently counted as a
non-return. A wallet first seen in the dataset's final week, for instance,
cannot yet have a W4 return measurement — including it in that denominator
would understate the true rate.

## Engagement

**Transactions per active wallet**: `total_events / active_wallets` over the
whole window (`int_wallet_lifecycle.total_events`, mean and median both
reported — the distribution is heavily right-skewed).

**Active days per wallet**: distinct calendar days with a qualifying event
(`int_wallet_lifecycle.active_days`), mean and median.

**Repeat wallet rate**: share of wallets with `total_events > 1`.

**Time between transactions**: `int_wallet_lifecycle.avg_days_between_active_days`,
computed per-wallet via `LAG()` over that wallet's own active days (a
wallet's gap baseline is relative to itself, not the population).

## Behavioral segments

Thresholds are **data-derived quantile cutoffs**, computed fresh from
whatever dataset is loaded (`sql/marts/mart_wallet_segments.sql`) — never a
hard-coded number like "more than 10 transactions," which would silently stop
meaning anything as the dataset's scale changes.

| Segment | Rule |
|---|---|
| `one_time` | exactly 1 lifetime qualifying event |
| `occasional` | more than 1 event, at or below the population **median** event count |
| `repeat` | above the median, at or below the population **90th percentile** event count |
| `high_frequency` | above the 90th percentile of events per wallet |
| `high_volume` (separate axis) | total transferred amount at/above the 90th percentile of per-wallet volume |

## Concentration

**Gini coefficient** — measured on **per-wallet total transferred amount**
(`int_wallet_lifecycle.total_amount`; a self-transfer-excluded, SEND+RECEIVE
combined figure via the wallet-interaction table), not event counts, unless
explicitly labeled "by transaction count." Computed via the standard
rank-weighted formula on sorted values:

```
gini = (2 * Σ(rank_i * x_i) - (n+1) * Σx_i) / (n * Σx_i)
```

**Top 1% / top 10% volume share** and **top 10% transaction-count share** —
computed via `NTILE(100)` percentile bucketing over per-wallet totals. This
is an *approximation under ties* (e.g. many wallets sharing the exact same
total-event count near a bucket boundary can shift slightly which bucket
they land in) — exact for continuous-valued volume, approximate for
discrete-valued event counts at small N. Documented here rather than
silently assumed exact.

**Lorenz curve**: cumulative share of wallets (x-axis, sorted ascending by
volume) vs. cumulative share of volume (y-axis) — plotted against the line
of perfect equality on the dashboard's Concentration tab.

## Activation proxy

**Definition** (configurable; default used throughout this project): a
wallet is "activated" if it was active on **≥ 2 distinct calendar days**
within its first 7 observed days after first-seen
(`int_wallet_activation.is_activated`).

**What this is, and is not**: this is an *associational* behavioral proxy
computed on observational data. The dashboard/CLI report it as, e.g.,
"activated wallets show an N× higher observed return rate" — never as a
causal claim ("activation causes retention"). See `docs/EXPERIMENTATION.md`
for why observational blockchain data cannot support a causal claim, and
where this project's actual causal-inference tooling lives instead (the
separate, synthetic-data-only experimentation module).

## Lifecycle / behavioral funnel

A progression of qualifying-event milestones, **not** a literal application
signup funnel:

1. first observed event (100% by definition — the cohort itself)
2. second observed event (≥ 2 lifetime events)
3. active on a second distinct calendar day
4. active again ≥ 7 days after first-seen
5. active again ≥ 30 days after first-seen

Stages 4 and 5 are **maturity-filtered**: a wallet only counts toward that
stage's denominator once `first_seen_at + 7` (or `+30`) days has actually
elapsed as of the dataset's most recent observed date
(`eventstream.analytics.lifecycle.funnel_conversion`) — the same censoring
principle as retention, applied to the funnel.

## Anomaly detection

**Method**: rolling-median / robust (MAD-based) z-score, computed for
`active_wallets`, `transfer_count`, `total_volume`, and `median_amount`
(`sql/marts/mart_anomalies.sql`, mirrored exactly by
`eventstream.analytics.anomalies` in pure Python/pandas):

1. `rolling_median[i]` = median of the metric over the **trailing 7 days**
   (`i-7 .. i-1`) — median, not mean, so a rolling baseline resists being
   pulled by the very outliers it's meant to flag.
2. `abs_dev[i]` = `|value[i] - rolling_median[i]|` — day _i_'s own deviation
   from its own preceding baseline.
3. `rolling_mad[i]` = median of `abs_dev` over the trailing 7 days
   (`i-7 .. i-1`, again excluding day _i_ itself).
4. `robust_z_score[i]` = `0.6745 * (value[i] - rolling_median[i]) / rolling_mad[i]`.
5. Flagged as an anomaly if `|robust_z_score| > 3.5`.

**Why 3.5 / why this method**: 0.6745 and the 3.5 threshold are the standard
Iglewicz & Hoaglin robust-outlier constants (0.6745 rescales MAD to be
comparable to a normal-distribution standard deviation; 3.5 is their
recommended flagging threshold). No deep learning / trained model is used —
see `docs/DECISIONS.md` for why that would be the wrong tool here.

A day with `rolling_mad == 0` (a perfectly flat trailing week) is never
flagged, regardless of that day's value, to avoid a division-by-zero-driven
spurious flag.
