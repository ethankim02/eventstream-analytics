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

## Time zone

Every day and week boundary is **UTC** (weeks start Monday 00:00 UTC). The warehouse
connection pins `TimeZone='UTC'` (`eventstream.warehouse.connection`): DuckDB truncates
`TIMESTAMPTZ` values in the *session* time zone, which defaults to the host's, so without
the pin the same data produced different days, weeks and cohorts on different machines
(found on a UTC+9 machine, where a window ending Sunday 23:59:59 UTC spilled into a phantom
extra week).

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

**This is gross Transfer-event volume, not net value moved.** Every `Transfer` log is
summed, so value routed through several contracts inside one transaction is counted at every
hop, and a transfer that is reversed within the same transaction is counted twice. On the real
dataset most events sit in transactions that emit more than one USDC `Transfer` log, and the
totals are dominated by a small number of very large transfers (the mean transfer is orders of
magnitude above the median; figures in `docs/DATA_SOURCE.md`). Read volume as a measure of
on-chain token movement, use counts and medians for typical behavior, and never present it as
spending or payment volume.

**"Active" includes passive counterparties.** A wallet is active if it appears on *either*
side of a qualifying transfer, so a wallet that merely *receives* a transfer counts, including
an unsolicited zero-value or dust transfer. On the real dataset a few percent of events carry a
zero amount and about a quarter are under 1 USDC. The results write-up therefore reports a
sender-only sensitivity beside the headline active-wallet counts.

**Median / distribution of transfer values**
`MEDIAN(amount)`, plus `p10`/`p50`/`p90`/`p99` via `quantile_cont` (exact, linearly
interpolated — the same definition as NumPy's default `quantile`). Exact rather than DuckDB's
t-digest `approx_quantile`, whose result changes from run to run with thread scheduling; see
`docs/DECISIONS.md`, "Exact quantiles". Transfer-value distributions are heavily right-skewed,
so these are reported alongside the mean where relevant, since the mean is pulled hard by the
tail.

## First-seen cohorts and retention

**First-seen wallet / first-seen cohort**
A wallet's earliest qualifying event timestamp within this dataset's
observation window (`int_wallet_first_seen.first_seen_at`), truncated to the
week for cohorting (`first_seen_week`). **Not** the wallet's true first-ever
on-chain activity — see `docs/DATA_MODEL.md`.

**The opening-week cohort is a baseline, not a new-wallet cohort.** A wallet is first-seen in
the window's first week if it transacted then, regardless of when it first ever used USDC, so
that cohort is dominated by wallets that were already active before observation began (on the
real dataset it is by far the largest cohort). Its return rates describe the established
active base. Later cohorts are contaminated the other way: a long-dormant wallet that
reappears in week 4 is "first-seen in week 4". Neither is acquisition. The window has no
lookback, so this cannot be corrected from the data itself.

**Observed return rate at week offset _k_**
Among wallets first-seen in cohort week _C_, the share with at least one
qualifying event during week `C + k`. Grain: cohort × week offset.

**Censoring (mature cohorts only)**
A `(cohort_week, week_offset)` cell is **mature** only if the *whole* target week
(`cohort_week + week_offset`) lies inside the observed window: its end
(`cohort_week + (week_offset + 1) weeks`) is at or before the end of the last observed
calendar day. Offset 0 is always mature (cohort membership is defined by activity in that
week, so it cannot be censored). A week the data only partly covers is immature: an earlier
version compared against the start of the last observed week and so treated a window ending
on, say, a Thursday as if that final week were fully observed, scoring wallets that had
simply not had the chance to return as non-returners. The last observed calendar day is
assumed complete, so **end a live window on a UTC midnight** (see `docs/DATA_SOURCE.md`).
**Window length decides what can be reported.** Because cells are weekly and a cell needs its
whole target week, a window shorter than about eight days has no mature cell beyond offset 0,
and a first genuinely new-wallet cohort with a mature W1 needs roughly three weeks. On the two-day real
window (`docs/DATA_SOURCE.md`), weekly retention is therefore reported as **not measurable**
(summary rows empty, no rate shown) instead of a number. `docs/DECISIONS.md`, "Bounded real-data
window", tabulates window length against reportable offsets.

Immature cells are excluded from every summary retention number
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

The cutoffs are exact `quantile_cont` values, so a rebuild on the same data always produces the
same segments. (With the t-digest `approx_quantile` this project used at first, the p90 events
cutoff on the real window flipped between 9 and 10 from run to run and moved every wallet with
exactly 10 events.) `p50` and `p90` of events per wallet may fall between integers on small
datasets; the rules above compare the integer event count against them directly.

## Concentration

**Gini coefficient** — measured on **per-wallet total transferred amount**
(`int_wallet_lifecycle.total_amount`; a self-transfer-excluded, SEND+RECEIVE
combined figure via the wallet-interaction table), not event counts, unless
explicitly labeled "by transaction count." Computed via the standard
rank-weighted formula on sorted values:

```
gini = (2 * Σ(rank_i * x_i) - (n+1) * Σx_i) / (n * Σx_i)
```

Per-wallet "volume" is gross: it sums both the SEND and RECEIVE legs, so a wallet is credited
with everything that flows through it. Wallets are all addresses that transacted, so
externally owned accounts, contracts (pools, routers, vaults) and exchanges are pooled and not
distinguished; on real data a handful of high-throughput addresses hold nearly all of the
volume and push the Gini to within a fraction of a percent of 1, so report it with at least
four decimals.

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
wallet is "activated" if it was active on **≥ 2 distinct UTC calendar days**
within its first 7 calendar days, counting the first-seen day as day 0 (days 0-6;
`int_wallet_activation.is_activated`). Because the first-seen day is always active, this
means "returned on at least one other day during the first week". (An earlier version
compared the day-truncated `activity_date` with the timestamped `first_seen_at`, which
silently dropped the first-seen day from the window, contradicting this definition; on the
test fixture only 46% of wallets' day counts agreed with the documented rule.)

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
4. active again on calendar day 7 or later after the first-seen day
5. active again on calendar day 30 or later after the first-seen day

Stages 4 and 5 are **maturity-filtered**: a wallet only counts toward that
stage's denominator once `first_seen_at + 7` (or `+30`) days has actually
elapsed as of the dataset's most recent observed date
(`eventstream.analytics.lifecycle.funnel_conversion`) — the same censoring
principle as retention, applied to the funnel. Stages 4 and 5 are open-ended ("ever active
again after day 7 / 30"), so a wallet first seen just after the eligibility cutoff has had far
less time to convert than one first seen at the window's start. The stage rates are
therefore exposure-weighted averages, not comparable across first-seen dates; use the
fixed-offset retention matrix for like-for-like comparison.

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
