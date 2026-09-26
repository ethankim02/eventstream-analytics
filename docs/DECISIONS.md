# Engineering Decisions

A short log of the non-obvious choices in this project and why they were
made, so a reviewer doesn't have to reverse-engineer intent from code.

## Warehouse: DuckDB, not Postgres/Spark

DuckDB gives real analytical SQL (window functions, `APPROX_QUANTILE`,
`NTILE`, `UNPIVOT`) with zero infrastructure — a recruiter can clone the repo
and run the whole pipeline with no database server, Docker, or cloud account.
Postgres would add operational weight for no analytical benefit at this data
scale (tens of thousands to low millions of rows); Spark would be pure
over-engineering for a dataset this size (see "Data size" below).

## Python: pandas, not Polars

Pandas remains the more universally recognized library for a reviewer
skimming code, and DuckDB's `.fetchdf()` returns pandas natively. Polars would
work fine here too, but the extra unfamiliarity cost isn't worth it for what
this project needs.

## CLI: Typer

Typer's declarative style keeps `cli.py` readable as a spec of the commands
this project exposes, with minimal boilerplate versus raw `argparse`.

## Chunk size / retry strategy for live ingestion

See `docs/DATA_SOURCE.md` — the exact `eth_getLogs` block-range cap on the
public Base RPC is not consistently documented (reports range 2,000–10,000
blocks). Rather than guess a single number and risk hard failures, the
ingestion client defaults to a conservative 2,000-block chunk and adaptively
halves it on a "range too large"-shaped error, so it self-corrects regardless
of the endpoint's actual limit on a given day.

## Deterministic hash-based experiment assignment

`eventstream.experimentation.assignment` buckets a unit via
`sha256(salt:unit_id)` rather than drawing from a stateful RNG per unit. This
mirrors how real bucketing systems work: a unit's assignment must be stable
regardless of query order, batch size, or which other units are looked up
alongside it — a property a simple `np.random.choice` over an array does not
guarantee.

## Quantile-based segmentation thresholds, not hard-coded cutoffs

`sql/marts/mart_wallet_segments.sql` derives its `one_time` / `occasional` /
`repeat` / `high_frequency` cutoffs from the dataset's own event-count median
and 90th percentile, rather than fixed numbers like ">10 transactions". Fixed
numeric thresholds silently stop meaning anything as dataset scale changes;
quantile cutoffs stay meaningful and are exactly reproducible from the SQL
alone. The exact rule is documented in `docs/METRICS.md`.

## Right-censoring handling for retention

`mart_retention_cohorts.sql` computes an explicit `is_mature` boolean per
`(cohort_week, week_offset)` cell: a cell is only mature once
`cohort_week + week_offset` weeks has actually elapsed within the observed
data window. `eventstream.analytics.retention.summary_retention` filters to
`is_mature` before averaging, so a wallet first seen last week never gets
silently counted as "didn't return by week 8" just because week 8 hasn't
happened yet for it. See `docs/METRICS.md` → "Censoring".

## Rolling-median MAD anomaly detection, not a trained model

A median-based robust z-score (Iglewicz & Hoaglin's rule, threshold 3.5) is
fully auditable in a few lines of SQL, requires no training data or model
lifecycle, and is robust to the very outliers it's trying to detect (unlike a
mean/stddev approach, which the outliers themselves pull around). A deep
learning approach would add operational and interpretability cost this
problem doesn't need. The current-day value is deliberately excluded from its
own baseline window (`ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING`), and the
Python reference implementation (`eventstream.analytics.anomalies`) mirrors
the SQL's windowing exactly row-for-row — proven by an integration test that
asserts the two agree to floating-point precision.

## CUPED over a second advanced method

CUPED was chosen over, e.g., stratified sampling or sequential testing
because it's directly demonstrable on the synthetic simulator (which already
generates a pre-period covariate) and has a clean, auditable derivation
(`eventstream.experimentation.cuped`) with a quantifiable variance-reduction
number — exactly the kind of "why this, not just that it exists" a reviewer
should be able to check for themselves.

## Self-transfers and address roles

Self-transfers (`from == to`) are retained in `stg_transfers` (never
silently dropped) but excluded from wallet-activity, retention, funnel, and
segmentation logic, since counting a wallet's transfer to itself as an
"interaction" would inflate activity metrics without representing real
behavior. Each transfer is exploded into two `stg_wallet_events` rows (SEND +
RECEIVE) for wallet-level analysis, but token-volume totals are always summed
directly over `stg_transfers` (never `stg_wallet_events`) to avoid double
counting a single transfer's value across both legs. See `docs/DATA_MODEL.md`.

## Data size

The fixture generator targets ~40k events over a 45-day window and ~1,400
wallets — large enough to show heavy-tailed, real-shaped behavior (retention
decay, concentration, occasional anomalies) but small enough that the full
`eventstream demo` pipeline (fixture generation → DuckDB build → 17 SQL
models → validation → analytics → experimentation) runs in well under a
minute on a laptop. A real Base pull is recommended over a 7–30 day block
range for the same reason — see `docs/DATA_SOURCE.md`.
