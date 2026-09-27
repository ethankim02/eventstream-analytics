# Engineering Decisions

A short log of the non-obvious choices in this project and why they were
made, so a reviewer doesn't have to reverse-engineer intent from code.

## Warehouse: DuckDB, not Postgres/Spark

DuckDB gives real analytical SQL (window functions, `QUANTILE_CONT`,
`NTILE`, `UNPIVOT`) with zero infrastructure — a recruiter can clone the repo
and run the whole pipeline with no database server, Docker, or cloud account.
The synthetic fixture is tens of thousands of rows; the real analysis window is 5.67M events,
which builds, validates and reports in one to three minutes on a 16 GB laptop (see
"Bounded real-data window" for what happens beyond that). Postgres would add operational weight
for no analytical benefit at this scale; Spark would be over-engineering (see "Data size" below).

## Python: pandas, not Polars

Pandas remains the more universally recognized library for a reviewer
skimming code, and DuckDB's `.fetchdf()` returns pandas natively. Polars would
work fine here too, but the extra unfamiliarity cost isn't worth it for what
this project needs.

## CLI: Typer

Typer's declarative style keeps `cli.py` readable as a spec of the commands
this project exposes, with minimal boilerplate versus raw `argparse`.

## Chunk size / retry strategy for live ingestion

See `docs/DATA_SOURCE.md`. `mainnet.base.org` publishes no `eth_getLogs` limits and
public reports disagree, so they were measured: a hard 2,000-block span cap
(HTTP 413, code `-32614`) and, well below it, a response-size cap of roughly 20,000
logs (HTTP 500, code `-32020`, "backend response too large") that USDC's ~95 logs per
block hits at around 300 blocks. The first version of the client matched neither error
(it raised on HTTP status before reading the JSON-RPC error body, and its message hints
did not include either text), so it could not have ingested from the real endpoint.
The shipped `AdaptiveChunker` shrinks on either signal, steers toward ~16,000 logs per
request from the observed log density, and recovers its target after rejections; it is
shared by all workers so one worker's discovery benefits the rest.

## Checkpointed segments for a multi-hour pull

The planned 42-day range is ~1.8M blocks (over 100M events at the observed ~84 events per
block), far too much to hold in memory or to lose to a dropped connection. The range is split
into 2,000-block segments; each is
fetched, written atomically to its own zstd Parquet file, and only then recorded in a
manifest. Re-running the same command skips every segment whose file and row count are
intact, refuses a different segmentation (which would overlap and duplicate events), and
raises — listing the ranges — if any segment fails, so an incomplete dataset can never be
mistaken for a complete one. Segments are dispatched newest-first so a partial run already
holds the most recent contiguous history — which is what made it possible to stop the pull at
617 of 908 segments and still have a gap-free, up-to-date extraction to cut a window from.

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
alone. The cutoffs are **exact** quantiles (`QUANTILE_CONT`), not DuckDB's t-digest
`APPROX_QUANTILE`: see "Exact quantiles" below. The exact rule is documented in
`docs/METRICS.md`.

## Right-censoring handling for retention

`mart_retention_cohorts.sql` computes an explicit `is_mature` boolean per
`(cohort_week, week_offset)` cell: a cell is only mature once the whole target
week (`cohort_week + week_offset`) lies inside the observed data window. (The
first version compared against the *start* of the last observed week, so a window
ending mid-week counted that partial week as fully observed; the real-data run made
whole-week alignment matter, and a boundary test now pins the rule.) `eventstream.analytics.retention.summary_retention` filters to
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

## Independent reconciliation instead of "it ran"

Passing `validate` shows the staged data is well-formed, not that the marts are right. A
model can run cleanly on real data and still be wrong (two such defects were found this way:
a funnel join that fans out per event x day x day, and an activation window that dropped
the first-seen day). `eventstream validate --reconcile-glob` recomputes each headline mart
directly from the raw Parquet by a different route (own SQL, epoch arithmetic, numpy
reference code, pandas re-derivation of the wallet-level definitions for a sample that always
includes the heaviest wallets) and compares cell by cell. `tests/integration/test_reconcile.py`
proves it can fail by corrupting each mart and asserting the matching check trips.

## Bounded real-data window

The published real-data analysis is a **two-UTC-day window (blocks 51,493,327 – 51,579,726,
5,670,488 events)** cut from a 103M-event extraction that was already on disk, not the whole
extraction. Two reasons.

*Cost.* The staging step's `ROW_NUMBER()` dedup window degraded faster than linearly on this
laptop: per-row cost measured 4.9 µs at 5.0M rows and 11.1 µs at 12.0M (133.7 s total), and a
21.8M-row build spent 1,710 s in that one model. Tuning DuckDB's memory, thread and
insertion-order settings roughly halved the 12M-row time (133.7 s to 68.1 s), while a
`GROUP BY` / `arg_max` formulation of the same dedup was far worse: 6,757 s at 12M rows with
default settings, and an out-of-memory error under a 3 GB cap. Chasing 100M+ rows would have
turned a portfolio project into an infrastructure exercise, so the target became "credible,
reproducible, finishes in minutes". The bounded window runs the whole pipeline in about 74–156 s
(three from-scratch runs; the spread is disk and cache state; the biggest steps are loading the
window and materialising `stg_transfers` and its 11.3M-row per-wallet-leg expansion,
`stg_wallet_events`).

*What a window can support.* Retention is weekly, and a cell `(cohort_week, k)` is mature only if
week `cohort_week + k` lies wholly inside the data. That ties the window length to the weeks
that can be reported, at about 3.6M events per day here:

| Window | Events | Mature weekly retention |
|---|---|---|
| 2 days (chosen) | 5.7M | none — every offset ≥ 1 is censored |
| 8 days (Sun + one full week) | 25.8M | W1 for the opening cohort only (a baseline, not new wallets) |
| 14 days (two full weeks) | 47.2M | W1 for the full-week opening cohort |
| 21 days (three full weeks) | 74.1M | W1 for a first genuinely new cohort; W2 for the opening one |

W4 needs the whole 28-day extraction (the opening week plus four full weeks). Because a 2-day window cannot support
weekly retention, the pipeline reports it as **not measurable** (all offset ≥ 1 cells excluded as
immature, summary rows empty, the dashboard says so) rather than manufacturing a number; the
same applies to the 7-day rolling anomaly baseline. Retention is demonstrated on the synthetic
fixture, whose 45-day window supports W1 to W4, and its logic is pinned by boundary tests. The
descriptive results (distribution, segments, concentration, funnel stages 1-3) describe this
two-day period and do not depend on window length beyond that, and the README says so.

## No global dedup for proven-disjoint segments

`(transaction_hash, log_index)` stays the event key, but the default `ROW_NUMBER()` dedup
partitions the whole table, which is exactly the step that fell over at scale. Live ingestion
cannot create duplicates: segments are non-overlapping by construction (`plan_segments`),
resuming skips intact segments or rewrites the same file atomically, every log is checked to
fall inside its segment, a re-run with a different segmentation is refused, and events are
already de-duplicated on the key within each segment. So `eventstream extract-window` cuts a
window only from segments proven to tile it exactly (reusing `verify_manifest`: no gap, no
overlap, every file present with its recorded row count), and `transform --no-global-dedup`
builds `stg_transfers` without the window function. The assumption is then **checked, not
trusted**: the extractor counts distinct keys, the build asserts uniqueness before any model that
depends on it (`DuplicateEventsError` otherwise; a few seconds here), and `validate` and
`reconcile` re-check against the raw Parquet. The default build (fixtures, arbitrary globs) keeps
the global dedup, and its test still passes.

## Exact quantiles, not t-digest

`APPROX_QUANTILE` is a t-digest whose per-thread sketches merge in a scheduling-dependent order.
On the real window the p90 events-per-wallet cutoff came out as 9 or 10 depending on the run
(re-labelling every wallet with exactly 10 events between `repeat` and `high_frequency`), the
p90 total-amount cutoff varied between 1,136 and 1,157 USDC, and the high-volume wallet count
moved between 40,451 and 40,728 on identical data. The exact values are 1,122.38 USDC and 10
events (40,852 high-volume wallets), and they are the same on every run and thread count.
Exactness costs nothing at this scale (408k wallets; 5.7M events), so the segmentation cutoffs,
the daily p90/p99 and the transfer-value distribution use `QUANTILE_CONT`.
`tests/integration/test_determinism.py` builds with 1 and 8 threads and compares against NumPy;
it fails on the old `APPROX_QUANTILE` code. Parallel `SUM` over doubles has the same kind of
run-to-run noise (about 1e-16 relative), which is why published figures are rounded to ten
significant digits and the findings file is byte-identical across rebuilds.

## Data size

Two datasets, on purpose. The committed synthetic fixture is ~40k events over a 45-day
window and ~1,400 wallets: large enough to show heavy-tailed behavior, small enough that
the full `eventstream demo` pipeline (fixture generation → DuckDB build → 17 SQL
models → validation → analytics → experimentation) runs in well under a minute and CI
needs no network. The real analysis is a 5.67M-event, 2-day window (408,518 wallets) cut from a
larger 103M-event local extraction; neither the extraction nor the window is committed, only a
small real sample (`data/samples/`), its provenance, and the findings JSON. See
`docs/DATA_SOURCE.md`.
