# Data Model

## Terminology

This project treats blockchain transaction activity as a behavioral event
stream, analogous to product analytics event data, using the following
mapping — and its limits:

| Product analytics concept | This project's analogue | Important caveat |
|---|---|---|
| User | Wallet / address | A wallet is a pseudonymous on-chain identifier. It is **never** treated as evidence of a unique human — one person can control many wallets, one wallet can be a shared custody/exchange/contract address, and automated (bot) activity is indistinguishable from human activity without external data. |
| Event | USDC Transfer (an on-chain log entry) | Only `Transfer` events for one contract are modeled — this is a narrow slice of a wallet's total on-chain behavior. |
| Acquisition cohort | First-seen cohort | See "First-seen ≠ true first-ever activity" below. |
| Retention | Observed return | See "Observed return ≠ retention" below. |
| Session / activation | Activation proxy | A configurable behavioral rule, not a claim about product usage. |

### First-seen ≠ true first-ever activity

A wallet's "first-seen" timestamp is the first `Transfer` event **within
this dataset's observation window**. Because the ingested window is bounded
(e.g. a 30-day live pull, or the fixture's 45-day synthetic window), a wallet
first observed on day 1 of the window may well have transacted long before
that — this project has no visibility before its own window starts. All
cohort/retention language in code, docs, and the dashboard says "first-seen"
rather than "acquired," specifically to keep this honest.

### Observed return ≠ retention (censoring)

"Retention" implies a fully-elapsed observation window; this project instead
reports **observed return rate**, and explicitly filters out any
`(cohort, week_offset)` combination where that much time hasn't actually
elapsed within the dataset's own observation window (`is_mature` in
`mart_retention_cohorts`: the whole target week must be observed). See
`docs/METRICS.md` → "Censoring" for the exact rule and `docs/DECISIONS.md` for why.

## Canonical event schema (`stg_transfers`)

| Field | Type | Notes |
|---|---|---|
| `event_id` | string | `{transaction_hash}-{log_index}`. Deterministic primary key. |
| `block_number` | int | |
| `block_timestamp` | timestamp (UTC) | Live ingestion takes the `blockTimestamp` the RPC attaches to each log (`eth_getBlockByNumber` is only a fallback for providers that omit it; an unresolvable block is an error, never a dropped event). |
| `transaction_hash` | string | |
| `log_index` | int | Position of this log within its transaction. |
| `token_address` | string | Lower-cased; validated against the expected USDC contract (`eventstream validate`). |
| `from_address` | string | Lower-cased 42-char hex (`0x` + 40 hex chars). |
| `to_address` | string | Same format. |
| `raw_amount` | int (base units) | The literal on-chain integer — **never** represented as a float. |
| `amount` | float (human units) | `raw_amount / 10^decimals`, derived once during normalization (see below), stored as a float for downstream SQL/analytics convenience. |
| `is_self_transfer` | bool | `from_address == to_address`. Retained, never dropped — see `docs/DECISIONS.md`. |
| `chain_id` | int | `8453` for Base mainnet. |
| `network` | string | e.g. `"base-mainnet"`. |
| `source` | string | `"base_rpc_live"` or `"synthetic_fixture"` — the single field that distinguishes real from synthetic data everywhere downstream (dashboard, docs, CLI output). |

### Primary key and deduplication

`(transaction_hash, log_index)` is a valid natural key: an EVM transaction
can only emit one log at a given `log_index`. Deduplication
(`eventstream.ingestion.normalize.rows_to_dataframe`, and again defensively
in `sql/staging/stg_transfers.sql` via `ROW_NUMBER()`) uses exactly this key,
so re-running an overlapping ingestion range is always safe. For a real-data window cut from
non-overlapping live segments (`eventstream extract-window`), `transform --no-global-dedup` builds
`stg_transfers_disjoint.sql` instead — the same columns without the whole-table window function,
which does not scale — and asserts the key is unique rather than assuming it; see
`docs/DECISIONS.md`, "No global dedup for proven-disjoint segments".

### Decimals and monetary precision

USDC uses 6 decimals on every chain it's issued on. The raw on-chain integer
(`raw_amount`, base units — "1 USDC" is stored on-chain as `1_000_000`) is
carried through ingestion untouched as a Python `int`. The human-readable
`amount` is derived exactly once. The reference implementation
(`eventstream.ingestion.normalize.build_event_row`) uses `decimal.Decimal`; the vectorized
bulk path used for live ingestion (`normalize.transfers_to_dataframe`) divides as a float
only when `raw_amount < 2**53` — where the quotient is the correctly rounded value of the
exact ratio, bit-identical to the `Decimal` result — and falls back to `Decimal` above that.
`tests/unit/test_bulk_normalize_parity.py` asserts the two paths agree byte for byte.

## First-seen day

`int_wallet_first_seen` carries `first_seen_at` (timestamp), `first_seen_date` (its UTC
calendar day, "day 0") and `first_seen_week`. Day-grain windows (activation, funnel) are
anchored on `first_seen_date` because `activity_date` is truncated to midnight.

## Wallet-interaction table (`stg_wallet_events`)

Each row in `stg_transfers` is exploded into **two** wallet-interaction rows:
one `SEND` (from the sender's perspective) and one `RECEIVE` (from the
receiver's). This lets wallet-level analytics (active wallets, first-seen,
lifecycle, segmentation) treat both counterparties symmetrically without
writing every query twice.

**This table is never used for token-volume totals.** Summing `amount` over
`stg_wallet_events` would double-count every transfer's value (once as a
SEND, once as a RECEIVE). Volume metrics always aggregate directly over
`stg_transfers`. See `docs/DECISIONS.md`.

## What this project does *not* attempt

- **Entity resolution**: no attempt is made to determine whether an address
  is a human EOA, a smart contract, an exchange hot wallet, or a bridge —
  doing so reliably requires external labeling data this project doesn't
  have. Extreme values in per-wallet volume/frequency are visible in the
  concentration and segmentation output, but are not automatically
  attributed to any entity type.
- **Sybil / bot detection**: no attempt is made to detect coordinated or
  automated wallet clusters.
- **Identity, demographic, or political inference**: never attempted, and
  wallets are treated strictly as pseudonymous identifiers throughout.
