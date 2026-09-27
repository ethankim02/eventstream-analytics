# Data Source

This document records the Phase 0 research this project's ingestion design is
based on: what network, contract, and event schema it targets, how it was
verified, and the constraints that shaped the ingestion strategy.

## Network

| Field | Value | Source |
|---|---|---|
| Network | Base (an OP Stack Ethereum L2) mainnet | Base official docs |
| Chain ID | `8453` (`0x2105`) | `docs.base.org` → "Connect to Base" |
| Public RPC | `https://mainnet.base.org` | `docs.base.org` → "Connect to Base" |
| Testnet | Base Sepolia, chain ID `84532` (`0x14a34`), RPC `https://sepolia.base.org` | `docs.base.org` |

The public `mainnet.base.org` endpoint is explicitly documented as rate-limited
and **not intended for production use** — Base's own docs recommend a paid/free
provider (Alchemy, Infura, QuickNode, etc.) for anything beyond light,
occasional use. This project defaults to the public endpoint (appropriate for
a small, bounded historical pull) but makes the RPC URL fully configurable via
`BASE_RPC_URL` so a provider URL can be dropped in for a larger pull.

## Token contract

| Field | Value | Source |
|---|---|---|
| Token | USDC (native, Circle-issued) | Circle developer docs |
| Contract address (Base mainnet) | `0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913` | `developers.circle.com/stablecoins/usdc-contract-addresses` |
| Decimals | `6` | Standard USDC decimals across all chains |

**Important distinction**: Base also has a *bridged* USDC variant historically
called "USDbC" (`0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA`), which is **not**
issued by Circle and is **not** used by this project. Using the wrong contract
address would silently mix bridged and native USDC activity — the
`token_contract_matches_expected` data-quality check (`sql` staging +
`eventstream validate`) exists specifically to catch this class of mistake.

## Event schema

USDC is a standard ERC-20 token. This project ingests only the `Transfer`
event:

```solidity
event Transfer(address indexed from, address indexed to, uint256 value)
```

- `topic0` (the event signature hash) is always
  `0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef`
  (`keccak256("Transfer(address,address,uint256)")`) — this is a fixed,
  universal ERC-20 constant, not something specific to USDC or Base.
- `from` and `to` are indexed (`topic1`, `topic2`); `value` is unindexed and
  lives in the log's `data` field, as a raw base-unit integer.

## Rate limits / block-range constraints

Public reporting on `mainnet.base.org`'s `eth_getLogs` block-range cap is
inconsistent — some sources cite a ~2,000-block cap, others ~10,000, and the
exact number is not pinned down in Base's own docs. The rate limit itself is
also undocumented precisely (only "the public endpoint is rate-limited").

Given that ambiguity, this project's ingestion client
(`eventstream.ingestion.base_rpc.RpcClient`) is defensive by design rather
than hard-coding a guessed number:

- a conservative **default chunk size of 2,000 blocks** (`EVENTSTREAM_BLOCK_CHUNK_SIZE`),
- exponential backoff and retry on JSON-RPC rate-limit error codes
  (`-32016`, `-32005`),
- automatic chunk-size **shrinking** (halving) when a response error message
  suggests the requested range was too large, then resuming from where it
  left off.

This means the exact provider-side limit doesn't need to be known in advance
for ingestion to complete correctly — it self-adjusts.

## What was actually extracted

Live ingestion **has been run against `mainnet.base.org`** (an earlier revision of this file,
written before that was possible, said otherwise). The planned range was blocks
`49,765,327 – 51,579,726` (about 42 days, 908 segments of 2,000 blocks). The pull started
2026-09-26 17:59 UTC and was **stopped deliberately** at 2026-09-27 04:42 UTC after 617 segments:
1,232,400 contiguous blocks (`50,347,327 – 51,579,726`), **103,268,795 events**, from
2026-08-23 11:20 UTC to 2026-09-20 23:59:59 UTC, about 5.3 GB of Parquet. The rest was dropped
because analysing 100M+ events is beyond what this portfolio project needs (see
`docs/DECISIONS.md`, "Bounded real-data window"). The extraction is checkpointed and the manifest
records `complete: false`, so the same command resumes it. It is kept out of git (`data/raw/` is
ignored); the published numbers come from a bounded window cut from it.

## The analysis window

| Field | Value |
|---|---|
| Blocks | `51,493,327 – 51,579,726` (86,400 blocks = exactly 2 UTC days at 2 s/block) |
| First / last event | 2026-09-19 00:00:01 UTC (Sat) / 2026-09-20 23:59:59 UTC (Sun) |
| Events | 5,670,488 (5,653,996 qualifying; 16,492 self-transfers retained but excluded) |
| Segments | 44 whole segments, proven contiguous and non-overlapping before use |
| Wallets | 408,518 distinct addresses with a qualifying event |

The window starts and ends on UTC midnights, so both days are complete (the retention and
funnel censoring logic assumes the last observed calendar day is whole). Every number below
and in the README is in [`real_data_findings.json`](real_data_findings.json), which
`scripts/build_findings.py` writes from the warehouse (rerunning it on the same window rewrites
the same bytes). Data-quality and independent reconciliation checks against the raw window all
pass.

### Measured properties of this data

- **Heavy right tail.** Median transfer 32.89 USDC; mean
  41,089 USDC; p90 25,665; p99 1,029,352; largest
  219,060,612. Gross event volume over the two days is
  $232.3B, so it says little about typical behaviour.
- **Dust and zero-value transfers.** 3.5% of qualifying events carry a
  zero amount and 25.5% are under 1 USDC. "Active wallets" therefore
  includes passive counterparties: 408,518 wallets appear on either
  side of a transfer, 312,111 sent at least once, and
  286,422 were party to a transfer of at least 1 USDC.
- **Multi-log transactions.** 58.4% of events sit
  in a transaction that emits more than one USDC `Transfer` log (up to
  430 in one transaction; 3,288,501
  transactions in all), so volume counts every hop.
- **Concentration.** One wallet holds 38.6% of gross per-wallet
  volume, the top 10 hold 91.9% and the top 100
  99.0% (Gini 0.9999).
  These are pooled addresses: contracts, routers, pools and exchanges are not distinguished from
  externally owned accounts.

## Reproducing it

```bash
# 1. Download just the window (needs network; about an hour at the public endpoint's observed
#    rate of ~1.3 minutes per 2,000-block segment with 6 workers). Skip this if the segments
#    are already in data/raw/.
uv run eventstream ingest --mode live --start-block 51493327 --end-block 51579726 --workers 6

# 2. Everything else runs offline and finishes in one to three minutes:
python scripts/run_real_analysis.py
```

`run_real_analysis.py` runs `extract-window` → `transform --no-global-dedup` → `validate
--reconcile-glob` → `analyze` → `build_findings.py` and prints per-step wall-clock times. Set
`EVENTSTREAM_DUCKDB_MEMORY_LIMIT` (for example `4GB`) if the machine has little free RAM (see
`.env.example`). Use a different `--start-block/--end-block` for another window; the extractor
refuses any range that is not covered by intact, contiguous segments. If `data/raw/` holds more
than one manifest (for example this project's larger one and your own), pass `--manifest`.

Dashboard screenshots: `python scripts/capture_dashboard.py data/processed/base_usdc_real.duckdb
docs/assets/real` (needs Playwright; see the script's docstring).

## What CI and the demo run on

CI, `eventstream demo` and the test suite need no network. They run on a **deterministic
synthetic fixture** (`eventstream.ingestion.fixtures`, `eventstream ingest --mode fixture`)
plus a small committed sample of real events (`data/samples/`, 200 blocks, independently
re-verified against the RPC). Synthetic rows are tagged `source = "synthetic_fixture"` end to
end and the dashboard shows a warning banner for them, so they cannot be mistaken for on-chain
activity; the README labels every figure with the data it came from.
