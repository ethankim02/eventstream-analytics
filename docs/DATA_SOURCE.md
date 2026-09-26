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

## Why this environment could not run live ingestion

This project's cloud sandbox denies outbound network access to
`mainnet.base.org` by policy (confirmed via a direct connectivity test: the
egress proxy returned a policy-denial `403` on the CONNECT tunnel). The live
ingestion code path (`eventstream.ingestion.live`) is implemented and unit/
integration-tested against a mocked RPC client, but has not been exercised
against the real endpoint from this environment.

**To pull real data locally** (once you have normal internet access):

```bash
uv sync
uv run eventstream ingest --mode live --start-block <START> --end-block <END>
uv run eventstream transform --source-glob "data/raw/*.parquet"
uv run eventstream validate
uv run eventstream analyze
```

Pick `--start-block`/`--end-block` for roughly a 7–30 day window (see
`docs/DECISIONS.md` for how to size that from Base's ~2-second block time).
You can find a current block number by querying `eth_blockNumber` against
`https://mainnet.base.org`, or by using an explorer (e.g. basescan.org).

## What ships in this repository instead

Since real ingestion could not be executed from this sandbox, this repository
ships (and its CI, tests, and demo run entirely on) a **deterministic
synthetic fixture** (`eventstream.ingestion.fixtures`, `eventstream ingest
--mode fixture`), explicitly tagged `source = "synthetic_fixture"` end to end
so it can never be mistaken for real on-chain activity. See the root README
for how any quantitative "finding" is labeled accordingly.
