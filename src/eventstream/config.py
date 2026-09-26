"""Central configuration and verified on-chain constants.

Chain/contract constants below were verified against official sources during
Phase 0 research (see docs/DATA_SOURCE.md for citations and dates). They are
not guessed or copied from an unrelated project.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
FIXTURES_DIR = DATA_DIR / "fixtures"
SQL_DIR = PROJECT_ROOT / "sql"

# --- Network identifiers (verified: docs.base.org, "Connect to Base") -----
BASE_MAINNET_CHAIN_ID = 8453
BASE_SEPOLIA_CHAIN_ID = 84532
BASE_MAINNET_RPC_DEFAULT = "https://mainnet.base.org"
BASE_SEPOLIA_RPC_DEFAULT = "https://sepolia.base.org"

# --- USDC contract (verified: developers.circle.com/stablecoins/usdc-contract-addresses) ---
# Native, Circle-issued USDC on Base mainnet. Distinct from the deprecated
# bridged "USDbC" token, which is intentionally NOT used by this project.
USDC_CONTRACT_BASE_MAINNET = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
USDC_DECIMALS = 6

# --- ERC-20 Transfer event -------------------------------------------------
# event Transfer(address indexed from, address indexed to, uint256 value)
# topic0 = keccak256("Transfer(address,address,uint256)")
TRANSFER_EVENT_TOPIC0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

DEFAULT_NETWORK_NAME = "base-mainnet"


@dataclass(frozen=True)
class Settings:
    """Runtime settings, overridable via environment variables."""

    network: str = DEFAULT_NETWORK_NAME
    chain_id: int = BASE_MAINNET_CHAIN_ID
    rpc_url: str = field(
        default_factory=lambda: os.environ.get("BASE_RPC_URL", BASE_MAINNET_RPC_DEFAULT)
    )
    usdc_contract: str = USDC_CONTRACT_BASE_MAINNET
    usdc_decimals: int = USDC_DECIMALS
    transfer_topic0: str = TRANSFER_EVENT_TOPIC0
    block_chunk_size: int = field(
        default_factory=lambda: int(os.environ.get("EVENTSTREAM_BLOCK_CHUNK_SIZE", "2000"))
    )
    warehouse_path: Path = PROCESSED_DIR / "eventstream.duckdb"
    request_timeout_seconds: int = 20
    max_retries: int = 5


def get_settings() -> Settings:
    return Settings()
