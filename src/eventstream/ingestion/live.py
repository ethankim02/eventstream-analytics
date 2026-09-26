"""Live ingestion pipeline: Base RPC -> decoded -> normalized -> Parquet.

This module performs read-only historical extraction of ERC-20 Transfer
events for the configured token contract. It never signs or broadcasts
anything and requires no private key.

Requires outbound network access to the configured RPC endpoint, which is
NOT available in every sandboxed environment (see docs/DATA_SOURCE.md).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from eventstream.config import Settings
from eventstream.ingestion.base_rpc import RpcClient
from eventstream.ingestion.normalize import build_event_row, rows_to_dataframe
from eventstream.ingestion.usdc import decode_transfer_log


@dataclass
class IngestionResult:
    parquet_path: Path
    metadata_path: Path
    row_count: int
    start_block: int
    end_block: int


def ingest_block_range(
    settings: Settings,
    start_block: int,
    end_block: int,
    out_dir: Path,
) -> IngestionResult:
    """Fetch USDC Transfer logs for [start_block, end_block] and write Parquet.

    Chunking, retry/backoff, and adaptive range-shrinking are handled by
    RpcClient.fetch_logs_chunked (see docs/DATA_SOURCE.md for why the public
    Base RPC needs this defensive handling).
    """
    client = RpcClient(
        rpc_url=settings.rpc_url,
        timeout_seconds=settings.request_timeout_seconds,
        max_retries=settings.max_retries,
    )

    rows: list[dict] = []
    for chunk in client.fetch_logs_chunked(
        address=settings.usdc_contract,
        topics=[settings.transfer_topic0],
        start_block=start_block,
        end_block=end_block,
        chunk_size=settings.block_chunk_size,
    ):
        if not chunk:
            continue
        decoded = [decode_transfer_log(log) for log in chunk]
        block_numbers = [d["block_number"] for d in decoded]
        timestamps = client.get_block_timestamps(block_numbers)
        for d in decoded:
            ts = timestamps.get(d["block_number"])
            if ts is None:
                continue
            rows.append(
                build_event_row(
                    transfer=d,
                    block_timestamp=datetime.fromtimestamp(ts, tz=UTC),
                    chain_id=settings.chain_id,
                    network=settings.network,
                    source="base_rpc_live",
                    decimals=settings.usdc_decimals,
                )
            )

    df = rows_to_dataframe(rows)

    out_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = out_dir / f"usdc_transfers_{start_block}_{end_block}.parquet"
    df.to_parquet(parquet_path, index=False)

    metadata = {
        "network": settings.network,
        "chain_id": settings.chain_id,
        "token_address": settings.usdc_contract,
        "start_block": start_block,
        "end_block": end_block,
        "extraction_timestamp": datetime.now(UTC).isoformat(),
        "row_count": len(df),
        "source": "base_rpc_live",
        "rpc_url": settings.rpc_url,
        "schema_version": 1,
    }
    metadata_path = out_dir / f"usdc_transfers_{start_block}_{end_block}.metadata.json"
    metadata_path.write_text(json.dumps(metadata, indent=2))

    return IngestionResult(
        parquet_path=parquet_path,
        metadata_path=metadata_path,
        row_count=len(df),
        start_block=start_block,
        end_block=end_block,
    )
