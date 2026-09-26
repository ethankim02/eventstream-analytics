"""Live ingestion pipeline: Base RPC -> decoded -> normalized -> checkpointed Parquet.

This module performs read-only historical extraction of ERC-20 Transfer
events for the configured token contract. It never signs or broadcasts
anything and requires no private key.

The requested block range is split into fixed-size *segments*. Each segment is
fetched (with adaptive sub-chunking, retry and backoff inside `RpcClient`),
written atomically to its own Parquet file, and then recorded in a run
manifest. A re-run with the same arguments skips every segment whose file and
manifest entry are intact, so an interrupted multi-hour pull resumes instead of
restarting. Segments never overlap, so no event can be ingested twice; a
segment that cannot be fetched is reported and fails the run — it is never
silently skipped.

Requires outbound network access to the configured RPC endpoint, which is
NOT available in every sandboxed environment (see docs/DATA_SOURCE.md).
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from eventstream.config import Settings
from eventstream.ingestion.base_rpc import AdaptiveChunker, RpcClient, RpcError
from eventstream.ingestion.normalize import CANONICAL_COLUMNS, transfers_to_dataframe
from eventstream.ingestion.usdc import decode_transfer_logs_columnar

DEFAULT_SEGMENT_BLOCKS = 2_000
MANIFEST_SCHEMA_VERSION = 2
SOURCE_LABEL = "base_rpc_live"

# Explicit schema so every segment file has identical column types, whatever
# rows it happens to contain (DuckDB's read_parquet(glob) needs that).
PARQUET_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("block_number", pa.int64()),
        ("block_timestamp", pa.timestamp("us", tz="UTC")),
        ("transaction_hash", pa.string()),
        ("log_index", pa.int64()),
        ("token_address", pa.string()),
        ("from_address", pa.string()),
        ("to_address", pa.string()),
        ("raw_amount", pa.int64()),
        ("amount", pa.float64()),
        ("is_self_transfer", pa.bool_()),
        ("chain_id", pa.int64()),
        ("network", pa.string()),
        ("source", pa.string()),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
    ]
)
assert [f.name for f in PARQUET_SCHEMA] == CANONICAL_COLUMNS


class LogClient(Protocol):
    """The slice of `RpcClient` this module uses (lets tests inject a fake)."""

    def get_chain_id(self) -> int: ...

    def get_block_timestamps(self, block_numbers: list[int]) -> dict[int, int]: ...

    def fetch_logs_chunked(
        self,
        address: str,
        topics: list[str | None],
        start_block: int,
        end_block: int,
        chunk_size: int,
        chunker: AdaptiveChunker | None = None,
    ) -> Any: ...


class IngestionIncompleteError(RuntimeError):
    """Raised when one or more segments could not be fetched after all retries."""

    def __init__(self, failed: list[tuple[int, int, str]]):
        self.failed = failed
        lines = ", ".join(f"[{s}-{e}]: {msg}" for s, e, msg in failed[:5])
        more = f" (+{len(failed) - 5} more)" if len(failed) > 5 else ""
        super().__init__(
            f"{len(failed)} segment(s) failed and are NOT in the dataset: {lines}{more}. "
            "Completed segments are checkpointed; re-run the same command to retry only these."
        )


@dataclass
class IngestionResult:
    manifest_path: Path
    row_count: int
    start_block: int
    end_block: int
    segments_total: int
    segments_fetched: int
    segments_reused: int
    complete: bool


def plan_segments(start_block: int, end_block: int, segment_blocks: int) -> list[tuple[int, int]]:
    """Split [start_block, end_block] into contiguous, non-overlapping segments."""
    if segment_blocks < 1:
        raise ValueError("segment_blocks must be >= 1")
    segments = []
    lo = start_block
    while lo <= end_block:
        hi = min(lo + segment_blocks - 1, end_block)
        segments.append((lo, hi))
        lo = hi + 1
    return segments


def segment_filename(start_block: int, end_block: int) -> str:
    return f"usdc_transfers_{start_block}_{end_block}.parquet"


def manifest_filename(start_block: int, end_block: int) -> str:
    return f"usdc_transfers_{start_block}_{end_block}.manifest.json"


def _iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=UTC).isoformat()


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _parquet_rows(path: Path) -> int | None:
    try:
        return int(pq.ParquetFile(path).metadata.num_rows)
    except (OSError, pa.ArrowException):
        return None


def _segment_intact(out_dir: Path, entry: dict[str, Any]) -> bool:
    """A checkpointed segment counts only if its file exists with the recorded row count."""
    if entry["row_count"] == 0:
        return entry["file"] is None
    path = out_dir / str(entry["file"])
    return path.exists() and _parquet_rows(path) == entry["row_count"]


def _fetch_segment(
    client: LogClient,
    settings: Settings,
    seg_start: int,
    seg_end: int,
    chunker: AdaptiveChunker,
) -> pd.DataFrame:
    expected_token = settings.usdc_contract.lower()
    frames: list[pd.DataFrame] = []
    for logs in client.fetch_logs_chunked(
        address=settings.usdc_contract,
        topics=[settings.transfer_topic0],
        start_block=seg_start,
        end_block=seg_end,
        chunk_size=settings.block_chunk_size,
        chunker=chunker,
    ):
        if not logs:
            continue
        cols = decode_transfer_logs_columnar(logs)
        blocks = cols["block_number"]
        if min(blocks) < seg_start or max(blocks) > seg_end:
            raise ValueError(
                f"log for block {min(blocks)}..{max(blocks)} outside requested "
                f"[{seg_start}, {seg_end}]"
            )
        foreign = {t for t in cols["token_address"] if t.lower() != expected_token}
        if foreign:
            raise ValueError(f"log from unexpected contract {sorted(foreign)[0]}")
        timestamps = cols["block_timestamp"]
        missing = sorted({b for b, t in zip(blocks, timestamps, strict=True) if t is None})
        if missing:
            # Provider did not attach blockTimestamp; look the blocks up. A block that
            # cannot be resolved raises inside get_block_timestamps — never a silent drop.
            looked_up = client.get_block_timestamps(missing)
            timestamps = [
                t if t is not None else looked_up[b]
                for b, t in zip(blocks, timestamps, strict=True)
            ]
        frames.append(
            transfers_to_dataframe(
                cols,
                timestamps,
                chain_id=settings.chain_id,
                network=settings.network,
                source=SOURCE_LABEL,
                decimals=settings.usdc_decimals,
            )
        )
    if not frames:
        return pd.DataFrame(columns=CANONICAL_COLUMNS)
    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(subset=["transaction_hash", "log_index"], keep="first")
    return df.sort_values(["block_number", "log_index"]).reset_index(drop=True)


def _write_segment(out_dir: Path, seg_start: int, seg_end: int, df: pd.DataFrame) -> str | None:
    if df.empty:
        return None
    name = segment_filename(seg_start, seg_end)
    tmp = out_dir / (name + ".tmp")
    table = pa.Table.from_pandas(df, schema=PARQUET_SCHEMA, preserve_index=False)
    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, out_dir / name)
    return name


_TRANSIENT_ERRORS = (RpcError, requests.RequestException, OSError)


def _process_segment(
    client_factory: Callable[[], LogClient],
    settings: Settings,
    out_dir: Path,
    seg_start: int,
    seg_end: int,
    chunker: AdaptiveChunker,
    max_attempts: int,
) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(max_attempts):
        try:
            df = _fetch_segment(client_factory(), settings, seg_start, seg_end, chunker)
            file = _write_segment(out_dir, seg_start, seg_end, df)
            return {
                "start_block": seg_start,
                "end_block": seg_end,
                "row_count": len(df),
                "file": file,
                "fetched_at": datetime.now(UTC).isoformat(),
            }
        except _TRANSIENT_ERRORS as exc:
            last_error = exc
            if attempt < max_attempts - 1:
                time.sleep(min(30 * 2**attempt, 300))
        except Exception as exc:  # deterministic failure (bad log, schema): retrying can't help
            raise RuntimeError(f"{type(exc).__name__}: {exc}") from exc
    raise RuntimeError(f"gave up after {max_attempts} attempts: {last_error}")


def verify_manifest(out_dir: Path, manifest: dict[str, Any]) -> list[str]:
    """Return a list of integrity problems (empty means the dataset is whole).

    Checks that segments tile [start_block, end_block] exactly (no gaps, no
    overlaps, so no duplicate events) and that every segment file is present
    with the recorded row count.
    """
    problems: list[str] = []
    segments = sorted(manifest["segments"], key=lambda s: s["start_block"])
    expected = manifest["start_block"]
    for seg in segments:
        if seg["start_block"] != expected:
            kind = "gap" if seg["start_block"] > expected else "overlap"
            problems.append(f"{kind} before block {seg['start_block']} (expected {expected})")
        expected = seg["end_block"] + 1
        if not _segment_intact(out_dir, seg):
            problems.append(
                f"segment [{seg['start_block']}, {seg['end_block']}] missing or wrong row count"
            )
    if expected != manifest["end_block"] + 1:
        problems.append(f"segments end at {expected - 1}, expected {manifest['end_block']}")
    return problems


def ingest_block_range(
    settings: Settings,
    start_block: int,
    end_block: int,
    out_dir: Path,
    *,
    workers: int = 4,
    segment_blocks: int = DEFAULT_SEGMENT_BLOCKS,
    client_factory: Callable[[], LogClient] | None = None,
    progress: Callable[[str], None] | None = None,
    max_segment_attempts: int = 4,
) -> IngestionResult:
    """Fetch USDC Transfer logs for [start_block, end_block] into checkpointed Parquet.

    Segments are dispatched newest-first, so a partially complete run already
    holds the most recent contiguous history. Raises `IngestionIncompleteError`
    (after checkpointing everything that succeeded) if any segment fails.
    """
    say = progress or (lambda _msg: None)
    if end_block < start_block:
        raise ValueError("end_block must be >= start_block")

    def _default_client() -> LogClient:
        return RpcClient(
            rpc_url=settings.rpc_url,
            timeout_seconds=settings.request_timeout_seconds,
            max_retries=settings.max_retries,
        )

    factory: Callable[[], LogClient] = client_factory or _default_client
    probe = factory()
    chain_id = probe.get_chain_id()
    if chain_id != settings.chain_id:
        raise RuntimeError(
            f"RPC endpoint reports chain id {chain_id}, expected {settings.chain_id} "
            f"({settings.network}); refusing to ingest from the wrong network."
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    for stale in out_dir.glob("usdc_transfers_*.tmp"):  # partial writes from an interrupted run
        stale.unlink()
    manifest_path = out_dir / manifest_filename(start_block, end_block)
    planned = plan_segments(start_block, end_block, segment_blocks)

    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest["segment_blocks"] != segment_blocks or manifest["token_address"] != (
            settings.usdc_contract
        ):
            raise RuntimeError(
                f"{manifest_path.name} was created with segment_blocks="
                f"{manifest['segment_blocks']}; resume with the same value (or remove the "
                "manifest and its segment files) — mixing segmentations would duplicate events."
            )
    else:
        manifest = {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "source": SOURCE_LABEL,
            "network": settings.network,
            "chain_id": settings.chain_id,
            "token_address": settings.usdc_contract,
            "rpc_url": settings.rpc_url,
            "start_block": start_block,
            "end_block": end_block,
            "segment_blocks": segment_blocks,
            "extraction_started_at": datetime.now(UTC).isoformat(),
            "extraction_completed_at": None,
            "complete": False,
            "segments": [],
        }

    planned_set = set(planned)
    done = {(s["start_block"], s["end_block"]): s for s in manifest["segments"]}
    reused = {k: v for k, v in done.items() if k in planned_set and _segment_intact(out_dir, v)}
    pending = [seg for seg in reversed(planned) if seg not in reused]
    manifest["segments"] = list(reused.values())
    say(
        f"{len(planned)} segments planned: {len(reused)} already checkpointed, "
        f"{len(pending)} to fetch ({workers} workers, newest first)"
    )

    chunker = AdaptiveChunker(initial=250, maximum=settings.block_chunk_size)
    failed: list[tuple[int, int, str]] = []
    fetched = 0
    started = time.monotonic()
    rows_this_run = 0
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {
            pool.submit(
                _process_segment,
                factory,
                settings,
                out_dir,
                s,
                e,
                chunker,
                max_segment_attempts,
            ): (s, e)
            for s, e in pending
        }
        for fut in as_completed(futures):
            s, e = futures[fut]
            try:
                entry = fut.result()
            except Exception as exc:
                failed.append((s, e, str(exc)))
                say(f"FAILED segment [{s}, {e}]: {exc}")
                continue
            manifest["segments"].append(entry)
            _atomic_write_json(manifest_path, manifest)
            fetched += 1
            rows_this_run += entry["row_count"]
            elapsed = time.monotonic() - started
            remaining = len(pending) - fetched - len(failed)
            eta = elapsed / fetched * remaining if fetched else 0.0
            say(
                f"[{fetched + len(failed)}/{len(pending)}] blocks {s}-{e}: "
                f"{entry['row_count']:,} rows | run total {rows_this_run:,} | "
                f"chunk={chunker.size} blocks | ETA {eta / 60:.0f} min"
            )

    manifest["segments"].sort(key=lambda seg: seg["start_block"])
    if failed:
        _atomic_write_json(manifest_path, manifest)
        raise IngestionIncompleteError(sorted(failed))

    problems = verify_manifest(out_dir, manifest)
    if problems:
        _atomic_write_json(manifest_path, manifest)
        raise RuntimeError("dataset failed integrity verification: " + "; ".join(problems[:5]))

    edge = factory().get_block_timestamps([start_block, end_block])
    manifest["start_block_timestamp"] = _iso(edge[start_block])
    manifest["end_block_timestamp"] = _iso(edge[end_block])
    manifest["row_count"] = sum(seg["row_count"] for seg in manifest["segments"])
    manifest["extraction_completed_at"] = datetime.now(UTC).isoformat()
    manifest["complete"] = True
    _atomic_write_json(manifest_path, manifest)
    say(f"complete: {manifest['row_count']:,} rows across {len(manifest['segments'])} segments")

    return IngestionResult(
        manifest_path=manifest_path,
        row_count=manifest["row_count"],
        start_block=start_block,
        end_block=end_block,
        segments_total=len(planned),
        segments_fetched=fetched,
        segments_reused=len(reused),
        complete=True,
    )
