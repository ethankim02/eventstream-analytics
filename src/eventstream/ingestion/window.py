"""Cut an exact block window out of segments already on disk.

A live pull (`eventstream.ingestion.live`) writes one Parquet file per fixed-size,
non-overlapping block segment and records each in a manifest. Analysing a bounded slice of a
larger pull should not mean downloading again, and it should not mean de-duplicating tens of
millions of rows either. This module selects the segments that cover `[start_block,
end_block]`, proves they tile that range exactly (no gap, no overlap, every file present with
its recorded row count), and writes just the window's rows to one Parquet file plus a
provenance sidecar.

Because the segments tile the window and ingestion already de-duplicates within a segment,
(transaction_hash, log_index) is unique by construction. The extractor still counts distinct
keys and records the result, and the warehouse build re-asserts it, so the claim is checked at
both ends rather than assumed.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from eventstream.ingestion.live import verify_manifest

MANIFEST_GLOB = "usdc_transfers_*.manifest.json"


class WindowError(ValueError):
    """The requested block window cannot be cut from the segments on disk."""


@dataclass(frozen=True)
class WindowReport:
    parquet_path: Path
    metadata_path: Path
    start_block: int
    end_block: int
    segments_used: int
    row_count: int
    distinct_keys: int
    first_event_timestamp: datetime
    last_event_timestamp: datetime
    elapsed_seconds: float

    @property
    def keys_unique(self) -> bool:
        return self.row_count == self.distinct_keys


def find_manifest(raw_dir: Path, manifest_path: Path | None = None) -> Path:
    """The manifest to cut from: the one given, or the only one in `raw_dir`."""
    if manifest_path is not None:
        return manifest_path
    found = sorted(raw_dir.glob(MANIFEST_GLOB))
    if len(found) != 1:
        raise WindowError(
            f"expected exactly one manifest in {raw_dir}, found {len(found)}; pass --manifest"
        )
    return found[0]


def plan_window(
    raw_dir: Path, manifest: dict[str, Any], start_block: int, end_block: int
) -> list[dict[str, Any]]:
    """Manifest segments covering [start_block, end_block], proven to tile it exactly."""
    if end_block < start_block:
        raise WindowError("end_block must be >= start_block")
    covering = sorted(
        (
            s
            for s in manifest["segments"]
            if s["end_block"] >= start_block and s["start_block"] <= end_block
        ),
        key=lambda s: s["start_block"],
    )
    if not covering or covering[0]["start_block"] > start_block:
        raise WindowError(f"no downloaded segment covers block {start_block}")
    if covering[-1]["end_block"] < end_block:
        raise WindowError(f"no downloaded segment covers block {end_block}")
    # Reuse the ingestion integrity check on just the covering segments: it reports gaps,
    # overlaps and missing or truncated files.
    problems = verify_manifest(
        raw_dir,
        {
            "start_block": covering[0]["start_block"],
            "end_block": covering[-1]["end_block"],
            "segments": covering,
        },
    )
    if problems:
        raise WindowError(
            f"segments do not tile [{start_block}, {end_block}]: " + "; ".join(problems[:5])
        )
    return covering


def _sql_list(paths: list[str]) -> str:
    return ", ".join("'" + p.replace("'", "''") + "'" for p in paths)


def extract_window(
    raw_dir: Path,
    start_block: int,
    end_block: int,
    out_path: Path,
    manifest_path: Path | None = None,
) -> WindowReport:
    """Write the events in [start_block, end_block] to `out_path` (plus a `.window.json`).

    Rows are ordered by (block_number, log_index), so the same window always produces the same
    content. Raises `WindowError` if the window is not fully covered by intact segments.
    """
    started = time.perf_counter()
    manifest_file = find_manifest(raw_dir, manifest_path)
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    segments = plan_window(raw_dir, manifest, start_block, end_block)
    files = [(raw_dir / s["file"]).as_posix() for s in segments if s["file"]]
    if not files:
        raise WindowError(f"window [{start_block}, {end_block}] contains no events")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_name(out_path.name + ".tmp")
    con = duckdb.connect(":memory:")
    con.execute("SET TimeZone='UTC'")
    con.execute(
        f"COPY (SELECT * FROM read_parquet([{_sql_list(files)}]) "
        f"WHERE block_number BETWEEN {int(start_block)} AND {int(end_block)} "
        "ORDER BY block_number, log_index) "
        f"TO '{tmp.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    os.replace(tmp, out_path)

    src = f"read_parquet('{out_path.as_posix()}')"
    row = con.execute(
        f"SELECT COUNT(*), MIN(block_timestamp), MAX(block_timestamp) FROM {src}"
    ).fetchone()
    assert row is not None
    n_rows, first_ts, last_ts = row
    drow = con.execute(
        f"SELECT COUNT(*) FROM (SELECT DISTINCT transaction_hash, log_index FROM {src})"
    ).fetchone()
    assert drow is not None
    n_distinct = int(drow[0])
    if n_rows == 0:
        raise WindowError(f"window [{start_block}, {end_block}] contains no events")

    metadata_path = out_path.with_name(out_path.stem + ".window.json")
    report = WindowReport(
        parquet_path=out_path,
        metadata_path=metadata_path,
        start_block=start_block,
        end_block=end_block,
        segments_used=len(segments),
        row_count=int(n_rows),
        distinct_keys=n_distinct,
        first_event_timestamp=first_ts,
        last_event_timestamp=last_ts,
        elapsed_seconds=time.perf_counter() - started,
    )
    metadata = {
        "description": (
            "Contiguous block window of REAL Base mainnet native-USDC Transfer events, cut from "
            "already-downloaded live segments by `eventstream extract-window`."
        ),
        "source": manifest["source"],
        "network": manifest["network"],
        "chain_id": manifest["chain_id"],
        "token_address": manifest["token_address"],
        "rpc_url": manifest["rpc_url"],
        "start_block": start_block,
        "end_block": end_block,
        "first_event_timestamp": first_ts.isoformat(),
        "last_event_timestamp": last_ts.isoformat(),
        "row_count": report.row_count,
        "distinct_transaction_hash_log_index": n_distinct,
        "keys_unique": report.keys_unique,
        "segments_tile_window": True,
        "source_manifest": manifest_file.name,
        "source_manifest_complete": bool(manifest.get("complete")),
        "segments": [
            {
                "start_block": s["start_block"],
                "end_block": s["end_block"],
                "row_count": s["row_count"],
                "file": s["file"],
            }
            for s in segments
        ],
        "extracted_at": datetime.now(UTC).isoformat(),
    }
    tmp_meta = metadata_path.with_name(metadata_path.name + ".tmp")
    tmp_meta.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    os.replace(tmp_meta, metadata_path)
    return report
