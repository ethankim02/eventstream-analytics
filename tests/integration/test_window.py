"""Block-window extraction from live segments, and the dedup-free warehouse build.

The committed real sample (200 contiguous Base blocks, ~25k events) is re-cut into 100-block
segments with a hand-written manifest, so these tests exercise real rows without any network:
the extractor's exact block cut, its refusal to cut across gaps / overlaps / truncated files,
and that skipping the global dedup changes nothing on unique data while failing loudly on
duplicates.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import duckdb
import pandas as pd
import pytest
from typer.testing import CliRunner

from eventstream.cli import app
from eventstream.ingestion.window import WindowError, extract_window
from eventstream.warehouse.build import (
    DuplicateEventsError,
    build_warehouse,
    load_raw_transfers,
    run_sql_models,
)
from eventstream.warehouse.connection import connect_memory

ROOT = Path(__file__).resolve().parents[2]
SQL_ROOT = ROOT / "sql"
SAMPLE = ROOT / "data" / "samples" / "base_usdc_real_sample.parquet"
META = json.loads(
    (ROOT / "data" / "samples" / "base_usdc_real_sample.metadata.json").read_text(encoding="utf-8")
)
LO, HI = META["start_block"], META["end_block"]  # 51428527 .. 51428726
SEGMENTS = [(LO, LO + 99), (LO + 100, HI)]


def _write_segment(raw_dir: Path, lo: int, hi: int) -> dict:
    name = f"usdc_transfers_{lo}_{hi}.parquet"
    con = duckdb.connect(":memory:")
    con.execute(
        f"COPY (SELECT * FROM read_parquet('{SAMPLE.as_posix()}') "
        f"WHERE block_number BETWEEN {lo} AND {hi}) "
        f"TO '{(raw_dir / name).as_posix()}' (FORMAT PARQUET)"
    )
    n = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{(raw_dir / name).as_posix()}')"
    ).fetchone()
    return {
        "start_block": lo,
        "end_block": hi,
        "row_count": n[0],
        "file": name,
        "fetched_at": "2026-09-27T00:00:00+00:00",
    }


def _write_manifest(raw_dir: Path, segments: list[dict]) -> Path:
    manifest = {
        "schema_version": 2,
        "source": "base_rpc_live",
        "network": "base-mainnet",
        "chain_id": 8453,
        "token_address": META["token_address"],
        "rpc_url": META["rpc_url"],
        "start_block": min(s["start_block"] for s in segments),
        "end_block": max(s["end_block"] for s in segments),
        "segment_blocks": 100,
        "complete": False,
        "segments": segments,
    }
    path = (
        raw_dir / f"usdc_transfers_{manifest['start_block']}_{manifest['end_block']}.manifest.json"
    )
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return path


@pytest.fixture
def raw_dir(tmp_path: Path) -> Path:
    d = tmp_path / "raw"
    d.mkdir()
    _write_manifest(d, [_write_segment(d, lo, hi) for lo, hi in SEGMENTS])
    return d


def _sample_rows(lo: int, hi: int) -> int:
    return int(
        duckdb.connect(":memory:")
        .execute(
            f"SELECT COUNT(*) FROM read_parquet('{SAMPLE.as_posix()}') "
            f"WHERE block_number BETWEEN {lo} AND {hi}"
        )
        .fetchone()[0]
    )


def test_window_is_the_exact_block_range_across_a_segment_boundary(raw_dir, tmp_path):
    lo, hi = LO + 40, LO + 160  # starts in segment 1, ends in segment 2
    out = tmp_path / "w.parquet"
    report = extract_window(raw_dir, lo, hi, out)

    df = pd.read_parquet(out)
    assert len(df) == report.row_count == _sample_rows(lo, hi)
    assert df["block_number"].min() >= lo and df["block_number"].max() <= hi
    assert report.segments_used == 2
    assert report.keys_unique
    # ordered by (block_number, log_index) so the same window is always byte-for-byte content-equal
    assert df[["block_number", "log_index"]].equals(
        df[["block_number", "log_index"]].sort_values(["block_number", "log_index"])
    )


def test_window_is_deterministic(raw_dir, tmp_path):
    a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
    extract_window(raw_dir, LO + 10, HI - 10, a)
    extract_window(raw_dir, LO + 10, HI - 10, b)
    pd.testing.assert_frame_equal(pd.read_parquet(a), pd.read_parquet(b))


def test_window_writes_provenance_sidecar(raw_dir, tmp_path):
    report = extract_window(raw_dir, LO, HI, tmp_path / "w.parquet")
    meta = json.loads(report.metadata_path.read_text(encoding="utf-8"))
    assert meta["row_count"] == report.row_count == META["row_count"]
    assert meta["keys_unique"] is True and meta["segments_tile_window"] is True
    assert (meta["start_block"], meta["end_block"]) == (LO, HI)
    assert [s["file"] for s in meta["segments"]] == [
        f"usdc_transfers_{lo}_{hi}.parquet" for lo, hi in SEGMENTS
    ]
    assert meta["token_address"] == META["token_address"]


def test_refuses_a_gap_between_segments(tmp_path):
    d = tmp_path / "raw"
    d.mkdir()
    segs = [
        _write_segment(d, LO, LO + 59),
        _write_segment(d, LO + 100, HI),  # blocks LO+60 .. LO+99 were never downloaded
    ]
    _write_manifest(d, segs)
    with pytest.raises(WindowError, match="gap"):
        extract_window(d, LO, HI, tmp_path / "w.parquet")


def test_refuses_overlapping_segments(tmp_path):
    d = tmp_path / "raw"
    d.mkdir()
    segs = [
        _write_segment(d, LO, LO + 99),
        _write_segment(d, LO + 50, HI),  # overlaps the first by 50 blocks: would duplicate events
    ]
    _write_manifest(d, segs)
    with pytest.raises(WindowError, match="overlap"):
        extract_window(d, LO, HI, tmp_path / "w.parquet")


def test_refuses_a_truncated_segment_file(raw_dir, tmp_path):
    manifest = next(raw_dir.glob("*.manifest.json"))
    data = json.loads(manifest.read_text(encoding="utf-8"))
    data["segments"][0]["row_count"] += 1  # manifest no longer matches the file
    manifest.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(WindowError, match="wrong row count"):
        extract_window(raw_dir, LO, HI, tmp_path / "w.parquet")


def test_refuses_a_window_outside_the_downloaded_range(raw_dir, tmp_path):
    with pytest.raises(WindowError, match="no downloaded segment covers"):
        extract_window(raw_dir, HI - 10, HI + 500, tmp_path / "w.parquet")
    with pytest.raises(WindowError, match="end_block must be"):
        extract_window(raw_dir, HI, LO, tmp_path / "w.parquet")


def test_requires_an_unambiguous_manifest(raw_dir, tmp_path):
    shutil.copy(next(raw_dir.glob("*.manifest.json")), raw_dir / "usdc_transfers_1_2.manifest.json")
    with pytest.raises(WindowError, match="exactly one manifest"):
        extract_window(raw_dir, LO, HI, tmp_path / "w.parquet")


def test_dedup_free_build_equals_deduped_build_on_a_unique_window(raw_dir, tmp_path):
    out = tmp_path / "w.parquet"
    extract_window(raw_dir, LO, HI, out)

    deduped, disjoint = connect_memory(), connect_memory()
    r1 = build_warehouse(deduped, out.as_posix(), SQL_ROOT)
    r2 = build_warehouse(disjoint, out.as_posix(), SQL_ROOT, global_dedup=False)

    assert r1.tables == r2.tables  # same tables, same row counts, under the same names
    # the assertion runs (and is timed) only on the dedup-free path
    assert "stg_transfers_uniqueness_check" in r2.model_seconds
    for table in ("stg_transfers", "mart_daily_metrics", "mart_wallet_segments"):
        a = deduped.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchdf()
        b = disjoint.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchdf()
        pd.testing.assert_frame_equal(a, b)


def test_dedup_free_build_fails_loudly_on_a_repeated_key(tmp_path):
    df = pd.read_parquet(SAMPLE)
    dup = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    path = tmp_path / "dup.parquet"
    dup.to_parquet(path, index=False)
    with pytest.raises(DuplicateEventsError, match="repeated"):
        build_warehouse(connect_memory(), path.as_posix(), SQL_ROOT, global_dedup=False)
    # ... whereas the default build collapses it, exactly as before
    con = connect_memory()
    build_warehouse(con, path.as_posix(), SQL_ROOT)
    assert con.execute("SELECT COUNT(*) FROM stg_transfers").fetchone()[0] == len(df)


def test_model_variant_reports_the_table_it_creates():
    con = connect_memory()
    load_raw_transfers(con, SAMPLE.as_posix())
    counts = run_sql_models(con, SQL_ROOT, ["staging/stg_transfers_disjoint.sql"])
    assert list(counts) == ["stg_transfers"]  # not "stg_transfers_disjoint"


def test_cli_extract_window_then_dedup_free_transform(raw_dir, tmp_path, monkeypatch):
    out = tmp_path / "w.parquet"
    monkeypatch.setenv("EVENTSTREAM_WAREHOUSE_PATH", str(tmp_path / "wh.duckdb"))
    runner = CliRunner()

    res = runner.invoke(
        app,
        ["extract-window", "--start-block", str(LO), "--end-block", str(HI)]
        + ["--raw-dir", str(raw_dir), "--out", str(out)],
    )
    assert res.exit_code == 0, res.output
    assert f"{META['row_count']:,} events" in res.output and "unique" in res.output

    res = runner.invoke(app, ["transform", "--source-glob", str(out), "--no-global-dedup"])
    assert res.exit_code == 0, res.output
    assert "key-uniqueness assertion" in res.output  # the dedup-free path ran
    assert f"stg_transfers: {META['row_count']} rows" in res.output

    res = runner.invoke(
        app,
        ["extract-window", "--start-block", str(LO), "--end-block", str(HI + 999)]
        + ["--raw-dir", str(raw_dir), "--out", str(tmp_path / "x.parquet")],
    )
    assert res.exit_code == 1 and "no downloaded segment covers" in res.output
