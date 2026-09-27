"""Reproduce the bounded real-data analysis end to end and time every step.

    python scripts/run_real_analysis.py

Needs no network: it cuts a block window out of live segments already in `data/raw/`
(`eventstream ingest --mode live ...` downloads them), builds the warehouse without the global
dedup (the segments tile the window, so keys are unique by construction; the build asserts it),
validates and reconciles against the raw window, runs the analytics report and writes the
findings JSON. Per-step wall-clock times are printed and saved next to the warehouse.

The default window is the last two whole UTC days of the local extraction (2026-09-19 and
2026-09-20, 5.67M events). Pass --start-block/--end-block for a different window; blocks must be
covered by intact segments or the extractor refuses.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
DEFAULT_START, DEFAULT_END = 51_493_327, 51_579_726


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--start-block", type=int, default=DEFAULT_START)
    ap.add_argument("--end-block", type=int, default=DEFAULT_END)
    ap.add_argument("--manifest", default="", help="Manifest to cut from, if raw_dir has several.")
    ap.add_argument("--warehouse", default="data/processed/base_usdc_real.duckdb")
    ap.add_argument("--findings-out", default="docs/real_data_findings.json")
    args = ap.parse_args()

    stem = f"window_{args.start_block}_{args.end_block}"
    window = f"data/processed/{stem}.parquet"
    meta = f"data/processed/{stem}.window.json"
    report = REPO / "data" / "processed" / f"{stem}.analyze.txt"
    env = {
        **os.environ,
        "EVENTSTREAM_WAREHOUSE_PATH": args.warehouse,
        "PYTHONIOENCODING": "utf-8",
    }
    cli = [sys.executable, "-m", "eventstream.cli"]
    steps: list[tuple[str, list[str]]] = [
        (
            "extract-window",
            [*cli, "extract-window", "--start-block", str(args.start_block)]
            + ["--end-block", str(args.end_block), "--out", window]
            + (["--manifest", args.manifest] if args.manifest else []),
        ),
        ("transform", [*cli, "transform", "--source-glob", window, "--no-global-dedup"]),
        ("validate + reconcile", [*cli, "validate", "--reconcile-glob", window]),
        ("analyze", [*cli, "analyze"]),
        (
            "findings",
            [sys.executable, "scripts/build_findings.py", "--warehouse", args.warehouse]
            + ["--window-meta", meta, "--out", args.findings_out],
        ),
    ]

    # start from a clean warehouse so the timing is a from-scratch build
    for suffix in ("", ".wal"):
        (REPO / (args.warehouse + suffix)).unlink(missing_ok=True)

    timings: dict[str, float] = {}
    outputs: dict[str, str] = {}
    t_all = time.perf_counter()
    for name, cmd in steps:
        t0 = time.perf_counter()
        proc = subprocess.run(
            cmd,
            cwd=REPO,
            env=env,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        timings[name] = time.perf_counter() - t0
        outputs[name] = proc.stdout + proc.stderr
        print(
            f"[{'ok' if proc.returncode == 0 else 'FAIL'}] {name}: {timings[name]:.1f}s", flush=True
        )
        if proc.returncode != 0:
            print(outputs[name][-3000:])
            sys.exit(proc.returncode)
    total = time.perf_counter() - t_all

    report.write_text(outputs["analyze"], encoding="utf-8")
    print(f"\nTOTAL wall-clock (fresh process per step, incl. interpreter start-up): {total:.1f}s")
    print(outputs["transform"])
    (REPO / "data" / "processed" / f"{stem}.timing.json").write_text(
        json.dumps(
            {
                "steps_seconds": timings,
                "total_seconds": total,
                "window": [args.start_block, args.end_block],
            },
            indent=2,
        ),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
