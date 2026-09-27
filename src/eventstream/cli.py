"""CLI entrypoint: `eventstream <command>` (or `uv run eventstream <command>`)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import typer

from eventstream import config as cfg
from eventstream.analytics import concentration, lifecycle, overview, retention, segmentation
from eventstream.analytics.anomalies import anomalies_from_warehouse
from eventstream.experimentation import inference, power
from eventstream.experimentation.simulator import SCENARIOS, generate_scenario
from eventstream.ingestion.fixtures import write_fixture
from eventstream.ingestion.window import WindowError, extract_window
from eventstream.validation.checks import all_passed, run_checks
from eventstream.validation.reconcile import reconcile_against_raw
from eventstream.warehouse.build import DuplicateEventsError, build_warehouse
from eventstream.warehouse.connection import connect

app = typer.Typer(add_completion=False, help="Product analytics and experimentation CLI.")


@app.command()
def ingest(
    mode: str = typer.Option(
        "fixture", help="'fixture' (synthetic, offline) or 'live' (Base RPC)."
    ),
    start_block: int = typer.Option(0, help="Live mode only: first block (inclusive)."),
    end_block: int = typer.Option(0, help="Live mode only: last block (inclusive)."),
    workers: int = typer.Option(4, help="Live mode only: parallel segment workers."),
    segment_blocks: int = typer.Option(
        2000, help="Live mode only: blocks per checkpointed segment file."
    ),
) -> None:
    """Produce raw event Parquet in data/raw or data/fixtures."""
    settings = cfg.get_settings()
    if mode == "fixture":
        out_path = cfg.FIXTURES_DIR / "synthetic_transfers.parquet"
        df = write_fixture(settings, out_path)
        typer.echo(f"Wrote {len(df)} SYNTHETIC rows to {out_path}")
    elif mode == "live":
        from eventstream.ingestion.live import IngestionIncompleteError, ingest_block_range

        if end_block <= start_block:
            typer.echo("--end-block must be greater than --start-block for live mode", err=True)
            raise typer.Exit(code=1)
        try:
            result = ingest_block_range(
                settings,
                start_block,
                end_block,
                cfg.RAW_DIR,
                workers=workers,
                segment_blocks=segment_blocks,
                progress=typer.echo,
            )
        except IngestionIncompleteError as exc:
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=1) from exc
        typer.echo(f"Wrote {result.row_count} real rows; manifest: {result.manifest_path}")
    else:
        typer.echo(f"unknown mode: {mode}", err=True)
        raise typer.Exit(code=1)


@app.command("extract-window")
def extract_window_cmd(
    start_block: int = typer.Option(..., help="First block of the window (inclusive)."),
    end_block: int = typer.Option(..., help="Last block of the window (inclusive)."),
    raw_dir: str = typer.Option(str(cfg.RAW_DIR), help="Directory holding the live segments."),
    manifest: str = typer.Option("", help="Manifest to cut from (default: the only one)."),
    out: str = typer.Option("", help="Output Parquet (default: data/processed/window_*)."),
) -> None:
    """Cut a contiguous block window out of already-downloaded segments (no network)."""
    out_path = Path(out) if out else cfg.PROCESSED_DIR / f"window_{start_block}_{end_block}.parquet"
    try:
        report = extract_window(
            Path(raw_dir), start_block, end_block, out_path, Path(manifest) if manifest else None
        )
    except WindowError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(
        f"Window blocks {start_block}..{end_block}: {report.row_count:,} events from "
        f"{report.segments_used} segments in {report.elapsed_seconds:.1f}s"
    )
    typer.echo(f"  first event {report.first_event_timestamp.isoformat()}")
    typer.echo(f"  last event  {report.last_event_timestamp.isoformat()}")
    status = "unique" if report.keys_unique else "DUPLICATES PRESENT"
    typer.echo(
        f"  distinct (transaction_hash, log_index) keys: {report.distinct_keys:,} ({status})"
    )
    typer.echo(f"  wrote {report.parquet_path} and {report.metadata_path.name}")
    if not report.keys_unique:
        raise typer.Exit(code=1)


@app.command()
def transform(
    source_glob: str = typer.Option("", help="Override the raw Parquet glob to load."),
    global_dedup: bool = typer.Option(
        True,
        "--global-dedup/--no-global-dedup",
        help="Skip the global dedup only for a window from `extract-window` (segments proven "
        "non-overlapping); key uniqueness is then asserted, and a violation fails the build.",
    ),
) -> None:
    """Build the DuckDB warehouse from raw/fixture Parquet + run all SQL models."""
    settings = cfg.get_settings()
    glob = source_glob or str(cfg.FIXTURES_DIR / "*.parquet")
    con = connect(settings.warehouse_path)
    try:
        report = build_warehouse(con, glob, cfg.SQL_DIR, global_dedup=global_dedup)
    except DuplicateEventsError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Built warehouse in {report.elapsed_seconds:.2f}s:")
    for table, count in report.tables.items():
        took = report.model_seconds.get(table)
        typer.echo(f"  {table}: {count} rows" + (f"  ({took:.1f}s)" if took is not None else ""))
    check = report.model_seconds.get("stg_transfers_uniqueness_check")
    if check is not None:
        typer.echo(f"  (stg_transfers key-uniqueness assertion: {check:.1f}s)")


@app.command()
def validate(
    reconcile_glob: str = typer.Option(
        "",
        help="Also recompute every mart from these raw Parquet files, independently of the "
        "SQL models, and compare (slow on large datasets).",
    ),
) -> None:
    """Run data-quality checks against the built warehouse."""
    settings = cfg.get_settings()
    con = connect(settings.warehouse_path)
    results = run_checks(con, settings.usdc_contract)
    if reconcile_glob:
        results += reconcile_against_raw(con, reconcile_glob)
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        typer.echo(f"[{status}] {r.name}: {r.detail}")
    if not all_passed(results):
        raise typer.Exit(code=1)


@app.command()
def analyze() -> None:
    """Print activity, retention, segmentation, concentration, and anomaly summaries."""
    settings = cfg.get_settings()
    con = connect(settings.warehouse_path)

    typer.echo("== Dataset ==")
    for key, value in overview.dataset_overview(con).items():
        typer.echo(f"  {key}: {value}")

    typer.echo("\n== Weekly activity (UTC weeks, self-transfers excluded) ==")
    typer.echo(
        con.execute("SELECT * FROM mart_weekly_metrics ORDER BY activity_week")
        .fetchdf()
        .to_string(index=False)
    )
    daily = con.execute("SELECT * FROM mart_daily_metrics ORDER BY activity_date").fetchdf()
    typer.echo(
        f"\nDaily active wallets: min {int(daily['active_wallets'].min()):,}, "
        f"median {int(daily['active_wallets'].median()):,}, "
        f"max {int(daily['active_wallets'].max()):,} over {len(daily)} days"
    )

    typer.echo("\n== Transfer value distribution (qualifying events) ==")
    typer.echo(segmentation.value_distribution(con).to_string(index=False))

    typer.echo("\n== Cohort sizes (first-seen week) ==")
    typer.echo(retention.cohort_sizes(con).to_string(index=False))
    typer.echo("\n== Observed-return matrix (mature cells only; blank = censored) ==")
    typer.echo(retention.retention_matrix(con).to_string(float_format=lambda x: f"{x:.4f}"))

    typer.echo("\n== Retention summary, all mature cohorts pooled ==")
    typer.echo(retention.summary_retention(con).to_string(index=False))

    for offset in (2, 4):
        for exclude in (False, True):
            label = "excluding opening cohort" if exclude else "all mature cohorts"
            typer.echo(f"\n== Activation proxy vs. W{offset} observed return ({label}) ==")
            comparison = lifecycle.activation_vs_retention(
                con, week_offset=offset, exclude_opening_cohort=exclude
            )
            typer.echo(json.dumps(vars(comparison), indent=2))

    typer.echo("\n== Behavioral funnel ==")
    typer.echo(lifecycle.funnel_conversion(con).to_string(index=False))

    typer.echo("\n== Wallet segments ==")
    typer.echo(segmentation.segment_distribution(con).to_string(index=False))
    typer.echo("\n== Segment cutoffs (data-derived) ==")
    typer.echo(segmentation.segment_cutoffs(con).to_string(index=False))
    typer.echo("\n== Engagement ==")
    typer.echo(json.dumps(segmentation.engagement_summary(con), indent=2, default=float))

    typer.echo("\n== Concentration ==")
    typer.echo(concentration.concentration_summary(con).to_string(index=False))
    typer.echo("\n== Top-10% wallet share of weekly volume ==")
    typer.echo(concentration.concentration_by_week(con).to_string(index=False))

    typer.echo("\n== Anomalies flagged ==")
    anomalies = anomalies_from_warehouse(con)
    typer.echo(
        f"evaluable metric-days: {int(anomalies['rolling_mad'].notna().sum())} "
        f"({anomalies['metric_name'].nunique()} metrics); flagged: {int(anomalies['is_anomaly'].sum())}"
    )
    flagged = anomalies[anomalies["is_anomaly"]]
    typer.echo(flagged.to_string(index=False) if not flagged.empty else "none flagged")


@app.command()
def experiment(
    scenario: str = typer.Option("positive_effect", help=f"One of {sorted(SCENARIOS)}"),
) -> None:
    """Run the SYNTHETIC experiment simulator + statistical inference. Never
    run against real on-chain data — see docs/EXPERIMENTATION.md."""
    df = generate_scenario(scenario)
    control = df[df["group"] == "control"]
    treatment = df[df["group"] == "treatment"]

    typer.echo(f"Scenario: {scenario} (n={len(df)})")

    srm = inference.chi_square_srm(len(control), len(treatment))
    typer.echo(f"\nSRM check: {json.dumps(srm, indent=2)}")

    z_result = inference.two_proportion_ztest(
        int(control["converted"].sum()),
        len(control),
        int(treatment["converted"].sum()),
        len(treatment),
    )
    typer.echo(f"\nBinary metric (two-proportion z-test): {json.dumps(z_result, indent=2)}")

    t_result = inference.welch_ttest(
        control["continuous_metric"].to_numpy(), treatment["continuous_metric"].to_numpy()
    )
    typer.echo(f"\nContinuous metric (Welch's t-test): {json.dumps(t_result, indent=2)}")

    boot = inference.bootstrap_ci(
        control["continuous_metric"].to_numpy(), treatment["continuous_metric"].to_numpy()
    )
    typer.echo(f"\nBootstrap CI (continuous metric): {json.dumps(boot, indent=2)}")

    n_required = power.sample_size_two_proportions(
        baseline_rate=float(control["converted"].mean()) or 0.1, mde_absolute=0.02
    )
    typer.echo(f"\nSample size needed per group for a 2pp MDE at 80% power: {n_required}")


@app.command()
def dashboard() -> None:
    """Launch the Streamlit dashboard."""
    import subprocess

    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", str(cfg.PROJECT_ROOT / "dashboard" / "app.py")],
        check=True,
    )


@app.command()
def demo() -> None:
    """Fixture -> warehouse -> validate -> analyze -> experiment, end to end."""
    typer.echo("1/5 Generating synthetic fixture data...")
    ingest(mode="fixture")

    typer.echo("\n2/5 Building DuckDB warehouse + SQL models...")
    transform(source_glob="")

    typer.echo("\n3/5 Validating data quality...")
    try:
        validate(reconcile_glob="")
    except typer.Exit as exc:
        if exc.exit_code != 0:
            raise

    typer.echo("\n4/5 Computing product analytics...")
    analyze()

    typer.echo("\n5/5 Running synthetic experimentation module...")
    experiment(scenario="positive_effect")


def main() -> None:
    """Console-script entry point.

    Click expands shell-style wildcards in arguments on Windows (mimicking a shell that
    does not glob). `--source-glob "data/raw/*.parquet"` would then reach `transform` as
    one path per file, which fails with "unexpected extra arguments". The glob must arrive
    intact so DuckDB can expand it itself.
    """
    app(windows_expand_args=False)


if __name__ == "__main__":
    main()
