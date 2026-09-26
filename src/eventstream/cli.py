"""CLI entrypoint: `eventstream <command>` (or `uv run eventstream <command>`)."""

from __future__ import annotations

import json
import sys

import typer

from eventstream import config as cfg
from eventstream.analytics import concentration, lifecycle, retention, segmentation
from eventstream.analytics.anomalies import anomalies_from_warehouse
from eventstream.experimentation import inference, power
from eventstream.experimentation.simulator import SCENARIOS, generate_scenario
from eventstream.ingestion.fixtures import write_fixture
from eventstream.validation.checks import all_passed, run_checks
from eventstream.warehouse.build import build_warehouse
from eventstream.warehouse.connection import connect

app = typer.Typer(add_completion=False, help="Product analytics and experimentation CLI.")


@app.command()
def ingest(
    mode: str = typer.Option(
        "fixture", help="'fixture' (synthetic, offline) or 'live' (Base RPC)."
    ),
    start_block: int = typer.Option(0, help="Live mode only: first block (inclusive)."),
    end_block: int = typer.Option(0, help="Live mode only: last block (inclusive)."),
) -> None:
    """Produce raw event Parquet in data/raw or data/fixtures."""
    settings = cfg.get_settings()
    if mode == "fixture":
        out_path = cfg.FIXTURES_DIR / "synthetic_transfers.parquet"
        df = write_fixture(settings, out_path)
        typer.echo(f"Wrote {len(df)} SYNTHETIC rows to {out_path}")
    elif mode == "live":
        from eventstream.ingestion.live import ingest_block_range

        if end_block <= start_block:
            typer.echo("--end-block must be greater than --start-block for live mode", err=True)
            raise typer.Exit(code=1)
        result = ingest_block_range(settings, start_block, end_block, cfg.RAW_DIR)
        typer.echo(f"Wrote {result.row_count} real rows to {result.parquet_path}")
    else:
        typer.echo(f"unknown mode: {mode}", err=True)
        raise typer.Exit(code=1)


@app.command()
def transform(
    source_glob: str = typer.Option("", help="Override the raw Parquet glob to load."),
) -> None:
    """Build the DuckDB warehouse from raw/fixture Parquet + run all SQL models."""
    settings = cfg.get_settings()
    glob = source_glob or str(cfg.FIXTURES_DIR / "*.parquet")
    con = connect(settings.warehouse_path)
    report = build_warehouse(con, glob, cfg.SQL_DIR)
    typer.echo(f"Built warehouse in {report.elapsed_seconds:.2f}s:")
    for table, count in report.tables.items():
        typer.echo(f"  {table}: {count} rows")


@app.command()
def validate() -> None:
    """Run data-quality checks against the built warehouse."""
    settings = cfg.get_settings()
    con = connect(settings.warehouse_path)
    results = run_checks(con, settings.usdc_contract)
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

    typer.echo("== Retention (mature cohorts only) ==")
    typer.echo(retention.summary_retention(con).to_string(index=False))

    typer.echo("\n== Activation proxy vs. W4 observed return ==")
    comparison = lifecycle.activation_vs_retention(con, week_offset=4)
    typer.echo(json.dumps(vars(comparison), indent=2))

    typer.echo("\n== Behavioral funnel ==")
    typer.echo(lifecycle.funnel_conversion(con).to_string(index=False))

    typer.echo("\n== Wallet segments ==")
    typer.echo(segmentation.segment_distribution(con).to_string(index=False))

    typer.echo("\n== Concentration ==")
    typer.echo(concentration.concentration_summary(con).to_string(index=False))

    typer.echo("\n== Anomalies flagged ==")
    anomalies = anomalies_from_warehouse(con)
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
        validate()
    except typer.Exit as exc:
        if exc.exit_code != 0:
            raise

    typer.echo("\n4/5 Computing product analytics...")
    analyze()

    typer.echo("\n5/5 Running synthetic experimentation module...")
    experiment(scenario="positive_effect")


if __name__ == "__main__":
    app()
