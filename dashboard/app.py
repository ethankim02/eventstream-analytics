"""Streamlit dashboard for EventStream Analytics.

Run with `eventstream dashboard` (or `uv run streamlit run dashboard/app.py`).
Reads from the DuckDB warehouse built by `eventstream transform`; the
Experiments tab uses ONLY the synthetic experiment simulator (see
docs/EXPERIMENTATION.md) — never the on-chain observational data.
"""

from __future__ import annotations

import sys
from pathlib import Path

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eventstream import config as cfg  # noqa: E402
from eventstream.analytics import concentration, lifecycle, retention, segmentation  # noqa: E402
from eventstream.analytics.anomalies import anomalies_from_warehouse  # noqa: E402
from eventstream.experimentation import inference, power  # noqa: E402
from eventstream.experimentation.simulator import SCENARIOS, generate_scenario  # noqa: E402
from eventstream.warehouse.connection import connect  # noqa: E402

st.set_page_config(page_title="EventStream Analytics", layout="wide")


@st.cache_resource
def get_connection():
    settings = cfg.get_settings()
    return connect(settings.warehouse_path)


def _warehouse_missing() -> bool:
    return not cfg.get_settings().warehouse_path.exists()


st.title("EventStream Analytics")
st.caption("Product analytics and experimentation for high-volume event streams.")

if _warehouse_missing():
    st.error("No warehouse found. Run `uv run eventstream demo` (or `ingest` + `transform`) first.")
    st.stop()

con = get_connection()

tabs = st.tabs(["Overview", "Retention", "Behavior", "Concentration", "Experiments", "Anomalies"])

# --- Overview -----------------------------------------------------------
with tabs[0]:
    daily = con.execute("SELECT * FROM mart_daily_metrics ORDER BY activity_date").fetchdf()
    src_counts = con.execute(
        "SELECT source, COUNT(*) AS n FROM stg_transfers GROUP BY source"
    ).fetchdf()

    is_synthetic = "synthetic_fixture" in set(src_counts["source"])
    if is_synthetic:
        st.warning(
            "This warehouse was built from the SYNTHETIC fixture generator, not live Base data. "
            "Numbers below demonstrate the pipeline only — see docs/DATA_SOURCE.md for how to run "
            "real ingestion locally.",
            icon="⚠️",
        )

    total_events = int(daily["transfer_count"].sum())
    active_wallets = con.execute(
        "SELECT COUNT(DISTINCT wallet_address) FROM int_wallet_daily_activity"
    ).fetchone()[0]
    total_volume = float(daily["total_volume"].sum())
    median_amount = segmentation.value_distribution(con)["median_amount"].iloc[0]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Transfer events", f"{total_events:,}")
    c2.metric("Active wallets (window)", f"{active_wallets:,}")
    c3.metric("USDC volume", f"{total_volume:,.0f}")
    c4.metric("Median transfer", f"{median_amount:,.2f}")

    st.subheader("Daily activity")
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(x=daily["activity_date"], y=daily["transfer_count"], name="Transfers/day")
    )
    fig.add_trace(
        go.Scatter(
            x=daily["activity_date"],
            y=daily["active_wallets"],
            name="Active wallets/day",
            yaxis="y2",
        )
    )
    fig.update_layout(
        yaxis=dict(title="Transfers"),
        yaxis2=dict(title="Active wallets", overlaying="y", side="right"),
        legend=dict(orientation="h"),
    )
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Daily USDC volume")
    st.plotly_chart(px.bar(daily, x="activity_date", y="total_volume"), use_container_width=True)

# --- Retention ------------------------------------------------------------
with tabs[1]:
    st.subheader("First-seen cohort retention (mature cells only)")
    st.caption(
        "'First-seen' = first observed event within this dataset's window, not necessarily the "
        "wallet's true first-ever on-chain activity. Blank cells are right-censored (not enough "
        "time has elapsed yet) — see docs/METRICS.md."
    )
    matrix = retention.retention_matrix(con)
    st.dataframe(matrix.style.format("{:.1%}", na_rep="—"), use_container_width=True)

    summary = retention.summary_retention(con)
    st.subheader("Mature-cohort summary")
    st.dataframe(summary, use_container_width=True)

    st.subheader("Activation proxy vs. observed W4 return")
    week_offset = st.slider("Week offset", min_value=1, max_value=8, value=4)
    comparison = lifecycle.activation_vs_retention(con, week_offset=week_offset)
    cc1, cc2, cc3 = st.columns(3)
    cc1.metric("Activated wallets", comparison.activated_wallets)
    cc2.metric(
        "Activated return rate",
        f"{comparison.activated_return_rate:.1%}"
        if comparison.activated_return_rate is not None
        else "—",
    )
    cc3.metric(
        "Non-activated return rate",
        f"{comparison.non_activated_return_rate:.1%}"
        if comparison.non_activated_return_rate is not None
        else "—",
    )
    st.caption(
        "Associational only: wallets satisfying the activation proxy show higher observed return "
        "rates in this dataset. This is NOT a causal claim — see docs/EXPERIMENTATION.md."
    )

    st.subheader("Behavioral (lifecycle) funnel")
    st.dataframe(lifecycle.funnel_conversion(con), use_container_width=True)

# --- Behavior ---------------------------------------------------------
with tabs[2]:
    st.subheader("Wallet segments")
    st.caption(
        "Thresholds are data-derived quantile cutoffs — see docs/METRICS.md for the exact rule."
    )
    seg = segmentation.segment_distribution(con)
    col1, col2 = st.columns(2)
    col1.plotly_chart(
        px.pie(seg, names="frequency_segment", values="wallets", title="Wallets by segment"),
        use_container_width=True,
    )
    col2.plotly_chart(
        px.bar(seg, x="frequency_segment", y="total_amount", title="Volume by segment"),
        use_container_width=True,
    )

    st.subheader("Engagement")
    engagement = segmentation.engagement_summary(con)
    e1, e2, e3, e4 = st.columns(4)
    e1.metric("Avg events / wallet", f"{engagement['avg_events_per_wallet']:.2f}")
    e2.metric("Median active days", f"{engagement['median_active_days']:.1f}")
    e3.metric("Repeat wallet rate", f"{engagement['repeat_wallet_rate']:.1%}")
    e4.metric("Avg active days", f"{engagement['avg_active_days']:.2f}")

    st.subheader("Transfer value distribution")
    st.caption("Event-level, excludes self-transfers. Heavy right tail — log scale below.")
    values = con.execute("SELECT amount FROM stg_transfers WHERE NOT is_self_transfer").fetchdf()
    st.plotly_chart(
        px.histogram(values, x="amount", log_y=True, nbins=80), use_container_width=True
    )

# --- Concentration ------------------------------------------------------
with tabs[3]:
    summary = concentration.concentration_summary(con).iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Gini coefficient", f"{summary['gini_coefficient']:.3f}")
    c2.metric("Top 1% volume share", f"{summary['top1pct_volume_share']:.1%}")
    c3.metric("Top 10% volume share", f"{summary['top10pct_volume_share']:.1%}")
    c4.metric("Top 10% tx-count share", f"{summary['top10pct_transaction_share']:.1%}")

    st.subheader("Lorenz curve")
    lorenz = concentration.lorenz_curve(con)
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=lorenz["cumulative_wallet_share"],
            y=lorenz["cumulative_volume_share"],
            name="Observed",
        )
    )
    fig.add_trace(go.Scatter(x=[0, 1], y=[0, 1], name="Perfect equality", line=dict(dash="dash")))
    fig.update_layout(
        xaxis_title="Cumulative share of wallets", yaxis_title="Cumulative share of volume"
    )
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Concentration over time")
    by_week = concentration.concentration_by_week(con)
    st.plotly_chart(
        px.line(by_week, x="activity_week", y="top10pct_volume_share"), use_container_width=True
    )

# --- Experiments (SYNTHETIC ONLY) ---------------------------------------
with tabs[4]:
    st.info(
        "This tab uses ONLY the synthetic experiment simulator. Historical on-chain activity is "
        "observational data and is never treated as a randomized experiment here.",
        icon="🧪",
    )
    scenario_name = st.selectbox("Scenario", sorted(SCENARIOS))
    df = generate_scenario(scenario_name)
    control = df[df["group"] == "control"]
    treatment = df[df["group"] == "treatment"]

    st.subheader("Sample sizes / SRM")
    srm = inference.chi_square_srm(len(control), len(treatment))
    s1, s2, s3 = st.columns(3)
    s1.metric("Control n", srm["n_control"])
    s2.metric("Treatment n", srm["n_treatment"])
    s3.metric("SRM detected", "YES" if srm["srm_detected"] else "no")
    st.json(srm)

    st.subheader("Binary metric — two-proportion z-test")
    z = inference.two_proportion_ztest(
        int(control["converted"].sum()),
        len(control),
        int(treatment["converted"].sum()),
        len(treatment),
    )
    st.json(z)

    st.subheader("Continuous metric — Welch's t-test + bootstrap CI")
    t = inference.welch_ttest(
        control["continuous_metric"].to_numpy(), treatment["continuous_metric"].to_numpy()
    )
    boot = inference.bootstrap_ci(
        control["continuous_metric"].to_numpy(), treatment["continuous_metric"].to_numpy()
    )
    colA, colB = st.columns(2)
    colA.json(t)
    colB.json(boot)

    if scenario_name == "cuped":
        from eventstream.experimentation.cuped import cuped_adjust

        st.subheader("CUPED variance reduction")
        adj_c = cuped_adjust(
            control["continuous_metric"].to_numpy(), control["pre_period_metric"].to_numpy()
        )
        st.write(f"Variance reduction on control arm: **{adj_c['variance_reduction_pct']:.1%}**")
        st.json({k: v for k, v in adj_c.items() if k != "y_adjusted"})

    st.subheader("Power / sample size")
    mde = st.slider("Absolute MDE (percentage points)", 0.005, 0.05, 0.02, step=0.005)
    n_req = power.sample_size_two_proportions(
        baseline_rate=float(control["converted"].mean()), mde_absolute=mde
    )
    st.metric("Required sample size per group", f"{n_req:,}")

# --- Anomalies -----------------------------------------------------------
with tabs[5]:
    st.caption(
        "Rolling-median / robust MAD z-score (Iglewicz & Hoaglin rule, threshold 3.5), computed "
        "entirely in SQL (sql/marts/mart_anomalies.sql). See docs/METRICS.md for method rationale."
    )
    anomalies = anomalies_from_warehouse(con)
    metric_choice = st.selectbox("Metric", sorted(anomalies["metric_name"].unique()))
    subset = anomalies[anomalies["metric_name"] == metric_choice]

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(x=subset["activity_date"], y=subset["metric_value"], name=metric_choice)
    )
    flagged = subset[subset["is_anomaly"]]
    fig.add_trace(
        go.Scatter(
            x=flagged["activity_date"],
            y=flagged["metric_value"],
            mode="markers",
            marker=dict(color="red", size=10),
            name="Flagged anomaly",
        )
    )
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(flagged, use_container_width=True)
