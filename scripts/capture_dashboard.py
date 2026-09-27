"""Capture dashboard tab screenshots from a warehouse.

    python scripts/capture_dashboard.py <warehouse.duckdb> <out_dir> [Tab ...]

With no tabs given it captures the five tabs that apply to real data (not Experiments, which is
synthetic-only).

Starts the Streamlit dashboard on a local port against the given warehouse, drives it with
Playwright (`pip install playwright`; uses the locally installed Chrome), and saves one PNG per
tab. Each capture waits until Streamlit is idle and the tab's charts have actually drawn, and
reports any exception box on the page, so a half-rendered or errored tab is never saved
silently. The Streamlit process is started with the interpreter named by STREAMLIT_PYTHON
(default: the one running this script), which must have the project and Streamlit installed. If
Playwright lives in a different interpreter, run this script with that one and set
STREAMLIT_PYTHON to the project's.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

from playwright.sync_api import sync_playwright

REPO = Path(__file__).resolve().parents[1]
PORT = 8598
# Minimum Plotly charts each tab must have drawn before it is captured. Retention is a table
# view (no chart) and Experiments shows metrics/JSON only (it is synthetic-only, so it is not in
# the default set); Concentration's second chart becomes a notice when only one week exists.
EXPECTED_CHARTS = {
    "Overview": 2,
    "Retention": 0,
    "Behavior": 3,
    "Concentration": 1,
    "Experiments": 0,
    "Anomalies": 1,
}
DEFAULT_TABS = ["Overview", "Retention", "Behavior", "Concentration", "Anomalies"]


def main() -> None:
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    warehouse, out_dir = str(Path(sys.argv[1]).resolve()), Path(sys.argv[2])
    tabs = sys.argv[3:] or DEFAULT_TABS
    out_dir.mkdir(parents=True, exist_ok=True)
    streamlit_python = os.environ.get("STREAMLIT_PYTHON", sys.executable)

    server = subprocess.Popen(
        [streamlit_python, "-m", "streamlit", "run", str(REPO / "dashboard" / "app.py")]
        + ["--server.headless", "true", "--server.address", "localhost"]
        + ["--server.port", str(PORT), "--browser.gatherUsageStats", "false"]
        + ["--client.toolbarMode", "minimal"],
        cwd=REPO,
        env={**os.environ, "EVENTSTREAM_WAREHOUSE_PATH": warehouse},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        for _ in range(60):
            try:
                if (
                    urllib.request.urlopen(
                        f"http://localhost:{PORT}/_stcore/health", timeout=2
                    ).status
                    == 200
                ):
                    break
            except Exception:
                time.sleep(1)
        with sync_playwright() as p:
            browser = p.chromium.launch(channel="chrome", headless=True)
            page = browser.new_page(viewport={"width": 1440, "height": 1700}, device_scale_factor=1)
            page.goto(f"http://localhost:{PORT}", wait_until="networkidle")
            page.get_by_text("EventStream Analytics").first.wait_for(timeout=120_000)
            for tab in tabs:
                page.get_by_role("tab", name=tab).click()
                page.wait_for_timeout(1500)
                page.wait_for_function(
                    "() => !document.querySelector('[data-testid=\"stStatusWidget\"]')",
                    timeout=300_000,
                )
                page.wait_for_function(
                    """(n) => {
                        const panel = document.querySelector('[role=tabpanel]:not([hidden])');
                        if (!panel) return false;
                        const charts = Array.from(panel.querySelectorAll('.stPlotlyChart'));
                        return charts.length >= n && charts.every(c => c.querySelector('.main-svg'));
                    }""",
                    arg=EXPECTED_CHARTS[tab],
                    timeout=300_000,
                )
                page.wait_for_timeout(2500)
                errors = page.locator('[data-testid="stException"]').count()
                path = out_dir / f"{tab.lower()}.png"
                page.screenshot(path=str(path))
                print(
                    f"{tab}: saved {path} ({path.stat().st_size / 1024:.0f} KB), exceptions: {errors}"
                )
                if errors:
                    sys.exit(
                        f"{tab}: the page shows {errors} exception box(es); not a valid capture"
                    )
            browser.close()
    finally:
        server.terminate()
        try:
            server.wait(timeout=10)
        except Exception:
            server.kill()


if __name__ == "__main__":
    main()
