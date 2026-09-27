.PHONY: sync fmt lint typecheck test validate demo dashboard real-analysis

sync:
	uv sync --all-extras

fmt:
	uv run ruff format .

lint:
	uv run ruff check .

typecheck:
	uv run mypy src

test:
	uv run pytest

validate:
	uv run eventstream validate

demo:
	uv run eventstream demo

dashboard:
	uv run eventstream dashboard

# Offline: cut the published block window from data/raw/ segments, build, validate, analyze, time it.
real-analysis:
	uv run python scripts/run_real_analysis.py
