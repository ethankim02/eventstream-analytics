.PHONY: sync fmt lint typecheck test validate demo dashboard

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
