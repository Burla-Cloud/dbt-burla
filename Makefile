.PHONY: help install sync lint format typecheck test test-unit test-duckdb test-integration cov build clean example-duckdb

UV ?= uv
PYTHON ?= python

help:
	@echo "dbt-burla development commands:"
	@echo ""
	@echo "  install        Install dev dependencies (uv sync --all-extras)"
	@echo "  lint           Run ruff check"
	@echo "  format         Run ruff format"
	@echo "  typecheck      Run mypy"
	@echo "  test-unit      Run unit tests"
	@echo "  test-duckdb    Run DuckDB integration tests"
	@echo "  test           Run unit + DuckDB tests"
	@echo "  cov            Run tests with coverage report"
	@echo "  build          Build sdist + wheel"
	@echo "  clean          Remove build artifacts"
	@echo "  example-duckdb Run the quickstart example end-to-end"

install sync:
	$(UV) sync --all-extras

lint:
	$(UV) run ruff check src tests

format:
	$(UV) run ruff format src tests
	$(UV) run ruff check --fix src tests

typecheck:
	$(UV) run mypy

test-unit:
	$(UV) run pytest -m "unit" tests/unit

test-duckdb:
	$(UV) run pytest -m "duckdb" tests/integration

test-integration:
	$(UV) run pytest tests/integration

test:
	$(UV) run pytest -m "unit or duckdb"

cov:
	$(UV) run pytest -m "unit or duckdb" --cov --cov-report=term-missing --cov-report=xml

build: clean
	$(UV) build

clean:
	rm -rf build dist *.egg-info .coverage .coverage.* htmlcov coverage.xml .pytest_cache .mypy_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

example-duckdb:
	cd examples/01-quickstart-duckdb && DBT_PROFILES_DIR=. $(UV) run dbt run --profiles-dir .
