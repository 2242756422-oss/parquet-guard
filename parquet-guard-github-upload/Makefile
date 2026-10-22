.PHONY: help install test lint format typecheck build clean check-all

PYTHON ?= python3

help:
	@echo "parquet-guard development targets:"
	@echo "  make install     Install package in editable mode with dev dependencies"
	@echo "  make test        Run all unit, integration, and compatibility tests"
	@echo "  make lint        Run ruff linter check"
	@echo "  make format      Run ruff code formatter"
	@echo "  make typecheck   Run mypy static type analysis"
	@echo "  make build       Build wheel and source distributions"
	@echo "  make clean       Remove temporary and build artifacts"
	@echo "  make check-all   Run lint, typecheck, and test in sequence"

install:
	$(PYTHON) -m pip install -e ".[dev]"

test:
	$(PYTHON) -m pytest tests/ -v

lint:
	$(PYTHON) -m ruff check .

format:
	$(PYTHON) -m ruff format .

typecheck:
	$(PYTHON) -m mypy --explicit-package-bases src tests examples benchmarks

build:
	uv build

clean:
	rm -rf dist/ build/ *.egg-info .pytest_cache .ruff_cache src/*.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +

check-all: lint typecheck test
