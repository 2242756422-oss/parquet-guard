# 🛡️ parquet-guard

[![CI](https://github.com/2242756422-oss/parquet-guard/actions/workflows/ci.yml/badge.svg)](https://github.com/2242756422-oss/parquet-guard/actions)
[![PyPI version](https://img.shields.io/badge/pypi-v0.2.0--alpha-blue.svg)](https://pypi.org/project/parquet-guard/)
[![Python 3.9+](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue)](https://pypi.org/project/parquet-guard/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-green.svg)](https://opensource.org/licenses/Apache-2.0)
[![SARIF 2.1.0](https://img.shields.io/badge/SARIF-2.1.0%20Compliant-purple.svg)](https://docs.github.com/en/code-security/code-scanning)
[![Zero Data Scan](https://img.shields.io/badge/I%2FO-Zero--Data--Scan-orange.svg)](#architecture)

> **Alpha Release Notice**: `parquet-guard` is currently classified as **Alpha (`v0.2.0-alpha`)**. It is functionally verified with 70 automated tests, strict Ruff/Mypy type compliance, and standard packaging, but community adoption and long-term production hardening are ongoing.

**High-performance, zero-data-scan Parquet schema contract & drift detection guard with experimental incremental caching and SARIF reporting for data pipelines, MLOps, and CI/CD.**

`parquet-guard` protects modern analytics platforms (DuckDB, Polars, PyArrow, Spark) and machine learning feature stores from silent schema drift, unexpected column drops, and incompatible Arrow type mutations.

---

## ⚡ Why parquet-guard?

In modern data engineering and AI training pipelines, Apache Parquet is the ubiquitous storage format. However, incremental pipelines and microservices frequently produce **silent schema drift**:

- Upstream pipelines convert an `int64` timestamp or ID to `int32` or `string`.
- Non-nullable columns suddenly receive null values or their schema constraint flips to `nullable=True`.
- A FixedSizeList length changes (e.g., embedding vector `fixed_size_list<float32>[512]` to `[768]`), causing matrix math kernels or tensor loaders to crash.
- A Map's `keys_sorted` attribute changes or Dictionary `ordered` flag flips, breaking binary search optimizations in query engines.
- A newly deployed worker writes a partitioned directory (`date=2026-10-22`) with an extra column, causing downstream federated queries across partitions to fail.
- Existing validation tools (like Great Expectations or Soda) require **scanning entire gigabyte-scale datasets into memory**, making them too slow and expensive for pre-commit hooks and pull request CI.

`parquet-guard` solves this by inspecting **only the Parquet metadata footer (typically 2KB–20KB)**. It performs comprehensive structural compatibility checks and multi-partition consistency audits in **sub-millisecond timeframes**.

---

## ✨ Features

- **🚀 Zero-Data-Scan Performance**: Reads metadata footers exclusively. Audits 100+ partitions in `< 20ms` without touching raw data pages.
- **⚡ Experimental Incremental Dataset Auditing**: Caches partition schema fingerprints with atomic mtime/size validation (`--incremental`). Bypasses redundant footer reads while ensuring 100% agreement with cold audits.
- **📐 Deep Structural Type Equality**: Enforces exact structural validation across Arrow types:
  - FixedSizeList length verification (`list_size`).
  - Dictionary sorting semantics (`ordered`).
  - Map key ordering semantics (`keys_sorted`) with bracket-aware parser.
  - Decimal precision and scale invariance (`DECIMAL_SCALE_MISMATCH`).
  - Timestamp resolution and timezone shifts (`TIMESTAMP_TIMEZONE_MISMATCH`).
  - Deep nested Struct and List child fields with ordering and metadata retention.
- **🔒 Golden Schema Contracts**: Export, enforce, and strictly validate declarative `.parquet-guard.json` contracts (`validate-contract`, `check`).
- **🛡️ SARIF 2.1.0 Native Integration**: Emits OASIS SARIF 2.1.0 reports (`--format sarif`), integrating directly into GitHub Code Scanning and security alerts.
- **🗂️ Deterministic Partition Drift Auditor**: Recursively scans partitioned trees or globs to ensure 100% schema consistency across partitioned datasets with permutation invariance.
- **💬 GitHub PR Automation**: Generates clean Markdown reports with status badges and collapsible diff tables, perfect for automated PR comments.
- **🪶 Ultra-lightweight**: Pure Python with Arrow foundation (`pyarrow`, `click`, `pydantic`, `rich`). Zero heavy server dependencies.

---

## 📦 Installation

```bash
# From PyPI (upon public distribution)
pip install parquet-guard

# Or install locally from source
git clone https://github.com/2242756422-oss/parquet-guard.git
cd parquet-guard
pip install .

# Or install from built wheel artifact
pip install dist/parquet_guard-0.2.0-py3-none-any.whl
```

---

## 🚀 Quick Start (CLI)

### 1. Inspect Parquet File Metadata
Inspect schema types, row groups, codecs, and column nullabilities without loading data:
```bash
parquet-guard inspect data/sample.parquet
```

### 2. Export and Validate Golden Schema Contracts
Capture the baseline schema of your production dataset and validate contract syntax:
```bash
# Generate contract
parquet-guard init-contract data/sample.parquet -o contracts/events.schema.json

# Validate contract structure & syntax
parquet-guard validate-contract contracts/events.schema.json
```

### 3. Validate in CI Against Contract
Verify if new data violates the contract:
```bash
# Permissive mode (allows safe additive nullable columns and widening)
parquet-guard check data/new_batch.parquet -c contracts/events.schema.json

# Strict mode (disallows any deviation or widening)
parquet-guard check data/new_batch.parquet -c contracts/events.schema.json --mode strict

# Emit SARIF for GitHub Code Scanning
parquet-guard check data/new_batch.parquet -c contracts/events.schema.json --format sarif --output check_results.sarif
```

### 4. Diff Two Parquet Schemas Directly
Compare schemas between two files (e.g., staging vs. production):
```bash
parquet-guard diff data/staging.parquet data/prod.parquet --format markdown
```

### 5. Audit Partitioned Datasets (with Experimental Caching)
Scan an entire multi-partition directory tree:
```bash
# Permissive audit allowing safe additive evolution
parquet-guard audit "data/lake/events/**/*.parquet" --mode permissive

# Incremental audit (experimental: caches mtime/size fingerprints)
parquet-guard audit "data/lake/events/**/*.parquet" --incremental

# Audit with strict drift gating (fail CI on any warning or safe drift)
parquet-guard audit "data/lake/events/**/*.parquet" --mode permissive --fail-on-warnings

# Strict identical schema enforcement
parquet-guard audit "data/lake/events/**/*.parquet" --mode strict

# SARIF export for security dashboards
parquet-guard audit "data/lake/events/**/*.parquet" --format sarif --output audit.sarif
```

---

## 🐍 Python API

You can use `parquet-guard` directly inside your Python pipelines, Airflow DAGs, or pytest suites:

```python
from parquet_guard import inspect_schema, compare_schemas, audit_dataset
from parquet_guard.core.reporter import format_diff_sarif

# 1. Inspect schema contract
contract = inspect_schema("data/events.parquet")
print(contract.field_names())

# 2. Assert schema compatibility
target = inspect_schema("data/new_batch.parquet")
report = compare_schemas(target, contract, mode="permissive")

if report.has_breaking_changes:
    # Generate GitHub PR comment markdown or SARIF
    from parquet_guard.core.reporter import format_diff_markdown

    print(format_diff_markdown(report))
    raise RuntimeError("Breaking Parquet schema regression detected!")

# 3. Incremental partition audit
result = audit_dataset("data/lake/partitions/", incremental=True)
assert result.is_consistent, f"Partitions drifted: {result.divergence_details}"
print(f"Audit completed: {result.total_files_scanned} files (Cache hits: {result.cache_hits})")
```

---

## 🤖 GitHub Actions CI Workflow

Add `parquet-guard` to your repository's `.github/workflows/data-quality.yml`:

```yaml
name: Parquet Schema Guard & Code Scanning

on:
  pull_request:
    paths:
      - 'data/**.parquet'
      - 'fixtures/**.parquet'
      - 'contracts/**.json'

permissions:
  contents: read
  security-events: write

jobs:
  schema-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install parquet-guard
        run: pip install parquet-guard

      - name: Validate Contract File
        run: parquet-guard validate-contract contracts/events.schema.json

      - name: Validate Parquet Fixtures & Generate SARIF
        run: |
          parquet-guard check fixtures/sample.parquet \
            --contract contracts/events.schema.json \
            --format sarif \
            --output results.sarif || true

      - name: Upload SARIF to GitHub Code Scanning
        uses: github/codeql-action/upload-sarif@v3
        if: always()
        with:
          sarif_file: results.sarif
          category: parquet-guard
```

---

## 📊 Benchmark & Performance

Empirical measurements on macOS arm64 (Apple Silicon, Python 3.13, PyArrow 21.0.0) executed via [`benchmarks/fair_benchmark.py`](benchmarks/fair_benchmark.py):

### 1. Single File Schema Inspection (10,000 rows, 7 columns)
| Method | Latency | Overhead vs Bare C++ |
| :--- | :---: | :---: |
| **PyArrow C++ Raw (`ParquetFile.schema_arrow`)** | **0.125 ms** | Baseline |
| **`parquet-guard inspect_schema`** (Pydantic Contract AST) | **0.135 ms** | **+0.010 ms** |
| **Full Data Scan (`pq.read_table`)** | **6.196 ms** | ~46x slower (scales with file size) |

*`parquet-guard` adds negligible (~10 microseconds) overhead over raw C++ footer parsing to construct typed Pydantic models with container attributes, metadata, and validation.*

### 2. Multi-Partition Dataset Auditing (`parquet-guard audit`)
| Partitions | Total Size | Cold Run | Hot Full Audit (Mean 10x) | Incremental Cached Audit | Manual Full Table Scan |
| :---: | :---: | :---: | :---: | :---: | :---: |
| **10 partitions** | 2.39 MB | 3.82 ms | **2.01 ms** (0.20 ms/file) | **1.10 ms** (0.11 ms/file) | 5.22 ms |
| **50 partitions** | 12.01 MB | 12.95 ms | **9.11 ms** (0.18 ms/file) | **3.30 ms** (0.066 ms/file) | 24.94 ms |
| **100 partitions** | 24.04 MB | 19.61 ms | **18.44 ms** (0.18 ms/file) | **5.95 ms** (0.060 ms/file) | 52.88 ms |

### Architectural Insight: Footer I/O vs Full Data Scan
- **Full Data Scan (`pq.read_table`)**: Reads dictionary pages, data pages, decompresses Snappy/ZSTD blocks, and constructs Arrow memory buffers. Scaling is $O(\text{Data Volume})$.
- **`parquet-guard` Cold Audit**: Seeks to the file footer (typically last 4KB-16KB of file) using `pq.ParquetFile`. Scaling is $O(N)$ with partition file count, completely independent of table row count.
- **`parquet-guard` Incremental Audit**: Evaluates `(mtime_ns, file_size_bytes)` against `.parquet_guard_cache.json`. When unchanged, avoids disk seeks entirely and achieves up to **3.1x speedup** over cold full audits.

---

## ⚠️ Known Limitations (Alpha)

1. **Local and Mounted Filesystems**: Direct streaming inspection over S3/GCS/Azure without downloading is on the roadmap for v0.3.0. Currently, remote buckets must be mounted (e.g., via s3fs or Cloud Storage FUSE) or downloaded locally.
2. **Experimental Incremental Cache**: The incremental cache relies on filesystem modification times (`mtime_ns`) and file sizes. In CI runners where repository clones do not preserve timestamps, use `--force-full` or standard audit.
3. **In-Memory Partition Manifests**: Pydantic models for partition reports and contracts are held in process memory. Datasets with tens of thousands of partitions run comfortably, but million-file datasets require batching.

---

## 🧪 Testing & Reproducibility for External Testers

To reproduce all test results and benchmarks independently:

```bash
# 1. Clone repository
git clone https://github.com/2242756422-oss/parquet-guard.git
cd parquet-guard

# 2. Install editable in a virtual environment
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# 3. Run automated test suite (70 tests, 100% passing)
pytest tests/ -v

# 4. Run static type checking and linters
ruff check .
ruff format --check .
mypy --explicit-package-bases src tests examples benchmarks

# 5. Run fair empirical benchmark suite
python benchmarks/fair_benchmark.py

# 6. Verify distribution build
python -m build
```

---

## 🤝 Contributing

Contributions are welcome! Please check out [CONTRIBUTING.md](CONTRIBUTING.md) and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

---

## 📄 License

Distributed under the **Apache License 2.0**. See [LICENSE](LICENSE) for details.
