# 🛡️ parquet-guard

[![CI](https://github.com/2242756422-oss/parquet-guard/actions/workflows/ci.yml/badge.svg)](https://github.com/2242756422-oss/parquet-guard/actions)
[![PyPI version](https://img.shields.io/badge/pypi-v0.4.0a1-blue.svg)](https://pypi.org/project/parquet-guard/)
[![Python 3.9+](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue)](https://pypi.org/project/parquet-guard/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-green.svg)](https://opensource.org/licenses/Apache-2.0)
[![SARIF 2.1.0](https://img.shields.io/badge/SARIF-2.1.0%20Compliant-purple.svg)](https://docs.github.com/en/code-security/code-scanning)
[![Zero Data Scan](https://img.shields.io/badge/I%2FO-Zero--Data--Scan-orange.svg)](#architecture)

> **Alpha Prerelease Notice**: `parquet-guard` is currently classified as **Alpha (`v0.4.0a1`)**. It is functionally verified with 162+ automated tests, strict Ruff/Mypy type compliance, standard packaging, reproducible 64-scenario AI evaluation benchmarks, and full cross-platform CI matrix support across Linux, macOS, and Windows. Community adoption and production hardening are ongoing.

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
- Validating schemas via full data payload ingestion (`pq.read_table`) scales with total data volume, whereas `parquet-guard` inspects the 2KB–20KB Parquet footer without reading row data into memory.

`parquet-guard` solves this by inspecting **only the Parquet metadata footer (typically 2KB–20KB)**. It performs comprehensive structural compatibility checks and multi-partition consistency audits in **sub-millisecond timeframes**.

---

## ✨ Features

- **🚀 Zero-Data-Scan Performance**: Reads metadata footers exclusively. Audits 100+ partitions in `< 20ms` without touching raw data pages.
- **⚡ Empirical Query Engine Verification (`verify-engine`)**: Probe real query engines (**PyArrow**, **DuckDB**, **Polars**) directly on disk. Distinguish static rule predictions from runtime reality (`RULE_OPTIMISTIC` vs `RULE_CONSERVATIVE`) to eliminate multi-partition union traps.
- **👥 Consumer-Aware Schema Contracts 2.0**: Track and evaluate downstream consumer compatibility (`impact`, `init-consumer`) with declarative directionality (`BACKWARD`, `FORWARD`, `FULL`), independent criticality levels, and explicit field rename mapping.
- **🔐 Hardened Footer Checksum Cache & Storage Abstraction**: Cryptographic O(1) footer SHA-256 validation (`HIGH_RELIABLE` confidence) with a pluggable storage abstraction layer for local and remote filesystems.
- **🗺️ Dataset-Wide Topology Grouping**: Cluster partitioned data lakes into structural schema groups (`analyze_dataset_topology`), identify canonical schemas, and quantify divergence concentration across partitions.
- **🧪 Verified Synthetic Migration Sandbox 2.0**: Test proposed schema migrations (`verify-migration`) in an isolated sandbox with boundary-probing synthetic data or authorized user samples (`user_data_authorized=True`).
- **📊 Standalone Interactive HTML Reports**: Generate zero-dependency, self-contained single-file HTML reports (`report`) featuring SVG consumer impact DAGs, schema group topology tables, and sandbox outcome matrices.
- **🔌 Model Context Protocol (MCP) Server**: Expose 8 schema inspection, contract validation, engine verification, and impact analysis tools over stdio JSON-RPC 2.0 (`serve-mcp`) for AI pair programmers and Cursor/Claude environments with path sandboxing.
- **🧪 Pytest Integration Plugin**: Built-in assertions (`assert_schema_compatible`, `assert_engine_verified`) and `parquet_guard` fixture for Python test suites.
- **⚡ Experimental Incremental Dataset Auditing**: Caches partition schema fingerprints with atomic mtime/size validation (`--incremental`). Bypasses redundant footer reads while ensuring 100% agreement with cold audits.
- **📐 Deep Structural Type Equality**: Enforces exact structural validation across Arrow types:
  - FixedSizeList length verification (`list_size`).
  - Dictionary sorting semantics (`ordered`).
  - Map key ordering semantics (`keys_sorted`) with bracket-aware parser.
  - Decimal precision and scale invariance (`DECIMAL_SCALE_MISMATCH`).
  - Timestamp resolution and timezone shifts (`TIMESTAMP_TIMEZONE_MISMATCH`).
  - Deep nested Struct and List child fields with ordering and metadata retention.
- **🔒 Golden Schema Contracts**: Export, enforce, and strictly validate declarative `.parquet-guard.json` contracts (`validate-contract`, `check`).
- **🤖 Evidence-Grounded AI Schema Evolution Copilot**: Optional AI diagnostic and planning subsystem (`explain`, `propose-migration`, `eval-ai`). Cites deterministic finding IDs (`F001`, `F002`), evaluates DuckDB/PyArrow/Spark operational consequences, and generates deterministically validated migration plans with zero raw data leaks and zero CI verdict override authority.
- **🧪 64-Scenario AI Evaluation Framework**: Reproducible 64-scenario evaluation suite (`eval-ai --split [all|dev|held_out]`) verifying citation validity, factual consistency, and prompt-injection resistance across 40 development and 24 held-out scenarios.
- **🛡️ SARIF 2.1.0 Native Integration**: Emits OASIS SARIF 2.1.0 reports (`--format sarif`), integrating directly into GitHub Code Scanning and security alerts.
- **🗂️ Deterministic Partition Drift Auditor**: Recursively scans partitioned trees or globs to ensure 100% schema consistency across partitioned datasets with permutation invariance.
- **💬 GitHub PR Automation**: Generates clean Markdown reports with status badges and collapsible diff tables, perfect for automated PR comments.
- **🪶 Ultra-lightweight**: Pure Python with Arrow foundation (`pyarrow`, `click`, `pydantic`, `rich`). Zero heavy server dependencies.

---

## 📦 Installation

```bash
# Install latest release from PyPI
pip install parquet-guard

# Or pin a specific Alpha release (v0.4.0a1 or v0.3.0a1)
pip install parquet-guard==0.4.0a1

# Or install locally from source
git clone https://github.com/2242756422-oss/parquet-guard.git
cd parquet-guard
pip install .

# Or install from built wheel artifact
pip install dist/parquet_guard-0.4.0a1-py3-none-any.whl
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

### 6. AI-Assisted Schema Drift Diagnosis (`explain`)
Generate an evidence-grounded diagnosis citing exact structural findings and downstream engine impact:
```bash
# Offline deterministic explanation (no API key required)
parquet-guard explain data/new_batch.parquet -c contracts/events.schema.json

# Markdown output for GitHub PR comments
parquet-guard explain data/new_batch.parquet -c contracts/events.schema.json --format markdown

# With live OpenAI provider (requires OPENAI_API_KEY)
parquet-guard explain data/new_batch.parquet -c contracts/events.schema.json --provider openai --model gpt-4o-mini
```

### 7. Consumer Impact Analysis (`impact` & `init-consumer`)
Initialize and evaluate downstream consumer compatibility contracts:
```bash
# Generate consumer contract template from an existing Parquet file
parquet-guard init-consumer data/sample.parquet -o contracts/duckdb_consumer.json --consumer-id duckdb-analytics --name "DuckDB Analytics"

# Evaluate downstream impact across consumer group specification
parquet-guard impact data/new_batch.parquet -c contracts/consumers.json

# Export machine-readable consumer impact JSON
parquet-guard impact data/new_batch.parquet -c contracts/consumers.json --format json -o impact.json
```

### 8. Synthetic Migration Verification Sandbox (`verify-migration`)
Validate migration plans against synthetic edge-case data in an isolated filesystem sandbox:
```bash
parquet-guard verify-migration migration_plan.json -c contracts/consumers.json --rows 100
```

### 9. Standalone Interactive HTML Report (`report`)
Generate an all-in-one HTML dashboard with SVG impact DAG, topology breakdown, and compatibility matrix:
```bash
parquet-guard report data/lake -c contracts/consumers.json -o diagnostic_report.html
```

### 10. Model Context Protocol (MCP) Server (`serve-mcp`)
Launch the official stdio JSON-RPC 2.0 MCP server for AI pair programming:
```bash
# Run stdio MCP server for Cursor, Claude Desktop, or Windsurf
parquet-guard serve-mcp --allowed-dir /path/to/project
```

### 11. Contract Evolution & Migration Planning (`propose-migration`)
Generate a proposed contract update and executable PyArrow transformation code:
```bash
parquet-guard propose-migration data/new_batch.parquet -c contracts/events.schema.json --save-contract contracts/events.v2.json
```

### 12. Reproducible AI Benchmark Evaluation (`eval-ai`)
Run the 64-scenario synthetic evaluation benchmark across development or held-out splits:
```bash
# Run full 64-scenario evaluation suite
parquet-guard eval-ai --provider mock

# Run specific split (dev: 40 scenarios, held_out: 24 scenarios)
parquet-guard eval-ai --provider mock --split dev
parquet-guard eval-ai --provider mock --split held_out
```

### 13. Empirical Query Engine Verification (`verify-engine`)
Directly probe runtime execution across PyArrow, DuckDB, and Polars:
```bash
# Metadata-only verification without reading row data (default)
parquet-guard verify-engine data/lake/ -c contracts/baseline.json

# Explicitly authorize bounded row-level probing across all installed engines
parquet-guard verify-engine data/lake/ -c contracts/baseline.json --allow-row-probe --row-limit 10

# Constrain aggregate row budget across engines and operations
parquet-guard verify-engine data/lake/ --allow-row-probe --max-total-rows 100 --engine polars --engine duckdb --fail-on-disagreement

# Export JSON report
parquet-guard verify-engine data/sample.parquet --allow-row-probe --format json -o engine_report.json
```

### 14. Pytest Integration
Assert schema and engine compatibility directly within your Python test suites:
```python
from parquet_guard.testing import assert_engine_verified, assert_schema_compatible


def test_pipeline_output(tmp_path):
    output_parquet = run_my_pipeline(tmp_path)

    # Assert structural compatibility against golden contract
    assert_schema_compatible(output_parquet, "contracts/golden.json", mode="permissive")

    # Probe real query engines for runtime compatibility
    assert_engine_verified(output_parquet, engines=["duckdb", "polars"])
```

---

## 💡 Practical Pipeline Demonstrations

Explore fully functioning, reproducible scenarios in `examples/`:

1. **[E-Commerce Orders Pipeline](examples/ecommerce_pipeline/)**: Simulates schema evolution (`decimal128(10,2)` to `(14,4)` and temporal precision widening) across 3 distinct consumers (DuckDB, Polars, PyArrow), executing an isolated sandbox migration and generating an HTML report.
2. **[AI Feature Store Vector Guard](examples/ai_feature_pipeline/)**: Detects and halts vector dimension mismatches (512 vs 768 dimensions in `fixed_size_list<float32>`), preventing vector database ANN index corruption.
3. **[Partitioned Data Lake Topology](examples/partitioned_lake/)**: Demonstrates clustering 10 lake partitions into 3 distinct schema groups, finding the canonical group, and computing divergence concentration hotspots.

---

## 🔒 Architectural Boundaries & Privacy Model

To ensure absolute clarity for enterprise and compliance reviewers, `parquet-guard` maintains strict boundaries across all subsystems:

### 1. Privacy & Data Transmission Boundary
- **Zero Raw Row Data Access**: `parquet-guard` never reads, buffers, or transmits table rows, record contents, or cell values.
- **Schema Metadata Disclosure**: When and only when a remote provider is explicitly configured (e.g., `--provider openai`), schema structural metadata (field names, Arrow data types, nullability flags, field/schema metadata keys/values, deterministic error codes, finding descriptions, and optional user context) is serialized into prompt payloads and transmitted via HTTPS to the user-specified provider endpoint.
- **Offline by Default**: In default operation (`check`, `diff`, `audit`, `inspect`), and when using the offline deterministic explainer or `--provider mock`, zero network requests are initiated.

### 2. Contract Schema Validation vs. Physical Data Migration
- **Schema Contract Validation**: Operations such as `validate-contract`, `check`, and `propose_contract_evolution` perform in-memory deterministic AST validation, ensuring that a target schema satisfies a declarative contract without breaking changes.
- **Physical Migration Script Generation**: The `propose-migration` command generates runnable Python/PyArrow transformation code for pipeline operators to review. `parquet-guard` does **not** physically rewrite, mutate, or migrate production data partitions on disk without manual execution by an authorized operator.

### 3. Agent Tool Calling vs. MCP Daemon
- **In-Process Agent Function Calling**: `parquet_guard.ai.agent_tools` provides OpenAI-compatible function-calling schemas (`AGENT_TOOL_DEFINITIONS`) and an in-process tool dispatcher (`call_agent_tool`) for direct Python agent integration.
- **Stdio MCP Server Daemon**: `parquet_guard.mcp.server` (`parquet-guard serve-mcp --allowed-dir <dir>`) provides a standalone Model Context Protocol (MCP) JSON-RPC 2.0 server over stdio with path-traversal sandboxing, payload size limits, and overwrite guards.

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

Use the official composite GitHub Action (`.github/actions/parquet-guard`):

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

      # Audit dataset partitions, emit SARIF report, and export verdict
      - name: Audit Schema Consistency
        id: parquet_guard
        uses: 2242756422-oss/parquet-guard/.github/actions/parquet-guard@main
        with:
          path: 'fixtures/'
          contract: 'contracts/events.schema.json'
          mode: 'permissive'
          sarif-file: 'parquet-guard.sarif'
          fail-on-warnings: 'false'

      # Upload SARIF diagnostic report even if breaking changes occurred
      - name: Upload SARIF to GitHub Code Scanning
        uses: github/codeql-action/upload-sarif@v3
        if: always()
        with:
          sarif_file: parquet-guard.sarif
          category: parquet-guard
```

---

## 📊 Benchmark & Performance

Empirical measurements on macOS arm64 (Apple Silicon, Python 3.13.9, PyArrow 21.0.0) executed via [`benchmarks/fair_benchmark.py`](benchmarks/fair_benchmark.py) with 5 warmup rounds and repeated statistical sampling:

### 1. Single File Schema Inspection (10,000 rows, 7 columns, 30 repetitions)
| Method | Mean Latency | Median | Std Dev | Description |
| :--- | :---: | :---: | :---: | :--- |
| **PyArrow C++ Raw (`ParquetFile.schema_arrow`)** | **0.058 ms** | 0.050 ms | 0.026 ms | Bare C++ footer read |
| **`parquet-guard inspect_schema`** (Pydantic AST) | **0.087 ms** | 0.080 ms | 0.018 ms | Footer read + Pydantic SchemaContract AST |
| **Full Data Scan (`pq.read_table`)** | **0.549 ms** | 0.524 ms | 0.067 ms | Reads all columns & rows into memory |

*Note: Comparing metadata footer inspection with `pq.read_table` illustrates the architectural difference between metadata-only extraction (~10KB footer) and full-data payload ingestion; it is not an assertion of superiority over other metadata-based schema tools.*

### 2. Multi-Partition Dataset Auditing (`parquet-guard audit`, 15 repetitions)
| Partitions | Total Size | Cold Run | Hot Full Audit (Mean) | Hot Incremental Audit | Full Payload Scan (`pq.read_table`) |
| :---: | :---: | :---: | :---: | :---: | :---: |
| **10 partitions** | 2.39 MB | 3.23 ms | **2.11 ms** (±0.14 ms) | **1.04 ms** (±0.13 ms) | 6.79 ms (±3.16 ms) |
| **50 partitions** | 12.01 MB | 10.26 ms | **9.50 ms** (±0.10 ms) | **3.42 ms** (±0.17 ms) | 26.34 ms (±0.30 ms) |
| **100 partitions** | 24.04 MB | 21.01 ms | **19.61 ms** (±0.50 ms) | **5.96 ms** (±0.20 ms) | 52.38 ms (±0.83 ms) |

### Architectural Insight: Footer I/O vs Full Data Scan
- **Full Data Scan (`pq.read_table`)**: Reads dictionary pages, data pages, decompresses Snappy blocks, and constructs Arrow memory buffers. Scaling is $O(\text{Data Volume})$.
- **`parquet-guard` Cold Audit**: Seeks to the file footer (typically last 4KB-16KB of file) using `pq.ParquetFile`. Scaling is $O(N)$ with partition file count, independent of table row count.
- **`parquet-guard` Incremental Audit**: Evaluates `(mtime_ns, file_size_bytes)` against `.parquet_guard_cache.json`. When unchanged, avoids disk seeks entirely and achieves up to **2.9x speedup** over hot full audits.

---

## ⚠️ Known Limitations (Alpha)

1. **Local and Mounted Filesystems**: Direct streaming inspection over S3/GCS/Azure without downloading is on the roadmap for v0.3.0. Currently, remote buckets must be mounted (e.g., via s3fs or Cloud Storage FUSE) or downloaded locally.
2. **Experimental Incremental Cache**: The incremental cache relies on filesystem modification times (`mtime_ns`) and file sizes. In CI runners where repository clones do not preserve timestamps, use `--force-full` or standard audit.
3. **Temporal Unit Precision vs. Range**: Finer temporal units (`s -> ms -> us -> ns`) increase precision but decrease representable dynamic range in signed 64-bit integer format (e.g. `timestamp[ns]` is limited to 1677–2262 AD, and `duration[ns]` to ±292 years). Zero-data-scan schema inspection cannot verify whether row values exceed this range without data scans; in permissive mode, this generates a `TEMPORAL_OVERFLOW_RISK` warning.
4. **In-Memory Partition Manifests**: Pydantic models for partition reports and contracts are held in process memory. Datasets with tens of thousands of partitions run comfortably, but million-file datasets require batching.

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

# 3. Run automated test suite (104 tests, 100% passing)
pytest tests/ -v

# 4. Run static type checking and linters
ruff check .
ruff format --check .
mypy --explicit-package-bases src tests examples benchmarks

# 5. Run fair empirical benchmark suite
python benchmarks/fair_benchmark.py

# 6. Run reproducible AI evaluation benchmark suite
parquet-guard eval-ai --provider mock

# 7. Verify distribution build
python -m build
```

---

## 🤝 Contributing

Contributions are welcome! Please check out [CONTRIBUTING.md](CONTRIBUTING.md) and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

---

## 📄 License

Distributed under the **Apache License 2.0**. See [LICENSE](LICENSE) for details.
