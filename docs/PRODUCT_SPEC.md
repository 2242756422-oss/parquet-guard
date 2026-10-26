# `parquet-guard` Product Specification

> **Version**: 0.4.0a1
> **Status**: Approved Alpha Prerelease
> **Target Audience**: Data Engineers, MLOps Engineers, Analytics Engineers, CI/CD Maintainers

---

## 1. Product Positioning & Value Proposition

**`parquet-guard`** is a high-performance, zero-data-scan Parquet schema contract, schema drift detection, and backward compatibility guard designed for modern data pipelines (DuckDB, Polars, PyArrow, Spark), MLOps feature stores, and CI/CD pull request validation.

It prevents costly production data pipeline crashes and silent type truncations by validating Parquet schemas against golden contracts and auditing partitioned dataset consistency in sub-millisecond timeframes.

---

## 2. Target User Personas

1. **Analytics & Data Engineers**:
   - Building incremental ETL pipelines using DuckDB, Polars, dbt, or PyArrow.
   - Suffer from silent schema drift when upstream services add, drop, or alter column types without notification.
2. **MLOps & AI Infrastructure Engineers**:
   - Managing offline feature stores, training datasets, and inference checkpoints stored in Parquet format.
   - Need to ensure training/eval data splits maintain strictly identical Arrow schemas.
3. **Open Source Library & Tool Maintainers**:
   - Developing libraries that serialize or ingest Parquet files.
   - Need automated regression tests in CI to guarantee that newer library releases do not write incompatible Parquet layouts.

---

## 3. Defects of Existing Solutions

| Existing Approach | Fatal Flaws |
| :--- | :--- |
| **Full-table data quality frameworks** | Focus on row-level business assertions and data payload ingestion; too resource-intensive for high-frequency, metadata-only schema drift gating in CI and pre-commit hooks. |
| **Ad-hoc Python Scripts** | Hand-rolled 50-line scripts in every repo; lack standardized backward compatibility rules, cannot handle nested structs/lists, no Markdown PR reporting. |
| **`parquet-linter` (Rust)** | Focuses exclusively on compression, page size, and dictionary layout efficiency; does not validate semantic schema compatibility or partitioned directories. |
| **DuckDB / Polars Direct Read** | Fails *at runtime* inside downstream jobs rather than *at commit/PR time*; cryptic C++ error messages without actionable diffs. |

---

## 4. Concrete Application Scenarios

1. **CI/CD Pull Request Validation**:
   - When a PR modifies data transformation scripts or adds sample Parquet files, `parquet-guard` checks them against `.parquet-guard.json` baseline contracts and fails the PR if breaking changes are introduced.
2. **Pre-commit Hook**:
   - Fast, local pre-commit check verifying that sample test Parquet fixtures conform to expected schemas before pushing to remote.
3. **Partition Consistency Audit**:
   - Audits an entire partitioned dataset directory (e.g., `s3://bucket/events/year=2026/month=10/day=*/*.parquet`) to guarantee that all partitions share a single coherent, non-divergent schema.
4. **Automated GitHub PR Commenting**:
   - Generates human-readable Markdown diff summaries showing added columns, safe widening, warnings, and breaking type regressions.

---

## 5. Core Inputs and Outputs

### Inputs:
- Single Parquet file path (`data/sample.parquet`)
- Directory or glob pattern (`data/partitions/**/*.parquet`)
- Schema contract file (`contracts/events.schema.json`)
- CLI configuration flags (`--mode strict|permissive`, `--fail-on-breaking`, `--format text|json|markdown`)

### Outputs:
- **CLI Table**: Rich terminal formatted view of schema comparison and partition status.
- **Markdown Report**: GitHub PR comment-ready summary with alert callouts and collapsible schema diffs.
- **JSON Structure**: Machine-readable payload for downstream CI integrations and dashboards.
- **Exit Codes**:
  - `0`: Success, schema conforms perfectly or only safe non-breaking additions detected.
  - `1`: Breaking schema regression detected (type incompatibility, dropped non-nullable column, partition divergence).
  - `2`: File I/O, corrupted footer, or syntax error.

---

## 6. Technical Architecture & Design Rationale

```
                     ┌────────────────────────────────────────┐
                     │          CLI Entry Point (Click)       │
                     └───────────────────┬────────────────────┘
                                         │
                   ┌─────────────────────┴─────────────────────┐
                   ▼                                           ▼
      ┌─────────────────────────┐                 ┌─────────────────────────┐
      │   Contract Subsystem    │                 │   Audit / Scan Engine   │
      │   (Baseline & Rules)    │                 │  (Zero-Data Footer I/O) │
      └────────────┬────────────┘                 └────────────┬────────────┘
                   │                                           │
                   └─────────────────────┬─────────────────────┘
                                         ▼
                     ┌────────────────────────────────────────┐
                     │   Semantic Compatibility Analyzer      │
                     │  - Type Promotion / Widening Rules     │
                     │  - Nullability Transition Logic        │
                     │  - Structural Drift & Nested Schemas   │
                     │  - Partition Divergence Evaluator      │
                     └───────────────────┬────────────────────┘
                                         │
                     ┌───────────────────┴────────────────────┐
                     ▼                                        ▼
      ┌─────────────────────────────┐         ┌─────────────────────────────┐
      │     Rich Terminal View      │         │    CI / Markdown / JSON     │
      └─────────────────────────────┘         └─────────────────────────────┘
```

### Design Principles:
1. **Zero Data Scan (Footer Only)**:
   - Only reads the `FileMetaData` footer (typically 2KB–20KB) located at the tail of the Parquet file.
   - Never deserializes data pages or uncompresses dictionary buffers.
   - Achieves sub-millisecond execution even on multi-gigabyte or terabyte files.
2. **Arrow-Native Type Modeling**:
   - Directly maps to standard Apache Arrow data types (`int8`, `int16`, `int32`, `int64`, `float32`, `float64`, `timestamp`, `string`, `struct`, `list`, `map`).
3. **Deterministic Compatibility Algebra**:
   - Formally defines safe promotions (e.g., `int32` -> `int64`, `float32` -> `float64`, `date32` -> `timestamp`).
   - Rejects unsafe promotions (e.g., `int64` -> `int32`, `float64` -> `int32`, `string` -> `int`).
   - Categorizes changes into `SAFE`, `WARNING` (e.g. nullable -> non-nullable without verification), and `BREAKING`.

---

## 7. Data Models & Interface Contracts

### 7.1 Schema Contract JSON Specification
```json
{
  "version": "1.0",
  "name": "user_events",
  "fields": [
    {
      "name": "event_id",
      "type": "string",
      "nullable": false
    },
    {
      "name": "user_id",
      "type": "int64",
      "nullable": false
    },
    {
      "name": "timestamp",
      "type": "timestamp[us, tz=UTC]",
      "nullable": false
    },
    {
      "name": "metadata",
      "type": "struct<ip: string, user_agent: string>",
      "nullable": true
    }
  ]
}
```

### 7.2 Core Python API
```python
from parquet_guard import inspect_schema, compare_schemas, audit_dataset

# 1. Quick inspection
schema = inspect_schema("sample.parquet")

# 2. Schema diff against contract
report = compare_schemas(schema, "contract.json", mode="permissive")
if report.has_breaking_changes:
    print(report.to_markdown())

# 3. Partition directory audit
audit_result = audit_dataset("data/partitions/**/*.parquet")
```

---

## 8. Performance & Compatibility Requirements

- **Python Compatibility**: Python 3.9, 3.10, 3.11, 3.12, 3.13.
- **OS Support**: Linux (x86_64, aarch64), macOS (Intel, Apple Silicon), Windows (x64).
- **Execution Latency**:
  - Single file inspection: `< 5ms`.
  - 100-file partitioned directory audit: `< 80ms`.
  - Memory consumption: `< 30MB` RSS.
- **Dependencies**: Minimal footprint (`pyarrow>=14.0.0`, `click>=8.0.0`, `pydantic>=2.0.0`, `rich>=13.0.0`).

---

## 9. Error Handling & Security Boundaries

- Corrupted footer files must fail gracefully with descriptive error messages without crashing the CLI.
- No network requests during standard validation (100% offline, zero security exfiltration vector).
- Path traversal defense: validates and normalizes all relative file paths.
- Path globs sanitized to prevent exponential regex explosion.

---

## 10. AI Subsystem Architecture & Trust Boundaries

The `parquet_guard.ai` module provides optional schema intelligence, diagnostic explanations, migration planning, and evaluation utilities with non-negotiable trust boundaries:

1. **Deterministic Verdict Authority**:
   - The AI subsystem has zero authority to overturn or weaken deterministic CI/CD verdicts (`ResultState.INCOMPATIBLE`).
   - If an AI provider attempts to assert a drift is non-breaking contrary to the deterministic compatibility engine, the invariant enforcement layer immediately reverts the verdict and flags the anomaly.

2. **Zero Row-Data Leakage Guarantee**:
   - Zero physical row records or cell values are ever read or transmitted.
   - Only structural AST schemas, Arrow data types, nullabilities, metadata keys/values, and deterministic error codes are serialized.
   - External HTTPS requests only occur when a remote provider (`--provider openai`) is explicitly configured with an API key.

3. **Contract Evolution vs. Physical Data Migration**:
   - `propose_contract_evolution` deterministically validates schema contract compatibility in memory.
   - `generate_migration_plan` synthesizes executable PyArrow scripts for operator review and manual execution; Parquet Guard never executes unreviewed batch disk writes against production datasets.

4. **Offline Evaluation vs. Live Frontier Models**:
   - The expanded 64 synthetic benchmark scenarios (40 dev + 24 held-out) run under `--provider mock` evaluate offline harness integrity, XML finding parsing, citation validity, and fallback logic against deterministic ground truth.
   - Live frontier model benchmarking requires explicit opt-in and live provider configuration.

5. **Agent Tools & Model Context Protocol (MCP) Server**:
   - `parquet_guard.ai.agent_tools` provides OpenAI-compatible function calling schemas and an in-process tool dispatcher.
   - `parquet_guard.mcp` provides a fully compliant Model Context Protocol (MCP) daemon communicating over stdio JSON-RPC 2.0 with path traversal sandboxing and 7 core analysis tools.

---

## 11. Consumer-Aware Schema Intelligence & Topology Grouping

### 11.1 Downstream Consumer Contracts
Data contracts must reflect actual downstream reader expectations rather than assuming identical requirements across all consumers:
- **`ConsumerContract`**: Encapsulates consumer identifier, human name, target dataset, required fields, and compatibility policies.
- **Policies**: `strict` (enforces exact type matching and forbids undeclared columns) vs `permissive` (permits safe unidirectional widening and optional extra columns).
- **`evaluate_consumer_impact()`**: Deterministically maps producer schema changes to downstream consumer outcomes, categorizing consumers as COMPATIBLE, CONDITIONALLY_COMPATIBLE, or INCOMPATIBLE.

### 11.2 Dataset Topology & Schema Grouping
- **`analyze_dataset_topology()`**: Analyzes multi-partition datasets across directories, grouping files sharing identical canonical fingerprints without reading data pages.
- **Canonical Cluster Identification**: Automatically determines the dominant schema group.
- **Divergence Concentration**: Surfaces specific columns causing schema drift across partitions with file counts and breaking status.

### 11.3 Verified Synthetic Migration Sandbox
- **`verify_migration_plan()`**: Executes proposed `ExecutableMigrationPlan` transformations on representative synthetic Parquet tables within isolated temporary directory environments.
- **Invariants Checked**: Row-count preservation, non-null value retention, type truncation/overflow prevention via `safe=True` PyArrow casting, and post-migration consumer compatibility verification.

### 11.4 Standalone Technical HTML Reporting
- **`generate_html_diagnostic_report()`**: Emits single-file, zero-CDN, offline-safe HTML reports containing SVG consumer impact graphs, cluster topology tables, schema diff matrices, and sandbox execution logs.

