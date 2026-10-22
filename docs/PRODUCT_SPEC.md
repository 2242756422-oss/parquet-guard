# `parquet-guard` Product Specification

> **Version**: 0.2.0-alpha
> **Status**: Approved Alpha Release
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
| **Great Expectations / Soda** | Heavyweight, complex configuration, requires scanning full dataset rows into memory; too slow for high-frequency CI and pre-commit hooks. |
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
- No network requests during validation (100% offline, zero security exfiltration vector).
- Path traversal defense: validates and normalizes all relative file paths.
- Path globs sanitized to prevent exponential regex explosion.
