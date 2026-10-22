# Guarded Parquet Data Pipeline Integration Example

> **Notice on Adoption Evidence**:  
> This directory contains an **in-tree reference integration example and regression test** authored by the `parquet-guard` maintainer to demonstrate canonical pipeline usage. It is strictly categorized as a **reference implementation**, not external third-party adoption.

---

## Architecture Overview

In a typical production data lake or Lakehouse architecture (DuckDB, Spark, Polars, Trino), upstream data services write daily Parquet batches. If an upstream service accidentally alters a column type or drops a field, downstream jobs scanning the partitioned folder crash with cryptic C++ type errors.

This reference pattern implements a **Guarded Ingestion Gate**:

```
[Incoming Batch] ──► [parquet-guard verify] ──► (Breaking?) ──► [Quarantine + Alert Markdown]
                                    │
                                 (Safe?)
                                    ▼
                         [Active Partition Lake] ──► [Clean Downstream Analytics]
```

### Key Workflow Steps
1. **Schema Extraction**: Reads the 5-20KB metadata footer of the incoming file via `inspect_schema`.
2. **Contract Verification**: Compares the batch schema against `contracts/events.schema.json` via `compare_schemas(..., mode="permissive")`.
3. **Quarantine On Drift**: If breaking changes exist, the pipeline halts ingestion for that partition, writes the corrupted file to a quarantine folder, and generates a GitHub-flavored Markdown alert (`alert.md`).
4. **Permissive Evolution**: If new columns are nullable or types undergo safe widening (`int32 -> int64`), the batch is accepted seamlessly.

---

## Running the Integration Test

```bash
PYTHONPATH=. python3 examples/data_pipeline_guard/test_pipeline.py
```
