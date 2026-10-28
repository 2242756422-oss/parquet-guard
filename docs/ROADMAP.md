# Parquet Guard AI — Project Roadmap

## Milestone Overview

### ✅ v0.3.0a1 — Alpha Foundation
- Zero-data-scan Parquet metadata extraction.
- Deterministic SchemaContract format.
- Permissive vs Strict schema comparison.
- Primitive & nested Arrow type checking.
- GitHub Actions CI/CD integration.
- Offline synthetic AI evaluation harness.

### ✅ v0.4.0a1 — Consumer-Aware Schema Intelligence
- Downstream consumer contracts (`ConsumerContract`, `ConsumerGroupSpecification`).
- Multi-consumer impact evaluation (`parquet-guard impact`).
- Dataset-wide schema topology grouping.
- Sandboxed schema migration verification (`verify_migration_plan`).
- Interactive standalone HTML diagnostic reporting.
- Model Context Protocol (MCP) server over stdio.

### 🚀 v0.5.0a1 (Project Atlas) — Real-World Schema Reliability & Interoperability
- **Empirical Engine Verification**:
  - Direct execution probes across PyArrow, DuckDB, and Polars (`src/parquet_guard/verification/`).
  - Runtime vs static metadata rule disagreement classification (`RULE_OPTIMISTIC` vs `RULE_CONSERVATIVE`).
  - CLI command: `parquet-guard verify-engine`.
- **Consumer Contracts 2.0**:
  - `contract_direction`: `BACKWARD`, `FORWARD`, `FULL`.
  - Independent criticality (`CRITICAL`, `STANDARD`, `INFORMATIONAL`) vs technical status.
  - Explicit field renames (`field_renames`) avoiding heuristic guessing.
- **Cache Hardening & Storage Abstraction**:
  - Fast O(1) footer SHA-256 checksum validation.
  - Distinguishes `HEURISTIC` (size/mtime) vs `HIGH_RELIABLE` (checksum/ETag).
  - Pluggable storage abstraction (`LocalStorageBackend`, `MemoryStorageBackend`, `RemoteStorageBackend`).
- **Sandboxed Migration 2.0**:
  - Mode 1 (Synthetic) boundary data generation.
  - Mode 2 (User Sample) with mandatory authorization opt-in (`user_data_authorized=True`).
- **Developer Integration**:
  - Pytest assertion helpers & fixture (`parquet_guard`).
  - Extended MCP server exposing `verify_engine_compatibility`.
  - End-to-end real-world pipeline demo and fair statistical benchmarks.

### 🔮 Future Directions (v0.6.0+ & v1.0.0 GA)
- Distributed query engine adapters: Spark / Trino / Presto remote executor probes.
- Open table format schema evolution connectors: Apache Iceberg, Delta Lake, and Apache Hudi.
- Streaming schema guard: Kafka / Redpanda zero-scan schema barrier checks.
- Pre-commit git hook and GitHub App for automated pull request comments.
