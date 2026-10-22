# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Planned
- Native GitHub Action wrapper (`2242756422-oss/parquet-guard-action`).
- Remote cloud storage streaming footer audit (AWS S3, Google Cloud Storage, Azure Blob).

## [0.2.0-alpha] - 2026-10-22

### Fixed
- **Composite GitHub Action Security & Reliability**: Hardened `.github/actions/parquet-guard/action.yml` by eliminating shell command injection risks (passing all inputs through safe environment variables with double-quoted arrays), adding prerequisite/pip installation checks, properly writing the `verdict` output to `$GITHUB_OUTPUT`, and ensuring valid SARIF 2.1.0 diagnostics are generated even when breaking schema regressions occur (without `|| true` exit code swallowing).
- **Temporal Unit Widening & Overflow Risk Policy**: Resolved latent int64 overflow risks when widening timestamp and duration units. Acknowledged that zero-data-scan schema inspection cannot verify value-level bounds without raw data scans. Permissive mode now flags temporal precision widening (`s -> ms -> us -> ns`) with a `TEMPORAL_OVERFLOW_RISK` warning and explicit dynamic range guidance (e.g. `ns` limited to 1677–2262 AD and ±292 years for duration), while strict mode rejects all temporal widening as breaking.
- **Incremental Audit Cache State Leakage**: Removed dataset-relative `is_divergent` flags from per-file cached manifests (`CachedFileInfo`). Consistency is now dynamically evaluated across the active set of files in the current audit, preventing stale divergence states when offending files are deleted or modified.
- **Corrupted Cache Resilience & Auto-Recovery**: Guarded against corrupted, truncated, or incompatible JSON cache files on disk. `load_audit_cache()` safely falls back to a clean full audit without crashing, and `save_audit_cache()` atomically overwrites the cache with valid metadata.
- **Portable Relative Cache Lookups**: Enabled dual-resolution cache key lookups supporting both absolute paths and dataset-relative paths, allowing cached audit manifests to remain valid across varying working directory prefixes or containerized CI mount points.
- **Bracket-Aware Map Type Parsing**: Replaced naive comma regex in `_parse_map_params` with a bracket-depth aware parser that cleanly handles `keys_sorted` attributes and nested values (e.g. `map<string, list<int32>, keys_sorted>`), ensuring safe widening rules evaluate correctly.
- **Decimal Scale Invariance**: Prohibited decimal scale shifts in `is_safe_widening` (enforcing `s_old == s_new`). In Parquet physical storage, values are stored as unscaled integers; scale modifications alter the numeric magnitude and are strictly breaking.
- **PyArrow Container Fallbacks**: Added fallback to `value_type` on `FixedSizeListType` and `ListType` when `value_field` is absent across Arrow binding variants.
- **Python 3.9 Annotations**: Added `from __future__ import annotations` across all test modules to ensure clean syntax compatibility across Python 3.9 through 3.13.
- **SARIF 2.1.0 Rules & URLs**: Expanded `tool.driver.rules` in SARIF exports to cover all known error codes (including `TEMPORAL_OVERFLOW_RISK`), and aligned repository URLs to `https://github.com/2242756422-oss/parquet-guard`.

### Added
- **Parquet Guard AI — Evidence-Grounded Schema Evolution Copilot**:
  - `explain` CLI command: Diagnoses schema drift with grounded citations to deterministic findings (`F001`, `F002`), operational consequences across DuckDB/PyArrow/Spark, and candidate remediations with zero raw data row access.
  - `propose-migration` CLI command: Generates proposed SchemaContract updates and executable PyArrow transformation code, deterministically validated against the target schema before reporting.
  - `eval-ai` CLI command: Reproducible benchmark evaluation suite running across 12 synthetic Arrow schema evolution scenarios, measuring factual consistency, finding accuracy, evidence citation validity, and prompt-injection defense.
  - Provider abstraction: `BaseAIProvider`, `MockAIProvider` (offline, deterministic for testing/CI), and `OpenAICompatibleProvider` (pure Python urllib, zero mandatory external AI dependencies).
  - Agent and tool-use interface: `parquet_guard.ai.agent_tools` providing JSON tool definitions for coding agents.
  - End-to-end runnable demonstration: `examples/ai_pipeline_copilot/run_copilot_demo.py`.
- **Temporal Boundary & Action Integration Test Suites**: Added `tests/test_temporal_boundaries.py`, `tests/test_action_integration.py`, and `tests/test_ai_subsystem.py`. Total automated test suite expanded to **104 tests (100% pass rate)**.
- **Deep Arrow Type System Extensions**:
  - Unsigned integer safe widening respecting bit-depth constraints (`uint8 -> uint16/32/64/int16/32/64/float32/64`, `uint16 -> uint32/64/int32/64/float32/64`, `uint32 -> uint64/int64/float64`, strictly forbidding `uint64 -> int64` and `uint64 -> float64`).
  - Duration unit precision widening (`duration[s] -> duration[ms] -> duration[us] -> duration[ns]`).
  - Union type child extraction (`dense_union`, `sparse_union`).
  - Maximum recursion depth limit (`max_depth=100`) preventing `RecursionError` crashes on pathological or circular schemas.
- **CI/CD & Developer Experience Integrations**:
  - Reusable composite GitHub Action definition in `.github/actions/parquet-guard/action.yml`.
  - Pre-commit hook configuration in `.pre-commit-hooks.yaml`.
  - Standalone, executable end-to-end demonstration in `examples/quickstart_demo.py`.
- **Fair Empirical Benchmark Suite with Dispersion Reporting**:
  - Enhanced `benchmarks/fair_benchmark.py` with 5 warmup rounds and repeated statistical sampling reporting mean, median, min, max, and standard deviation.
  - Automatically exports raw logs to `benchmarks/raw_benchmark_output.txt` and metrics to `benchmarks/benchmark_results.json`.
  - Added architectural clarification differentiating metadata footer reads from full-table payload ingestion.
- **Deep Structural Type Equality**:
  - `FixedSizeList` list length verification (`list_size`). Structural size mismatches (e.g. vector embedding dimensions 512 vs 768) are classified as `BREAKING` with `CONTAINER_PARAM_CHANGED`.
  - `Dictionary` ordered flag tracking (`ordered`). Semantics mismatch triggers `BREAKING` in strict mode and `WARNING` in permissive mode (`ORDERED_FLAG_CHANGED`).
  - `Map` key order flag tracking (`keys_sorted`). Flag change triggers `BREAKING` in strict mode and `WARNING` in permissive mode (`KEYS_SORTED_CHANGED`).
  - `Decimal` scale and precision shift detection (`DECIMAL_SCALE_MISMATCH`). Prevents silent precision loss or misaligned arithmetic in financial workflows.
  - `Timestamp` timezone and resolution mismatch detection (`TIMESTAMP_TIMEZONE_MISMATCH`).
- **Experimental Incremental Dataset Audit Cache**:
  - Implemented `DatasetAuditCache` in `src/parquet_guard/core/cache.py` with atomic write semantics (`.parquet_guard_cache.json`).
  - Tracks partition `mtime_ns` and `file_size_bytes` and caches parsed `SchemaContract` definitions to bypass redundant footer deserialization on unmodified files.
  - Added CLI options `--incremental`, `--force-full`, and `--cache-path` to `parquet-guard audit`.
- **OASIS SARIF 2.1.0 Native Reporting**:
  - Added SARIF export functions `format_diff_sarif` and `format_audit_sarif` in `src/parquet_guard/core/reporter.py`.
  - Added `--format sarif` support to `parquet-guard check`, `parquet-guard diff`, and `parquet-guard audit` commands.
- **Contract Schema Validation & Integrity**:
  - Added `parquet-guard validate-contract` CLI command and `validate_contract()` API in `src/parquet_guard/core/inspector.py`.
  - Enforced strict contract structure (`SUPPORTED_VERSIONS = {"1.0"}`, `extra="forbid"`, mandatory non-empty column fields, strict duplicate column rejection).
- **ResultState State Machine**:
  - Unified operational outcomes across all audit and comparison operations: `VALID`, `INVALID`, `DRIFTED`, `COMPATIBLE`, `INCOMPATIBLE`, `INDETERMINATE`, and `INSPECTION_ERROR`.

## [0.1.2] - 2026-10-22

### Fixed
- **Nested Container Compatibility**: Disentangled parent container type matching from child recursion in `analyze_compatibility`. Eliminated contradictory bug where child field widening (e.g. `struct<a: int32>` -> `struct<a: int64>`) was marked `SAFE` while parent container was marked `BREAKING`.
- **Recursive Duplicate Column Detection**: Extended duplicate column detection across all schema hierarchy levels (`find_all_duplicate_paths()`). Detects duplicate child fields within Struct and nested containers with exact hierarchical paths (e.g., `meta.dup_col`), preventing downstream crash risks in DuckDB/Polars.
- **Permissive Drift & Warning Transparency**: Un-hid schema drift in permissive mode. When `check_field_order=True` or `check_metadata=True`, drift details are populated as `[WARNING]` in `divergence_details`, `warnings`, and `drifts`, preventing silent suppression while maintaining backward compatibility.
- **Global Incompatibility Diagnostics**: When no universal baseline schema satisfies all dataset partitions under permissive evolution, explicitly diagnoses mutually incompatible partition pairs (`s1 <-> s2`) and recommends `--contract` instead of arbitrarily blaming files.

### Added
- `--fail-on-warnings` flag to `parquet-guard audit` CLI, allowing CI/CD workflows to strictly gate on non-breaking schema drift or warnings.
- Structured compatibility properties `is_compatible`, `has_drift`, `warnings_count`, and `breaking_count` on `PartitionAuditResult`.

## [0.1.1] - 2026-10-22

### Fixed
- **Partition Audit Directionality & Permutation Invariance**: Eliminated file-ordering and frequency tie-break sensitivity in permissive baseline selection (`A_int32 + B_int64` vs `A_int64 + B_int32` now produce strictly consistent, deterministic results). Ungrounded datasets identify canonical baselines via mutual compatibility graph evaluation.
- **Duplicate Column Detection**: Parquet files with duplicate column names at any position are strictly rejected across all auditing flows with exit code 1, even if all files in the dataset share the identical duplicate columns.
- **Deep Arrow Nested Types**: Child fields for Struct, List, and Map fully retain child nullability, child-level metadata, and child ordering without string truncation.
- **Recursive Canonical Fingerprint**: `compute_schema_fingerprint` recursively hashes nested child structure and metadata using deterministic SHA-256.

### Added
- Explicit baseline contract option (`-c` / `--contract` / `-b` / `--baseline`) for `parquet-guard audit` command and `audit_dataset(baseline_contract=...)` API.

## [0.1.0] - 2026-10-22

### Added
- Zero-data-scan Parquet metadata footer inspector (`parquet-guard inspect`).
- Golden schema contract generation (`parquet-guard init-contract`).
- Schema validation against contracts with permissive and strict modes (`parquet-guard check`).
- Direct schema diffing between two Parquet files (`parquet-guard diff`).
- Multi-partition directory dataset audit engine (`parquet-guard audit`).
- Semantic compatibility algebra supporting unidirectional type widening.
