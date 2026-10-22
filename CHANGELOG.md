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
- **Incremental Audit Cache State Leakage**: Removed dataset-relative `is_divergent` flags from per-file cached manifests (`CachedFileInfo`). Consistency is now dynamically evaluated across the active set of files in the current audit, preventing stale divergence states when offending files are deleted or modified.
- **Bracket-Aware Map Type Parsing**: Replaced naive comma regex in `_parse_map_params` with a bracket-depth aware parser that cleanly handles `keys_sorted` attributes and nested values (e.g. `map<string, list<int32>, keys_sorted>`), ensuring safe widening rules evaluate correctly.
- **Decimal Scale Invariance**: Prohibited decimal scale shifts in `is_safe_widening` (enforcing `s_old == s_new`). In Parquet physical storage, values are stored as unscaled integers; scale modifications alter the numeric magnitude and are strictly breaking.
- **PyArrow Container Fallbacks**: Added fallback to `value_type` on `FixedSizeListType` and `ListType` when `value_field` is absent across Arrow binding variants.
- **Python 3.9 Annotations**: Added `from __future__ import annotations` across all test modules to ensure clean syntax compatibility across Python 3.9 through 3.13.
- **SARIF 2.1.0 Rules & URLs**: Expanded `tool.driver.rules` in SARIF exports to cover all known error codes, and aligned repository URLs to `https://github.com/2242756422-oss/parquet-guard`.

### Added
- **Adversarial & Metamorphic Test Suite**: Added `tests/test_adversarial_v020_alpha.py` verifying incremental cache resilience under file deletion, bracket-aware Map parsing, decimal scale preservation, contract error handling, and CLI SARIF export. Total automated test suite expanded to **70 tests (100% pass rate)**.
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
