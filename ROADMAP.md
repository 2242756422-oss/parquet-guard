# Project Roadmap: `parquet-guard`

This document tracks completed milestones, current Alpha capabilities, and planned architectural features for `parquet-guard`.

---

## Current Status: v0.2.0 (Alpha)

- [x] **Zero-Data-Scan Footer Inspection**: Pure metadata inspection reading trailing Parquet bytes via PyArrow without decoding data pages.
- [x] **Declarative Golden Contracts**: JSON `SchemaContract` (v1.0) generation, round-trip serialization, and syntax validation (`init-contract`, `validate-contract`).
- [x] **Directional Compatibility Algebra**: Formal classification of breaking regressions vs safe unidirectional type widenings (e.g. `int32` -> `int64`, string widening).
- [x] **Deep Structural Type Equality**:
  - `FixedSizeList` list length verification (`list_size`).
  - `Dictionary` ordered sorting flag tracking (`ordered`).
  - `Map` key order flag tracking (`keys_sorted`) with bracket-aware parser.
  - `Decimal` scale and precision invariance (prohibiting unscaled integer shifts).
  - `Timestamp` timezone and resolution mismatch detection.
  - Deep nested `Struct`, `List`, and `Map` child field order and metadata retention.
- [x] **Deterministic Partition Auditor**: Permutation-invariant consistency checking across directory trees with mutual compatibility evaluation.
- [x] **Experimental Incremental Auditing**: Local manifest caching (`.parquet_guard_cache.json`) with mtime/size validation, avoiding stale dataset-relative divergence leakage.
- [x] **OASIS SARIF 2.1.0 Reporting**: Machine-readable vulnerability and code-scanning reports integrating with GitHub Code Scanning (`upload-sarif`).
- [x] **Multi-format Reporting**: Rich ANSI terminal, GitHub-Flavored Markdown for PR comments, raw JSON, and SARIF 2.1.0.

---

## Planned for v0.3.0 (Beta)

- [ ] **Native GitHub Action Wrapper**: Publish `2242756422-oss/parquet-guard-action` on GitHub Marketplace for one-line workflow integration.
- [ ] **Official Pre-commit Hook**: Publish `.pre-commit-hooks.yaml` for pre-commit linting in developer repositories.
- [ ] **Cloud Storage Direct Footer Streaming**: Support `s3://`, `gs://`, and `az://` URIs by leveraging HTTP range requests to download only trailing footer bytes without transferring entire objects.
- [ ] **Statistical Metadata Drift Detection**: Monitor min/max/null_count shifts recorded in Parquet column statistics across partitions.
- [ ] **OpenDataContract Interoperability**: Support import/export of OpenDataContract standard YAML definitions.

---

## Known Current Limitations (Alpha)

1. **Local and Mounted Filesystems Only**: Cloud storage URIs (`s3://`, `gs://`) require mounting (e.g. via FUSE or AWS S3 mount) or pre-downloading files locally.
2. **Incremental Auditing is Experimental**: Incremental caching relies on filesystem `mtime_ns` and `file_size_bytes`. In environments where file modification times are not preserved (e.g. fresh `git clone` or ephemeral CI runners without persistent cache mounts), full audits should be used (`--force-full`).
3. **In-Memory Schema Contracts**: While Parquet files are never scanned into memory, contract ASTs and partition file metadata are held in Python process memory. Auditing millions of partition files in a single invocation will require paging or streaming aggregators.
