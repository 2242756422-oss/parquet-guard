# Project Roadmap: `parquet-guard`

This document tracks planned architectural features, community milestones, and integration targets for `parquet-guard`.

---

## Phase 1: MVP & Core Validation (v0.1.0 - Current)
- [x] Zero-data-scan Parquet footer inspection engine.
- [x] Declarative JSON SchemaContract specification.
- [x] Backward-compatibility rules (safe widening vs breaking narrowing).
- [x] Multi-partition dataset consistency auditor.
- [x] CLI commands (`inspect`, `check`, `diff`, `audit`, `init-contract`).
- [x] Terminal, Markdown, and JSON reporting formats.
- [x] Comprehensive test suite & packaging with `uv build`.

---

## Phase 2: Ecosystem Integration (v0.2.0 - Q4 2026)
- [ ] **Official Pre-commit Hook**: Publish `.pre-commit-hooks.yaml` for zero-install developer checks.
- [ ] **GitHub Action Wrapper**: Publish `liujingye/parquet-guard-action` on GitHub Marketplace with automated PR commenting.
- [ ] **Cloud Storage Direct Footer Streaming**: Support `s3://`, `gs://`, and `az://` URIs by leveraging HTTP range requests to download only the trailing 32KB footer without downloading entire remote files.
- [ ] **Polars & DuckDB Plugin / Macro**: Provide native SQL macros (`SELECT * FROM parquet_guard_check(...)`).

---

## Phase 3: Advanced MLOps & Enterprise Features (v0.3.0 - Q1 2027)
- [ ] **Data Contract Spec (OpenDataContract) Interoperability**: Support import/export of Linux Foundation OpenDataContract YAMLs.
- [ ] **Statistical Metadata Drift Detection**: Detect min/max outlier shifts recorded in Parquet column statistics across partitions.
- [ ] **Codex-Assisted Automated PR Review Bot**: Leverage OpenAI Codex API credits to automatically generate pull request fix suggestions when schema drift is detected.
