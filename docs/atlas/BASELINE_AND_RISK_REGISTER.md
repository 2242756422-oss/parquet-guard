# Project Atlas — Baseline & Risk Register

> **Project Name**: Parquet Guard AI (`parquet-guard`)  
> **Iteration Codename**: Project Atlas — Real-World Schema Reliability & Interoperability  
> **Starting Baseline**: `v0.4.0a1` (git commit `78e5cca`, merged PR #5)  
> **Public Distribution**: [PyPI parquet-guard 0.4.0a1](https://pypi.org/project/parquet-guard/0.4.0a1/)  
> **Active Branch**: `feat/project-atlas-real-world-interoperability`  
> **Target Release Candidate**: `v0.5.0a1` (Strict Gate: **Must NOT tag or publish without passing full review**)  

---

## 1. Verified Starting Baseline

| Metric | Status / Value | Verification Command & Timestamp |
| :--- | :--- | :--- |
| **Git HEAD** | `78e5cca` (Merge PR #5 into `main`) | `git log -1 --oneline` (2026-10-27) |
| **Base Tests** | **164 / 164 passed** (100%) in 7.65s | `.venv/bin/pytest -q` |
| **Linter** | Clean (0 errors across 88 files) | `.venv/bin/ruff check .` |
| **Type Checker** | Clean (0 issues across 46 source files) | `.venv/bin/mypy --explicit-package-bases src tests` |
| **Package Matrix** | 15 OS × Python 3.9–3.13 matrix jobs | Verified passing on GitHub Actions |
| **Core Capabilities** | - Zero-row-scan footer inspection<br>- Pydantic AST schema contracts & diffs<br>- Partition dataset auditor & fingerprinting<br>- Dataset topology grouping & divergence concentration<br>- Consumer-aware contract evaluation (permissive & strict)<br>- Synthetic migration verification sandbox<br>- Standalone interactive single-file HTML reporter<br>- Stdio JSON-RPC 2.0 MCP server (7 tools)<br>- 64-scenario offline AI evaluation framework |

---

## 2. Risk Register & Known Technical Boundaries

| Risk ID | Category | Description | Severity | Mitigation & Atlas Resolution |
| :--- | :--- | :--- | :---: | :--- |
| **RSK-01** | Rule vs. Runtime Disparity | Metadata-only schema comparison may predict compatibility (e.g. decimal widening or temporal shifts), but downstream query engines (DuckDB, Polars, PyArrow) may fail at query/ingest time due to engine-specific casting semantics or partition unification behaviors. | **P0** | Implement explicit **Engine Verification Layer** (`src/parquet_guard/verification/`) with PyArrow, DuckDB, and Polars adapters; report structured `metadata_verdict` vs. `runtime_verdict` (`PASS`, `FAIL`, `INCONCLUSIVE`, `NOT_RUN`, `UNSUPPORTED`, `ERROR`). |
| **RSK-02** | Synthetic Sandbox vs. Production Reality | Passing a synthetic migration sandbox does not guarantee production safety when value distributions or unannounced business rules are involved. | **P0** | Establish two explicit modes: Mode 1 (Metadata/Synthetic default, no data row read) vs Mode 2 (User-authorized sample validation); classify unprovable semantic operations as `REQUIRES_BUSINESS_SEMANTICS`. |
| **RSK-03** | Caching Invalidation Hazards | Heuristic cache key `mtime_ns + file_size` fails to detect in-place file modifications on file systems where timestamps are coarse or reset during git/object-storage transfers. | **P1** | Distinguish between reliable content hashes / ETags / version IDs vs. heuristic mtime+size in `DatasetAuditCache`; provide clear cache confidence levels and cold-audit fallback. |
| **RSK-04** | Privacy & Metadata Disclosure | While zero data rows are scanned by default, Parquet footers and schema metadata may contain sensitive schema definitions or user comments. | **P0** | Strict isolation: zero row data transmission, explicit opt-in for remote AI prompts, redacting metadata unless authorized, and zero secret leakage in CI/HTML reports. |
| **RSK-05** | Overstated AI Evaluation Claims | The 64-scenario AI benchmark runs with `MockAIProvider` offline, which measures test harness integrity, not live LLM reasoning accuracy. | **P1** | Explicitly label evaluation outputs as `MOCK HARNESS (OFFLINE)` vs `LIVE PROVIDER EVAL`; eliminate any claim of "100% LLM accuracy". |
| **RSK-06** | Supply Chain & Dependency Bloat | Requiring heavyweight database engines for core installation breaks lightweight CI/CD usage. | **P0** | Keep `duckdb` and `polars` as optional extras (`[project.optional-dependencies] engines = [...]`); core package remains ultra-lightweight pure Python + PyArrow. |

---

## 3. Architecture & Module Plan

```
src/parquet_guard/
  core/                            # Existing core metadata & contract modules
    inspector.py
    compatibility.py
    consumer.py                    # Enhanced in Atlas (Directional contracts, strict/permissive)
    auditor.py
    grouping.py
    cache.py                       # Enhanced in Atlas (ETag / hash / identity support)
    migration_verifier.py          # Enhanced in Atlas (Mode 1 vs Mode 2)
    html_reporter.py               # Enhanced in Atlas (Engine verification matrix)
  verification/                    # NEW: Real-world engine verification layer
    models.py                      # EngineProbeRequest, EngineProbeResult, EngineVerificationReport
    adapters.py                    # BaseEngineAdapter interface & engine normalizer
    pyarrow_engine.py              # PyArrow dataset read & cast probe
    duckdb_engine.py               # DuckDB read_parquet & union probe
    polars_engine.py               # Polars scan_parquet & collect probe
    runner.py                      # Multi-engine probe runner & rule-vs-runtime discrepancy reporter
  storage/                         # NEW: Storage abstraction & reliable cache invalidation
    backends.py                    # Local, object-storage (emulator-tested), and fsspec-compatible
  ai/                              # Offline evaluator, explainer, normalizer, models
  mcp/                             # Stdio MCP server (hardened with path traversal & bounds checks)
  testing/                         # NEW: Pytest helpers & fixtures for downstream pipeline developers
```

---

## 4. Rollback Strategy & Safety Invariants

1. **Private Directory Protection**: `application/`, `community/`, `research/` must remain untracked and never committed or archived.
2. **Backward Compatibility**: All existing CLI commands (`check`, `diff`, `audit`, `impact`, `verify-migration`, `report`, `serve-mcp`, `eval-ai`) and Python API signatures must remain fully supported.
3. **No Premature Tagging**: Branch `feat/project-atlas-real-world-interoperability` will be submitted as a GitHub Pull Request against `main`. Release tagging for `v0.5.0a1` is strictly reserved for post-PR merge maintainer action.
