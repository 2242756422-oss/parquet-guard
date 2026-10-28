# Project Atlas: Corrective Engineering & Reliability Verification Report

**Branch:** `feat/project-atlas-real-world-interoperability`  
**Base:** `main` (Tag: `v0.4.0a1`)  
**Target Pull Request:** PR #6 (`https://github.com/2242756422-oss/parquet-guard/pull/6`)  
**Status:** Verification Complete — **READY FOR HUMAN MAINTAINER REVIEW**  
**Release Action:** DO NOT AUTO-MERGE · DO NOT PUBLISH TAGS · PRESERVE PRIVATE MATERIALS  

---

## 1. Executive Summary

During the hardening review of PR #6 (Project Atlas — real-world schema reliability and query engine interoperability), rigorous adversarial testing identified three critical P0 reliability defects and four P1 semantic flaws:

1. **P0-A (Vacuum PASS Bug):** When target engines were uninstalled, unsupported, or when `operations=[]`, the verification runner and computed property `overall_compatible` defaulted to boolean `True` due to Python's vacuous truth behavior on empty sets. Furthermore, CLI exit codes permitted `0` exit on execution failures and inconclusive probes.
2. **P0-B (Unbounded In-Memory Materialization):** PyArrow and Polars runtime probe adapters and migration sandbox verification loaded entire multi-gigabyte Parquet files into memory before applying `.slice(0, row_limit)`, defeating the declared row limits and risking out-of-memory crashes on large partitions.
3. **P0-C (Truncated 16KB Footer Checksum):** The incremental audit cache's `compute_footer_checksum` only read the tail 16KB of the file. In real-world schemas with 100+ columns or extensive row-group statistics (where Parquet metadata footers exceed 50–100KB), modifications to early column descriptors or metadata outside the tail 16KB produced the identical SHA-256 hash, causing false-positive `HIGH_RELIABLE` cache hits.
4. **P1-A (DuckDB `TRY_CAST` Silent NULL Masking):** DuckDB's `probe_cast` utilized `TRY_CAST(col AS type)`. When conversion failed on corrupt or non-convertible strings (e.g. `'abc'` to `INT32`), DuckDB returned NULL without throwing an exception. The query succeeded, resulting in an erroneous `RuntimeVerdict.PASS`.
5. **P1-B (Disagreement Scope Blindspots):** `classify_disagreement` was only wired to `READ` operations; runtime failures in `CAST` and multi-partition `SCHEMA_UNION` were ignored by `RuleVsRuntimeDisagreement`, leaving `--fail-on-disagreement` unable to block CI on dangerous CAST or UNION breaks.
6. **P1-C (Consumer Contracts 2.0 Criticality & Direction Inactivity):** `contract_direction` (`BACKWARD`, `FORWARD`, `FULL`) and consumer-level `criticality` (`INFORMATIONAL`, `STANDARD`, `CRITICAL`) were present as passive data fields without enforcing runtime behavior. `INFORMATIONAL` consumers unexpectedly blocked CI pipelines on non-critical breaks.
7. **P1-D (Storage & Sandboxing Inconsistencies):** `RemoteStorageBackend` raised `TypeError` when parsing datetime `mtime` objects returned by cloud drivers (`s3fs`, `gcsfs`), and MCP did not enforce explicit data authorization before executing row-level engine probes.

All defects were isolated with reproduction tests (`tests/test_atlas_pr6_reproductions.py`, checkpoints QA-01 to QA-20), repaired with verified zero-regression implementations, and fully passed against the complete 233-test regression suite.

---

## 2. Defect Inventory, Root Causes, and Corrective Fixes

### 2.1 P0-A: Elimination of Vacuous PASS & CLI Exit Code Contract

#### Defect Reproduction
- `verify_engine_compatibility(target, engines=["nonexistent"], operations=[READ])` returned `runtime_verdict=RuntimeVerdict.PASS` and `overall_compatible=True`.
- `verify_engine_compatibility(target, engines=["pyarrow"], operations=[])` returned `runtime_verdict=RuntimeVerdict.PASS` and `overall_compatible=True`.
- `classify_disagreement(MetadataVerdict.ERROR, RuntimeVerdict.FAIL)` returned `AGREEMENT`.
- `parquet-guard verify-engine` exited `0` even when no probes ran or when probes errored.

#### Root Cause
In `EngineVerificationReport.overall_compatible`:
```python
# Old flawed code:
runtime_ok = all(
    r.runtime_verdict in (RuntimeVerdict.PASS, RuntimeVerdict.NOT_RUN, RuntimeVerdict.UNSUPPORTED)
    for r in self.engine_results
)
```
When `self.engine_results` was empty or contained only `UNSUPPORTED` entries, `all(...)` evaluated to `True` (vacuous truth in Python). In addition, `runner.py` synthesized `PASS` whenever `[v for v in all_runtime_verdicts if v != UNSUPPORTED]` was empty.

#### Fix Implemented
1. **Defensive Aggregation (`src/parquet_guard/verification/runner.py`):**
   - If `all_runtime_verdicts` is empty or no operations were probed: `RuntimeVerdict.NOT_RUN`.
   - If all probes are `UNSUPPORTED`: `RuntimeVerdict.UNSUPPORTED`.
   - `RuntimeVerdict.PASS` is synthesized **strictly** if at least one executed probe ran and `all(v == PASS for v in executed_verdicts)`.
2. **Fail-Closed `overall_compatible` (`src/parquet_guard/verification/models.py`):**
   - Requires `meta_ok` (`COMPATIBLE` or `WARNING`).
   - Requires non-empty `engine_results`.
   - Requires `any(r.runtime_verdict == PASS)`.
   - Forbids any `FAIL` or `ERROR`.
   - Requires `runtime_verdict == PASS` and `not has_rule_optimistic_disagreement`.
3. **Execution Scope Transparency:** Added `requested_engines`, `executed_engines`, `skipped_engines`, `requested_operations`, and `executed_operations` to `EngineVerificationReport`.
4. **Disagreement Classification:** Metadata `ERROR` or runtime `ERROR`/`UNSUPPORTED` now strictly classifies as `INCONCLUSIVE`.
5. **CLI Exit Code Contract (`src/parquet_guard/cli.py`):**
   - Exit `0`: `overall_compatible is True` (safe metadata + all executed probes passed).
   - Exit `1`: `overall_compatible is False` or unhandled `RULE_OPTIMISTIC` disagreement.
   - Exit `2`: CLI argument errors or fatal system exceptions.

---

### 2.2 P0-B: Bounded Reads & Authorization Boundaries

#### Defect Reproduction
Reading a 1,000,000-row file with `row_limit=5` in PyArrow or Polars called `pq.read_table(p)` or `pl.read_parquet(p)` first, materializing all 1,000,000 rows into memory before slicing.

#### Root Cause
Missing streaming and projection iterators in engine adapters.

#### Fix Implemented
1. **PyArrow Adapter (`src/parquet_guard/verification/pyarrow_engine.py`):**
   - Replaced `pq.read_table(p)` with `pq.ParquetFile(p).iter_batches(batch_size=row_limit, columns=expected_columns)`.
   - Evaluates streaming batch chunks and breaks immediately once `row_limit` rows are materialized.
   - Traverses nested Arrow schemas (`user.email`) to detect nested column drops without full data reads.
2. **Polars Adapter (`src/parquet_guard/verification/polars_engine.py`):**
   - Replaced eager `pl.read_parquet()` with lazy frame streaming: `pl.scan_parquet().limit(row_limit).collect()`.
3. **Migration Sandbox (`src/parquet_guard/core/migration_verifier.py`):**
   - Uses `pq.ParquetFile.iter_batches()` for `sample_file`.
   - Tracks accurate `validation_mode`: `"synthetic"`, `"user_sample"`, or `"provided_in_memory"`. Caller-provided `test_table` is no longer falsely branded as synthetic.
4. **Privacy Disclosure:** Updated disclosure statements to reflect local ephemeral process execution without claiming untestable "zero data leak" absolutes.

---

### 2.3 P0-C: Parquet Trailer Dynamic Footer Length & Cache Invalidation

#### Defect Reproduction
A Parquet file with 86KB of metadata was modified in its early column schema (before the tail 16KB). File size remained identical. Old `compute_footer_checksum` read only the tail 16KB (`fh.seek(size - 16384)`), producing an identical SHA-256 hash and causing a false-positive cache hit.

#### Root Cause
Hardcoded tail chunk reading rather than parsing the Apache Parquet specification trailer format.

#### Parquet Specification Architecture
According to the Apache Parquet format specification:
- Last 4 bytes: Magic string `b"PAR1"`.
- Bytes `-8` to `-4`: 4-byte little-endian unsigned 32-bit integer `footer_len`.
- FileMetaData Thrift structure starts at `file_size - 8 - footer_len` and spans `footer_len` bytes.

#### Fix Implemented (`src/parquet_guard/core/cache.py` & `src/parquet_guard/storage/backends.py`)
1. Reads the last 8 bytes of the file.
2. Verifies trailing magic `b"PAR1"` (fails closed if corrupted or non-Parquet).
3. Unpacks `footer_len = struct.unpack("<I", trailer[:4])[0]`.
4. Bounds checks: `0 < footer_len <= (file_size - 8)` and `footer_len <= MAX_PARQUET_FOOTER_BYTES` (64MB).
5. Seeks to `file_size - 8 - footer_len` and reads the exact `footer_len + 8` bytes.
6. Computes cryptographic SHA-256 over the complete metadata footer.
7. `verify_cached_file_identity`: With `require_hash=True`, fails closed (`MISSING_CACHED_HASH`) if cached content hash is absent.

---

### 2.4 P1-A: DuckDB Strict CAST & Null Injection Detection

#### Defect Reproduction
Parquet file containing string column with `'not_a_number'` cast to `int32` executed `TRY_CAST('not_a_number' AS INT32)`, which returned `NULL` and reported `RuntimeVerdict.PASS`.

#### Fix Implemented (`src/parquet_guard/verification/duckdb_engine.py`)
1. **Strict CAST:** Evaluates SQL `CAST("col" AS ddb_type)`. If DuckDB encounters an invalid format or overflow, it throws `ConversionException`, properly caught and recorded as `RuntimeVerdict.FAIL`.
2. **Null Injection Check:** Executes dual-count verification:
   ```sql
   SELECT COUNT("col") AS orig_nn, COUNT(TRY_CAST("col" AS target_type)) AS cast_nn FROM ...
   ```
   If `cast_nn < orig_nn`, conversion loss occurred. Returns `RuntimeVerdict.FAIL` with error category `"NULL_INJECTION"`.
3. **Type Mapping Safety:** `_map_to_duckdb_type` and `_map_to_polars_type` now return `None` for unmappable types, triggering an explicit `UNSUPPORTED` verdict rather than falling back to `VARCHAR`.

---

### 2.5 P1-B: Operation-Level Rule vs. Runtime Disagreements

#### Fix Implemented (`src/parquet_guard/verification/runner.py`)
1. Extended `classify_disagreement` across all operations (`READ`, `CAST`, `SCHEMA_UNION`).
2. Added `operation: Optional[EngineProbeOperation]` to `RuleVsRuntimeDisagreement`.
3. If static rules predict `COMPATIBLE` but engine `CAST` fails, a `RULE_OPTIMISTIC` disagreement is recorded with remediation guidance.
4. If multi-file `SCHEMA_UNION` fails across partitions, a `RULE_OPTIMISTIC` disagreement is recorded.
5. Single-file union now returns `INCONCLUSIVE` (`INSUFFICIENT_PARTITIONS`) rather than a vacuous PASS.

---

### 2.6 P1-C: Consumer Contracts 2.0 Criticality & Direction

#### Fix Implemented (`src/parquet_guard/core/consumer.py`)
1. **Criticality CI Policy:**
   - `CRITICAL` / `STANDARD`: Broken critical requirements set `is_blocking = True`.
   - `INFORMATIONAL`: Technical status accurately reports `INCOMPATIBLE`, but `is_blocking` is unconditionally `False`. Downstream informational dashboards never block CI.
2. **Direction Validation:** `@field_validator("contract_direction")` enforces `BACKWARD`, `FORWARD`, or `FULL`. Unknown directions raise `ValueError`.
3. **Rename Integrity:** `@field_validator("field_renames")` rejects self-renames (`a -> a`), target collisions (`a -> c, b -> c`), and circular renaming chains (`a -> b -> a`).

---

### 2.7 P1-E & P1-F: Remote Storage & MCP Security

#### Fix Implemented
1. **`RemoteStorageBackend.stat()` (`src/parquet_guard/storage/backends.py`):** Gracefully parses `datetime.datetime`, ISO 8601 strings, and integer/float timestamps into `mtime_ns`.
2. **MCP Security (`src/parquet_guard/mcp/server.py`):**
   - Added `user_data_authorized` flag to `verify_engine_compatibility`.
   - If row-level probes are invoked without operator authorization (`user_data_authorized=False`), raises `MCPSecurityError`.

---

## 3. Regression Test Coverage Matrix (QA-01 to QA-20)

| ID | Verified Risk / Old Flaw | Corrected Behavior | Status |
|---|---|---|---|
| **QA-01** | All engines unsupported -> vacuous PASS | `runtime_verdict != PASS`, `overall_compatible = False` | **VERIFIED** |
| **QA-02** | `operations=[]` -> vacuous PASS | `runtime_verdict = NOT_RUN`, `overall_compatible = False` | **VERIFIED** |
| **QA-03** | Metadata `ERROR` + Runtime `FAIL` -> `AGREEMENT` | Classifies as `INCONCLUSIVE` | **VERIFIED** |
| **QA-04** | CLI verify-engine exits 0 on failure | Exit code 1 on incompatible/unsupported | **VERIFIED** |
| **QA-05** | PyArrow reads full file before slice | Streaming `iter_batches` with row limit | **VERIFIED** |
| **QA-06** | Polars cast reads full file before slice | Lazy scan with `limit(row_limit)` | **VERIFIED** |
| **QA-07** | Unauthorized user data access in probes | Requires explicit `user_data_authorized` in MCP/migration | **VERIFIED** |
| **QA-08** | Synthetic data label applied to real table | Preserves `provided_in_memory` validation mode | **VERIFIED** |
| **QA-09** | Tail 16KB footer hash missed early changes | Dynamic `footer_len` parsing hashes complete footer | **VERIFIED** |
| **QA-10** | `require_hash=True` with missing hash passed | Fails closed (`MISSING_CACHED_HASH`) | **VERIFIED** |
| **QA-11** | DuckDB `TRY_CAST` masked cast failure as NULL | Detects null injection / cast error -> `FAIL` | **VERIFIED** |
| **QA-12** | Single-file union was vacuous PASS | Returns `INCONCLUSIVE` (`INSUFFICIENT_PARTITIONS`) | **VERIFIED** |
| **QA-13** | CAST failure ignored by disagreements | Records `RULE_OPTIMISTIC` with `operation=CAST` | **VERIFIED** |
| **QA-14** | Unchecked contract direction string | Enforces `BACKWARD`, `FORWARD`, `FULL` | **VERIFIED** |
| **QA-15** | Informational consumer blocked CI | Technical `INCOMPATIBLE` but `is_blocking = False` | **VERIFIED** |
| **QA-16** | Circular or colliding renames accepted | Rejected with descriptive `ValueError` | **VERIFIED** |
| **QA-17** | Migration sandbox full read of sample file | Bounded batch loading via `ParquetFile` | **VERIFIED** |
| **QA-18** | `RemoteStorageBackend` raised on datetime mtime | Parses datetime, float, int, ISO strings | **VERIFIED** |
| **QA-19** | MCP allowed unauthorized row reads | Fails with `MCPSecurityError` | **VERIFIED** |
| **QA-20** | Regressions in v0.4 baseline features | 233/233 tests pass cleanly | **VERIFIED** |

---

## 4. Test & Verification Results

### Local Command Execution
```bash
# Pytest Suite:
$ ./.venv/bin/pytest -q
251 passed in 7.32s

# Ruff Linter:
$ ./.venv/bin/ruff check .
All checks passed!

# Ruff Formatter:
$ ./.venv/bin/ruff format --check .
119 files already formatted

# Mypy Static Type Checker:
$ ./.venv/bin/mypy src/parquet_guard
Success: no issues found in 35 source files

# Distribution Build & Twine Check:
$ uv build && twine check --strict dist/*
Checking dist/parquet_guard-0.4.0a1-py3-none-any.whl: PASSED
Checking dist/parquet_guard-0.4.0a1.tar.gz: PASSED
```

---

## 5. Final Adversarial Hardening (QA-B01 through QA-B18)

A second, rigorous adversarial reliability pass addressed remaining subtle boundary conditions:
1. **MCP Strict Default-Deny (QA-B01 - QA-B05):** `user_data_authorized` defaults strictly to `False`. Omitted, non-boolean (`"false"`, `"true"`, `1`, `None`), or unauthorized calls fail closed with verified zero data row decoding calls.
2. **Unified Capability Gate (QA-B06 - QA-B07):** Both Python API (`allow_row_probe=False`) and CLI (`--allow-row-probe`) require explicit authorization for row data access, failing closed when unauthorized.
3. **Multi-Engine Aggregation (QA-B08 - QA-B14):** Explicitly empty engines/operations or unsupported requested engines fail overall compatibility; single-file union and missing CAST targets report `INCONCLUSIVE`.
4. **Bounded Row Sampling (QA-B15):** Strictly validated positive integer $1 \le \text{row\_limit} \le 10,000$ across all layers; prevents infinite streaming loops or unbounded allocations.
5. **Consumer Evolution Directionality (QA-B16):** Implemented `evaluate_consumer_evolution()` to rigorously test `BACKWARD`, `FORWARD`, and `FULL` bidirectional schema evolution with distinct outcomes.
6. **Criticality Consistency (QA-B17):** Reconciled `INFORMATIONAL` (never blocks, zero blocking fields), `STANDARD` (blocks on critical violations), and `CRITICAL` (blocks on all violations); properly mapped renamed fields in extra-field and ordering checks.
7. **Disagreement Classifications (QA-B18):** Low-level execution `ERROR` classifications map to `INCONCLUSIVE`, preventing technical failures from masquerading as `AGREEMENT`.

---

## 6. Release Verdict & Maintainer Recommendations

### Verdict: READY FOR MAINTAINER REVIEW ON PR #6

- **PR Branch:** `feat/project-atlas-real-world-interoperability`
- **Base Branch:** `main`
- **Merge State:** OPEN (Not merged)
- **Tag State:** None created (Preserving `v0.4.0a1` release tag unchanged)
- **Private Data:** Untracked directories `application/`, `community/`, `research/` untouched and unstaged.
- **Next Steps for Maintainer:**
  1. Review the commits pushed to `feat/project-atlas-real-world-interoperability`.
  2. Inspect GitHub Actions CI runs on PR #6.
  3. Perform manual review of the corrective engineering changes.
  4. Merge PR #6 when ready.
