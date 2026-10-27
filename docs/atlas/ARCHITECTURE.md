# Project Atlas — Architectural Specification

**Project Atlas** establishes the real-world schema reliability and engine interoperability foundation for **Parquet Guard AI**.

---

## 1. Architectural Overview

```
                                  +------------------------------------+
                                  |         CLI / MCP / Pytest         |
                                  +-----------------+------------------+
                                                    |
                         +--------------------------+--------------------------+
                         |                                                     |
                         v                                                     v
          +-----------------------------+                       +-----------------------------+
          |    Zero-Data-Scan Static    |                       |   Real-World Query Engine   |
          |      Metadata Analysis      |                       |     Verification Layer      |
          |   (O(1) Header & Footer)    |                       |  (PyArrow, DuckDB, Polars)  |
          +--------------+--------------+                       +--------------+--------------+
                         |                                                     |
                         | MetadataVerdict                                     | RuntimeVerdict
                         | (COMPATIBLE/WARNING/INCOMPATIBLE)                   | (PASS/FAIL)
                         +--------------------------+--------------------------+
                                                    |
                                                    v
                                  +------------------------------------+
                                  |   Disagreement Classifier Matrix   |
                                  |   - AGREEMENT                      |
                                  |   - RULE_OPTIMISTIC (CI hazard!)   |
                                  |   - RULE_CONSERVATIVE              |
                                  |   - INCONCLUSIVE                   |
                                  +-----------------+------------------+
                                                    |
                         +--------------------------+--------------------------+
                         |                                                     |
                         v                                                     v
          +-----------------------------+                       +-----------------------------+
          |    Consumer Contract 2.0    |                       |    Hardened Cache Layer     |
          | - Criticality vs Technical  |                       | - Footer SHA-256 Checksum   |
          | - BACKWARD/FORWARD/FULL dir |                       | - HEURISTIC vs HIGH_RELIABLE|
          | - Explicit Rename Mapping   |                       | - Object Storage ETag/VerID |
          +-----------------------------+                       +-----------------------------+
```

---

## 2. Core Subsystems

### 2.1 Engine Verification Subsystem (`src/parquet_guard/verification/`)
- **`BaseEngineAdapter`**: Pluggable protocol defining `probe_read`, `probe_cast`, and `probe_schema_union`.
- **`PyArrowEngineAdapter`**: Evaluates PyArrow `parquet.read_table`, `pyarrow.compute.cast`, and `pyarrow.unify_schemas`.
- **`DuckDBEngineAdapter`**: In-memory DuckDB runner executing parameterized `read_parquet` queries with safe identifier quoting (`quote_sql_identifier`).
- **`PolarsEngineAdapter`**: Lazy frame evaluator executing `pl.scan_parquet` with struct field selection (`pl.col().struct.field()`).
- **Safety Guarantees**:
  - Bound row limits (`row_limit=10` by default).
  - Sanitized error messages (`sanitize_error_message` strips absolute paths and SQL queries).
  - Zero raw row leakage into reports or logs.

### 2.2 Consumer Contracts 2.0 (`src/parquet_guard/core/consumer.py`)
- **Directionality**: `contract_direction` supports `BACKWARD` (default), `FORWARD`, and `FULL`.
- **Criticality Independence**: Differentiates technical incompatibility (`status`) from CI blocking behavior (`is_blocking`), allowing non-critical consumers to report warnings without failing producer pipelines.
- **Explicit Field Renames**: `field_renames: Dict[str, str]` maps deprecated column names to new producer paths deterministically, eliminating fragile heuristic guessing.

### 2.3 Incremental Cache Hardening & Storage Abstraction (`src/parquet_guard/core/cache.py`, `src/parquet_guard/storage/`)
- **Footer SHA-256 Checksum**: Reads only the tail 16KB of Parquet files to compute cryptographic identity (O(1) disk I/O).
- **Cache Confidence Levels**:
  - `HEURISTIC`: Matches file size and `mtime_ns`.
  - `HIGH_RELIABLE`: Matches cryptographic footer checksum or object storage ETag/VersionId.
- **Storage Backend Abstraction**:
  - `LocalStorageBackend`: High-speed local filesystem operations.
  - `MemoryStorageBackend`: Hermetic in-memory mock storage for unit testing.
  - `RemoteStorageBackend`: Clean integration with `fsspec` (`s3fs`, `gcsfs`, `adlfs`) without imposing mandatory heavy cloud dependencies on the base package.

### 2.4 Isolated Migration Sandbox 2.0 (`src/parquet_guard/core/migration_verifier.py`)
- **Dual Validation Modes**:
  - `Mode 1 (SYNTHETIC)`: Generates boundary-probing synthetic data covering numeric ranges, unicode, nulls, and timestamp limits.
  - `Mode 2 (USER_SAMPLE)`: Safely samples bounded user data with mandatory explicit authorization (`user_data_authorized=True`).
- **Security Safeguard**: Unconditional `PermissionError` if private user data files are passed without explicit user opt-in.
