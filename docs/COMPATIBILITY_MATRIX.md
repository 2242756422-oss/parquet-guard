# Parquet Guard AI — Query Engine Compatibility Matrix

This matrix documents the real, empirical runtime behavior of popular query engines (**PyArrow**, **DuckDB**, **Polars**) when executing operations on Parquet datasets subjected to common schema evolution and drift patterns.

Observations are verified directly by Parquet Guard's real-world probe adapters (`src/parquet_guard/verification/`).

---

## 1. Engine Evolution Behavior Matrix

| Evolution Scenario | PyArrow (Dataset API) | DuckDB (`read_parquet`) | Polars (`scan_parquet`) | Parquet Guard Static Rule | Disagreement Classification |
|---|---|---|---|---|---|
| **Int32 → Int64 (Single File Read)** | ✅ Pass (reads int64 natively) | ✅ Pass | ✅ Pass | ✅ COMPATIBLE | AGREEMENT |
| **Int32 + Int64 Multi-Partition Union** | ⚠️ Order sensitive: Fails if int32 read first & int64 has values > 2^31 (`ArrowInvalid`) | ✅ Pass: Automatically widens int32 to int64 (`union_by_name=true`) | ❌ Fail: Strict schema mismatch `SchemaError: type mismatch Int32 != Int64` | ⚠️ WARNING (Widened) | RULE_CONSERVATIVE for DuckDB; RULE_OPTIMISTIC for Polars multi-file |
| **Int64 → Int32 Narrowing** | ❌ Fail: PyArrow cast overflow if values > 2^31 | ❌ Fail: Conversion error on overflow | ❌ Fail: Integer overflow during cast | ❌ INCOMPATIBLE | AGREEMENT |
| **Decimal Precision Widening (10,2 → 14,4)** | ⚠️ Cast requires explicit safe cast | ✅ Pass: Automatically rescales decimals to widest precision/scale | ❌ Fail on multi-file union: `Decimal(12,4) != Decimal(10,2)` | ⚠️ WARNING | RULE_CONSERVATIVE for DuckDB; RULE_OPTIMISTIC for Polars multi-file |
| **Timestamp Unit Shift (`ms` → `ns`)** | ⚠️ Nanosecond timestamp physical bounds (1677–2262 CE). Overflow raises `ArrowInvalid` | ✅ Pass: duckdb timestamps support microsecond/nanosecond representations | ✅ Pass: Polars handles datetime[ns] and [ms] | ⚠️ WARNING (Temporal overflow risk) | AGREEMENT |
| **Nested Struct Field Added** | ✅ Pass | ✅ Pass | ✅ Pass | ✅ COMPATIBLE (Permissive) | AGREEMENT |
| **Nested Struct Field Dropped** | ❌ Fail: Projecting dropped field raises binder/schema error | ❌ Fail: `Binder Error: column path not found` | ❌ Fail: `ComputeError: struct field not found` | ❌ INCOMPATIBLE | AGREEMENT |
| **Nullability Relaxation (Non-Null → Nullable)** | ✅ Pass (reading permits nulls) | ✅ Pass | ✅ Pass | ⚠️ WARNING (Downstream consumer may crash on null) | RULE_CONSERVATIVE (Engine reads data, but consumer application contract may break) |
| **FixedSizeList Dimension Mismatch** | ❌ Fail: `ArrowInvalid: list length mismatch` | ❌ Fail | ❌ Fail: `Array(8) != Array(4)` | ❌ INCOMPATIBLE | AGREEMENT |
| **Corrupted Magic Bytes / Truncated Footer** | ❌ Fail: `ArrowInvalid: not a parquet file` | ❌ Fail: `IO Error: Magic bytes mismatch` | ❌ Fail: `ComputeError: invalid parquet` | ❌ ERROR (Corrupted) | AGREEMENT |
| **Adversarial SQL Identifiers in Columns** | ✅ Pass: Quoting safely isolates identifiers | ✅ Pass: Double-quote escaping prevents SQL injection | ✅ Pass: Quoting handles unescaped characters | ✅ Pass | AGREEMENT |

---

## 2. Key Insights for Data Engineers & Pipeline Authors

### 2.1 The Multi-File Partition Union Trap
In distributed systems (such as Spark, Athena, Snowflake, or Trino), different worker tasks or monthly runs may write slightly different column types (e.g., `int32` vs `int64`, or `decimal(10,2)` vs `decimal(12,4)`).
- **DuckDB** is the most permissive: with `union_by_name=true`, it automatically merges and widens integers and decimals.
- **Polars** is strict: `pl.scan_parquet([f1, f2])` checks partition schema equality and immediately aborts if any column type differs.
- **PyArrow Dataset** is sensitive to file scan order and physical integer value ranges.

Parquet Guard's `verify-engine` detects these discrepancies upfront without requiring developers to write complex multi-engine integration pipelines.

### 2.2 Semantic vs Technical Disagreements
Parquet Guard's classification engine distinguishes:
- **`RULE_OPTIMISTIC`**: Static rules considered the schema change safe (permissive widening), but a specific runtime engine (e.g. Polars multi-partition scan) failed. This is a critical risk that must fail CI when `--fail-on-disagreement` is enabled.
- **`RULE_CONSERVATIVE`**: Static rules flagged a potential incompatibility (e.g., decimal scale shift), but the runtime engine (e.g. DuckDB) successfully tolerated it.
