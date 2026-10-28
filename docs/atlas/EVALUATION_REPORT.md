# Project Atlas — Corpus Evaluation Report

## 1. Test Corpus Definition & Coverage

The real-world evaluation corpus (`tests/corpus/`) consists of 9 deterministic scenarios reflecting schema changes observed in production data pipelines:

| Corpus ID | Scenario Name | File 1 (Base) | File 2 (Target) | Intended Drift / Hazard |
|---|---|---|---|---|
| **`CORPUS-01`** | `INT-WIDEN` | `int32` | `int64` | Safe integer widening |
| **`CORPUS-02`** | `INT-NARROW` | `int64` | `int32` | Breaking narrowing (overflow risk) |
| **`CORPUS-03`** | `DECIMAL-SCALE-SHIFT` | `decimal(10,2)` | `decimal(14,4)` | Widening precision and scale |
| **`CORPUS-04`** | `TS-UNIT-OVERFLOW` | `timestamp[ms]` | `timestamp[ns]` | Temporal unit shift & pre-1677 boundary |
| **`CORPUS-05`** | `NESTED-STRUCT-FIELD-DROP`| `struct<name, email>` | `struct<name>` | Dropped nested struct member |
| **`CORPUS-06`** | `NULLABILITY-RELAXATION` | non-null `int64` | nullable `int64` | Relaxed null constraint |
| **`CORPUS-07`** | `VECTOR-DIMENSION-MISMATCH`| `fixed_size_list[float32, 4]` | `fixed_size_list[float32, 8]` | Embedding vector dimension drift |
| **`CORPUS-08`** | `CORRUPTED-MAGIC-BYTES` | valid parquet | truncated bytes | File corruption detection |
| **`CORPUS-09`** | `ADVERSARIAL-SQL-COLUMN` | standard column | malicious SQL name | SQL injection attempt defense |

---

## 2. Empirical Verification Results

Running `tests/test_runtime_disagreements.py` across all 9 corpus cases yields 100% agreement between expected engine oracles and observed engine behavior:

```
tests/test_runtime_disagreements.py::test_corpus_case_engine_verification[CORPUS-01-INT-WIDEN] PASSED
tests/test_runtime_disagreements.py::test_corpus_case_engine_verification[CORPUS-02-INT-NARROW] PASSED
tests/test_runtime_disagreements.py::test_corpus_case_engine_verification[CORPUS-03-DECIMAL-SCALE-SHIFT] PASSED
tests/test_runtime_disagreements.py::test_corpus_case_engine_verification[CORPUS-04-TS-UNIT-OVERFLOW] PASSED
tests/test_runtime_disagreements.py::test_corpus_case_engine_verification[CORPUS-05-NESTED-STRUCT-FIELD-DROP] PASSED
tests/test_runtime_disagreements.py::test_corpus_case_engine_verification[CORPUS-06-NULLABILITY-RELAXATION] PASSED
tests/test_runtime_disagreements.py::test_corpus_case_engine_verification[CORPUS-07-VECTOR-DIMENSION-MISMATCH] PASSED
tests/test_runtime_disagreements.py::test_corpus_case_engine_verification[CORPUS-08-CORRUPTED-MAGIC-BYTES] PASSED
tests/test_runtime_disagreements.py::test_corpus_case_engine_verification[CORPUS-09-ADVERSARIAL-SQL-COLUMN] PASSED
```

---

## 3. Disagreement Findings Summary

1. **Conservative Static Rules**:
   - For `CORPUS-01` multi-file union, DuckDB automatically unifies `int32` and `int64` without error, which Parquet Guard classifies as `RULE_CONSERVATIVE` (rules flagged potential divergence, but DuckDB succeeded).
2. **Optimistic Static Hazards**:
   - For `CORPUS-01` multi-file union in Polars, `pl.scan_parquet([f1, f2])` rejects differing column types with `SchemaError`. Static rules allowing permissive widening would be optimistic; Parquet Guard's multi-file union probe flags this as `FAIL`, protecting Polars pipelines from runtime crashes.
3. **Struct Projection Parity**:
   - All three engines consistently fail with descriptive binder/schema errors when querying `customer.email` after the field is dropped (`CORPUS-05`), validating Parquet Guard's nested path traversal rules.
