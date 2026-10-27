"""Deterministic schema drift corpus and manifest generator.

Generates reproducible, open-source compliant Apache Parquet test fixtures
and `corpus_manifest.json` with independent oracles across PyArrow, DuckDB, and Polars.
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

CORPUS_DIR = Path(__file__).parent
FIXTURES_DIR = CORPUS_DIR / "fixtures"
MANIFEST_PATH = CORPUS_DIR / "corpus_manifest.json"


def generate_all_corpus_fixtures() -> None:
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    manifest = []

    # -------------------------------------------------------------
    # 1. CORPUS-01-INT-WIDEN
    # -------------------------------------------------------------
    base_p1 = FIXTURES_DIR / "case01_int_base.parquet"
    target_p1 = FIXTURES_DIR / "case01_int_target.parquet"
    t_base1 = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "counter": pa.array([100, 200, 300], type=pa.int32()),
        }
    )
    t_target1 = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "counter": pa.array([100, 200, 300], type=pa.int64()),
        }
    )
    pq.write_table(t_base1, base_p1)
    pq.write_table(t_target1, target_p1)
    manifest.append(
        {
            "case_id": "CORPUS-01-INT-WIDEN",
            "category": "numeric_widening_narrowing",
            "description": "Safe integer widening from int32 to int64.",
            "source": "GENERATED",
            "license": "Apache-2.0",
            "writer": f"pyarrow {pa.__version__}",
            "probe_operation": "READ",
            "baseline_file": str(base_p1.relative_to(CORPUS_DIR)),
            "target_file": str(target_p1.relative_to(CORPUS_DIR)),
            "expected_metadata_verdict": "COMPATIBLE",
            "expected_pyarrow_verdict": "PASS",
            "expected_duckdb_verdict": "PASS",
            "expected_polars_verdict": "PASS",
            "expected_error_codes": ["TYPE_WIDENED"],
            "known_uncertainty": None,
            "independent_oracle": "Signed 32-bit integer values fit completely within signed 64-bit int without loss.",
        }
    )

    # -------------------------------------------------------------
    # 2. CORPUS-02-INT-NARROW
    # -------------------------------------------------------------
    base_p2 = FIXTURES_DIR / "case02_narrow_base.parquet"
    target_p2 = FIXTURES_DIR / "case02_narrow_target.parquet"
    t_base2 = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "amount": pa.array([5_000_000_000, 10], type=pa.int64()),  # 5B overflows int32
        }
    )
    t_target2 = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "amount": pa.array([100, 200], type=pa.int32()),
        }
    )
    pq.write_table(t_base2, base_p2)
    pq.write_table(t_target2, target_p2)
    manifest.append(
        {
            "case_id": "CORPUS-02-INT-NARROW",
            "category": "numeric_widening_narrowing",
            "description": "Unsafe integer narrowing from int64 to int32 in multi-partition union.",
            "source": "GENERATED",
            "license": "Apache-2.0",
            "writer": f"pyarrow {pa.__version__}",
            "probe_operation": "SCHEMA_UNION",
            "probe_files": [str(target_p2.relative_to(CORPUS_DIR)), str(base_p2.relative_to(CORPUS_DIR))],
            "baseline_file": str(base_p2.relative_to(CORPUS_DIR)),
            "target_file": str(target_p2.relative_to(CORPUS_DIR)),
            "expected_metadata_verdict": "INCOMPATIBLE",
            "expected_pyarrow_verdict": "FAIL",
            "expected_duckdb_verdict": "PASS",  # DuckDB widens to int64
            "expected_polars_verdict": "FAIL",  # Polars rejects Int32 != Int64
            "expected_error_codes": ["TYPE_NARROWED"],
            "known_uncertainty": "DuckDB automatically unifies int32 and int64 partitions by widening to int64, whereas Polars and PyArrow dataset (when encountering int32 first) fail.",
            "independent_oracle": "Narrowing int64 to int32 causes numeric overflow when int64 partition has values >= 2^31.",
        }
    )

    # -------------------------------------------------------------
    # 3. CORPUS-03-DECIMAL-SCALE-SHIFT
    # -------------------------------------------------------------
    base_p3 = FIXTURES_DIR / "case03_dec_base.parquet"
    target_p3 = FIXTURES_DIR / "case03_dec_target.parquet"
    t_base3 = pa.table(
        {
            "tx_id": pa.array(["A", "B"]),
            "price": pa.array([Decimal("12.34"), Decimal("56.78")], type=pa.decimal128(10, 2)),
        }
    )
    t_target3 = pa.table(
        {
            "tx_id": pa.array(["A", "B"]),
            "price": pa.array([Decimal("12.3400"), Decimal("56.7800")], type=pa.decimal128(12, 4)),
        }
    )
    pq.write_table(t_base3, base_p3)
    pq.write_table(t_target3, target_p3)
    manifest.append(
        {
            "case_id": "CORPUS-03-DECIMAL-SCALE-SHIFT",
            "category": "decimal_precision_scale",
            "description": "Decimal scale modification from (10, 2) to (12, 4) probed in multi-partition union.",
            "source": "GENERATED",
            "license": "Apache-2.0",
            "writer": f"pyarrow {pa.__version__}",
            "probe_operation": "SCHEMA_UNION",
            "probe_files": [str(base_p3.relative_to(CORPUS_DIR)), str(target_p3.relative_to(CORPUS_DIR))],
            "baseline_file": str(base_p3.relative_to(CORPUS_DIR)),
            "target_file": str(target_p3.relative_to(CORPUS_DIR)),
            "expected_metadata_verdict": "INCOMPATIBLE",
            "expected_pyarrow_verdict": "FAIL",  # PyArrow unify_schemas rejects scale mismatch
            "expected_duckdb_verdict": "PASS",  # DuckDB unifies and rescales decimal to (12, 4)
            "expected_polars_verdict": "FAIL",  # Polars rejects Decimal(12, 4) != Decimal(10, 2)
            "expected_error_codes": ["DECIMAL_SCALE_MISMATCH"],
            "known_uncertainty": "DuckDB automatically unifies decimal scales dynamically during multi-partition queries, whereas Polars and PyArrow schema unification strictly reject scale shifts.",
            "independent_oracle": "In Parquet physical storage, decimals are stored as unscaled integers. 1234 at scale 2 is $12.34; at scale 4 it represents $0.1234.",
        }
    )

    # -------------------------------------------------------------
    # 4. CORPUS-04-TS-UNIT-OVERFLOW
    # -------------------------------------------------------------
    base_p4 = FIXTURES_DIR / "case04_ts_base.parquet"
    target_p4 = FIXTURES_DIR / "case04_ts_target.parquet"
    t_base4 = pa.table(
        {
            "event_id": pa.array([1]),
            "ts": pa.array([1700000000], type=pa.timestamp("s")),
        }
    )
    t_target4 = pa.table(
        {
            "event_id": pa.array([1]),
            "ts": pa.array([1700000000000000000], type=pa.timestamp("ns")),
        }
    )
    pq.write_table(t_base4, base_p4)
    pq.write_table(t_target4, target_p4)
    manifest.append(
        {
            "case_id": "CORPUS-04-TS-UNIT-OVERFLOW",
            "category": "temporal_boundaries",
            "description": "Timestamp unit precision widening from seconds to nanoseconds.",
            "source": "GENERATED",
            "license": "Apache-2.0",
            "writer": f"pyarrow {pa.__version__}",
            "probe_operation": "READ",
            "baseline_file": str(base_p4.relative_to(CORPUS_DIR)),
            "target_file": str(target_p4.relative_to(CORPUS_DIR)),
            "expected_metadata_verdict": "WARNING",
            "expected_pyarrow_verdict": "PASS",
            "expected_duckdb_verdict": "PASS",
            "expected_polars_verdict": "PASS",
            "expected_error_codes": ["TEMPORAL_OVERFLOW_RISK"],
            "known_uncertainty": "Timestamps after 2262-04-11 overflow signed 64-bit nanoseconds.",
            "independent_oracle": "Nanosecond timestamps increase resolution 10^9x but restrict dynamic range to 1677-2262 AD.",
        }
    )

    # -------------------------------------------------------------
    # 5. CORPUS-05-NESTED-STRUCT-FIELD-DROP
    # -------------------------------------------------------------
    base_p5 = FIXTURES_DIR / "case05_struct_base.parquet"
    target_p5 = FIXTURES_DIR / "case05_struct_target.parquet"
    struct_type_base = pa.struct(
        [
            pa.field("name", pa.string()),
            pa.field("email", pa.string()),
        ]
    )
    struct_type_target = pa.struct(
        [
            pa.field("name", pa.string()),
        ]
    )
    t_base5 = pa.table(
        {
            "user": pa.array([{"name": "Alice", "email": "a@ex.com"}], type=struct_type_base),
        }
    )
    t_target5 = pa.table(
        {
            "user": pa.array([{"name": "Alice"}], type=struct_type_target),
        }
    )
    pq.write_table(t_base5, base_p5)
    pq.write_table(t_target5, target_p5)
    manifest.append(
        {
            "case_id": "CORPUS-05-NESTED-STRUCT-FIELD-DROP",
            "category": "nested_structures",
            "description": "Dropping a child field (email) inside a nested Struct.",
            "source": "GENERATED",
            "license": "Apache-2.0",
            "writer": f"pyarrow {pa.__version__}",
            "probe_operation": "READ",
            "expected_columns": ["user.name", "user.email"],
            "baseline_file": str(base_p5.relative_to(CORPUS_DIR)),
            "target_file": str(target_p5.relative_to(CORPUS_DIR)),
            "expected_metadata_verdict": "INCOMPATIBLE",
            "expected_pyarrow_verdict": "FAIL",
            "expected_duckdb_verdict": "FAIL",
            "expected_polars_verdict": "FAIL",
            "expected_error_codes": ["FIELD_DROPPED"],
            "known_uncertainty": None,
            "independent_oracle": "Downstream readers selecting user.email fail with column missing or binder error.",
        }
    )

    # -------------------------------------------------------------
    # 6. CORPUS-06-NULLABILITY-RELAXATION
    # -------------------------------------------------------------
    base_p6 = FIXTURES_DIR / "case06_null_base.parquet"
    target_p6 = FIXTURES_DIR / "case06_null_target.parquet"
    f_not_null = pa.field("code", pa.string(), nullable=False)
    f_nullable = pa.field("code", pa.string(), nullable=True)
    t_base6 = pa.Table.from_arrays([pa.array(["ok", "valid"])], schema=pa.schema([f_not_null]))
    t_target6 = pa.Table.from_arrays([pa.array(["ok", None])], schema=pa.schema([f_nullable]))
    pq.write_table(t_base6, base_p6)
    pq.write_table(t_target6, target_p6)
    manifest.append(
        {
            "case_id": "CORPUS-06-NULLABILITY-RELAXATION",
            "category": "nullability_transitions",
            "description": "Baseline promises non-null, but target introduces null values.",
            "source": "GENERATED",
            "license": "Apache-2.0",
            "writer": f"pyarrow {pa.__version__}",
            "probe_operation": "READ",
            "baseline_file": str(base_p6.relative_to(CORPUS_DIR)),
            "target_file": str(target_p6.relative_to(CORPUS_DIR)),
            "expected_metadata_verdict": "INCOMPATIBLE",
            "expected_pyarrow_verdict": "PASS",
            "expected_duckdb_verdict": "PASS",
            "expected_polars_verdict": "PASS",
            "expected_error_codes": ["NULLABILITY_CHANGED"],
            "known_uncertainty": "Engines can physically read null strings without crashing, but downstream business logic expecting non-null invariants experiences null-pointer exceptions.",
            "independent_oracle": "A non-nullable assertion cannot be satisfied by partitions containing physical null values.",
        }
    )

    # -------------------------------------------------------------
    # 7. CORPUS-07-VECTOR-DIMENSION-MISMATCH
    # -------------------------------------------------------------
    base_p7 = FIXTURES_DIR / "case07_vec_base.parquet"
    target_p7 = FIXTURES_DIR / "case07_vec_target.parquet"
    t_base7 = pa.table(
        {
            "doc_id": pa.array([1]),
            "vec": pa.array([[0.1] * 4], type=pa.list_(pa.float32(), 4)),
        }
    )
    t_target7 = pa.table(
        {
            "doc_id": pa.array([1]),
            "vec": pa.array([[0.1] * 8], type=pa.list_(pa.float32(), 8)),
        }
    )
    pq.write_table(t_base7, base_p7)
    pq.write_table(t_target7, target_p7)
    manifest.append(
        {
            "case_id": "CORPUS-07-VECTOR-DIMENSION-MISMATCH",
            "category": "fixed_size_list_dimensions",
            "description": "FixedSizeList length mismatch between vector 4-d and 8-d in partition union.",
            "source": "GENERATED",
            "license": "Apache-2.0",
            "writer": f"pyarrow {pa.__version__}",
            "probe_operation": "SCHEMA_UNION",
            "probe_files": [str(base_p7.relative_to(CORPUS_DIR)), str(target_p7.relative_to(CORPUS_DIR))],
            "baseline_file": str(base_p7.relative_to(CORPUS_DIR)),
            "target_file": str(target_p7.relative_to(CORPUS_DIR)),
            "expected_metadata_verdict": "INCOMPATIBLE",
            "expected_pyarrow_verdict": "PASS",  # PyArrow dataset unifies fixed_size_list to variable list
            "expected_duckdb_verdict": "PASS",
            "expected_polars_verdict": "FAIL",  # Polars strict Array(4) != Array(8)
            "expected_error_codes": ["CONTAINER_PARAM_CHANGED"],
            "known_uncertainty": "PyArrow dataset tolerates vector dimension shifts by decaying to variable-length list, whereas Polars strictly rejects dimension shifts.",
            "independent_oracle": "Fixed dimension vector databases and matrix operators crash on dimension length mismatch.",
        }
    )

    # -------------------------------------------------------------
    # 8. CORPUS-08-CORRUPTED-MAGIC-BYTES
    # -------------------------------------------------------------
    bad_p8 = FIXTURES_DIR / "case08_corrupted_magic.parquet"
    with open(bad_p8, "wb") as fh:
        fh.write(b"CORR\x00\x00\x00\x00NOT_PARQUET_FOOTER_CORR")
    manifest.append(
        {
            "case_id": "CORPUS-08-CORRUPTED-MAGIC-BYTES",
            "category": "corrupted_or_truncated",
            "description": "Corrupted file with invalid magic bytes instead of PAR1.",
            "source": "GENERATED",
            "license": "Apache-2.0",
            "writer": "synthetic-raw",
            "probe_operation": "READ",
            "baseline_file": str(bad_p8.relative_to(CORPUS_DIR)),
            "target_file": str(bad_p8.relative_to(CORPUS_DIR)),
            "expected_metadata_verdict": "ERROR",
            "expected_pyarrow_verdict": "FAIL",
            "expected_duckdb_verdict": "FAIL",
            "expected_polars_verdict": "FAIL",
            "expected_error_codes": ["CORRUPTED_FILE"],
            "known_uncertainty": None,
            "independent_oracle": "Parquet files must begin and end with 4-byte magic string PAR1.",
        }
    )

    # -------------------------------------------------------------
    # 9. CORPUS-09-ADVERSARIAL-SQL-COLUMN
    # -------------------------------------------------------------
    base_p9 = FIXTURES_DIR / "case09_adversarial_base.parquet"
    target_p9 = FIXTURES_DIR / "case09_adversarial_target.parquet"
    malicious_col = "col'; DROP TABLE users; --"
    quoted_col = 'col"with""quotes'
    t_base9 = pa.table(
        {
            malicious_col: pa.array([1, 2]),
            quoted_col: pa.array(["hello", "world"]),
        }
    )
    pq.write_table(t_base9, base_p9)
    pq.write_table(t_base9, target_p9)
    manifest.append(
        {
            "case_id": "CORPUS-09-ADVERSARIAL-SQL-COLUMN",
            "category": "adversarial_inputs",
            "description": "Adversarial column names containing SQL quotes, semicolons, and injection patterns.",
            "source": "GENERATED",
            "license": "Apache-2.0",
            "writer": f"pyarrow {pa.__version__}",
            "probe_operation": "READ",
            "expected_columns": [malicious_col, quoted_col],
            "baseline_file": str(base_p9.relative_to(CORPUS_DIR)),
            "target_file": str(target_p9.relative_to(CORPUS_DIR)),
            "expected_metadata_verdict": "COMPATIBLE",
            "expected_pyarrow_verdict": "PASS",
            "expected_duckdb_verdict": "PASS",
            "expected_polars_verdict": "PASS",
            "expected_error_codes": [],
            "known_uncertainty": None,
            "independent_oracle": "Engines with safe identifier quoting must project weird column names without SQL injection execution.",
        }
    )

    with open(MANIFEST_PATH, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)

    print(f"Generated {len(manifest)} corpus cases and wrote manifest to {MANIFEST_PATH}")


if __name__ == "__main__":
    generate_all_corpus_fixtures()
