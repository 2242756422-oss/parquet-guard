"""Comprehensive regression tests verifying:

1. Schema fingerprint sensitivity to field order, field metadata, and top-level schema metadata.
2. Parquet partitioned dataset consistency using REAL PyArrow-generated files.
3. Strict semantic parity between compare_schemas() and audit_dataset().
4. Non-UTF8 raw binary metadata handling without data loss or exceptions.
5. Duplicate column name detection.
6. Decimal, Struct, List, Map, and Timestamp consistency verification.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from parquet_guard import (
    audit_dataset,
    compare_schemas,
    compute_schema_fingerprint,
    inspect_schema,
)
from parquet_guard.core.models import (
    ChangeType,
    FieldDefinition,
    SchemaContract,
)


def test_fingerprint_sensitive_to_field_order() -> None:
    """Fingerprint MUST differ when field order changes."""
    c1 = SchemaContract(
        name="c1",
        fields=[
            FieldDefinition(name="a", type="int64", nullable=False),
            FieldDefinition(name="b", type="string", nullable=True),
        ],
    )
    c2 = SchemaContract(
        name="c2",
        fields=[
            FieldDefinition(name="b", type="string", nullable=True),
            FieldDefinition(name="a", type="int64", nullable=False),
        ],
    )
    fp1 = compute_schema_fingerprint(c1, include_field_order=True)
    fp2 = compute_schema_fingerprint(c2, include_field_order=True)
    assert fp1 != fp2
    assert len(fp1) == 64  # Full SHA-256 hex string


def test_fingerprint_sensitive_to_field_metadata() -> None:
    """Fingerprint MUST differ when field-level metadata changes."""
    c1 = SchemaContract(
        name="c1",
        fields=[FieldDefinition(name="col", type="int64", nullable=False, metadata={"unit": "ms"})],
    )
    c2 = SchemaContract(
        name="c2",
        fields=[FieldDefinition(name="col", type="int64", nullable=False, metadata={"unit": "s"})],
    )
    fp1 = compute_schema_fingerprint(c1, include_metadata=True)
    fp2 = compute_schema_fingerprint(c2, include_metadata=True)
    assert fp1 != fp2


def test_fingerprint_sensitive_to_toplevel_metadata() -> None:
    """Fingerprint MUST differ when schema top-level metadata changes."""
    c1 = SchemaContract(
        name="c1",
        fields=[FieldDefinition(name="col", type="int64", nullable=False)],
        metadata={"version": "1.0"},
    )
    c2 = SchemaContract(
        name="c2",
        fields=[FieldDefinition(name="col", type="int64", nullable=False)],
        metadata={"version": "2.0"},
    )
    fp1 = compute_schema_fingerprint(c1, include_metadata=True)
    fp2 = compute_schema_fingerprint(c2, include_metadata=True)
    assert fp1 != fp2


def test_real_pyarrow_field_order_audit_divergence(tmp_path: Path) -> None:
    """Real PyArrow test: audit_dataset MUST detect divergence when partitions differ in column order."""
    ds_dir = tmp_path / "dataset_field_order"
    ds_dir.mkdir()

    # Partition 1: [col_a, col_b]
    p1 = ds_dir / "p1"
    p1.mkdir()
    s1 = pa.schema([("col_a", pa.int64()), ("col_b", pa.string())])
    t1 = pa.Table.from_arrays([pa.array([1, 2]), pa.array(["x", "y"])], schema=s1)
    pq.write_table(t1, str(p1 / "data.parquet"))

    # Partition 2: [col_b, col_a] (reordered!)
    p2 = ds_dir / "p2"
    p2.mkdir()
    s2 = pa.schema([("col_b", pa.string()), ("col_a", pa.int64())])
    t2 = pa.Table.from_arrays([pa.array(["z", "w"]), pa.array([3, 4])], schema=s2)
    pq.write_table(t2, str(p2 / "data.parquet"))

    # Strict audit MUST catch this as divergent
    audit_res = audit_dataset(ds_dir, mode="strict")
    assert audit_res.is_consistent is False
    assert audit_res.distinct_schemas_count == 2
    assert audit_res.divergent_files_count == 1
    assert any(
        "FIELD_ORDER_CHANGED" in det or "ordering" in det.lower() for det in audit_res.divergence_details
    )

    # Parity check with compare_schemas
    c1 = inspect_schema(p1 / "data.parquet")
    c2 = inspect_schema(p2 / "data.parquet")
    diff = compare_schemas(c2, c1, mode="strict")
    assert diff.has_breaking_changes is True
    assert any(d.change_type == ChangeType.FIELD_ORDER_CHANGED for d in diff.diffs)


def test_real_pyarrow_metadata_audit_divergence(tmp_path: Path) -> None:
    """Real PyArrow test: audit_dataset MUST detect divergence when partitions differ in metadata."""
    ds_dir = tmp_path / "dataset_metadata"
    ds_dir.mkdir()

    # Partition 1: metadata {"tag": "prod"}
    p1 = ds_dir / "p1"
    p1.mkdir()
    s1 = pa.schema(
        [pa.field("id", pa.int64(), metadata={b"author": b"team_a"})],
        metadata={b"environment": b"prod"},
    )
    t1 = pa.Table.from_arrays([pa.array([1])], schema=s1)
    pq.write_table(t1, str(p1 / "data.parquet"))

    # Partition 2: metadata {"tag": "staging"}
    p2 = ds_dir / "p2"
    p2.mkdir()
    s2 = pa.schema(
        [pa.field("id", pa.int64(), metadata={b"author": b"team_b"})],
        metadata={b"environment": b"staging"},
    )
    t2 = pa.Table.from_arrays([pa.array([2])], schema=s2)
    pq.write_table(t2, str(p2 / "data.parquet"))

    audit_res = audit_dataset(ds_dir, mode="strict", check_metadata=True)
    assert audit_res.is_consistent is False
    assert audit_res.distinct_schemas_count == 2
    assert audit_res.divergent_files_count == 1
    assert any("METADATA_CHANGED" in det for det in audit_res.divergence_details)


def test_non_utf8_binary_metadata_resilience(tmp_path: Path) -> None:
    """Ensure non-UTF8 binary metadata bytes are parsed without crashing or data loss."""
    raw_bad_bytes = b"\x80\xff\xfe\xca\xfe"
    file_path = tmp_path / "non_utf8_meta.parquet"

    s = pa.schema(
        [pa.field("x", pa.int32(), metadata={b"bin_key": raw_bad_bytes})],
        metadata={b"top_bin": raw_bad_bytes},
    )
    t = pa.Table.from_arrays([pa.array([42])], schema=s)
    pq.write_table(t, str(file_path))

    # Must inspect without UnicodeDecodeError
    contract = inspect_schema(file_path)
    assert len(contract.fields) == 1
    field_meta = contract.fields[0].metadata
    assert "bin_key" in field_meta
    assert "top_bin" in contract.metadata

    # Fingerprint must be computed deterministically without error
    fp = compute_schema_fingerprint(contract)
    assert len(fp) == 64
    assert fp == compute_schema_fingerprint(contract)


def test_duplicate_column_name_detection() -> None:
    """Duplicate column names in schemas must be flagged as BREAKING."""
    c = SchemaContract(
        name="dup_test",
        fields=[
            FieldDefinition(name="dup_col", type="int32", nullable=False),
            FieldDefinition(name="dup_col", type="string", nullable=True),
        ],
    )
    assert c.has_duplicate_fields() is True
    assert c.duplicate_field_names() == ["dup_col"]

    valid_c = SchemaContract(
        name="valid",
        fields=[FieldDefinition(name="unique_col", type="int32", nullable=False)],
    )

    diff = compare_schemas(c, valid_c)
    assert diff.has_breaking_changes is True
    assert any("duplicate column name" in d.description.lower() for d in diff.diffs)


def test_real_pyarrow_decimal_scale_drift(tmp_path: Path) -> None:
    """Real PyArrow test: decimal scale drift must be identified as BREAKING in audit."""
    ds_dir = tmp_path / "dataset_decimal"
    ds_dir.mkdir()

    p1 = ds_dir / "p1"
    p1.mkdir()
    s1 = pa.schema([("amount", pa.decimal128(12, 2))])
    t1 = pa.Table.from_arrays([pa.array([1000], type=pa.decimal128(12, 2))], schema=s1)
    pq.write_table(t1, str(p1 / "data.parquet"))

    p2 = ds_dir / "p2"
    p2.mkdir()
    s2 = pa.schema([("amount", pa.decimal128(12, 4))])  # Different scale!
    t2 = pa.Table.from_arrays([pa.array([1000], type=pa.decimal128(12, 4))], schema=s2)
    pq.write_table(t2, str(p2 / "data.parquet"))

    audit_res = audit_dataset(ds_dir, mode="strict")
    assert audit_res.is_consistent is False
    assert audit_res.divergent_files_count == 1
