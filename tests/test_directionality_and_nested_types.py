"""Regression and verification tests for:
1. Directional invariance and permutation independence in partition auditing.
2. Robust rejection of duplicate column names across all files.
3. Explicit baseline contract auditing.
4. Deep Arrow nested types (Struct, List, Map) child metadata, nullability, and order checks.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from click.testing import CliRunner

from parquet_guard import (
    audit_dataset,
    compare_schemas,
    compute_schema_fingerprint,
    inspect_schema,
)
from parquet_guard.cli import main
from parquet_guard.core.models import (
    ChangeType,
    FieldDefinition,
    SchemaContract,
)


def test_directionality_invariance_int32_int64_pyarrow(tmp_path: Path):
    """Verify that A_int32 + B_int64 vs A_int64 + B_int32 produce identical results.

    Renaming files or swapping file order must NOT change permissive or strict audit verdicts.
    """
    dir1 = tmp_path / "dataset_dir1"
    dir1.mkdir()
    f1_a = dir1 / "A_int32.parquet"
    f1_b = dir1 / "B_int64.parquet"

    schema_int32 = pa.schema([("id", pa.int32(), False)])
    schema_int64 = pa.schema([("id", pa.int64(), False)])

    pq.write_table(pa.Table.from_arrays([pa.array([1, 2], type=pa.int32())], schema=schema_int32), str(f1_a))
    pq.write_table(pa.Table.from_arrays([pa.array([3, 4], type=pa.int64())], schema=schema_int64), str(f1_b))

    dir2 = tmp_path / "dataset_dir2"
    dir2.mkdir()
    f2_a = dir2 / "A_int64.parquet"
    f2_b = dir2 / "B_int32.parquet"

    pq.write_table(pa.Table.from_arrays([pa.array([3, 4], type=pa.int64())], schema=schema_int64), str(f2_a))
    pq.write_table(pa.Table.from_arrays([pa.array([1, 2], type=pa.int32())], schema=schema_int32), str(f2_b))

    # Permissive mode: Both datasets must be consistent because int32 -> int64 is a safe widening
    res1_perm = audit_dataset(dir1, mode="permissive")
    res2_perm = audit_dataset(dir2, mode="permissive")

    assert res1_perm.is_consistent is True, f"dir1 failed: {res1_perm.divergence_details}"
    assert res2_perm.is_consistent is True, f"dir2 failed: {res2_perm.divergence_details}"
    assert res1_perm.divergent_files_count == 0
    assert res2_perm.divergent_files_count == 0

    # Strict mode: Both datasets must fail consistency because int32 and int64 are not identical
    res1_strict = audit_dataset(dir1, mode="strict")
    res2_strict = audit_dataset(dir2, mode="strict")

    assert res1_strict.is_consistent is False
    assert res2_strict.is_consistent is False


def test_frequency_invariance_in_permissive_audit(tmp_path: Path):
    """Verify that changing relative file frequency does not flip permissive audit verdict."""
    dir_mostly_64 = tmp_path / "mostly_64"
    dir_mostly_64.mkdir()
    schema_32 = pa.schema([("id", pa.int32(), False)])
    schema_64 = pa.schema([("id", pa.int64(), False)])

    # 5 files of int64, 1 file of int32
    for i in range(5):
        pq.write_table(
            pa.Table.from_arrays([pa.array([i], type=pa.int64())], schema=schema_64),
            str(dir_mostly_64 / f"file_64_{i}.parquet"),
        )
    pq.write_table(
        pa.Table.from_arrays([pa.array([99], type=pa.int32())], schema=schema_32),
        str(dir_mostly_64 / "file_32_single.parquet"),
    )

    dir_mostly_32 = tmp_path / "mostly_32"
    dir_mostly_32.mkdir()
    # 5 files of int32, 1 file of int64
    for i in range(5):
        pq.write_table(
            pa.Table.from_arrays([pa.array([i], type=pa.int32())], schema=schema_32),
            str(dir_mostly_32 / f"file_32_{i}.parquet"),
        )
    pq.write_table(
        pa.Table.from_arrays([pa.array([99], type=pa.int64())], schema=schema_64),
        str(dir_mostly_32 / "file_64_single.parquet"),
    )

    res_64 = audit_dataset(dir_mostly_64, mode="permissive")
    res_32 = audit_dataset(dir_mostly_32, mode="permissive")

    assert res_64.is_consistent is True
    assert res_32.is_consistent is True


def test_explicit_baseline_contract_auditing(tmp_path: Path):
    """Verify auditing with an explicit baseline contract."""
    ds_dir = tmp_path / "data"
    ds_dir.mkdir()
    f1 = ds_dir / "p1.parquet"
    f2 = ds_dir / "p2.parquet"

    s1 = pa.schema([("id", pa.int32(), False)])
    s2 = pa.schema([("id", pa.int64(), False)])

    pq.write_table(pa.Table.from_arrays([pa.array([1], type=pa.int32())], schema=s1), str(f1))
    pq.write_table(pa.Table.from_arrays([pa.array([2], type=pa.int64())], schema=s2), str(f2))

    # Baseline is int32 contract: p1 matches, p2 is safe widening -> both pass in permissive
    contract_int32 = SchemaContract(fields=[FieldDefinition(name="id", type="int32", nullable=False)])
    res_b32 = audit_dataset(ds_dir, mode="permissive", baseline_contract=contract_int32)
    assert res_b32.is_consistent is True

    # Baseline is int64 contract: p1 has int32 (narrowing relative to contract) -> fails!
    contract_int64 = SchemaContract(fields=[FieldDefinition(name="id", type="int64", nullable=False)])
    res_b64 = audit_dataset(ds_dir, mode="permissive", baseline_contract=contract_int64)
    assert res_b64.is_consistent is False
    assert res_b64.divergent_files_count == 1
    assert any("p1.parquet" in det for det in res_b64.divergence_details)


def test_duplicate_column_names_rejected_in_all_files(tmp_path: Path):
    """Verify that duplicate columns are rejected even if all files have identical duplicate columns."""
    dup_dir = tmp_path / "dup_dataset"
    dup_dir.mkdir()

    # PyArrow Schema with duplicated field names
    schema_dup = pa.schema(
        [
            pa.field("dup_col", pa.int32()),
            pa.field("normal_col", pa.string()),
            pa.field("dup_col", pa.int64()),  # Duplicate name
        ]
    )

    f1 = dup_dir / "file1.parquet"
    f2 = dup_dir / "file2.parquet"
    t1 = pa.Table.from_arrays([pa.array([1]), pa.array(["a"]), pa.array([100])], schema=schema_dup)
    t2 = pa.Table.from_arrays([pa.array([2]), pa.array(["b"]), pa.array([200])], schema=schema_dup)
    pq.write_table(t1, str(f1))
    pq.write_table(t2, str(f2))

    # Both files share identical schema with duplicate column
    res = audit_dataset(dup_dir, mode="permissive")
    assert res.is_consistent is False
    assert res.divergent_files_count == 2
    assert any("duplicate column" in det.lower() for det in res.divergence_details)

    # CLI check: must exit with code 1
    runner = CliRunner()
    cli_res = runner.invoke(main, ["audit", str(dup_dir)])
    assert cli_res.exit_code == 1
    assert "duplicate" in cli_res.output.lower()


def test_duplicate_column_at_different_positions(tmp_path: Path):
    """Test duplicate column names at beginning, middle, and end."""
    f_path = tmp_path / "dup_pos.parquet"
    s = pa.schema(
        [
            ("x", pa.string()),
            ("y", pa.int32()),
            ("x", pa.string()),
        ]
    )
    pq.write_table(
        pa.Table.from_arrays([pa.array(["a"]), pa.array([1]), pa.array(["b"])], schema=s), str(f_path)
    )

    contract = inspect_schema(f_path)
    assert contract.has_duplicate_fields() is True
    assert contract.duplicate_field_names() == ["x"]

    res = audit_dataset(f_path)
    assert res.is_consistent is False
    assert res.divergent_files_count == 1


def test_nested_struct_child_metadata_retention_and_drift(tmp_path: Path):
    """Verify child metadata in nested struct is preserved and drift is detected."""
    sub1_v1 = pa.field("inner_a", pa.int32(), nullable=False, metadata={b"doc": b"version 1"})
    sub2 = pa.field("inner_b", pa.string(), nullable=True)
    st1 = pa.struct([sub1_v1, sub2])
    schema1 = pa.schema([pa.field("user_info", st1)])

    sub1_v2 = pa.field("inner_a", pa.int32(), nullable=False, metadata={b"doc": b"version 2"})
    st2 = pa.struct([sub1_v2, sub2])
    schema2 = pa.schema([pa.field("user_info", st2)])

    f1 = tmp_path / "nested_v1.parquet"
    f2 = tmp_path / "nested_v2.parquet"
    pq.write_table(pa.Table.from_arrays([[{"inner_a": 1, "inner_b": "alice"}]], schema=schema1), str(f1))
    pq.write_table(pa.Table.from_arrays([[{"inner_a": 2, "inner_b": "bob"}]], schema=schema2), str(f2))

    c1 = inspect_schema(f1)
    c2 = inspect_schema(f2)

    assert len(c1.fields[0].children) == 2
    assert c1.fields[0].children[0].metadata == {"doc": "version 1"}
    assert c2.fields[0].children[0].metadata == {"doc": "version 2"}

    # Strict mode diff must detect child metadata drift
    diff_report = compare_schemas(c2, c1, mode="strict")
    assert diff_report.has_breaking_changes is True
    assert any(d.change_type == ChangeType.METADATA_CHANGED for d in diff_report.diffs)

    # Fingerprints must differ
    fp1 = compute_schema_fingerprint(c1)
    fp2 = compute_schema_fingerprint(c2)
    assert fp1 != fp2


def test_nested_struct_child_ordering_drift(tmp_path: Path):
    """Verify child field ordering in nested struct is checked in strict mode."""
    st_ab = pa.struct([pa.field("a", pa.int32()), pa.field("b", pa.string())])
    st_ba = pa.struct([pa.field("b", pa.string()), pa.field("a", pa.int32())])

    schema_ab = pa.schema([pa.field("data", st_ab)])
    schema_ba = pa.schema([pa.field("data", st_ba)])

    f_ab = tmp_path / "st_ab.parquet"
    f_ba = tmp_path / "st_ba.parquet"
    pq.write_table(pa.Table.from_arrays([[{"a": 1, "b": "x"}]], schema=schema_ab), str(f_ab))
    pq.write_table(pa.Table.from_arrays([[{"b": "x", "a": 1}]], schema=schema_ba), str(f_ba))

    c_ab = inspect_schema(f_ab)
    c_ba = inspect_schema(f_ba)

    diff_report = compare_schemas(c_ba, c_ab, mode="strict")
    assert diff_report.has_breaking_changes is True
    assert any(d.change_type == ChangeType.FIELD_ORDER_CHANGED for d in diff_report.diffs)


def test_nested_list_and_map_preservation(tmp_path: Path):
    """Verify List and Map child types, nullabilities, and metadata are preserved."""
    from parquet_guard.core.inspector import arrow_field_to_definition

    list_item = pa.field("item", pa.int64(), nullable=False, metadata={b"elem": b"1"})
    lt = pa.list_(list_item)

    key_f = pa.field("key", pa.string(), nullable=False)
    val_f = pa.field("value", pa.float64(), nullable=True, metadata={b"val": b"2"})
    mt = pa.map_(key_f, val_f)

    # 1. In-memory arrow_field_to_definition directly preserves nested map metadata
    in_mem_map = arrow_field_to_definition(pa.field("attributes", mt, metadata={b"top": b"meta"}))
    assert in_mem_map.metadata == {"top": "meta"}
    assert len(in_mem_map.children) == 2
    assert in_mem_map.children[0].name == "key"
    assert in_mem_map.children[0].nullable is False
    assert in_mem_map.children[1].name == "value"
    assert in_mem_map.children[1].nullable is True
    assert in_mem_map.children[1].metadata == {"val": "2"}

    # 2. On-disk Parquet metadata inspection
    s = pa.schema(
        [
            pa.field("tags", lt),
            pa.field("attributes", mt, metadata={b"top": b"meta"}),
        ]
    )
    f = tmp_path / "complex_nested.parquet"
    pq.write_table(
        pa.Table.from_arrays(
            [
                pa.array([[1, 2, 3]], type=lt),
                pa.array([[("k1", 1.5)]], type=mt),
            ],
            schema=s,
        ),
        str(f),
    )

    contract = inspect_schema(f)
    assert len(contract.fields) == 2
    tags_field = contract.get_field("tags")
    assert tags_field is not None
    assert len(tags_field.children) == 1
    assert tags_field.children[0].metadata == {"elem": "1"}

    attr_field = contract.get_field("attributes")
    assert attr_field is not None
    assert attr_field.metadata == {"top": "meta"}
    assert len(attr_field.children) == 2
    assert attr_field.children[0].name == "key"
    assert attr_field.children[0].type == "string"
    assert attr_field.children[0].nullable is False
    assert attr_field.children[1].name == "value"
    assert attr_field.children[1].type == "double"
    assert attr_field.children[1].nullable is True
