"""Comprehensive tests for v0.1.2:
1. Nested struct, list, and map compatibility without parent-child contradictions.
2. Deep nested duplicate field path detection and rejection.
3. Permissive schema drift and warning visibility without silent swallowing.
4. Global baseline diagnostics when no universal schema exists across partitions.
5. CLI --fail-on-warnings flag verification.
"""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from click.testing import CliRunner

from parquet_guard import (
    audit_dataset,
    compare_schemas,
    inspect_schema,
)
from parquet_guard.cli import main
from parquet_guard.core.models import (
    ChangeType,
    FieldDefinition,
    SchemaContract,
    Severity,
)


def test_nested_struct_safe_widening_no_parent_conflict():
    """Verify struct<a: int32> -> struct<a: int64> is SAFE in permissive mode.

    Crucially, parent struct MUST NOT be marked BREAKING while child is marked SAFE.
    """
    baseline = SchemaContract(
        name="base",
        fields=[
            FieldDefinition(
                name="payload",
                type="struct<a: int32>",
                nullable=True,
                children=[FieldDefinition(name="a", type="int32", nullable=True)],
            )
        ],
    )
    target = SchemaContract(
        name="target",
        fields=[
            FieldDefinition(
                name="payload",
                type="struct<a: int64>",
                nullable=True,
                children=[FieldDefinition(name="a", type="int64", nullable=True)],
            )
        ],
    )

    diff = compare_schemas(target=target, baseline=baseline, mode="permissive")
    assert diff.is_compatible is True
    assert diff.has_breaking_changes is False
    assert len(diff.diffs) == 1
    d = diff.diffs[0]
    assert d.field_name == "payload.a"
    assert d.change_type == ChangeType.TYPE_WIDENED
    assert d.severity == Severity.SAFE

    # Strict mode must flag type change as BREAKING
    strict_diff = compare_schemas(target=target, baseline=baseline, mode="strict")
    assert strict_diff.is_compatible is False
    assert strict_diff.has_breaking_changes is True
    assert len(strict_diff.diffs) == 1
    assert strict_diff.diffs[0].severity == Severity.BREAKING


def test_deep_nested_struct_widening():
    """Verify deep nesting struct<sub: struct<inner: int32>> -> int64."""
    baseline = SchemaContract(
        name="base",
        fields=[
            FieldDefinition(
                name="user",
                type="struct",
                children=[
                    FieldDefinition(
                        name="sub",
                        type="struct",
                        children=[FieldDefinition(name="inner", type="int32")],
                    )
                ],
            )
        ],
    )
    target = SchemaContract(
        name="target",
        fields=[
            FieldDefinition(
                name="user",
                type="struct",
                children=[
                    FieldDefinition(
                        name="sub",
                        type="struct",
                        children=[FieldDefinition(name="inner", type="int64")],
                    )
                ],
            )
        ],
    )

    diff = compare_schemas(target=target, baseline=baseline, mode="permissive")
    assert diff.is_compatible is True
    assert diff.has_breaking_changes is False
    assert len(diff.diffs) == 1
    assert diff.diffs[0].field_name == "user.sub.inner"
    assert diff.diffs[0].severity == Severity.SAFE


def test_nested_struct_child_narrowing_is_breaking():
    """Verify struct<a: int64> -> struct<a: int32> is flagged as BREAKING."""
    baseline = SchemaContract(
        name="base",
        fields=[
            FieldDefinition(
                name="payload",
                type="struct<a: int64>",
                children=[FieldDefinition(name="a", type="int64")],
            )
        ],
    )
    target = SchemaContract(
        name="target",
        fields=[
            FieldDefinition(
                name="payload",
                type="struct<a: int32>",
                children=[FieldDefinition(name="a", type="int32")],
            )
        ],
    )

    diff = compare_schemas(target=target, baseline=baseline, mode="permissive")
    assert diff.is_compatible is False
    assert diff.has_breaking_changes is True
    assert any(d.severity == Severity.BREAKING and d.field_name == "payload.a" for d in diff.diffs)


def test_nested_struct_child_reordering():
    """Verify struct<a: int32, b: string> -> struct<b: string, a: int32>.

    In permissive mode: WARNING when check_field_order=True.
    In strict mode: BREAKING.
    """
    baseline = SchemaContract(
        name="base",
        fields=[
            FieldDefinition(
                name="data",
                type="struct",
                children=[
                    FieldDefinition(name="a", type="int32"),
                    FieldDefinition(name="b", type="string"),
                ],
            )
        ],
    )
    target = SchemaContract(
        name="target",
        fields=[
            FieldDefinition(
                name="data",
                type="struct",
                children=[
                    FieldDefinition(name="b", type="string"),
                    FieldDefinition(name="a", type="int32"),
                ],
            )
        ],
    )

    perm_diff = compare_schemas(target=target, baseline=baseline, mode="permissive", check_field_order=True)
    assert perm_diff.is_compatible is True
    assert perm_diff.has_breaking_changes is False
    assert any(
        d.change_type == ChangeType.FIELD_ORDER_CHANGED and d.severity == Severity.WARNING
        for d in perm_diff.diffs
    )

    strict_diff = compare_schemas(target=target, baseline=baseline, mode="strict", check_field_order=True)
    assert strict_diff.is_compatible is False
    assert strict_diff.has_breaking_changes is True
    assert any(
        d.change_type == ChangeType.FIELD_ORDER_CHANGED and d.severity == Severity.BREAKING
        for d in strict_diff.diffs
    )


def test_nested_duplicate_column_rejection():
    """Verify that duplicate child field names inside a struct are detected with exact path."""
    contract = SchemaContract(
        name="dup_nested",
        fields=[
            FieldDefinition(
                name="event",
                type="struct",
                children=[
                    FieldDefinition(name="id", type="int32"),
                    FieldDefinition(name="id", type="string"),
                ],
            )
        ],
    )
    assert contract.has_duplicate_fields() is True
    assert contract.duplicate_field_names() == ["event.id"]


def test_real_pyarrow_parquet_with_duplicate_struct_children(tmp_path: Path):
    """Verify PyArrow Parquet file with duplicate struct fields is rejected in audit and inspect."""
    f_path = tmp_path / "dup_struct.parquet"
    f1 = pa.field("dup_col", pa.int32())
    f2 = pa.field("dup_col", pa.string())

    a1 = pa.array([10], type=pa.int32())
    a2 = pa.array(["hello"], type=pa.string())
    st_arr = pa.StructArray.from_arrays([a1, a2], fields=[f1, f2])
    tbl = pa.Table.from_arrays([st_arr], names=["meta"])
    pq.write_table(tbl, str(f_path))

    # inspect_schema should detect duplicate nested fields
    contract = inspect_schema(f_path)
    assert contract.has_duplicate_fields() is True
    assert contract.duplicate_field_names() == ["meta.dup_col"]

    # audit_dataset should fail consistency and report the duplicate nested path
    res = audit_dataset(f_path)
    assert res.is_consistent is False
    assert res.is_compatible is False
    assert any("meta.dup_col" in det for det in res.divergence_details)

    # CLI inspect and audit exit code checks
    runner = CliRunner()
    cli_audit = runner.invoke(main, ["audit", str(f_path)])
    assert cli_audit.exit_code == 1
    assert "meta.dup_col" in cli_audit.output


def test_permissive_drift_visibility_not_swallowed(tmp_path: Path):
    """Verify permissive audit populates divergence_details and warnings when order or metadata drifts."""
    dir_path = tmp_path / "permissive_drift"
    dir_path.mkdir()

    s1 = pa.schema(
        [
            pa.field("x", pa.int32()),
            pa.field("y", pa.string()),
        ],
        metadata={b"pipeline": b"v1"},
    )
    s2 = pa.schema(
        [
            pa.field("y", pa.string()),
            pa.field("x", pa.int32()),
        ],
        metadata={b"pipeline": b"v2"},
    )

    f1 = dir_path / "p1.parquet"
    f2 = dir_path / "p2.parquet"
    pq.write_table(pa.Table.from_arrays([[1], ["a"]], schema=s1), str(f1))
    pq.write_table(pa.Table.from_arrays([["b"], [2]], schema=s2), str(f2))

    res = audit_dataset(dir_path, mode="permissive", check_field_order=True, check_metadata=True)
    # Permissive: is_compatible is True, but has_drift is True!
    assert res.is_compatible is True
    assert res.has_drift is True
    assert res.warnings_count > 0
    assert len(res.divergence_details) > 0
    assert any("[WARNING]" in det for det in res.divergence_details)


def test_cli_fail_on_warnings(tmp_path: Path):
    """Verify CLI --fail-on-warnings flag terminates with exit code 1 when warnings exist."""
    dir_path = tmp_path / "drift_cli"
    dir_path.mkdir()

    s1 = pa.schema([pa.field("x", pa.int32())], metadata={b"ver": b"1"})
    s2 = pa.schema([pa.field("x", pa.int32())], metadata={b"ver": b"2"})

    pq.write_table(pa.Table.from_arrays([[1]], schema=s1), str(dir_path / "f1.parquet"))
    pq.write_table(pa.Table.from_arrays([[2]], schema=s2), str(dir_path / "f2.parquet"))

    runner = CliRunner()

    # Default permissive audit without --fail-on-warnings -> exit 0
    res_default = runner.invoke(main, ["audit", str(dir_path), "-m", "permissive"])
    assert res_default.exit_code == 0
    assert "PASS with WARNINGS" in res_default.output

    # With --fail-on-warnings -> exit 1
    res_strict_warnings = runner.invoke(
        main, ["audit", str(dir_path), "-m", "permissive", "--fail-on-warnings"]
    )
    assert res_strict_warnings.exit_code == 1


def test_global_baseline_incompatibility_diagnostics(tmp_path: Path):
    """Verify that when no single schema can serve as baseline, auditor diagnoses pair conflicts."""
    ds_dir = tmp_path / "no_common_baseline"
    ds_dir.mkdir()

    # Partition 1 has column 'x' (no 'y')
    s1 = pa.schema([pa.field("x", pa.int32())])
    # Partition 2 has column 'y' (no 'x')
    s2 = pa.schema([pa.field("y", pa.int32())])

    pq.write_table(pa.Table.from_arrays([[10]], schema=s1), str(ds_dir / "p1.parquet"))
    pq.write_table(pa.Table.from_arrays([[20]], schema=s2), str(ds_dir / "p2.parquet"))

    res = audit_dataset(ds_dir, mode="permissive")
    assert res.is_consistent is False
    assert res.is_compatible is False
    assert res.canonical_schema is None
    assert any("Dataset Incompatibility" in det for det in res.divergence_details)
    assert any("--contract" in det for det in res.divergence_details)
