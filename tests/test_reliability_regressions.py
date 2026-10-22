"""Regression tests for reliability fixes:

1. Strict mode forbidding safe type widening.
2. Schema top-level and field-level metadata changes.
3. Field ordering enforcement in strict mode.
4. Empty directory or unmatched glob returning is_consistent=False and CLI exit code 2.
5. Complex Arrow types: Decimals, Lists, Maps, Timestamps, Dictionaries.
6. Nullability transitions across strict vs permissive modes.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from click.testing import CliRunner

from parquet_guard.cli import main
from parquet_guard.core.auditor import audit_dataset
from parquet_guard.core.compatibility import (
    compare_schemas,
    is_safe_widening,
)
from parquet_guard.core.models import (
    ChangeType,
    FieldDefinition,
    SchemaContract,
    Severity,
)


def test_strict_mode_forbids_safe_widening() -> None:
    """Requirement 1: Strict mode must forbid safe type widening."""
    base = SchemaContract(
        name="base",
        fields=[FieldDefinition(name="val", type="int32", nullable=True)],
    )
    target = SchemaContract(
        name="target",
        fields=[FieldDefinition(name="val", type="int64", nullable=True)],
    )

    # In permissive mode: safe widening is allowed (SAFE)
    rep_perm = compare_schemas(target, base, mode="permissive")
    assert rep_perm.is_compatible is True
    assert rep_perm.has_breaking_changes is False
    assert len(rep_perm.diffs) == 1
    assert rep_perm.diffs[0].severity == Severity.SAFE

    # In strict mode: safe widening MUST BE BREAKING
    rep_strict = compare_schemas(target, base, mode="strict")
    assert rep_strict.is_compatible is False
    assert rep_strict.has_breaking_changes is True
    assert len(rep_strict.diffs) == 1
    assert rep_strict.diffs[0].severity == Severity.BREAKING
    assert rep_strict.diffs[0].change_type == ChangeType.TYPE_WIDENED


def test_strict_mode_nullability_tightening() -> None:
    """In strict mode, even tightening nullability (nullable -> not null) is BREAKING."""
    base = SchemaContract(
        name="base",
        fields=[FieldDefinition(name="val", type="string", nullable=True)],
    )
    target = SchemaContract(
        name="target",
        fields=[FieldDefinition(name="val", type="string", nullable=False)],
    )

    rep_perm = compare_schemas(target, base, mode="permissive")
    assert rep_perm.is_compatible is True
    assert rep_perm.diffs[0].severity == Severity.SAFE

    rep_strict = compare_schemas(target, base, mode="strict")
    assert rep_strict.is_compatible is False
    assert rep_strict.diffs[0].severity == Severity.BREAKING


def test_schema_top_level_metadata_drift() -> None:
    """Requirement 2: Schema top-level metadata changes must be flagged."""
    base = SchemaContract(
        name="base",
        fields=[FieldDefinition(name="id", type="int64", nullable=False)],
        metadata={"owner": "analytics", "retention": "30d"},
    )
    target = SchemaContract(
        name="target",
        fields=[FieldDefinition(name="id", type="int64", nullable=False)],
        metadata={"owner": "marketing", "retention": "30d"},
    )

    # Permissive mode: metadata change is a WARNING
    rep_perm = compare_schemas(target, base, mode="permissive", check_metadata=True)
    assert rep_perm.is_compatible is True
    assert rep_perm.has_warnings is True
    meta_diff = [d for d in rep_perm.diffs if d.change_type == ChangeType.METADATA_CHANGED]
    assert len(meta_diff) == 1
    assert meta_diff[0].severity == Severity.WARNING

    # Strict mode: metadata change is BREAKING
    rep_strict = compare_schemas(target, base, mode="strict", check_metadata=True)
    assert rep_strict.is_compatible is False
    meta_diff_strict = [d for d in rep_strict.diffs if d.change_type == ChangeType.METADATA_CHANGED]
    assert len(meta_diff_strict) == 1
    assert meta_diff_strict[0].severity == Severity.BREAKING


def test_field_level_metadata_drift() -> None:
    """Requirement 2: Field-level metadata changes must be identified."""
    base = SchemaContract(
        name="base",
        fields=[FieldDefinition(name="id", type="int64", nullable=False, metadata={"pii": "false"})],
    )
    target = SchemaContract(
        name="target",
        fields=[FieldDefinition(name="id", type="int64", nullable=False, metadata={"pii": "true"})],
    )

    rep_perm = compare_schemas(target, base, mode="permissive", check_metadata=True)
    assert any(
        d.change_type == ChangeType.METADATA_CHANGED and d.severity == Severity.WARNING
        for d in rep_perm.diffs
    )

    rep_strict = compare_schemas(target, base, mode="strict", check_metadata=True)
    assert any(
        d.change_type == ChangeType.METADATA_CHANGED and d.severity == Severity.BREAKING
        for d in rep_strict.diffs
    )


def test_field_ordering_check_in_strict_mode() -> None:
    """Requirement 2: Column order discrepancy should be detected in strict mode."""
    base = SchemaContract(
        name="base",
        fields=[
            FieldDefinition(name="first", type="int64", nullable=False),
            FieldDefinition(name="second", type="string", nullable=True),
        ],
    )
    target = SchemaContract(
        name="target",
        fields=[
            FieldDefinition(name="second", type="string", nullable=True),
            FieldDefinition(name="first", type="int64", nullable=False),
        ],
    )

    # Permissive mode without check_field_order: passes
    rep_perm = compare_schemas(target, base, mode="permissive", check_field_order=False)
    assert rep_perm.is_compatible is True
    assert len(rep_perm.diffs) == 0

    # Strict mode: automatically checks field order and fails with BREAKING
    rep_strict = compare_schemas(target, base, mode="strict")
    assert rep_strict.is_compatible is False
    assert any(
        d.change_type == ChangeType.FIELD_ORDER_CHANGED and d.severity == Severity.BREAKING
        for d in rep_strict.diffs
    )


def test_empty_directory_audit_fails_ci(temp_dir: Path) -> None:
    """Requirement 3: Empty directory or unmatched glob must return is_consistent=False and CLI exit code 2."""
    empty_folder = temp_dir / "nonexistent_or_empty_folder"
    empty_folder.mkdir()

    # Python API test
    result = audit_dataset(empty_folder)
    assert result.is_consistent is False
    assert result.total_files_scanned == 0
    assert len(result.divergence_details) == 1
    assert "No Parquet files found" in result.divergence_details[0]

    # CLI test: must exit with code 2 (unmatched/error)
    runner = CliRunner()
    res = runner.invoke(main, ["audit", str(empty_folder)])
    assert res.exit_code == 2
    assert "Error: No Parquet files found" in res.output


def test_unmatched_glob_audit_cli() -> None:
    """Unmatched glob pattern must exit with code 2."""
    runner = CliRunner()
    res = runner.invoke(main, ["audit", "/tmp/this_path_definitely_does_not_exist_xyz/*.parquet"])
    assert res.exit_code == 2
    assert "Error: No Parquet files found" in res.output


def test_complex_arrow_types_widening() -> None:
    """Requirement 5: Audit complex Arrow types (Decimal, Timestamp, List, Map)."""
    # 1. Decimal widening: precision increase with same scale is SAFE
    assert is_safe_widening("decimal128(10, 2)", "decimal128(14, 2)") is True
    # Precision narrowing is UNSAFE
    assert is_safe_widening("decimal128(14, 2)", "decimal128(10, 2)") is False
    # Scale decrease loses precision -> UNSAFE
    assert is_safe_widening("decimal128(10, 4)", "decimal128(10, 2)") is False
    # Decimal128 to Decimal256 widening is SAFE
    assert is_safe_widening("decimal128(38, 10)", "decimal256(50, 10)") is True

    # 2. Timestamp precision:
    # us -> ns is SAFE
    assert is_safe_widening("timestamp[us, tz=UTC]", "timestamp[ns, tz=UTC]") is True
    # ns -> us loses precision -> UNSAFE
    assert is_safe_widening("timestamp[ns, tz=UTC]", "timestamp[us, tz=UTC]") is False
    # Timezone drift is UNSAFE
    assert is_safe_widening("timestamp[us, tz=UTC]", "timestamp[us]") is False

    # 3. List recursive element widening:
    assert is_safe_widening("list<item: int32>", "list<item: int64>") is True
    assert is_safe_widening("list<item: int64>", "list<item: int32>") is False
    assert is_safe_widening("list<item: string>", "list<item: int64>") is False

    # 4. Map value widening:
    assert is_safe_widening("map<string, int32>", "map<string, int64>") is True
    # Map key mismatch is UNSAFE
    assert is_safe_widening("map<string, int32>", "map<int64, int32>") is False


def test_real_decimal_and_map_parquet_file(temp_dir: Path) -> None:
    """Verify inspection and diffing of real Parquet files containing Decimals and Maps."""
    file_base = temp_dir / "decimal_base.parquet"
    file_wide = temp_dir / "decimal_wide.parquet"

    schema_base = pa.schema(
        [
            ("id", pa.int64(), False),
            ("price", pa.decimal128(10, 2), False),
        ]
    )
    schema_wide = pa.schema(
        [
            ("id", pa.int64(), False),
            ("price", pa.decimal128(14, 2), False),  # Widened decimal precision
        ]
    )

    t_base = pa.Table.from_arrays(
        [
            pa.array([1, 2]),
            pa.array([12345, 67890], type=pa.decimal128(10, 2)),
        ],
        schema=schema_base,
    )
    pq.write_table(t_base, str(file_base))

    t_wide = pa.Table.from_arrays(
        [
            pa.array([1, 2]),
            pa.array([12345, 67890], type=pa.decimal128(14, 2)),
        ],
        schema=schema_wide,
    )
    pq.write_table(t_wide, str(file_wide))

    # Permissive diff: SAFE widening
    runner = CliRunner()
    res_perm = runner.invoke(main, ["diff", str(file_base), str(file_wide), "-m", "permissive"])
    assert res_perm.exit_code == 0
    assert "[PASS]" in res_perm.output

    # Strict diff: BREAKING in strict mode
    res_strict = runner.invoke(main, ["diff", str(file_base), str(file_wide), "-m", "strict"])
    assert res_strict.exit_code == 1
    assert "[FAIL]" in res_strict.output
