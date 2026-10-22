"""Unit tests for schema compatibility algebra and drift analysis."""

from __future__ import annotations

from parquet_guard.core.compatibility import (
    analyze_compatibility,
    compare_schemas,
    is_safe_widening,
)
from parquet_guard.core.models import (
    ChangeType,
    FieldDefinition,
    SchemaContract,
    Severity,
)


def test_safe_widening_rules() -> None:
    # Signed int promotions
    assert is_safe_widening("int8", "int16") is True
    assert is_safe_widening("int8", "int64") is True
    assert is_safe_widening("int32", "int64") is True
    assert is_safe_widening("int64", "int32") is False  # Narrowing is unsafe

    # Float promotions
    assert is_safe_widening("float32", "float64") is True
    assert is_safe_widening("float", "double") is True
    assert is_safe_widening("float64", "float32") is False

    # String widening
    assert is_safe_widening("string", "large_string") is True
    assert is_safe_widening("string", "int64") is False

    # Timestamp widening (s -> ms -> us -> ns)
    assert is_safe_widening("timestamp[us, tz=UTC]", "timestamp[ns, tz=UTC]") is True
    assert is_safe_widening("timestamp[ns, tz=UTC]", "timestamp[us, tz=UTC]") is False
    # Timezone change is not pure widening
    assert is_safe_widening("timestamp[us, tz=UTC]", "timestamp[us, tz=America/New_York]") is False


def test_analyze_field_compatibility() -> None:
    # Safe widening
    f_old = FieldDefinition(name="val", type="int32", nullable=True)
    f_new = FieldDefinition(name="val", type="int64", nullable=True)
    diffs = analyze_compatibility(f_old, f_new)
    assert len(diffs) == 1
    assert diffs[0].severity == Severity.SAFE
    assert diffs[0].change_type == ChangeType.TYPE_WIDENED

    # Breaking type conversion
    f_bad = FieldDefinition(name="val", type="string", nullable=True)
    diffs_bad = analyze_compatibility(f_old, f_bad)
    assert len(diffs_bad) == 1
    assert diffs_bad[0].severity == Severity.BREAKING
    assert diffs_bad[0].change_type == ChangeType.TYPE_MISMATCH

    # Breaking nullability relaxation (NOT NULL -> NULLABLE)
    f_not_null = FieldDefinition(name="val", type="int32", nullable=False)
    diffs_null = analyze_compatibility(f_not_null, f_old)
    assert len(diffs_null) == 1
    assert diffs_null[0].severity == Severity.BREAKING
    assert diffs_null[0].change_type == ChangeType.NULLABILITY_CHANGED


def test_compare_schemas_identical() -> None:
    schema = SchemaContract(
        name="test",
        fields=[
            FieldDefinition(name="a", type="int64", nullable=False),
            FieldDefinition(name="b", type="string", nullable=True),
        ],
    )
    report = compare_schemas(schema, schema, mode="permissive")
    assert report.is_compatible is True
    assert report.has_breaking_changes is False
    assert len(report.diffs) == 0


def test_compare_schemas_dropped_column() -> None:
    base = SchemaContract(
        name="base",
        fields=[
            FieldDefinition(name="a", type="int64", nullable=False),
            FieldDefinition(name="b", type="string", nullable=True),
        ],
    )
    target = SchemaContract(
        name="target",
        fields=[
            FieldDefinition(name="a", type="int64", nullable=False),
        ],
    )
    report = compare_schemas(target, base, mode="permissive")
    assert report.is_compatible is False
    assert report.has_breaking_changes is True
    assert any(d.change_type == ChangeType.FIELD_REMOVED for d in report.diffs)


def test_compare_schemas_added_column_modes() -> None:
    base = SchemaContract(
        name="base",
        fields=[FieldDefinition(name="a", type="int64", nullable=False)],
    )
    target = SchemaContract(
        name="target",
        fields=[
            FieldDefinition(name="a", type="int64", nullable=False),
            FieldDefinition(name="extra", type="string", nullable=True),
        ],
    )

    # Permissive mode: adding nullable field is SAFE
    report_perm = compare_schemas(target, base, mode="permissive")
    assert report_perm.is_compatible is True
    assert report_perm.has_breaking_changes is False
    assert any(d.severity == Severity.SAFE for d in report_perm.diffs)

    # Strict mode: adding any field is BREAKING
    report_strict = compare_schemas(target, base, mode="strict")
    assert report_strict.is_compatible is False
    assert report_strict.has_breaking_changes is True
    assert any(d.severity == Severity.BREAKING and d.field_name == "extra" for d in report_strict.diffs)
