"""Unit and integration tests for verified migration sandbox and negative testing."""

from __future__ import annotations

from parquet_guard.core.consumer import (
    ConsumerCompatibilityStatus,
    ConsumerContract,
    ConsumerFieldRequirement,
)
from parquet_guard.core.migration_verifier import (
    ExecutableMigrationPlan,
    MigrationActionType,
    MigrationOperation,
    generate_synthetic_parquet_table,
    verify_migration_plan,
)
from parquet_guard.core.models import FieldDefinition, SchemaContract


def test_synthetic_table_generator_types() -> None:
    """Verify synthetic Parquet table generation with various Arrow types."""
    contract = SchemaContract(
        fields=[
            FieldDefinition(name="c_int", type="int32"),
            FieldDefinition(name="c_int64", type="int64"),
            FieldDefinition(name="c_float", type="float32"),
            FieldDefinition(name="c_str", type="string"),
            FieldDefinition(name="c_dec", type="decimal128(10, 2)"),
            FieldDefinition(name="c_ts", type="timestamp[ms]"),
            FieldDefinition(name="c_vec", type="fixed_size_list<float32>[4]"),
        ]
    )
    table = generate_synthetic_parquet_table(contract, row_count=20)
    assert len(table) == 20
    assert set(table.column_names) == {
        "c_int",
        "c_int64",
        "c_float",
        "c_str",
        "c_dec",
        "c_ts",
        "c_vec",
    }


def test_successful_safe_cast_migration_verification() -> None:
    """Verify successful end-to-end migration verification for safe integer widening."""
    source_contract = SchemaContract(
        fields=[
            FieldDefinition(name="id", type="int32"),
            FieldDefinition(name="name", type="string"),
        ]
    )
    target_contract = SchemaContract(
        fields=[
            FieldDefinition(name="id", type="int64"),
            FieldDefinition(name="name", type="string"),
        ]
    )
    plan = ExecutableMigrationPlan(
        plan_id="MIG-SAFE-01",
        title="Widen ID from int32 to int64",
        description="Safe widening with zero data loss",
        source_contract=source_contract,
        target_contract=target_contract,
        operations=[
            MigrationOperation(
                action=MigrationActionType.SAFE_CAST,
                source_column="id",
                target_type="int64",
                description="Cast int32 to int64 safely",
            )
        ],
    )
    res = verify_migration_plan(plan, sample_rows=30)
    assert res.success is True
    assert res.row_count_preserved is True
    assert res.schema_matched is True
    assert res.value_invariants_passed is True
    assert len(res.errors) == 0


def test_negative_migration_rejected_for_numeric_overflow() -> None:
    """Section 9.4 Negative Test: Migration rejected for precision loss / integer overflow."""
    source_contract = SchemaContract(
        fields=[
            FieldDefinition(name="big_id", type="int64"),
        ]
    )
    target_contract = SchemaContract(
        fields=[
            FieldDefinition(name="big_id", type="int32"),
        ]
    )
    plan = ExecutableMigrationPlan(
        plan_id="MIG-OVERFLOW-01",
        title="Unsafe narrowing from int64 to int32",
        description="Risks integer overflow on values > 2^31-1",
        source_contract=source_contract,
        target_contract=target_contract,
        operations=[
            MigrationOperation(
                action=MigrationActionType.SAFE_CAST,
                source_column="big_id",
                target_type="int32",
                description="Attempt narrowing cast",
            )
        ],
    )

    # Inject overflow int64 value (9_000_000_000)
    overflow_table = generate_synthetic_parquet_table(
        source_contract,
        row_count=10,
        overflow_int_values=True,
    )
    res = verify_migration_plan(plan, test_table=overflow_table)
    assert res.success is False
    assert any("overflow" in err.lower() or "failed" in err.lower() for err in res.errors)


def test_negative_migration_rejected_for_temporal_overflow() -> None:
    """Section 9.4 Negative Test: Migration rejected for temporal overflow beyond 2262 AD."""
    source_contract = SchemaContract(
        fields=[
            FieldDefinition(name="event_time", type="timestamp[s]"),
        ]
    )
    target_contract = SchemaContract(
        fields=[
            FieldDefinition(name="event_time", type="timestamp[ns]"),
        ]
    )
    plan = ExecutableMigrationPlan(
        plan_id="MIG-TEMP-OVERFLOW",
        title="Timestamp s to ns with Year 2300 values",
        description="Converting year 2300 to nanoseconds overflows signed int64 storage",
        source_contract=source_contract,
        target_contract=target_contract,
        operations=[
            MigrationOperation(
                action=MigrationActionType.SAFE_CAST,
                source_column="event_time",
                target_type="timestamp[ns]",
                description="Cast to nanoseconds",
            )
        ],
    )

    # Inject extreme year 2300 timestamps
    extreme_table = generate_synthetic_parquet_table(
        source_contract,
        row_count=10,
        extreme_timestamp_years=True,
    )
    res = verify_migration_plan(plan, test_table=extreme_table)
    assert res.success is False
    assert any("overflow" in err.lower() or "failed" in err.lower() for err in res.errors)


def test_negative_migration_breaks_legacy_consumer() -> None:
    """Section 9.4 Negative Test: Migration satisfies target contract but breaks a legacy consumer."""
    source_contract = SchemaContract(
        fields=[
            FieldDefinition(name="account_id", type="int64"),
        ]
    )
    # Target evolves to string
    target_contract = SchemaContract(
        fields=[
            FieldDefinition(name="account_id", type="string"),
        ]
    )
    plan = ExecutableMigrationPlan(
        plan_id="MIG-LEGACY-BREAK",
        title="Cast account_id to string",
        description="Satisfies new web service, but breaks legacy analytics expecting int64",
        source_contract=source_contract,
        target_contract=target_contract,
        operations=[
            MigrationOperation(
                action=MigrationActionType.SAFE_CAST,
                source_column="account_id",
                target_type="string",
                description="Cast to string",
            )
        ],
    )

    legacy_consumer = ConsumerContract(
        consumer_id="legacy-warehouse",
        consumer_name="Legacy Data Warehouse",
        fields=[
            ConsumerFieldRequirement(
                name="account_id",
                required=True,
                permitted_types=["int64"],
                critical=True,
            )
        ],
    )

    res = verify_migration_plan(plan, consumers=[legacy_consumer], sample_rows=20)
    # The data cast itself succeeds, BUT the verification reports the legacy consumer breakage!
    assert "legacy-warehouse" in res.incompatible_consumers
    assert res.consumer_compatibilities["legacy-warehouse"] == ConsumerCompatibilityStatus.INCOMPATIBLE
    assert any("INCOMPATIBLE with migrated output schema" in w for w in res.warnings)


def test_reject_unsafe_primitive() -> None:
    """Verify explicit REJECT_UNSAFE primitive halts migration."""
    source_contract = SchemaContract(fields=[FieldDefinition(name="col", type="string")])
    target_contract = SchemaContract(fields=[FieldDefinition(name="col", type="int32")])
    plan = ExecutableMigrationPlan(
        plan_id="MIG-REJECT",
        title="Unsafe string to int32 migration",
        description="Cannot safely convert unconstrained arbitrary strings to int32",
        source_contract=source_contract,
        target_contract=target_contract,
        operations=[
            MigrationOperation(
                action=MigrationActionType.REJECT_UNSAFE,
                source_column="col",
                description="Arbitrary string parsing cannot be guaranteed safe automatically",
            )
        ],
    )
    res = verify_migration_plan(plan, sample_rows=10)
    assert res.success is False
    assert any("rejected as unsafe" in err.lower() for err in res.errors)
