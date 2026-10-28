"""Unit and integration tests for verified migration sandbox and negative testing."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

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


def test_p0_b_negative_decimal_precision_or_scale_loss() -> None:
    """Negative test (1): Decimal precision or scale loss is rejected by safe cast."""
    from parquet_guard.core.migration_verifier import MigrationReadinessStatus

    source_contract = SchemaContract(fields=[FieldDefinition(name="price", type="decimal128(10, 4)")])
    target_contract = SchemaContract(fields=[FieldDefinition(name="price", type="decimal128(10, 2)")])
    plan = ExecutableMigrationPlan(
        plan_id="MIG-DEC-LOSS",
        title="Decimal scale truncation",
        description="Truncating scale 4 to 2 loses fractional precision",
        source_contract=source_contract,
        target_contract=target_contract,
        operations=[
            MigrationOperation(
                action=MigrationActionType.SAFE_CAST,
                source_column="price",
                target_type="decimal128(10, 2)",
                description="Attempt scale truncation",
            )
        ],
    )
    res = verify_migration_plan(plan, sample_rows=10)
    assert res.success is False
    assert res.transformation_succeeded is False
    assert res.deployment_ready is False
    assert res.readiness_status == MigrationReadinessStatus.TRANSFORMATION_FAILED


def test_p0_b_negative_unexpected_extra_output_columns() -> None:
    """Negative test (4): Unexpected extra columns in output are rejected under strict schema validation."""
    from parquet_guard.core.migration_verifier import MigrationReadinessStatus

    source_contract = SchemaContract(
        fields=[
            FieldDefinition(name="id", type="int64"),
            FieldDefinition(name="legacy_col", type="string"),
        ]
    )
    target_contract = SchemaContract(
        fields=[
            FieldDefinition(name="id", type="int64"),
        ]
    )
    # Plan forgets to DROP legacy_col
    plan = ExecutableMigrationPlan(
        plan_id="MIG-EXTRA-COL",
        title="Incomplete drop migration",
        description="Leaves unexpected extra column in migrated output",
        source_contract=source_contract,
        target_contract=target_contract,
        operations=[],
    )
    res = verify_migration_plan(plan, sample_rows=10)
    assert res.transformation_succeeded is True
    assert res.schema_matched is False
    assert "legacy_col" in res.unexpected_output_columns
    assert res.deployment_ready is False
    assert res.success is False
    assert res.readiness_status == MigrationReadinessStatus.SCHEMA_MISMATCH


def test_p0_b_negative_missing_required_output_columns() -> None:
    """Negative test (5): Missing required target columns are rejected."""
    from parquet_guard.core.migration_verifier import MigrationReadinessStatus

    source_contract = SchemaContract(fields=[FieldDefinition(name="id", type="int64")])
    target_contract = SchemaContract(
        fields=[
            FieldDefinition(name="id", type="int64"),
            FieldDefinition(name="currency", type="string"),
        ]
    )
    # Plan omits ADD_COLUMN for currency
    plan = ExecutableMigrationPlan(
        plan_id="MIG-MISSING-COL",
        title="Missing currency column",
        description="Omits required target column",
        source_contract=source_contract,
        target_contract=target_contract,
        operations=[],
    )
    res = verify_migration_plan(plan, sample_rows=10)
    assert res.transformation_succeeded is True
    assert res.schema_matched is False
    assert "currency" in res.missing_output_columns
    assert res.deployment_ready is False
    assert res.success is False
    assert res.readiness_status == MigrationReadinessStatus.SCHEMA_MISMATCH


def test_p0_b_negative_invalid_added_column_type() -> None:
    """Negative test (6): Invalid added-column type fails transformation instead of silently defaulting to string."""
    from parquet_guard.core.migration_verifier import MigrationReadinessStatus

    source_contract = SchemaContract(fields=[FieldDefinition(name="id", type="int64")])
    target_contract = SchemaContract(
        fields=[
            FieldDefinition(name="id", type="int64"),
            FieldDefinition(name="new_col", type="hyper_tensor<int128>"),
        ]
    )
    plan = ExecutableMigrationPlan(
        plan_id="MIG-BAD-TYPE",
        title="Invalid added column type",
        description="Uses unrecognized Arrow type on ADD_COLUMN",
        source_contract=source_contract,
        target_contract=target_contract,
        operations=[
            MigrationOperation(
                action=MigrationActionType.ADD_COLUMN,
                source_column="new_col",
                target_column="new_col",
                target_type="hyper_tensor<int128>",
                default_value="x",
                description="Add column with bogus type",
            )
        ],
    )
    res = verify_migration_plan(plan, sample_rows=10)
    assert res.transformation_succeeded is False
    assert res.deployment_ready is False
    assert res.success is False
    assert res.readiness_status == MigrationReadinessStatus.TRANSFORMATION_FAILED
    assert any("unsupported" in e.lower() or "unrecognized" in e.lower() for e in res.errors)


def test_p0_b_negative_nullability_violation() -> None:
    """Negative test (7): Nullability violation (nullable source into non-null target or null default into non-null)."""
    from parquet_guard.core.migration_verifier import MigrationReadinessStatus

    source_contract = SchemaContract(fields=[FieldDefinition(name="id", type="int32", nullable=True)])
    target_contract = SchemaContract(fields=[FieldDefinition(name="id", type="int64", nullable=False)])
    plan = ExecutableMigrationPlan(
        plan_id="MIG-NULL-VIOLATION",
        title="Nullable source into non-null target",
        description="Source contains nulls which violate target non-null constraint",
        source_contract=source_contract,
        target_contract=target_contract,
        operations=[
            MigrationOperation(
                action=MigrationActionType.SAFE_CAST,
                source_column="id",
                target_type="int64",
                description="Cast nullable int32 to int64",
            )
        ],
    )
    res = verify_migration_plan(plan, sample_rows=10)
    assert res.transformation_succeeded is True
    assert res.value_invariants_passed is False
    assert res.deployment_ready is False
    assert res.success is False
    assert res.readiness_status == MigrationReadinessStatus.INVARIANT_VIOLATION


def test_p0_b_negative_legacy_consumer_broken_marks_deployment_not_ready() -> None:
    """Negative test (8): Legacy consumer broken by valid transformation sets transformation_succeeded=True but success=False."""
    from parquet_guard.core.migration_verifier import MigrationReadinessStatus

    source_contract = SchemaContract(fields=[FieldDefinition(name="account_id", type="int64")])
    target_contract = SchemaContract(fields=[FieldDefinition(name="account_id", type="string")])
    plan = ExecutableMigrationPlan(
        plan_id="MIG-LEGACY-BREAK-P0B",
        title="Cast account_id to string",
        description="Valid cast that breaks critical legacy consumer",
        source_contract=source_contract,
        target_contract=target_contract,
        operations=[
            MigrationOperation(
                action=MigrationActionType.SAFE_CAST,
                source_column="account_id",
                target_type="string",
                description="Cast int64 to string",
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
    assert res.transformation_succeeded is True
    assert res.schema_matched is True
    assert res.value_invariants_passed is True
    assert res.consumer_compatible is False
    assert "legacy-warehouse" in res.blocking_consumers
    assert res.readiness_status == MigrationReadinessStatus.CONSUMER_BREAKING
    assert res.deployment_ready is False
    assert res.success is False


def test_p0_b_negative_multi_operation_partial_failure() -> None:
    """Negative test (9 & 10): Multi-operation partial failure records exact completed step count and fails."""
    from parquet_guard.core.migration_verifier import MigrationReadinessStatus

    source_contract = SchemaContract(
        fields=[
            FieldDefinition(name="a", type="int32"),
            FieldDefinition(name="b", type="string"),
        ]
    )
    target_contract = SchemaContract(
        fields=[
            FieldDefinition(name="a_wide", type="int64"),
            FieldDefinition(name="b", type="int32"),
        ]
    )
    plan = ExecutableMigrationPlan(
        plan_id="MIG-PARTIAL-FAIL",
        title="Multi-step migration failing at step 3",
        description="Step 1 & 2 succeed, step 3 fails casting arbitrary string to int32",
        source_contract=source_contract,
        target_contract=target_contract,
        operations=[
            MigrationOperation(
                action=MigrationActionType.SAFE_CAST,
                source_column="a",
                target_type="int64",
                description="Step 1: Widen a",
            ),
            MigrationOperation(
                action=MigrationActionType.RENAME_COLUMN,
                source_column="a",
                target_column="a_wide",
                description="Step 2: Rename a to a_wide",
            ),
            MigrationOperation(
                action=MigrationActionType.SAFE_CAST,
                source_column="b",
                target_type="int32",
                description="Step 3: Fail casting string to int32",
            ),
        ],
    )
    res = verify_migration_plan(plan, sample_rows=10)
    assert res.operations_total == 3
    assert res.operations_completed == 2
    assert res.transformation_succeeded is False
    assert res.deployment_ready is False
    assert res.success is False
    assert res.readiness_status == MigrationReadinessStatus.TRANSFORMATION_FAILED


def test_p0_b_negative_missing_business_semantics() -> None:
    """Negative test (12): Migration step requiring domain business semantics is marked UNCERTAIN_SEMANTICS and not deployment-ready."""
    from parquet_guard.core.migration_verifier import MigrationReadinessStatus

    source_contract = SchemaContract(fields=[FieldDefinition(name="amount_usd", type="double")])
    target_contract = SchemaContract(
        fields=[
            FieldDefinition(name="amount_usd", type="double"),
            FieldDefinition(name="amount_eur", type="double"),
        ]
    )
    plan = ExecutableMigrationPlan(
        plan_id="MIG-SEMANTICS",
        title="FX conversion requiring external rate table",
        description="Requires historical FX rate lookup",
        source_contract=source_contract,
        target_contract=target_contract,
        operations=[
            MigrationOperation(
                action=MigrationActionType.DUAL_WRITE,
                source_column="amount_usd",
                target_column="amount_eur",
                requires_business_semantics=True,
                description="Placeholder copy; actual FX conversion requires historical rates",
            )
        ],
    )
    res = verify_migration_plan(plan, sample_rows=10)
    assert res.transformation_succeeded is True
    assert res.schema_matched is True
    assert res.uncertain_semantics is True
    assert res.readiness_status == MigrationReadinessStatus.UNCERTAIN_SEMANTICS
    assert res.deployment_ready is False
    assert res.success is False


def test_migration_mode2_user_sample_data_requires_authorization(tmp_path: Path) -> None:
    """Verify Mode 2 (user sample data) enforces authorization and leaves original file untouched."""
    p_orig = tmp_path / "orders_sample.parquet"
    tbl_orig = pa.table(
        {
            "order_id": pa.array([101, 102], type=pa.int64()),
            "status": pa.array(["shipped", "pending"], type=pa.string()),
        }
    )
    pq.write_table(tbl_orig, p_orig)
    orig_bytes = p_orig.read_bytes()

    contract = SchemaContract(
        name="orders_schema",
        fields=[
            FieldDefinition(name="order_id", type="int64", nullable=True),
            FieldDefinition(name="status", type="string", nullable=True),
        ],
    )
    plan = ExecutableMigrationPlan(
        plan_id="MIG-SAMPLE-TEST",
        title="Sample verification",
        description="Verify sample data",
        source_contract=contract,
        target_contract=contract,
        operations=[],
    )

    # 1. Without authorization: raises PermissionError
    with pytest.raises(PermissionError, match="authorization"):
        verify_migration_plan(plan, sample_file=str(p_orig), user_data_authorized=False)

    # 2. With authorization: verifies Mode 2 and records audit disclosures
    res = verify_migration_plan(plan, sample_file=str(p_orig), user_data_authorized=True)
    assert res.success is True
    assert res.validation_mode == "user_sample"
    assert res.data_rows_read == 2
    assert res.user_data_authorized is True
    assert "sandbox" in res.privacy_statement.lower()

    # 3. Invariant: Original sample file is 100% byte-for-byte identical (unmutated!)
    assert p_orig.read_bytes() == orig_bytes
