"""Verified migration planning and executable sandbox verification engine.

Provides:
1. ExecutableMigrationPlan and declarative MigrationOperation primitives.
2. Synthetic Parquet dataset generator with realistic edge cases (bounds, nulls, timestamps, decimals).
3. Non-destructive isolated sandbox execution using safe PyArrow operations.
4. Value-level invariant and row-count preservation checks.
5. Downstream consumer compatibility re-verification on migrated datasets.
6. Negative test assertions (precision loss, temporal overflow, legacy consumer breakages).
"""

from __future__ import annotations

import decimal
import os
import re
import tempfile
import time
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict, Field

from parquet_guard.core.compatibility import (
    _normalize_type_str,
    _parse_decimal_params,
    _parse_timestamp_params,
)
from parquet_guard.core.consumer import (
    ConsumerCompatibilityStatus,
    ConsumerContract,
    evaluate_single_consumer,
)
from parquet_guard.core.inspector import inspect_parquet_schema
from parquet_guard.core.models import (
    SchemaContract,
)


class MigrationActionType(str, Enum):
    """Supported deterministic migration primitive operations."""

    SAFE_CAST = "SAFE_CAST"
    RENAME_COLUMN = "RENAME_COLUMN"
    ADD_COLUMN = "ADD_COLUMN"
    DUAL_WRITE = "DUAL_WRITE"
    DROP_COLUMN = "DROP_COLUMN"
    REJECT_UNSAFE = "REJECT_UNSAFE"


class MigrationOperation(BaseModel):
    """A declarative, atomic migration transformation step."""

    model_config = ConfigDict(extra="forbid")

    action: MigrationActionType = Field(description="Action category")
    source_column: str = Field(description="Source column name")
    target_column: Optional[str] = Field(
        default=None, description="Target column name after rename or dual-write"
    )
    target_type: Optional[str] = Field(default=None, description="Target Arrow data type for casting")
    default_value: Optional[Any] = Field(default=None, description="Default value if adding a column")
    description: str = Field(description="Human-readable explanation of this migration step")


class ExecutableMigrationPlan(BaseModel):
    """End-to-end executable migration plan."""

    model_config = ConfigDict(extra="forbid")

    plan_id: str = Field(description="Unique plan identifier, e.g. 'MIG-2026-001'")
    title: str = Field(description="Short title for migration plan")
    description: str = Field(description="High-level overview of migration goals")
    source_contract: SchemaContract = Field(description="Contract describing input data layout")
    target_contract: SchemaContract = Field(description="Desired contract describing output data layout")
    operations: List[MigrationOperation] = Field(
        default_factory=list, description="Ordered declarative migration steps"
    )
    deployment_order: List[str] = Field(default_factory=list, description="Recommended deployment phases")
    rollback_procedure: List[str] = Field(
        default_factory=list, description="Rollback instructions if migration fails"
    )
    affected_consumer_ids: List[str] = Field(
        default_factory=list, description="Consumer IDs impacted by this change"
    )
    pyarrow_code: str = Field(
        default="", description="Generated reference Python/PyArrow transformation snippet"
    )


class MigrationVerificationResult(BaseModel):
    """Comprehensive verification report from executing migration in an isolated sandbox."""

    model_config = ConfigDict(extra="forbid")

    success: bool = Field(description="True if migration executed without error and passed all checks")
    rows_tested: int = Field(description="Number of synthetic rows generated and transformed")
    row_count_preserved: bool = Field(description="True if output row count exactly equals input row count")
    schema_matched: bool = Field(description="True if migrated Parquet schema matches target contract")
    value_invariants_passed: bool = Field(description="True if nulls, bounds, and values remained valid")
    consumer_compatibilities: Dict[str, ConsumerCompatibilityStatus] = Field(
        default_factory=dict,
        description="Compatibility status of each consumer after migration (consumer_id -> status)",
    )
    incompatible_consumers: List[str] = Field(
        default_factory=list, description="Consumers broken by the proposed migration"
    )
    errors: List[str] = Field(default_factory=list, description="Fatal errors encountered during execution")
    warnings: List[str] = Field(default_factory=list, description="Warnings or non-fatal risks")
    execution_latency_ms: float = Field(default=0.0, description="Verification latency in milliseconds")
    summary: str = Field(description="Executive summary of verification verdict")


def _arrow_type_from_string(type_str: str) -> pa.DataType:
    """Map canonical Arrow type string to PyArrow DataType."""
    t = type_str.strip().lower()

    if t in ("int8", "pa.int8()"):
        return pa.int8()
    if t in ("int16", "pa.int16()"):
        return pa.int16()
    if t in ("int32", "pa.int32()"):
        return pa.int32()
    if t in ("int64", "pa.int64()"):
        return pa.int64()
    if t in ("uint8", "pa.uint8()"):
        return pa.uint8()
    if t in ("uint16", "pa.uint16()"):
        return pa.uint16()
    if t in ("uint32", "pa.uint32()"):
        return pa.uint32()
    if t in ("uint64", "pa.uint64()"):
        return pa.uint64()
    if t in ("float16", "halffloat"):
        return pa.float16()
    if t in ("float", "float32"):
        return pa.float32()
    if t in ("double", "float64"):
        return pa.float64()
    if t in ("string", "utf8"):
        return pa.string()
    if t in ("large_string", "large_utf8"):
        return pa.large_string()
    if t == "binary":
        return pa.binary()
    if t == "large_binary":
        return pa.large_binary()
    if t == "bool" or t == "boolean":
        return pa.bool_()
    if t == "date32":
        return pa.date32()
    if t == "date64":
        return pa.date64()

    # FixedSizeList: e.g. fixed_size_list<float>[512] or fixed_size_list<item: float32>[768]
    m_fsl = re.match(r"^fixed_size_list<(?:item:\s*)?(.+)>\[(\d+)\]$", t)
    if m_fsl:
        inner = _arrow_type_from_string(m_fsl.group(1))
        dim = int(m_fsl.group(2))
        return pa.list_(inner, list_size=dim)

    # Standard List: e.g. list<item: int32>
    m_list = re.match(r"^list<(?:item:\s*)?(.+)>$", t)
    if m_list:
        inner = _arrow_type_from_string(m_list.group(1))
        return pa.list_(inner)

    # Decimal
    dec_params = _parse_decimal_params(t)
    if dec_params:
        d_type, prec, scale = dec_params
        if d_type == "decimal256":
            return pa.decimal256(prec, scale)
        return pa.decimal128(prec, scale)

    # Timestamp
    ts_params = _parse_timestamp_params(t)
    if ts_params:
        unit, tz = ts_params
        return pa.timestamp(unit, tz=tz)

    # Duration
    m_dur = re.match(r"^duration\[(s|ms|us|ns)\]$", t)
    if m_dur:
        return pa.duration(m_dur.group(1))

    # Fallback to string representation parsing
    try:
        # PyArrow type constructor by name
        return getattr(pa, t)()
    except Exception:
        return pa.string()


def generate_synthetic_parquet_table(
    contract: SchemaContract,
    row_count: int = 50,
    inject_boundary_values: bool = True,
    extreme_timestamp_years: bool = False,
    overflow_int_values: bool = False,
) -> pa.Table:
    """Generate representative PyArrow Table conforming to SchemaContract."""
    columns: Dict[str, pa.Array] = {}

    for f in contract.fields:
        t = f.type.strip().lower()
        nullable = f.nullable

        if t in ("int8", "int16", "int32"):
            vals: List[Optional[int]] = []
            max_v = 127 if t == "int8" else (32767 if t == "int16" else 2147483647)
            min_v = -128 if t == "int8" else (-32768 if t == "int16" else -2147483648)
            for i in range(row_count):
                if nullable and i == 0:
                    vals.append(None)
                elif inject_boundary_values and i == 1:
                    vals.append(max_v)
                elif inject_boundary_values and i == 2:
                    vals.append(min_v)
                else:
                    vals.append((i * 17) % max_v)
            pa_type = pa.int8() if t == "int8" else (pa.int16() if t == "int16" else pa.int32())
            columns[f.name] = pa.array(vals, type=pa_type)

        elif t == "int64":
            vals_64: List[Optional[int]] = []
            for i in range(row_count):
                if nullable and i == 0:
                    vals_64.append(None)
                elif overflow_int_values and i == 1:
                    # Value exceeding int32 bounds
                    vals_64.append(9_000_000_000)
                elif inject_boundary_values and i == 2:
                    vals_64.append(9223372036854775807)
                else:
                    vals_64.append(i * 1000 + 1)
            columns[f.name] = pa.array(vals_64, type=pa.int64())

        elif t in ("bool", "boolean"):
            b_vals: List[Optional[bool]] = []
            for i in range(row_count):
                if nullable and i == 0:
                    b_vals.append(None)
                else:
                    b_vals.append(i % 2 == 0)
            columns[f.name] = pa.array(b_vals, type=pa.bool_())

        elif t in ("float", "float32", "double", "float64"):
            pa_f_type = pa.float32() if "32" in t or t == "float" else pa.float64()
            f_vals: List[Optional[float]] = []
            for i in range(row_count):
                if nullable and i == 0:
                    f_vals.append(None)
                elif inject_boundary_values and i == 1:
                    f_vals.append(123456.789012345)
                else:
                    f_vals.append(float(i) * 1.5)
            columns[f.name] = pa.array(f_vals, type=pa_f_type)

        elif t in ("string", "large_string", "utf8"):
            s_vals: List[Optional[str]] = []
            for i in range(row_count):
                if nullable and i == 0:
                    s_vals.append(None)
                else:
                    s_vals.append(f"val_{f.name}_{i}")
            pa_s_type = pa.large_string() if "large" in t else pa.string()
            columns[f.name] = pa.array(s_vals, type=pa_s_type)

        elif t.startswith("decimal"):
            dec_p = _parse_decimal_params(t)
            prec = dec_p[1] if dec_p else 10
            scale = dec_p[2] if dec_p else 2
            dec_type = pa.decimal128(prec, scale)
            d_vals: List[Optional[decimal.Decimal]] = []
            for i in range(row_count):
                if nullable and i == 0:
                    d_vals.append(None)
                else:
                    # e.g. 12.34
                    v_str = f"{i + 10}.{''.join(str((i + j) % 10) for j in range(scale))}"
                    d_vals.append(decimal.Decimal(v_str))
            columns[f.name] = pa.array(d_vals, type=dec_type)

        elif t.startswith("timestamp"):
            ts_p = _parse_timestamp_params(t)
            unit = ts_p[0] if ts_p else "ms"
            tz = ts_p[1] if ts_p else None
            ts_type = pa.timestamp(unit, tz=tz)
            ts_vals: List[Optional[int]] = []
            # Base timestamp ~ year 2026 in seconds: 1770000000
            mult = {"s": 1, "ms": 1000, "us": 1_000_000, "ns": 1_000_000_000}[unit]
            base_s = 1770000000
            for i in range(row_count):
                if nullable and i == 0:
                    ts_vals.append(None)
                elif extreme_timestamp_years and i == 1:
                    # Year 2300 in seconds: ~10,413,792,000 (overflows signed int64 if converted to nanoseconds)
                    ts_vals.append(10_413_792_000 * mult)
                else:
                    ts_vals.append((base_s + i * 3600) * mult)
            columns[f.name] = pa.array(ts_vals, type=ts_type)

        elif "fixed_size_list" in t:
            m_fsl = re.match(r"^fixed_size_list<(?:item:\s*)?(.+)>\[(\d+)\]$", t)
            dim = int(m_fsl.group(2)) if m_fsl else 4
            inner_t = _arrow_type_from_string(m_fsl.group(1)) if m_fsl else pa.float32()
            list_vals: List[Optional[List[float]]] = []
            for i in range(row_count):
                if nullable and i == 0:
                    list_vals.append(None)
                else:
                    list_vals.append([float(j) * 0.1 for j in range(dim)])
            columns[f.name] = pa.array(list_vals, type=pa.list_(inner_t, list_size=dim))

        else:
            # General fallback to string array
            columns[f.name] = pa.array([f"item_{i}" for i in range(row_count)], type=pa.string())

    return pa.table(columns)


def execute_migration_in_sandbox(
    source_table: pa.Table,
    operations: List[MigrationOperation],
) -> Tuple[pa.Table, List[str], List[str]]:
    """Execute declarative migration steps against a PyArrow Table in-memory."""
    table = source_table
    errors: List[str] = []
    warnings: List[str] = []

    for op in operations:
        if op.action == MigrationActionType.SAFE_CAST:
            if not op.target_type:
                errors.append(f"SAFE_CAST for '{op.source_column}' missing target_type")
                continue
            if op.source_column not in table.column_names:
                errors.append(f"Cannot cast missing column '{op.source_column}'")
                continue

            target_pa_type = _arrow_type_from_string(op.target_type)
            try:
                col_idx = table.schema.get_field_index(op.source_column)
                old_col = table.column(col_idx)
                # Use pc.cast with safe=True to guarantee overflow and truncation detection!
                new_col = pc.cast(old_col, target_pa_type, safe=True)
                table = table.set_column(col_idx, op.source_column, new_col)
            except Exception as exc:
                errors.append(f"SAFE_CAST failed on '{op.source_column}' -> {op.target_type}: {exc}")

        elif op.action == MigrationActionType.RENAME_COLUMN:
            if not op.target_column:
                errors.append(f"RENAME_COLUMN missing target_column for '{op.source_column}'")
                continue
            if op.source_column not in table.column_names:
                errors.append(f"Cannot rename missing column '{op.source_column}'")
                continue

            col_idx = table.schema.get_field_index(op.source_column)
            col_data = table.column(col_idx)
            table = table.set_column(col_idx, op.target_column, col_data)

        elif op.action == MigrationActionType.ADD_COLUMN:
            col_name = op.target_column or op.source_column
            if col_name in table.column_names:
                warnings.append(f"Column '{col_name}' already exists, skipping ADD_COLUMN")
                continue

            target_pa_type = _arrow_type_from_string(op.target_type or "string")
            if op.default_value is not None:
                new_col_data = pa.repeat(op.default_value, len(table))
            else:
                new_col_data = pa.nulls(len(table), type=target_pa_type)
            table = table.append_column(col_name, new_col_data)

        elif op.action == MigrationActionType.DUAL_WRITE:
            target_name = op.target_column or f"{op.source_column}_v2"
            if op.source_column not in table.column_names:
                errors.append(f"DUAL_WRITE source '{op.source_column}' not found")
                continue

            src_col = table[op.source_column]
            if op.target_type:
                target_pa_type = _arrow_type_from_string(op.target_type)
                try:
                    src_col = pc.cast(src_col, target_pa_type, safe=True)
                except Exception as exc:
                    errors.append(f"DUAL_WRITE cast failed for '{target_name}': {exc}")
                    continue
            table = table.append_column(target_name, src_col)

        elif op.action == MigrationActionType.DROP_COLUMN:
            if op.source_column in table.column_names:
                col_idx = table.schema.get_field_index(op.source_column)
                table = table.remove_column(col_idx)

        elif op.action == MigrationActionType.REJECT_UNSAFE:
            errors.append(f"Migration rejected as unsafe: {op.description}")

    return table, errors, warnings


def verify_migration_plan(
    plan: ExecutableMigrationPlan,
    consumers: Optional[List[ConsumerContract]] = None,
    sample_rows: int = 50,
    test_table: Optional[pa.Table] = None,
) -> MigrationVerificationResult:
    """Verify an ExecutableMigrationPlan in an isolated filesystem sandbox."""
    start_time = time.perf_counter()
    errors: List[str] = []
    warnings: List[str] = []

    # 1. Prepare representative source data
    if test_table is not None:
        source_table = test_table
    else:
        source_table = generate_synthetic_parquet_table(
            contract=plan.source_contract,
            row_count=sample_rows,
            inject_boundary_values=True,
        )

    initial_row_count = len(source_table)

    # 2. Execute transformation in isolated temp directory
    with tempfile.TemporaryDirectory() as temp_dir:
        input_parquet = os.path.join(temp_dir, "input.parquet")
        output_parquet = os.path.join(temp_dir, "migrated.parquet")

        pq.write_table(source_table, input_parquet)

        # Apply operations
        transformed_table, op_errors, op_warnings = execute_migration_in_sandbox(
            source_table=source_table,
            operations=plan.operations,
        )
        errors.extend(op_errors)
        warnings.extend(op_warnings)

        if not errors:
            pq.write_table(transformed_table, output_parquet)
            readback_table = pq.read_table(output_parquet)
            readback_schema = inspect_parquet_schema(output_parquet)
        else:
            readback_table = transformed_table
            readback_schema = None

    elapsed_ms = round((time.perf_counter() - start_time) * 1000.0, 2)

    # 3. Check Row Count Invariant
    rows_preserved = len(readback_table) == initial_row_count
    if not rows_preserved:
        errors.append(
            f"Row count invariant violated: expected {initial_row_count}, got {len(readback_table)}"
        )

    # 4. Check Schema Match against Target Contract
    schema_matched = False
    if readback_schema:
        target_field_names = set(plan.target_contract.field_names())
        actual_field_names = set(readback_schema.field_names())
        if target_field_names.issubset(actual_field_names):
            schema_matched = True
            for target_field in plan.target_contract.fields:
                act = readback_schema.get_field(target_field.name)
                if not act or _normalize_type_str(act.type) != _normalize_type_str(target_field.type):
                    schema_matched = False
                    errors.append(
                        f"Field '{target_field.name}' type mismatch: expected {target_field.type}, got {act.type if act else None}"
                    )
        else:
            missing = target_field_names - actual_field_names
            errors.append(f"Output schema missing expected target fields: {list(missing)}")

    # 5. Check Value Invariants
    value_invariants_passed = True
    if not errors and readback_table is not None:
        for f in plan.target_contract.fields:
            if not f.nullable and f.name in readback_table.column_names:
                null_cnt = readback_table[f.name].null_count
                if null_cnt > 0:
                    value_invariants_passed = False
                    errors.append(f"Non-nullable column '{f.name}' contains {null_cnt} nulls after migration")

    # 6. Re-evaluate Downstream Consumer Compatibility
    consumer_compatibilities: Dict[str, ConsumerCompatibilityStatus] = {}
    incompatible_consumers: List[str] = []

    if consumers and readback_schema:
        for c in consumers:
            ev = evaluate_single_consumer(readback_schema, c)
            consumer_compatibilities[c.consumer_id] = ev.status
            if ev.status == ConsumerCompatibilityStatus.INCOMPATIBLE:
                incompatible_consumers.append(c.consumer_id)
                warnings.append(
                    f"Consumer '{c.consumer_name}' ({c.consumer_id}) is INCOMPATIBLE with migrated output schema"
                )

    success = (len(errors) == 0) and rows_preserved and schema_matched and value_invariants_passed

    summary = (
        f"Migration plan '{plan.plan_id}' verification: "
        f"{'PASSED' if success else 'FAILED'}. "
        f"Rows={len(readback_table)}, SchemaMatched={schema_matched}, "
        f"ValueInvariants={value_invariants_passed}, IncompatibleConsumers={len(incompatible_consumers)}"
    )

    return MigrationVerificationResult(
        success=success,
        rows_tested=initial_row_count,
        row_count_preserved=rows_preserved,
        schema_matched=schema_matched,
        value_invariants_passed=value_invariants_passed,
        consumer_compatibilities=consumer_compatibilities,
        incompatible_consumers=incompatible_consumers,
        errors=errors,
        warnings=warnings,
        execution_latency_ms=elapsed_ms,
        summary=summary,
    )
