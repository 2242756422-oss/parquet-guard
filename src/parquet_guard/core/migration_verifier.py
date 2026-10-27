"""Verified migration planning and executable sandbox verification engine.

Provides:
1. ExecutableMigrationPlan and declarative MigrationOperation primitives.
2. Synthetic Parquet dataset generator with realistic edge cases (bounds, nulls, timestamps, decimals, nested structs/lists/maps).
3. Non-destructive isolated sandbox execution using safe PyArrow operations.
4. Explicit separation of:
   - Transformation execution result (`transformation_succeeded`)
   - Output schema structural validation (`schema_matched`, including extra fields, nullability, field order, nested structure, metadata)
   - Value-level preservation (`row_count_preserved`, `value_invariants_passed`)
   - Downstream consumer compatibility (`consumer_compatible`, `blocking_consumers`, `incompatible_consumers`)
   - Overall deployment readiness (`deployment_ready` / `readiness_status`, and overall `success`)
5. Negative test assertions (precision loss, temporal overflow, integer narrowing, unexpected/missing columns, invalid types, nullability violations, legacy consumer breakages, multi-op partial failure, missing business semantics).
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
    _parse_decimal_params,
    _parse_list_inner,
    _parse_map_params,
    _parse_timestamp_params,
    compare_schemas,
)
from parquet_guard.core.consumer import (
    ConsumerCompatibilityStatus,
    ConsumerContract,
    canonical_type,
    evaluate_single_consumer,
    is_recognized_type,
)
from parquet_guard.core.inspector import inspect_parquet_schema
from parquet_guard.core.models import (
    ChangeType,
    FieldDefinition,
    SchemaContract,
    Severity,
)


class MigrationActionType(str, Enum):
    """Supported deterministic migration primitive operations."""

    SAFE_CAST = "SAFE_CAST"
    RENAME_COLUMN = "RENAME_COLUMN"
    ADD_COLUMN = "ADD_COLUMN"
    DUAL_WRITE = "DUAL_WRITE"
    DROP_COLUMN = "DROP_COLUMN"
    REJECT_UNSAFE = "REJECT_UNSAFE"


class MigrationReadinessStatus(str, Enum):
    """Explicit classification of migration sandbox verification and deployment readiness."""

    DEPLOYMENT_READY = "DEPLOYMENT_READY"
    CONSUMER_BREAKING = "CONSUMER_BREAKING"
    SCHEMA_MISMATCH = "SCHEMA_MISMATCH"
    INVARIANT_VIOLATION = "INVARIANT_VIOLATION"
    TRANSFORMATION_FAILED = "TRANSFORMATION_FAILED"
    UNCERTAIN_SEMANTICS = "UNCERTAIN_SEMANTICS"


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
    nullable: Optional[bool] = Field(
        default=None, description="Explicit nullability override for target/added column"
    )
    requires_business_semantics: bool = Field(
        default=False,
        description="True if this step depends on domain business logic not verifiable from schema/type casts alone",
    )
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
    strict_schema_validation: bool = Field(
        default=True,
        description="Reject unexpected extra output columns and enforce structural contract equality",
    )
    enforce_field_order: bool = Field(
        default=False,
        description="Enforce exact column ordering between migrated output and target_contract",
    )
    enforce_metadata: bool = Field(
        default=False,
        description="Enforce top-level and field-level metadata equality against target_contract",
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
    """Comprehensive verification report from executing migration in an isolated sandbox.

    Separates:
    1. `transformation_succeeded`: Whether the PyArrow operations executed without error.
    2. `schema_matched`: Whether the output Parquet schema conforms to `target_contract`.
    3. `row_count_preserved` & `value_invariants_passed`: Value-level preservation.
    4. `consumer_compatible`: Whether all declared blocking consumers remain compatible.
    5. `deployment_ready` & `readiness_status` & `success`: Overall deployment readiness.
       A migration that transforms data cleanly but breaks a critical declared consumer
       has `transformation_succeeded=True`, `consumer_compatible=False`,
       `readiness_status=CONSUMER_BREAKING`, `deployment_ready=False`, and `success=False`.
    """

    model_config = ConfigDict(extra="forbid")

    success: bool = Field(
        description="True ONLY if transformation succeeded, schema/value invariants passed, and no declared blocking consumer is broken"
    )
    transformation_succeeded: bool = Field(
        default=False,
        description="True if the physical PyArrow transformation steps executed without runtime errors",
    )
    deployment_ready: bool = Field(
        default=False,
        description="True if migration is safe for deployment across transformation, schema, value invariants, and declared consumers",
    )
    readiness_status: MigrationReadinessStatus = Field(
        default=MigrationReadinessStatus.TRANSFORMATION_FAILED,
        description="Categorical readiness verdict",
    )
    rows_tested: int = Field(description="Number of synthetic rows generated and transformed")
    operations_total: int = Field(default=0, description="Total number of operations in the plan")
    operations_completed: int = Field(
        default=0, description="Number of operations that completed without error"
    )
    row_count_preserved: bool = Field(description="True if output row count exactly equals input row count")
    schema_matched: bool = Field(description="True if migrated Parquet schema matches target contract")
    unexpected_output_columns: List[str] = Field(
        default_factory=list, description="Extra columns present in output but absent in target_contract"
    )
    missing_output_columns: List[str] = Field(
        default_factory=list, description="Columns required by target_contract but absent in output"
    )
    schema_discrepancies: List[str] = Field(
        default_factory=list,
        description="Detailed structural, type, nullability, ordering, nested, or metadata discrepancies",
    )
    value_invariants_passed: bool = Field(description="True if nulls, bounds, and values remained valid")
    consumer_compatible: bool = Field(
        default=True,
        description="True if no declared consumer is broken (or uncertain on critical fields) by the migrated output",
    )
    consumer_compatibilities: Dict[str, ConsumerCompatibilityStatus] = Field(
        default_factory=dict,
        description="Compatibility status of each consumer after migration (consumer_id -> status)",
    )
    incompatible_consumers: List[str] = Field(
        default_factory=list, description="Consumers technically incompatible with the migrated output"
    )
    blocking_consumers: List[str] = Field(
        default_factory=list, description="Consumers whose critical requirements are blocked by the migration"
    )
    uncertain_semantics: bool = Field(
        default=False,
        description="True if any migration step requires domain business semantics that cannot be verified synthetically",
    )
    validation_mode: str = Field(
        default="synthetic",
        description="'synthetic' (Mode 1, zero customer data) or 'user_sample' (Mode 2, user-authorized sample)",
    )
    data_rows_read: int = Field(
        default=0,
        description="Number of user production or sample data rows loaded into isolated sandbox memory",
    )
    user_data_authorized: bool = Field(
        default=False,
        description="True if operator explicitly authorized loading sample data rows",
    )
    privacy_statement: str = Field(
        default="Ephemeral in-process sandbox verification. No customer data uploaded or transmitted across external networks.",
        description="Privacy and data transmission disclosure statement",
    )
    errors: List[str] = Field(default_factory=list, description="Fatal errors encountered during execution")
    warnings: List[str] = Field(default_factory=list, description="Warnings or non-fatal risks")
    execution_latency_ms: float = Field(default=0.0, description="Verification latency in milliseconds")
    summary: str = Field(description="Executive summary of verification verdict")


def _arrow_type_from_string(type_str: str) -> pa.DataType:
    """Map canonical Arrow type string to PyArrow DataType; raises ValueError on unrecognized types."""
    t = type_str.strip()
    t_low = t.lower()

    if not is_recognized_type(t):
        raise ValueError(f"Unsupported or unrecognized Arrow type representation: '{type_str}'")

    if t_low in ("int8", "pa.int8()"):
        return pa.int8()
    if t_low in ("int16", "pa.int16()"):
        return pa.int16()
    if t_low in ("int32", "pa.int32()"):
        return pa.int32()
    if t_low in ("int64", "pa.int64()"):
        return pa.int64()
    if t_low in ("uint8", "pa.uint8()"):
        return pa.uint8()
    if t_low in ("uint16", "pa.uint16()"):
        return pa.uint16()
    if t_low in ("uint32", "pa.uint32()"):
        return pa.uint32()
    if t_low in ("uint64", "pa.uint64()"):
        return pa.uint64()
    if t_low in ("float16", "halffloat"):
        return pa.float16()
    if t_low in ("float", "float32"):
        return pa.float32()
    if t_low in ("double", "float64"):
        return pa.float64()
    if t_low in ("string", "utf8"):
        return pa.string()
    if t_low in ("large_string", "large_utf8"):
        return pa.large_string()
    if t_low == "binary":
        return pa.binary()
    if t_low == "large_binary":
        return pa.large_binary()
    if t_low in ("bool", "boolean"):
        return pa.bool_()
    if t_low in ("date32", "date32[day]"):
        return pa.date32()
    if t_low in ("date64", "date64[ms]"):
        return pa.date64()

    # FixedSizeList: e.g. fixed_size_list<float>[512] or fixed_size_list<item: float32>[768]
    m_fsl = re.match(r"^fixed_size_list<(?:item:\s*)?(.+)>\[(\d+)\]$", t_low)
    if m_fsl:
        inner = _arrow_type_from_string(m_fsl.group(1))
        dim = int(m_fsl.group(2))
        return pa.list_(inner, list_size=dim)

    # Standard List / LargeList
    m_llist = re.match(r"^large_list<(?:item:\s*)?(.+)>$", t_low)
    if m_llist:
        return pa.large_list(_arrow_type_from_string(m_llist.group(1)))

    m_list = re.match(r"^list<(?:item:\s*)?(.+)>$", t_low)
    if m_list:
        return pa.list_(_arrow_type_from_string(m_list.group(1)))

    # Map
    mp = _parse_map_params(t)
    if mp:
        k_t = _arrow_type_from_string(mp[0])
        v_t = _arrow_type_from_string(mp[1])
        keys_sorted = "keys_sorted" in t_low
        return pa.map_(k_t, v_t, keys_sorted=keys_sorted)

    # Decimal
    dec_params = _parse_decimal_params(t)
    if dec_params:
        d_type, prec, scale = dec_params
        if d_type == "decimal256":
            return pa.decimal256(prec, scale)
        return pa.decimal128(prec, scale)

    # Timestamp (preserve case of timezone!)
    ts_params = _parse_timestamp_params(t)
    if ts_params:
        unit, tz = ts_params
        return pa.timestamp(unit, tz=tz)

    # Duration
    m_dur = re.match(r"^duration\[(s|ms|us|ns)\]$", t_low)
    if m_dur:
        return pa.duration(m_dur.group(1))

    # Time32 / Time64
    m_t32 = re.match(r"^time32\[(s|ms)\]$", t_low)
    if m_t32:
        return pa.time32(m_t32.group(1))
    m_t64 = re.match(r"^time64\[(us|ns)\]$", t_low)
    if m_t64:
        return pa.time64(m_t64.group(1))

    raise ValueError(f"Unsupported Arrow type string for sandbox execution: '{type_str}'")


def _field_def_to_arrow_field(f: FieldDefinition) -> pa.Field:
    """Convert a FieldDefinition (including nested children and metadata) into a pa.Field."""
    meta = {k.encode("utf-8"): v.encode("utf-8") for k, v in f.metadata.items()} if f.metadata else None
    t_low = f.type.strip().lower()
    if t_low.startswith("struct") and f.children:
        child_fields = [_field_def_to_arrow_field(c) for c in f.children]
        pa_type: pa.DataType = pa.struct(child_fields)
    else:
        pa_type = _arrow_type_from_string(f.type)
    return pa.field(f.name, pa_type, nullable=f.nullable, metadata=meta)


def _generate_synthetic_array(
    f: FieldDefinition,
    row_count: int,
    inject_boundary_values: bool = True,
    extreme_timestamp_years: bool = False,
    overflow_int_values: bool = False,
) -> Tuple[pa.Field, pa.Array]:
    """Generate a synthetic pa.Field and pa.Array for a FieldDefinition."""
    t = f.type.strip()
    t_low = t.lower()
    nullable = f.nullable
    pa_field = _field_def_to_arrow_field(f)

    if t_low.startswith("struct") and f.children:
        child_arrays: List[pa.Array] = []
        child_fields: List[pa.Field] = []
        for c in f.children:
            cf, ca = _generate_synthetic_array(
                c,
                row_count=row_count,
                inject_boundary_values=inject_boundary_values,
                extreme_timestamp_years=extreme_timestamp_years,
                overflow_int_values=overflow_int_values,
            )
            child_fields.append(cf)
            child_arrays.append(ca)
        struct_arr = pa.StructArray.from_arrays(child_arrays, fields=child_fields)
        return pa_field, struct_arr

    if t_low in ("int8", "int16", "int32"):
        vals: List[Optional[int]] = []
        max_v = 127 if t_low == "int8" else (32767 if t_low == "int16" else 2147483647)
        min_v = -128 if t_low == "int8" else (-32768 if t_low == "int16" else -2147483648)
        for i in range(row_count):
            if nullable and i == 0:
                vals.append(None)
            elif inject_boundary_values and i == 1:
                vals.append(max_v)
            elif inject_boundary_values and i == 2:
                vals.append(min_v)
            else:
                vals.append((i * 17) % max_v)
        return pa_field, pa.array(vals, type=pa_field.type)

    if t_low == "int64":
        vals_64: List[Optional[int]] = []
        for i in range(row_count):
            if nullable and i == 0:
                vals_64.append(None)
            elif overflow_int_values and i == 1:
                vals_64.append(9_000_000_000)
            elif inject_boundary_values and i == 2:
                vals_64.append(9223372036854775807)
            else:
                vals_64.append(i * 1000 + 1)
        return pa_field, pa.array(vals_64, type=pa.int64())

    if t_low in ("uint8", "uint16", "uint32", "uint64"):
        max_u = {"uint8": 255, "uint16": 65535, "uint32": 4294967295, "uint64": 18446744073709551615}[t_low]
        u_vals: List[Optional[int]] = []
        for i in range(row_count):
            if nullable and i == 0:
                u_vals.append(None)
            elif inject_boundary_values and i == 1:
                u_vals.append(max_u)
            else:
                u_vals.append(i * 7)
        return pa_field, pa.array(u_vals, type=pa_field.type)

    if t_low in ("bool", "boolean"):
        b_vals: List[Optional[bool]] = [
            None if (nullable and i == 0) else (i % 2 == 0) for i in range(row_count)
        ]
        return pa_field, pa.array(b_vals, type=pa.bool_())

    if t_low in ("float", "float32", "double", "float64", "float16", "halffloat"):
        f_vals: List[Optional[float]] = []
        for i in range(row_count):
            if nullable and i == 0:
                f_vals.append(None)
            elif inject_boundary_values and i == 1 and t_low in ("double", "float64"):
                f_vals.append(123456.789012345)
            else:
                f_vals.append(float(i) * 1.5)
        return pa_field, pa.array(f_vals, type=pa_field.type)

    if t_low in ("string", "large_string", "utf8", "large_utf8"):
        s_vals: List[Optional[str]] = [
            None if (nullable and i == 0) else f"val_{f.name}_{i}" for i in range(row_count)
        ]
        return pa_field, pa.array(s_vals, type=pa_field.type)

    if t_low.startswith("decimal"):
        dec_p = _parse_decimal_params(t)
        scale = dec_p[2] if dec_p else 2
        d_vals: List[Optional[decimal.Decimal]] = []
        for i in range(row_count):
            if nullable and i == 0:
                d_vals.append(None)
            else:
                frac = "".join(str((i + j + 1) % 9 + 1) for j in range(max(0, scale)))
                v_str = f"{i + 10}.{frac}" if scale > 0 else f"{i + 10}"
                d_vals.append(decimal.Decimal(v_str))
        return pa_field, pa.array(d_vals, type=pa_field.type)

    if t_low.startswith("timestamp"):
        ts_p = _parse_timestamp_params(t)
        unit = ts_p[0] if ts_p else "ms"
        mult = {"s": 1, "ms": 1000, "us": 1_000_000, "ns": 1_000_000_000}[unit]
        base_s = 1770000000
        ts_vals: List[Optional[int]] = []
        for i in range(row_count):
            if nullable and i == 0:
                ts_vals.append(None)
            elif extreme_timestamp_years and i == 1:
                ts_vals.append(10_413_792_000 * mult)
            else:
                ts_vals.append((base_s + i * 3600) * mult)
        return pa_field, pa.array(ts_vals, type=pa_field.type)

    if "fixed_size_list" in t_low:
        m_fsl = re.match(r"^fixed_size_list<(?:item:\s*)?(.+)>\[(\d+)\]$", t_low)
        dim = int(m_fsl.group(2)) if m_fsl else 4
        list_vals: List[Optional[List[float]]] = [
            None if (nullable and i == 0) else [float(j) * 0.1 for j in range(dim)] for i in range(row_count)
        ]
        return pa_field, pa.array(list_vals, type=pa_field.type)

    if t_low.startswith("list<") or t_low.startswith("large_list<"):
        inner_str = _parse_list_inner(t) or "int32"
        inner_f = FieldDefinition(name="item", type=inner_str, nullable=True)
        _, inner_arr = _generate_synthetic_array(inner_f, row_count=max(1, row_count))
        sample_val = inner_arr[0].as_py() if len(inner_arr) > 0 else None
        l_vals = [None if (nullable and i == 0) else [sample_val] for i in range(row_count)]
        return pa_field, pa.array(l_vals, type=pa_field.type)

    return pa_field, pa.array(
        [None if (nullable and i == 0) else f"item_{i}" for i in range(row_count)],
        type=pa.string(),
    )


def generate_synthetic_parquet_table(
    contract: SchemaContract,
    row_count: int = 50,
    inject_boundary_values: bool = True,
    extreme_timestamp_years: bool = False,
    overflow_int_values: bool = False,
) -> pa.Table:
    """Generate representative PyArrow Table conforming to SchemaContract."""
    fields: List[pa.Field] = []
    arrays: List[pa.Array] = []
    for f in contract.fields:
        pf, arr = _generate_synthetic_array(
            f,
            row_count=row_count,
            inject_boundary_values=inject_boundary_values,
            extreme_timestamp_years=extreme_timestamp_years,
            overflow_int_values=overflow_int_values,
        )
        fields.append(pf)
        arrays.append(arr)
    meta = (
        {k.encode("utf-8"): v.encode("utf-8") for k, v in contract.metadata.items()}
        if contract.metadata
        else None
    )
    schema = pa.schema(fields, metadata=meta)
    return pa.Table.from_arrays(arrays, schema=schema)


def execute_migration_in_sandbox(
    input_table: pa.Table, plan: ExecutableMigrationPlan
) -> Tuple[pa.Table, List[str], List[str], int, bool]:
    """Execute migration operations in-memory using safe PyArrow transformations.

    Returns:
        (migrated_table, errors, warnings, operations_completed, uncertain_semantics)
    """
    current_table = input_table
    errors: List[str] = []
    warnings: List[str] = []
    completed = 0
    uncertain_semantics = False

    target_field_map = {f.name: f for f in plan.target_contract.fields}

    for idx, op in enumerate(plan.operations):
        step_label = f"Step {idx + 1} ({op.action}): "
        if op.requires_business_semantics:
            uncertain_semantics = True
            warnings.append(
                f"{step_label}Operation on '{op.source_column}' requires domain business semantics (`requires_business_semantics=True`) that cannot be verified synthetically."
            )
        try:
            action_val = op.action.value if isinstance(op.action, MigrationActionType) else str(op.action)
            if action_val == MigrationActionType.REJECT_UNSAFE.value:
                errors.append(
                    f"{step_label}Operation on '{op.source_column}' rejected as unsafe: {op.description}"
                )
                break

            elif action_val == MigrationActionType.SAFE_CAST.value:
                if op.source_column not in current_table.column_names:
                    errors.append(f"{step_label}Source column '{op.source_column}' not found for SAFE_CAST.")
                    break
                if not op.target_type:
                    errors.append(f"{step_label}SAFE_CAST on '{op.source_column}' requires `target_type`.")
                    break
                col_idx = current_table.schema.get_field_index(op.source_column)
                orig_field = current_table.schema.field(col_idx)
                col_arr = current_table.column(col_idx)
                target_pa_type = _arrow_type_from_string(op.target_type)
                casted_arr = pc.cast(col_arr, target_pa_type, safe=True)
                t_def = target_field_map.get(op.source_column)
                new_nullable = (
                    op.nullable
                    if op.nullable is not None
                    else (t_def.nullable if t_def is not None else orig_field.nullable)
                )
                new_field = pa.field(
                    op.source_column, target_pa_type, nullable=new_nullable, metadata=orig_field.metadata
                )
                current_table = current_table.set_column(col_idx, new_field, casted_arr)
                completed += 1

            elif action_val == MigrationActionType.RENAME_COLUMN.value:
                if op.source_column not in current_table.column_names:
                    errors.append(
                        f"{step_label}Source column '{op.source_column}' not found for RENAME_COLUMN."
                    )
                    break
                if not op.target_column:
                    errors.append(f"{step_label}RENAME_COLUMN missing target_column.")
                    break
                new_names = [
                    op.target_column if name == op.source_column else name
                    for name in current_table.column_names
                ]
                current_table = current_table.rename_columns(new_names)
                completed += 1

            elif action_val == MigrationActionType.DUAL_WRITE.value:
                if op.source_column not in current_table.column_names:
                    errors.append(f"{step_label}Source column '{op.source_column}' not found for DUAL_WRITE.")
                    break
                if not op.target_column:
                    errors.append(f"{step_label}DUAL_WRITE missing target_column.")
                    break
                src_idx = current_table.schema.get_field_index(op.source_column)
                src_field = current_table.schema.field(src_idx)
                col_arr = current_table.column(src_idx)
                t_def = target_field_map.get(op.target_column)
                new_nullable = (
                    op.nullable
                    if op.nullable is not None
                    else (t_def.nullable if t_def is not None else src_field.nullable)
                )
                if op.target_type:
                    target_pa_type = _arrow_type_from_string(op.target_type)
                    col_arr = pc.cast(col_arr, target_pa_type, safe=True)
                    new_field = pa.field(op.target_column, target_pa_type, nullable=new_nullable)
                else:
                    new_field = pa.field(op.target_column, src_field.type, nullable=new_nullable)
                current_table = current_table.append_column(new_field, col_arr)
                completed += 1

            elif action_val == MigrationActionType.ADD_COLUMN.value:
                col_name = op.target_column or op.source_column
                if not col_name:
                    errors.append(f"{step_label}ADD_COLUMN requires a column name.")
                    break
                target_type_str = op.target_type
                if not target_type_str and col_name in target_field_map:
                    target_type_str = target_field_map[col_name].type
                if not target_type_str:
                    errors.append(f"{step_label}ADD_COLUMN for '{col_name}' requires `target_type`.")
                    break
                target_pa_type = _arrow_type_from_string(target_type_str)
                t_def = target_field_map.get(col_name)
                new_nullable = (
                    op.nullable
                    if op.nullable is not None
                    else (t_def.nullable if t_def is not None else (op.default_value is None))
                )
                if op.default_value is None and not new_nullable:
                    errors.append(
                        f"{step_label}Cannot ADD_COLUMN '{col_name}' as non-nullable (`nullable=False`) with `default_value=None`."
                    )
                    break
                if op.default_value is None:
                    arr = pa.nulls(current_table.num_rows, type=target_pa_type)
                else:
                    raw_arr = pa.array([op.default_value] * current_table.num_rows)
                    arr = pc.cast(raw_arr, target_pa_type, safe=True)
                new_meta = (
                    {k.encode("utf-8"): v.encode("utf-8") for k, v in t_def.metadata.items()}
                    if (t_def and t_def.metadata)
                    else None
                )
                new_field = pa.field(col_name, target_pa_type, nullable=new_nullable, metadata=new_meta)
                current_table = current_table.append_column(new_field, arr)
                completed += 1

            elif action_val == MigrationActionType.DROP_COLUMN.value:
                if op.source_column not in current_table.column_names:
                    errors.append(
                        f"{step_label}Source column '{op.source_column}' not found for DROP_COLUMN."
                    )
                    break
                current_table = current_table.drop([op.source_column])
                warnings.append(
                    f"{step_label}Dropped column '{op.source_column}'. Ensure all downstream consumers have migrated."
                )
                completed += 1

            else:
                errors.append(f"{step_label}Unsupported migration operation '{op.action}'.")
                break

        except (
            pa.ArrowInvalid,
            pa.ArrowTypeError,
            pa.ArrowNotImplementedError,
            ValueError,
            TypeError,
        ) as exc:
            errors.append(
                f"{step_label}Operation failed on '{op.source_column}' -> '{op.target_type or op.target_column}' (overflow/precision loss or invalid cast): {exc}"
            )
            break
        except Exception as exc:
            errors.append(f"{step_label}Unexpected error (operation failed) on '{op.source_column}': {exc}")
            break

    return current_table, errors, warnings, completed, uncertain_semantics


def _validate_output_schema_against_target(
    readback_schema: SchemaContract,
    target_contract: SchemaContract,
    strict_schema_validation: bool = True,
    enforce_field_order: bool = False,
    enforce_metadata: bool = False,
) -> Tuple[bool, List[str], List[str], List[str]]:
    """Validate migrated output schema against target_contract.

    Checks:
    - Missing required target columns
    - Unexpected extra columns in output (rejected when `strict_schema_validation=True`)
    - Canonical type equality and nested children structure via `compare_schemas`
    - Nullability violations (output column is nullable when target_contract requires `nullable=False`)
    - Field ordering (when `enforce_field_order=True`)
    - Schema and field metadata (when `enforce_metadata=True`)
    """
    actual_map = {f.name: f for f in readback_schema.fields}
    target_map = {f.name: f for f in target_contract.fields}

    missing_cols = [name for name in target_map if name not in actual_map]
    unexpected_cols = [name for name in actual_map if name not in target_map]
    discrepancies: List[str] = []

    for m in missing_cols:
        discrepancies.append(f"Missing target column '{m}' in migrated output.")

    if strict_schema_validation and unexpected_cols:
        for u in unexpected_cols:
            discrepancies.append(
                f"Unexpected extra column '{u}' present in migrated output (not declared in target_contract)."
            )

    if enforce_field_order:
        actual_names = [f.name for f in readback_schema.fields]
        target_names = [f.name for f in target_contract.fields]
        if actual_names != target_names:
            discrepancies.append(
                f"Field order mismatch: migrated output has {actual_names}, expected {target_names}."
            )

    if enforce_metadata and target_contract.metadata != readback_schema.metadata:
        discrepancies.append(
            f"Top-level schema metadata mismatch: actual={readback_schema.metadata}, expected={target_contract.metadata}."
        )

    # Check common fields for type, nullability, nested structure, and field metadata
    for name, target_field in target_map.items():
        if name not in actual_map:
            continue
        actual_field = actual_map[name]
        if canonical_type(actual_field.type) != canonical_type(target_field.type):
            discrepancies.append(
                f"Column '{name}' type mismatch: migrated output has '{actual_field.type}', target contract expects '{target_field.type}'."
            )
        if not target_field.nullable and actual_field.nullable:
            discrepancies.append(
                f"Column '{name}' nullability mismatch: migrated output is nullable (`nullable=True`), but target contract requires `nullable=False`."
            )
        if enforce_metadata and target_field.metadata != actual_field.metadata:
            discrepancies.append(
                f"Column '{name}' metadata mismatch: actual={actual_field.metadata}, expected={target_field.metadata}."
            )

    # Also run structural diff (`compare_schemas`) for nested struct/list/map children
    diff_report = compare_schemas(target_contract, readback_schema)
    for ch in diff_report.diffs:
        if "." in ch.field_name or "[" in ch.field_name or "<" in ch.field_name:
            if (
                ch.severity in (Severity.BREAKING, Severity.WARNING)
                and ch.change_type != ChangeType.FIELD_ORDER_CHANGED
            ):
                discrepancies.append(
                    f"Nested structural discrepancy at '{ch.field_name}' ({ch.change_type.value}): {ch.description}"
                )

    schema_matched = len(discrepancies) == 0
    return schema_matched, missing_cols, unexpected_cols, discrepancies


def verify_migration_plan(
    plan: ExecutableMigrationPlan,
    consumers: Optional[List[ConsumerContract]] = None,
    sample_rows: int = 100,
    test_table: Optional[pa.Table] = None,
    extreme_timestamp_years: bool = False,
    overflow_int_values: bool = False,
    sample_file: Optional[str] = None,
    user_data_authorized: bool = False,
    validation_mode: str = "synthetic",
) -> MigrationVerificationResult:
    """Verify a migration plan end-to-end in a temporary Parquet sandbox."""
    if sample_rows < 1:
        raise ValueError(f"sample_rows must be >= 1, got {sample_rows}")

    t0 = time.perf_counter()
    errors: List[str] = []
    warnings: List[str] = []
    actual_validation_mode = validation_mode
    data_rows_read = 0

    if sample_file is not None and not user_data_authorized:
        raise PermissionError(
            "Explicit operator authorization (user_data_authorized=True) required to load sample user data rows."
        )

    try:
        if sample_file is not None:
            # Bounded batch reading: do not read full file into memory before slicing
            pf = pq.ParquetFile(sample_file)
            batches = []
            total_read = 0
            for batch in pf.iter_batches(batch_size=sample_rows):
                batches.append(batch)
                total_read += len(batch)
                if total_read >= sample_rows:
                    break
            input_table = pa.Table.from_batches(batches) if batches else pf.schema_arrow.empty_table()
            if sample_rows > 0 and input_table.num_rows > sample_rows:
                input_table = input_table.slice(0, sample_rows)
            sample_rows = input_table.num_rows
            data_rows_read = sample_rows
            actual_validation_mode = "user_sample"
        elif test_table is not None:
            input_table = test_table
            if sample_rows > 0 and input_table.num_rows > sample_rows:
                input_table = input_table.slice(0, sample_rows)
            sample_rows = input_table.num_rows
            data_rows_read = sample_rows
            actual_validation_mode = "user_sample" if user_data_authorized else "provided_in_memory"
        else:
            input_table = generate_synthetic_parquet_table(
                plan.source_contract,
                row_count=sample_rows,
                inject_boundary_values=True,
                extreme_timestamp_years=extreme_timestamp_years,
                overflow_int_values=overflow_int_values,
            )
            actual_validation_mode = "synthetic"
    except Exception as exc:
        elapsed = (time.perf_counter() - t0) * 1000.0
        return MigrationVerificationResult(
            success=False,
            transformation_succeeded=False,
            deployment_ready=False,
            readiness_status=MigrationReadinessStatus.TRANSFORMATION_FAILED,
            rows_tested=0,
            operations_total=len(plan.operations),
            operations_completed=0,
            row_count_preserved=False,
            schema_matched=False,
            value_invariants_passed=False,
            consumer_compatible=False,
            errors=[f"Failed to synthesize source table from source_contract: {exc}"],
            execution_latency_ms=round(elapsed, 2),
            summary=f"Migration verification FAILED during synthetic source table generation: {exc}",
        )

    with tempfile.TemporaryDirectory(prefix="parquet_guard_mig_") as tmpdir:
        src_path = os.path.join(tmpdir, "source.parquet")
        dst_path = os.path.join(tmpdir, "migrated.parquet")

        pq.write_table(input_table, src_path)
        reloaded_input = pq.read_table(src_path)

        migrated_table, op_errors, op_warnings, ops_completed, uncertain_semantics = (
            execute_migration_in_sandbox(reloaded_input, plan)
        )
        errors.extend(op_errors)
        warnings.extend(op_warnings)
        transformation_succeeded = len(op_errors) == 0

        if not transformation_succeeded:
            elapsed = (time.perf_counter() - t0) * 1000.0
            return MigrationVerificationResult(
                success=False,
                transformation_succeeded=False,
                deployment_ready=False,
                readiness_status=MigrationReadinessStatus.TRANSFORMATION_FAILED,
                rows_tested=sample_rows,
                operations_total=len(plan.operations),
                operations_completed=ops_completed,
                row_count_preserved=False,
                schema_matched=False,
                value_invariants_passed=False,
                consumer_compatible=False,
                uncertain_semantics=uncertain_semantics,
                errors=errors,
                warnings=warnings,
                execution_latency_ms=round(elapsed, 2),
                summary=(
                    f"Migration transformation FAILED in sandbox ({ops_completed}/{len(plan.operations)} steps completed): "
                    f"{'; '.join(errors)}"
                ),
            )

        try:
            pq.write_table(migrated_table, dst_path)
            readback_table = pq.read_table(dst_path)
            readback_schema = inspect_parquet_schema(dst_path)
        except Exception as exc:
            elapsed = (time.perf_counter() - t0) * 1000.0
            errors.append(f"Parquet write/readback invariant violation: {exc}")
            return MigrationVerificationResult(
                success=False,
                transformation_succeeded=True,
                deployment_ready=False,
                readiness_status=MigrationReadinessStatus.INVARIANT_VIOLATION,
                rows_tested=sample_rows,
                operations_total=len(plan.operations),
                operations_completed=ops_completed,
                row_count_preserved=migrated_table.num_rows == sample_rows,
                schema_matched=False,
                value_invariants_passed=False,
                consumer_compatible=False,
                uncertain_semantics=uncertain_semantics,
                errors=errors,
                warnings=warnings,
                execution_latency_ms=round(elapsed, 2),
                summary=f"Migration plan '{plan.plan_id}' failed Parquet write/readback invariant check: {exc}",
            )

        # Preserve explicit non-nullable field declarations from migrated_table when Parquet readback marks fields nullable
        migrated_nullability = {
            migrated_table.schema.field(i).name: migrated_table.schema.field(i).nullable
            for i in range(len(migrated_table.schema))
        }
        for f_def in readback_schema.fields:
            if f_def.name in migrated_nullability:
                f_def.nullable = migrated_nullability[f_def.name]
        if migrated_table.schema.metadata:
            readback_schema.metadata = {
                k.decode("utf-8", errors="replace"): v.decode("utf-8", errors="replace")
                for k, v in migrated_table.schema.metadata.items()
            }

        rows_preserved = readback_table.num_rows == sample_rows
        if not rows_preserved:
            errors.append(
                f"Row count mismatch: input had {sample_rows} rows, output has {readback_table.num_rows} rows."
            )

        # Check value invariants (non-null target columns must contain 0 nulls; cast values must not silently null out non-null inputs)
        value_invariants_passed = True
        for f_target in plan.target_contract.fields:
            if f_target.name in readback_table.column_names:
                col_data = readback_table.column(f_target.name)
                if not f_target.nullable and col_data.null_count > 0:
                    value_invariants_passed = False
                    errors.append(
                        f"Nullability invariant violated: non-nullable target column '{f_target.name}' contains {col_data.null_count} nulls after migration."
                    )

        # Check target schema structural conformance
        schema_matched, missing_cols, unexpected_cols, schema_discrepancies = (
            _validate_output_schema_against_target(
                readback_schema=readback_schema,
                target_contract=plan.target_contract,
                strict_schema_validation=plan.strict_schema_validation,
                enforce_field_order=plan.enforce_field_order,
                enforce_metadata=plan.enforce_metadata,
            )
        )
        if not schema_matched:
            errors.extend(schema_discrepancies)

        # Evaluate downstream consumer contracts
        consumer_compat: Dict[str, ConsumerCompatibilityStatus] = {}
        incompatible_consumers: List[str] = []
        blocking_consumers: List[str] = []

        if consumers:
            for c in consumers:
                ev = evaluate_single_consumer(readback_schema, c)
                consumer_compat[c.consumer_id] = ev.status
                if ev.status in (
                    ConsumerCompatibilityStatus.INCOMPATIBLE,
                    ConsumerCompatibilityStatus.UNCERTAIN,
                ):
                    incompatible_consumers.append(c.consumer_id)
                if ev.is_blocking:
                    blocking_consumers.append(c.consumer_id)
                    msg = (
                        f"Downstream consumer '{c.consumer_id}' is INCOMPATIBLE with migrated output schema "
                        f"({ev.status.value}, blocking=True): {ev.summary}"
                    )
                    errors.append(msg)
                    warnings.append(msg)
                elif ev.status == ConsumerCompatibilityStatus.INCOMPATIBLE:
                    warnings.append(
                        f"Downstream consumer '{c.consumer_id}' is INCOMPATIBLE with migrated output schema "
                        f"(non-critical, blocking=False): {ev.summary}"
                    )

        consumer_compatible = len(blocking_consumers) == 0 and len(incompatible_consumers) == 0

        if not rows_preserved or not value_invariants_passed:
            readiness_status = MigrationReadinessStatus.INVARIANT_VIOLATION
        elif not schema_matched:
            readiness_status = MigrationReadinessStatus.SCHEMA_MISMATCH
        elif not consumer_compatible:
            readiness_status = MigrationReadinessStatus.CONSUMER_BREAKING
        elif uncertain_semantics:
            readiness_status = MigrationReadinessStatus.UNCERTAIN_SEMANTICS
            errors.append(
                "Migration contains steps with `requires_business_semantics=True`; cannot certify automatic deployment readiness."
            )
        else:
            readiness_status = MigrationReadinessStatus.DEPLOYMENT_READY

        deployment_ready = readiness_status == MigrationReadinessStatus.DEPLOYMENT_READY
        overall_success = deployment_ready

        elapsed = (time.perf_counter() - t0) * 1000.0
        if deployment_ready:
            summary_msg = (
                f"Migration plan '{plan.plan_id}' VERIFIED & DEPLOYMENT_READY on {sample_rows} synthetic rows "
                f"({ops_completed}/{len(plan.operations)} operations, {len(consumer_compat)} consumers compatible) in {elapsed:.1f}ms."
            )
        else:
            summary_msg = (
                f"Migration plan '{plan.plan_id}' NOT deployment-ready (status={readiness_status.value}, "
                f"transformation_succeeded={transformation_succeeded}, schema_matched={schema_matched}, "
                f"value_invariants_passed={value_invariants_passed}, consumer_compatible={consumer_compatible}): "
                f"{'; '.join(errors)}"
            )

        return MigrationVerificationResult(
            success=overall_success,
            transformation_succeeded=transformation_succeeded,
            deployment_ready=deployment_ready,
            readiness_status=readiness_status,
            rows_tested=sample_rows,
            operations_total=len(plan.operations),
            operations_completed=ops_completed,
            row_count_preserved=rows_preserved,
            schema_matched=schema_matched,
            unexpected_output_columns=unexpected_cols,
            missing_output_columns=missing_cols,
            schema_discrepancies=schema_discrepancies,
            value_invariants_passed=value_invariants_passed,
            consumer_compatible=consumer_compatible,
            consumer_compatibilities=consumer_compat,
            incompatible_consumers=incompatible_consumers,
            blocking_consumers=blocking_consumers,
            uncertain_semantics=uncertain_semantics,
            validation_mode=actual_validation_mode,
            data_rows_read=data_rows_read,
            user_data_authorized=user_data_authorized,
            errors=errors,
            warnings=warnings,
            execution_latency_ms=round(elapsed, 2),
            summary=summary_msg,
        )
