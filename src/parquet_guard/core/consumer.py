"""Consumer-aware data contract intelligence engine.

Defines:
1. ConsumerContract and ConsumerGroupSpecification models.
2. ConsumerFieldRequirement and compatibility policies.
3. ConsumerImpactEvaluation, ConsumerImpactReport, and ConsumerImpactGraph.
4. Deterministic consumer impact analysis linking schema findings to downstream consumers.
"""

from __future__ import annotations

import re
from enum import Enum
from typing import Any, ClassVar, Dict, List, Optional, Set, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

from parquet_guard.core.compatibility import (
    _normalize_type_str,
    _parse_decimal_params,
    _parse_timestamp_params,
    is_safe_widening,
)
from parquet_guard.core.models import (
    FieldDefinition,
    SchemaContract,
)


class ConsumerCompatibilityStatus(str, Enum):
    """Classification of consumer compatibility."""

    COMPATIBLE = "COMPATIBLE"
    CONDITIONALLY_COMPATIBLE = "CONDITIONALLY_COMPATIBLE"
    POTENTIALLY_BREAKING = "POTENTIALLY_BREAKING"
    INCOMPATIBLE = "INCOMPATIBLE"
    UNCERTAIN = "UNCERTAIN"


class ConsumerFieldRequirement(BaseModel):
    """Specification of a consumer's expectations for a single column/field."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(description="Field name or dotted path, e.g. 'user_id' or 'payload.event_time'")
    required: bool = Field(default=True, description="Whether this field must be present in producer schema")
    permitted_types: List[str] = Field(
        default_factory=list,
        description="List of permitted Arrow types. Empty list means any type is accepted.",
    )
    allow_null: bool = Field(
        default=True, description="Whether null values are permitted. False requires non-null column."
    )
    list_size: Optional[int] = Field(
        default=None, description="Exact dimension constraint for FixedSizeList / embedding vector"
    )
    precision: Optional[int] = Field(default=None, description="Maximum or required decimal precision")
    scale: Optional[int] = Field(default=None, description="Required decimal scale")
    time_unit: Optional[str] = Field(
        default=None, description="Required timestamp or duration unit (s, ms, us, ns)"
    )
    timezone: Optional[str] = Field(
        default=None, description="Required timezone string (e.g. 'UTC') or None for naive"
    )
    critical: bool = Field(
        default=True,
        description="Whether violation of this requirement is a critical breaking failure for this consumer",
    )
    description: Optional[str] = Field(
        default=None, description="Human description of how this consumer uses this field"
    )

    @field_validator("time_unit")
    @classmethod
    def validate_time_unit(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v.lower() not in {"s", "ms", "us", "ns"}:
            raise ValueError(f"Invalid time_unit '{v}'. Must be one of: s, ms, us, ns")
        return v.lower() if v else None


class ConsumerContract(BaseModel):
    """Declarative contract specifying downstream consumer expectations."""

    model_config = ConfigDict(extra="forbid")

    SUPPORTED_VERSIONS: ClassVar[Set[str]] = {"1.0"}

    version: str = Field(default="1.0", description="Consumer contract format version")
    consumer_id: str = Field(description="Unique machine-readable identifier, e.g. 'duckdb-analytics'")
    consumer_name: str = Field(description="Human-readable consumer name, e.g. 'DuckDB Analytics Engine'")
    description: Optional[str] = Field(
        default=None, description="Context regarding the downstream pipeline or consumer system"
    )
    target_dataset: Optional[str] = Field(
        default=None, description="Optional dataset identifier or pattern this consumer attaches to"
    )
    fields: List[ConsumerFieldRequirement] = Field(
        default_factory=list, description="List of declared field expectations"
    )
    allow_extra_fields: bool = Field(
        default=True,
        description="Whether the producer having unrequested extra columns is acceptable to this consumer",
    )
    strict_ordering: bool = Field(
        default=False,
        description="Whether columns must appear in the exact order declared in this contract",
    )
    strict_nullability: bool = Field(
        default=True,
        description="If True, producer column being nullable when non-null requested is INCOMPATIBLE",
    )
    compatibility_policy: str = Field(
        default="permissive", description="'permissive' (safe widening allowed) or 'strict' (exact match)"
    )
    metadata: Dict[str, str] = Field(default_factory=dict, description="Arbitrary consumer metadata")

    @field_validator("version")
    @classmethod
    def validate_version(cls, v: str) -> str:
        if v not in cls.SUPPORTED_VERSIONS:
            raise ValueError(
                f"Unsupported consumer contract version '{v}'. Supported: {sorted(cls.SUPPORTED_VERSIONS)}"
            )
        return v

    def get_field_requirement(self, name: str) -> Optional[ConsumerFieldRequirement]:
        """Find requirement for a field by name."""
        for f in self.fields:
            if f.name == name:
                return f
        return None

    def required_field_names(self) -> List[str]:
        """Return list of strictly required field names."""
        return [f.name for f in self.fields if f.required]


class ConsumerGroupSpecification(BaseModel):
    """Versioned container describing multiple consumers for a producer dataset."""

    model_config = ConfigDict(extra="forbid")

    SUPPORTED_VERSIONS: ClassVar[Set[str]] = {"1.0"}

    version: str = Field(default="1.0", description="Group specification version")
    description: Optional[str] = Field(default=None, description="Description of the consumer group")
    consumers: List[ConsumerContract] = Field(
        default_factory=list, description="List of declared consumer contracts"
    )

    @field_validator("version")
    @classmethod
    def validate_version(cls, v: str) -> str:
        if v not in cls.SUPPORTED_VERSIONS:
            raise ValueError(f"Unsupported version '{v}'. Supported: {sorted(cls.SUPPORTED_VERSIONS)}")
        return v

    def get_consumer(self, consumer_id: str) -> Optional[ConsumerContract]:
        for c in self.consumers:
            if c.consumer_id == consumer_id:
                return c
        return None


class ConsumerFieldEvaluation(BaseModel):
    """Detailed evaluation result for a single field requirement against producer schema."""

    model_config = ConfigDict(extra="forbid")

    field_name: str = Field(description="Field path evaluated")
    status: ConsumerCompatibilityStatus = Field(description="Compatibility classification for this field")
    requirement: Optional[ConsumerFieldRequirement] = Field(default=None, description="Declared requirement")
    producer_type: Optional[str] = Field(default=None, description="Actual producer column type")
    producer_nullable: Optional[bool] = Field(default=None, description="Actual producer nullability")
    violation_reason: Optional[str] = Field(default=None, description="Explanation of violation or risk")
    linked_finding_id: Optional[str] = Field(default=None, description="Stable finding ID if linked")
    is_critical: bool = Field(default=True, description="Whether this is a critical breaking issue")


class ConsumerImpactEvaluation(BaseModel):
    """Compatibility evaluation outcome for a single downstream consumer."""

    model_config = ConfigDict(extra="forbid")

    consumer_id: str = Field(description="Consumer identifier")
    consumer_name: str = Field(description="Human-readable consumer name")
    status: ConsumerCompatibilityStatus = Field(description="Overall consumer compatibility status")
    is_blocking: bool = Field(description="True if this consumer is blocked by the schema change")
    field_evaluations: List[ConsumerFieldEvaluation] = Field(
        default_factory=list, description="Field-by-field evaluation breakdown"
    )
    missing_required_fields: List[str] = Field(
        default_factory=list, description="Fields required by consumer but missing in producer"
    )
    type_mismatches: List[str] = Field(
        default_factory=list, description="Fields whose type is incompatible with consumer requirements"
    )
    nullability_violations: List[str] = Field(
        default_factory=list, description="Fields where producer nullability violates consumer requirement"
    )
    unexpected_extra_fields: List[str] = Field(
        default_factory=list, description="Extra fields rejected by strict consumer policy"
    )
    dimension_mismatches: List[str] = Field(
        default_factory=list, description="Fixed-size list or vector dimension mismatches"
    )
    summary: str = Field(description="Concise evaluation summary")
    recommendations: List[str] = Field(
        default_factory=list, description="Actionable recommendations for this consumer"
    )


class ImpactGraphNode(BaseModel):
    """A node in the producer-dataset-consumer impact graph."""

    model_config = ConfigDict(extra="forbid")

    id: str
    label: str
    node_type: str = Field(description="'producer', 'dataset', or 'consumer'")
    status: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ImpactGraphEdge(BaseModel):
    """A directed edge in the producer-dataset-consumer impact graph."""

    model_config = ConfigDict(extra="forbid")

    source: str
    target: str
    relationship: str = Field(description="'produces', 'consumes', etc.")
    status: Optional[str] = None
    violations_count: int = 0
    details: List[str] = Field(default_factory=list)


class ConsumerImpactGraph(BaseModel):
    """Structured graph representing producer, dataset, and downstream consumers."""

    model_config = ConfigDict(extra="forbid")

    nodes: List[ImpactGraphNode] = Field(default_factory=list)
    edges: List[ImpactGraphEdge] = Field(default_factory=list)


class ConsumerImpactReport(BaseModel):
    """Comprehensive consumer-aware schema intelligence report."""

    model_config = ConfigDict(extra="forbid")

    report_version: str = Field(default="1.0", description="Report schema version")
    dataset_identifier: str = Field(description="Target dataset or Parquet file identifier")
    overall_status: ConsumerCompatibilityStatus = Field(
        description="Aggregate compatibility verdict across all evaluated consumers"
    )
    total_consumers: int = Field(description="Number of consumers evaluated")
    compatible_consumers: int = Field(description="Count of fully compatible consumers")
    conditionally_compatible_consumers: int = Field(
        default=0, description="Count of conditionally compatible consumers"
    )
    incompatible_consumers: int = Field(description="Count of incompatible consumers")
    uncertain_consumers: int = Field(default=0, description="Count of uncertain consumers")
    evaluations: List[ConsumerImpactEvaluation] = Field(
        default_factory=list, description="Per-consumer evaluation results"
    )
    impact_graph: ConsumerImpactGraph = Field(
        default_factory=ConsumerImpactGraph, description="Graph representation of impact topology"
    )
    compatibility_matrix: Dict[str, Dict[str, str]] = Field(
        default_factory=dict,
        description="Matrix mapping consumer_id -> {field_name: status_code}",
    )
    summary: str = Field(description="Executive summary of downstream consumer impact")


def _evaluate_type_compatibility(
    req: ConsumerFieldRequirement,
    producer_field: FieldDefinition,
    policy: str,
) -> tuple[ConsumerCompatibilityStatus, Optional[str]]:
    """Evaluate whether a producer field's type satisfies the consumer field requirement."""
    p_type = _normalize_type_str(producer_field.type)

    # 1. Permitted types check
    if req.permitted_types:
        norm_permitted = [_normalize_type_str(t) for t in req.permitted_types]
        if p_type in norm_permitted:
            return ConsumerCompatibilityStatus.COMPATIBLE, None

        # Check if producer type can be safely widened to any permitted type in permissive policy
        if policy == "permissive":
            for allowed in norm_permitted:
                if is_safe_widening(p_type, allowed):
                    return (
                        ConsumerCompatibilityStatus.CONDITIONALLY_COMPATIBLE,
                        f"Producer type '{producer_field.type}' is a safe widening towards permitted '{allowed}'",
                    )
            # Or if consumer permitted type can safely accept producer type
            for allowed in norm_permitted:
                if is_safe_widening(allowed, p_type):
                    return (
                        ConsumerCompatibilityStatus.CONDITIONALLY_COMPATIBLE,
                        f"Producer type '{producer_field.type}' is wider than permitted '{allowed}' (requires runtime check)",
                    )
        return (
            ConsumerCompatibilityStatus.INCOMPATIBLE,
            f"Producer type '{producer_field.type}' is not in declared permitted types: {req.permitted_types}",
        )

    # 2. FixedSizeList dimension check (critical for embeddings)
    if req.list_size is not None:
        p_list_size = producer_field.list_size
        if p_list_size is None and "fixed_size_list" in p_type:
            m = re.search(r"\[(\d+)\]$", p_type)
            if m:
                p_list_size = int(m.group(1))

        if p_list_size is None:
            return (
                ConsumerCompatibilityStatus.INCOMPATIBLE,
                f"Field requires FixedSizeList of dimension [{req.list_size}], but producer provides variable list or non-fixed type '{producer_field.type}'",
            )
        if p_list_size != req.list_size:
            return (
                ConsumerCompatibilityStatus.INCOMPATIBLE,
                f"FixedSizeList dimension mismatch: consumer requires [{req.list_size}], producer provides [{p_list_size}]",
            )

    # 3. Decimal precision and scale check
    if req.precision is not None or req.scale is not None:
        dec_params = _parse_decimal_params(producer_field.type)
        if not dec_params:
            return (
                ConsumerCompatibilityStatus.INCOMPATIBLE,
                f"Consumer requires decimal type with scale={req.scale}, but producer type is '{producer_field.type}'",
            )
        _, p_prec, p_scale = dec_params
        if req.scale is not None and p_scale != req.scale:
            return (
                ConsumerCompatibilityStatus.INCOMPATIBLE,
                f"Decimal scale mismatch: consumer requires scale={req.scale}, producer has scale={p_scale}",
            )
        if req.precision is not None and p_prec > req.precision:
            return (
                ConsumerCompatibilityStatus.CONDITIONALLY_COMPATIBLE,
                f"Decimal precision warning: producer precision ({p_prec}) exceeds consumer maximum ({req.precision})",
            )

    # 4. Timestamp unit and timezone check
    if req.time_unit is not None or req.timezone is not None:
        ts_params = _parse_timestamp_params(producer_field.type)
        if not ts_params:
            return (
                ConsumerCompatibilityStatus.INCOMPATIBLE,
                f"Consumer requires timestamp[{req.time_unit or '*'}, tz={req.timezone or '*'}], but producer type is '{producer_field.type}'",
            )
        p_unit, p_tz = ts_params
        if req.timezone is not None and p_tz != req.timezone:
            return (
                ConsumerCompatibilityStatus.INCOMPATIBLE,
                f"Timestamp timezone mismatch: consumer requires timezone '{req.timezone}', producer has '{p_tz}'",
            )
        if req.time_unit is not None:
            units = ["s", "ms", "us", "ns"]
            if req.time_unit not in units or p_unit not in units:
                return ConsumerCompatibilityStatus.INCOMPATIBLE, "Invalid timestamp units"
            if p_unit != req.time_unit:
                if policy == "permissive" and units.index(p_unit) >= units.index(req.time_unit):
                    return (
                        ConsumerCompatibilityStatus.CONDITIONALLY_COMPATIBLE,
                        f"Timestamp unit '{p_unit}' is finer than consumer requirement '{req.time_unit}' (potential temporal overflow risk)",
                    )
                return (
                    ConsumerCompatibilityStatus.INCOMPATIBLE,
                    f"Timestamp unit mismatch: consumer requires '{req.time_unit}', producer has '{p_unit}'",
                )

    return ConsumerCompatibilityStatus.COMPATIBLE, None


def evaluate_single_consumer(
    schema: SchemaContract,
    consumer: ConsumerContract,
) -> ConsumerImpactEvaluation:
    """Evaluate a single downstream consumer contract against a producer schema."""
    field_evals: List[ConsumerFieldEvaluation] = []
    missing_fields: List[str] = []
    type_mismatches: List[str] = []
    nullability_violations: List[str] = []
    unexpected_extra: List[str] = []
    dimension_mismatches: List[str] = []

    producer_fields_by_name = {f.name: f for f in schema.fields}
    req_fields_by_name = {r.name: r for r in consumer.fields}

    # 1. Check each declared field requirement
    for req in consumer.fields:
        p_field = producer_fields_by_name.get(req.name)
        if p_field is None:
            if req.required:
                missing_fields.append(req.name)
                field_evals.append(
                    ConsumerFieldEvaluation(
                        field_name=req.name,
                        status=ConsumerCompatibilityStatus.INCOMPATIBLE,
                        requirement=req,
                        violation_reason=f"Required field '{req.name}' is missing in producer schema",
                        is_critical=req.critical,
                    )
                )
            else:
                field_evals.append(
                    ConsumerFieldEvaluation(
                        field_name=req.name,
                        status=ConsumerCompatibilityStatus.COMPATIBLE,
                        requirement=req,
                        violation_reason="Optional field absent in producer schema",
                        is_critical=False,
                    )
                )
            continue

        # Nullability check
        null_status = ConsumerCompatibilityStatus.COMPATIBLE
        null_reason = None
        if not req.allow_null and p_field.nullable:
            if consumer.strict_nullability:
                null_status = ConsumerCompatibilityStatus.INCOMPATIBLE
                null_reason = f"Field '{req.name}' is nullable in producer, but consumer requires strictly non-null values"
                nullability_violations.append(req.name)
            else:
                null_status = ConsumerCompatibilityStatus.CONDITIONALLY_COMPATIBLE
                null_reason = f"Field '{req.name}' permits null in producer; runtime null checks required"

        # Type and constraint compatibility check
        type_status, type_reason = _evaluate_type_compatibility(
            req=req,
            producer_field=p_field,
            policy=consumer.compatibility_policy,
        )

        if type_status == ConsumerCompatibilityStatus.INCOMPATIBLE:
            if "dimension mismatch" in (type_reason or "").lower():
                dimension_mismatches.append(req.name)
            else:
                type_mismatches.append(req.name)

        # Combine status for this field
        if (
            type_status == ConsumerCompatibilityStatus.INCOMPATIBLE
            or null_status == ConsumerCompatibilityStatus.INCOMPATIBLE
        ):
            eff_status = ConsumerCompatibilityStatus.INCOMPATIBLE
            combined_reasons = [r for r in [type_reason, null_reason] if r]
            eff_reason = "; ".join(combined_reasons)
        elif (
            type_status == ConsumerCompatibilityStatus.CONDITIONALLY_COMPATIBLE
            or null_status == ConsumerCompatibilityStatus.CONDITIONALLY_COMPATIBLE
        ):
            eff_status = ConsumerCompatibilityStatus.CONDITIONALLY_COMPATIBLE
            combined_reasons = [r for r in [type_reason, null_reason] if r]
            eff_reason = "; ".join(combined_reasons)
        else:
            eff_status = ConsumerCompatibilityStatus.COMPATIBLE
            eff_reason = None

        field_evals.append(
            ConsumerFieldEvaluation(
                field_name=req.name,
                status=eff_status,
                requirement=req,
                producer_type=p_field.type,
                producer_nullable=p_field.nullable,
                violation_reason=eff_reason,
                is_critical=req.critical,
            )
        )

    # 2. Check for unexpected extra fields if allow_extra_fields is False
    if not consumer.allow_extra_fields:
        for p_name in schema.field_names():
            if p_name not in req_fields_by_name:
                unexpected_extra.append(p_name)
                field_evals.append(
                    ConsumerFieldEvaluation(
                        field_name=p_name,
                        status=ConsumerCompatibilityStatus.POTENTIALLY_BREAKING,
                        producer_type=producer_fields_by_name[p_name].type,
                        violation_reason=f"Extra field '{p_name}' not permitted by consumer policy",
                        is_critical=False,
                    )
                )

    # 3. Check field ordering if strict_ordering is True
    ordering_violation = False
    if consumer.strict_ordering and len(consumer.fields) > 1:
        producer_order = [f.name for f in schema.fields if f.name in req_fields_by_name]
        expected_order = [f.name for f in consumer.fields if f.name in producer_fields_by_name]
        if producer_order != expected_order:
            ordering_violation = True

    # 4. Compute overall consumer status
    has_critical_incompatible = any(
        fe.status == ConsumerCompatibilityStatus.INCOMPATIBLE and fe.is_critical for fe in field_evals
    )
    has_noncritical_incompatible = any(
        fe.status == ConsumerCompatibilityStatus.INCOMPATIBLE and not fe.is_critical for fe in field_evals
    )
    has_conditional = any(
        fe.status == ConsumerCompatibilityStatus.CONDITIONALLY_COMPATIBLE for fe in field_evals
    )
    has_extra_violation = len(unexpected_extra) > 0

    if has_critical_incompatible:
        overall_status = ConsumerCompatibilityStatus.INCOMPATIBLE
        is_blocking = True
    elif ordering_violation or has_noncritical_incompatible or has_extra_violation:
        overall_status = ConsumerCompatibilityStatus.POTENTIALLY_BREAKING
        is_blocking = False
    elif has_conditional:
        overall_status = ConsumerCompatibilityStatus.CONDITIONALLY_COMPATIBLE
        is_blocking = False
    else:
        overall_status = ConsumerCompatibilityStatus.COMPATIBLE
        is_blocking = False

    # Summary and recommendations
    recs: List[str] = []
    if missing_fields:
        recs.append(f"Restore required fields: {missing_fields}")
    if type_mismatches:
        recs.append(f"Reconcile type mismatches: {type_mismatches}")
    if dimension_mismatches:
        recs.append(f"Reconcile vector dimension constraints: {dimension_mismatches}")
    if nullability_violations:
        recs.append(
            f"Enforce non-null constraints or update consumer to handle nulls: {nullability_violations}"
        )
    if unexpected_extra:
        recs.append(f"Filter out extra fields before emitting to consumer: {unexpected_extra}")
    if ordering_violation:
        recs.append("Restore declared column order for strict positional consumer")

    summary = (
        f"{consumer.consumer_name} ({consumer.consumer_id}): "
        f"Status={overall_status.value}. "
        f"Missing={len(missing_fields)}, TypeMismatches={len(type_mismatches)}, "
        f"DimensionMismatches={len(dimension_mismatches)}, NullViolations={len(nullability_violations)}"
    )

    return ConsumerImpactEvaluation(
        consumer_id=consumer.consumer_id,
        consumer_name=consumer.consumer_name,
        status=overall_status,
        is_blocking=is_blocking,
        field_evaluations=field_evals,
        missing_required_fields=missing_fields,
        type_mismatches=type_mismatches,
        nullability_violations=nullability_violations,
        unexpected_extra_fields=unexpected_extra,
        dimension_mismatches=dimension_mismatches,
        summary=summary,
        recommendations=recs,
    )


def evaluate_consumer_impact(
    schema: SchemaContract,
    consumers: Union[List[ConsumerContract], ConsumerGroupSpecification],
    dataset_identifier: str = "dataset",
) -> ConsumerImpactReport:
    """Evaluate a producer schema against all registered downstream consumers."""
    consumer_list: List[ConsumerContract] = (
        consumers.consumers if isinstance(consumers, ConsumerGroupSpecification) else list(consumers)
    )
    evaluations: List[ConsumerImpactEvaluation] = []
    matrix: Dict[str, Dict[str, str]] = {}

    for c in consumer_list:
        res = evaluate_single_consumer(schema, c)
        evaluations.append(res)

        matrix[c.consumer_id] = {}
        for fe in res.field_evaluations:
            matrix[c.consumer_id][fe.field_name] = fe.status.value

    # Build Impact Graph
    nodes: List[ImpactGraphNode] = [
        ImpactGraphNode(
            id="producer",
            label="Producer Pipeline",
            node_type="producer",
            metadata={"fields_count": len(schema.fields)},
        ),
        ImpactGraphNode(
            id="dataset",
            label=f"Dataset: {dataset_identifier}",
            node_type="dataset",
            metadata={"columns": schema.field_names()},
        ),
    ]
    edges: List[ImpactGraphEdge] = [
        ImpactGraphEdge(
            source="producer",
            target="dataset",
            relationship="produces",
        )
    ]

    compatible_cnt = 0
    conditional_cnt = 0
    incompatible_cnt = 0
    uncertain_cnt = 0

    for ev in evaluations:
        if ev.status == ConsumerCompatibilityStatus.COMPATIBLE:
            compatible_cnt += 1
        elif ev.status == ConsumerCompatibilityStatus.CONDITIONALLY_COMPATIBLE:
            conditional_cnt += 1
        elif ev.status == ConsumerCompatibilityStatus.INCOMPATIBLE:
            incompatible_cnt += 1
        else:
            uncertain_cnt += 1

        nodes.append(
            ImpactGraphNode(
                id=ev.consumer_id,
                label=ev.consumer_name,
                node_type="consumer",
                status=ev.status.value,
                metadata={"blocking": ev.is_blocking},
            )
        )

        all_violations = (
            ev.missing_required_fields
            + ev.type_mismatches
            + ev.nullability_violations
            + ev.dimension_mismatches
        )
        edges.append(
            ImpactGraphEdge(
                source="dataset",
                target=ev.consumer_id,
                relationship="consumes",
                status=ev.status.value,
                violations_count=len(all_violations),
                details=all_violations,
            )
        )

    # Determine overall status
    if incompatible_cnt > 0:
        overall = ConsumerCompatibilityStatus.INCOMPATIBLE
    elif conditional_cnt > 0 or uncertain_cnt > 0:
        overall = ConsumerCompatibilityStatus.CONDITIONALLY_COMPATIBLE
    else:
        overall = ConsumerCompatibilityStatus.COMPATIBLE

    graph = ConsumerImpactGraph(nodes=nodes, edges=edges)

    summary = (
        f"Evaluated {len(consumer_list)} consumers against '{dataset_identifier}': "
        f"{compatible_cnt} compatible, {conditional_cnt} conditional, {incompatible_cnt} incompatible. "
        f"Overall verdict: {overall.value}"
    )

    return ConsumerImpactReport(
        dataset_identifier=dataset_identifier,
        overall_status=overall,
        total_consumers=len(consumer_list),
        compatible_consumers=compatible_cnt,
        conditionally_compatible_consumers=conditional_cnt,
        incompatible_consumers=incompatible_cnt,
        uncertain_consumers=uncertain_cnt,
        evaluations=evaluations,
        impact_graph=graph,
        compatibility_matrix=matrix,
        summary=summary,
    )
