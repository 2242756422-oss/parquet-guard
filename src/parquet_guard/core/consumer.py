"""Consumer-aware data contract intelligence engine.

Defines:
1. ConsumerContract and ConsumerGroupSpecification models.
2. ConsumerFieldRequirement and compatibility policies.
3. ConsumerImpactEvaluation, ConsumerImpactReport, and ConsumerImpactGraph.
4. Deterministic consumer impact analysis linking schema findings to downstream consumers.

Semantics (see inline documentation below):
- Every declared constraint is evaluated independently and composed with a
  deterministic severity order; no constraint can short-circuit another.
- Dotted requirement names are resolved as nested Arrow paths (struct child,
  list element, map key/value); literal dots must be escaped as ``\\.``.
- Technical compatibility (``status``) is separate from CI policy (``is_blocking``).
- Results describe declared consumer requirements only. They are NOT observations
  of how a real engine (DuckDB, Polars, Spark, ...) behaves.
"""

from __future__ import annotations

import re
from enum import Enum
from typing import Any, ClassVar, Dict, Iterable, List, Optional, Set, Tuple, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

from parquet_guard.core.compatibility import (
    _is_temporal_widening,
    _normalize_type_str,
    _parse_decimal_params,
    _parse_map_params,
    _parse_timestamp_params,
    get_container_kind,
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

    name: str = Field(
        description=(
            "Field name or dotted nested path, e.g. 'user_id', 'payload.event_time', 'items.item.sku', "
            "'tags.value'. Escape literal dots in names as '\\.'"
        )
    )
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

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not v or any(seg == "" for seg in split_field_path(v)):
            raise ValueError(f"Invalid field path '{v}': empty path segment")
        return v

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
    contract_direction: str = Field(
        default="BACKWARD",
        description="Contract compatibility direction: 'BACKWARD' (default), 'FORWARD', or 'FULL'",
    )
    criticality: str = Field(
        default="CRITICAL",
        description="Consumer importance level: 'CRITICAL', 'STANDARD', or 'INFORMATIONAL'",
    )
    field_renames: Dict[str, str] = Field(
        default_factory=dict, description="Explicit mapping of old field names to new producer field names"
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


class ConstraintCheck(BaseModel):
    """Outcome of one independently evaluated constraint for a field requirement."""

    model_config = ConfigDict(extra="forbid")

    constraint: str = Field(description="Constraint name, e.g. 'permitted_types', 'list_size', 'timezone'")
    status: ConsumerCompatibilityStatus = Field(description="Classification for this constraint alone")
    reason: Optional[str] = Field(default=None, description="Explanation when not plainly compatible")


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
    is_critical: bool = Field(default=True, description="Whether the requirement is declared critical")
    is_blocking: bool = Field(
        default=False,
        description="CI policy decision: critical requirement with INCOMPATIBLE or UNCERTAIN status",
    )
    constraint_checks: List[ConstraintCheck] = Field(
        default_factory=list, description="Independent per-constraint outcomes composing this status"
    )


class ConsumerImpactEvaluation(BaseModel):
    """Compatibility evaluation outcome for a single downstream consumer."""

    model_config = ConfigDict(extra="forbid")

    consumer_id: str = Field(description="Consumer identifier")
    consumer_name: str = Field(description="Human-readable consumer name")
    status: ConsumerCompatibilityStatus = Field(
        description="Technical compatibility status (most severe field status, independent of criticality)"
    )
    is_blocking: bool = Field(
        description="CI policy decision: True if any critical requirement is INCOMPATIBLE or UNCERTAIN"
    )
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
    path_resolution_errors: List[str] = Field(
        default_factory=list, description="Requirement paths that are ambiguous or not traversable"
    )
    uncertain_fields: List[str] = Field(
        default_factory=list, description="Requirements whose compatibility could not be determined"
    )
    ordering_violation: bool = Field(default=False, description="True if strict column ordering is violated")
    blocking_fields: List[str] = Field(default_factory=list, description="Fields that block CI")
    non_blocking_violations: List[str] = Field(
        default_factory=list,
        description="Technically incompatible/uncertain fields that do not block CI (non-critical)",
    )
    summary: str = Field(description="Concise evaluation summary")
    contract_direction: str = Field(default="BACKWARD", description="Evaluated contract direction")
    criticality: str = Field(default="CRITICAL", description="Consumer criticality level")
    explicit_renames_applied: Dict[str, str] = Field(
        default_factory=dict, description="Explicit rename mappings applied during evaluation"
    )
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
    potentially_breaking_consumers: int = Field(
        default=0, description="Count of potentially breaking consumers"
    )
    incompatible_consumers: int = Field(description="Count of incompatible consumers")
    uncertain_consumers: int = Field(default=0, description="Count of uncertain consumers")
    blocking_consumers: int = Field(default=0, description="Count of consumers blocked under CI policy")
    is_blocking: bool = Field(default=False, description="True if any consumer is blocking under CI policy")
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


# ---------------------------------------------------------------------------
# Status composition policy
# ---------------------------------------------------------------------------
#
# Every independent constraint (path resolution, type recognition, permitted
# types, list_size, decimal precision/scale, temporal unit/timezone,
# nullability) produces its own ConstraintCheck. The field status is the most
# severe status among its checks, using this deterministic order:
#
#   COMPATIBLE < CONDITIONALLY_COMPATIBLE < POTENTIALLY_BREAKING < UNCERTAIN < INCOMPATIBLE
#
# No check can short-circuit another: a successful permitted-type match never
# hides a contradictory list_size / scale / unit / timezone requirement.
#
# Technical classification is independent of CI policy. A field's status is
# never downgraded because its requirement is non-critical; instead the
# ``is_blocking`` flag records the CI policy decision:
#
#   is_blocking = requirement.critical and status in {INCOMPATIBLE, UNCERTAIN}
#
# UNCERTAIN blocks critical requirements (fail-closed) because insufficient
# knowledge must not be reported as a pass.

_STATUS_RANK: Dict[ConsumerCompatibilityStatus, int] = {
    ConsumerCompatibilityStatus.COMPATIBLE: 0,
    ConsumerCompatibilityStatus.CONDITIONALLY_COMPATIBLE: 1,
    ConsumerCompatibilityStatus.POTENTIALLY_BREAKING: 2,
    ConsumerCompatibilityStatus.UNCERTAIN: 3,
    ConsumerCompatibilityStatus.INCOMPATIBLE: 4,
}

_BLOCKING_STATUSES = frozenset(
    {ConsumerCompatibilityStatus.INCOMPATIBLE, ConsumerCompatibilityStatus.UNCERTAIN}
)


def compose_statuses(statuses: Iterable[ConsumerCompatibilityStatus]) -> ConsumerCompatibilityStatus:
    """Return the most severe status according to the documented composition order."""
    result = ConsumerCompatibilityStatus.COMPATIBLE
    for s in statuses:
        if _STATUS_RANK[s] > _STATUS_RANK[result]:
            result = s
    return result


# ---------------------------------------------------------------------------
# Nested field path semantics
# ---------------------------------------------------------------------------
#
# Requirement names are dotted paths. Each segment names a child field:
#   * struct   -> child field name             e.g. ``payload.event_time``
#   * list / large_list / fixed_size_list -> element field name as stored by
#     Arrow (usually ``item``; ``element`` and ``item`` are accepted as aliases
#     for the single element child)            e.g. ``items.item.sku``
#   * map      -> ``key`` or ``value``         e.g. ``tags.value``
# A literal dot inside a field name must be escaped as ``\.`` (``metric\.v1``)
# and a literal backslash as ``\\``. A dotted path is NEVER silently matched
# against a top-level field whose name literally contains dots.
# Traversal into scalars and dictionary-encoded fields is rejected; traversal
# into unions, or into containers without declared children, is UNCERTAIN.

_LIST_KINDS = frozenset({"list", "large_list", "fixed_size_list"})


def split_field_path(path: str) -> List[str]:
    """Split a dotted requirement path into segments honouring ``\\.`` escapes."""
    segments: List[str] = []
    buf: List[str] = []
    i = 0
    while i < len(path):
        ch = path[i]
        if ch == "\\" and i + 1 < len(path) and path[i + 1] in ".\\":
            buf.append(path[i + 1])
            i += 2
            continue
        if ch == ".":
            segments.append("".join(buf))
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    segments.append("".join(buf))
    return segments


def escape_field_name(name: str) -> str:
    """Escape a literal field name so it can be used as a single path segment."""
    return name.replace("\\", "\\\\").replace(".", "\\.")


class FieldPathResolution(BaseModel):
    """Outcome of resolving a requirement path against a producer schema."""

    model_config = ConfigDict(extra="forbid")

    path: str
    segments: List[str]
    field: Optional[FieldDefinition] = None
    error_kind: Optional[str] = Field(
        default=None,
        description="None when resolved; otherwise 'missing', 'duplicate', 'not_traversable' or 'uncertain'",
    )
    message: Optional[str] = None
    effective_nullable: bool = Field(
        default=True,
        description="True if the leaf or any traversed ancestor is nullable (leaf may be null)",
    )


def resolve_field_path(fields: List[FieldDefinition], path: str) -> FieldPathResolution:
    """Resolve a dotted requirement path against producer fields (see module semantics)."""
    segments = split_field_path(path)
    candidates = fields
    parent: Optional[FieldDefinition] = None
    ancestor_nullable = False
    for idx, seg in enumerate(segments):
        matches = [f for f in candidates if f.name == seg]
        if not matches and parent is not None:
            kind = get_container_kind(parent.type)
            if kind in _LIST_KINDS and seg in ("item", "element") and len(parent.children) == 1:
                matches = [parent.children[0]]
        if len(matches) > 1:
            where = "top level" if parent is None else f"'{parent.name}'"
            return FieldPathResolution(
                path=path,
                segments=segments,
                error_kind="duplicate",
                message=(
                    f"Path '{path}' is ambiguous: duplicate field name '{seg}' at {where}; "
                    "readers may resolve it unpredictably"
                ),
            )
        if not matches:
            msg = f"Required path '{path}' is missing in producer schema (segment '{seg}' not found)"
            if idx == 0 and len(segments) > 1 and any(f.name == path for f in fields):
                escaped = escape_field_name(path)
                msg += (
                    f"; a top-level field literally named '{path}' exists - "
                    f"escape literal dots as '{escaped}'"
                )
            return FieldPathResolution(path=path, segments=segments, error_kind="missing", message=msg)
        current = matches[0]
        if idx == len(segments) - 1:
            return FieldPathResolution(
                path=path,
                segments=segments,
                field=current,
                effective_nullable=ancestor_nullable or current.nullable,
            )
        kind = get_container_kind(current.type)
        if kind is None or kind == "dictionary":
            label = "dictionary-encoded" if kind == "dictionary" else "non-nested scalar"
            return FieldPathResolution(
                path=path,
                segments=segments,
                error_kind="not_traversable",
                message=(
                    f"Cannot traverse '{path}': '{current.name}' is a {label} field of type '{current.type}'"
                ),
            )
        if kind == "union":
            return FieldPathResolution(
                path=path,
                segments=segments,
                error_kind="uncertain",
                message=f"Traversal through union field '{current.name}' is not supported; result uncertain",
            )
        if not current.children:
            return FieldPathResolution(
                path=path,
                segments=segments,
                error_kind="uncertain",
                message=(
                    f"Container '{current.name}' ({current.type}) declares no child fields; "
                    f"nested requirement '{path}' cannot be verified"
                ),
            )
        ancestor_nullable = ancestor_nullable or current.nullable
        parent = current
        candidates = current.children
    return FieldPathResolution(path=path, segments=segments, error_kind="missing", message="Empty path")


# ---------------------------------------------------------------------------
# Type canonicalization and recognition
# ---------------------------------------------------------------------------

_PRIMITIVE_TYPES = frozenset(
    {
        "null",
        "bool",
        "int8",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "float16",
        "float32",
        "float64",
        "string",
        "large_string",
        "binary",
        "large_binary",
        "string_view",
        "binary_view",
        "date32",
        "date64",
    }
)


def canonical_type(type_str: str) -> str:
    """Canonicalize Arrow type spellings (``float`` == ``float32``, list item names, spacing)."""
    s = _normalize_type_str(type_str)
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\s*,\s*", ", ", s)
    s = re.sub(r"<\s+", "<", s)
    s = re.sub(r"\s+>", ">", s)
    s = re.sub(r"\bhalffloat\b", "float16", s)
    s = re.sub(r"\bfloat\b", "float32", s)
    s = re.sub(r"\bboolean\b", "bool", s)
    s = re.sub(r"\bdate32\[day\]", "date32", s)
    s = re.sub(r"\bdate64\[ms\]", "date64", s)
    # Strip list element field names: list<item: int32> == list<int32>
    s = re.sub(r"\b((?:large_|fixed_size_)?list<)[a-z_][\w\-]*: ", r"\1", s)
    return s


def _strip_element_name(inner: str) -> str:
    inner = re.sub(r"\s+not null$", "", inner.strip())
    m = re.match(r"^[a-z_][\w\-]*:\s*(.+)$", inner)
    return m.group(1) if m else inner


def is_recognized_type(type_str: str, _depth: int = 0) -> bool:
    """Return True if ``type_str`` is an Arrow type representation this engine can reason about."""
    if _depth > 64:
        return False
    s = re.sub(r"\s+not null$", "", canonical_type(type_str))
    if s in _PRIMITIVE_TYPES:
        return True
    if _parse_decimal_params(s) or _parse_timestamp_params(s):
        return True
    if re.match(r"^duration\[(s|ms|us|ns)\]$", s):
        return True
    if re.match(r"^time32\[(s|ms)\]$", s) or re.match(r"^time64\[(us|ns)\]$", s):
        return True
    if re.match(r"^fixed_size_binary\[\d+\]$", s):
        return True
    m = re.match(r"^fixed_size_list<(.+)>\[\d+\]$", s)
    if m:
        return is_recognized_type(_strip_element_name(m.group(1)), _depth + 1)
    m = re.match(r"^(?:large_list|list)<(.+)>$", s)
    if m:
        return is_recognized_type(_strip_element_name(m.group(1)), _depth + 1)
    mp = _parse_map_params(s)
    if mp:
        return is_recognized_type(_strip_element_name(mp[0]), _depth + 1) and is_recognized_type(
            _strip_element_name(mp[1]), _depth + 1
        )
    if (s.startswith("struct<") or s.startswith("dictionary<")) and s.endswith(">"):
        return True
    return False


# ---------------------------------------------------------------------------
# Independent constraint checks
# ---------------------------------------------------------------------------


def _check(
    constraint: str, status: ConsumerCompatibilityStatus, reason: Optional[str] = None
) -> ConstraintCheck:
    return ConstraintCheck(constraint=constraint, status=status, reason=reason)


def _check_recognition(field: FieldDefinition) -> ConstraintCheck:
    if is_recognized_type(field.type):
        return _check("type_recognition", ConsumerCompatibilityStatus.COMPATIBLE)
    return _check(
        "type_recognition",
        ConsumerCompatibilityStatus.UNCERTAIN,
        f"Unsupported or unrecognized producer type '{field.type}'; compatibility cannot be determined",
    )


def _check_permitted_types(
    req: ConsumerFieldRequirement, field: FieldDefinition, policy: str
) -> ConstraintCheck:
    p_canon = canonical_type(field.type)
    permitted = [(canonical_type(t), t) for t in req.permitted_types]
    if any(c == p_canon for c, _ in permitted):
        return _check("permitted_types", ConsumerCompatibilityStatus.COMPATIBLE)

    if policy == "permissive":
        for c, orig in permitted:
            if is_safe_widening(p_canon, c):
                reason = (
                    f"Producer type '{field.type}' is a safe widening towards permitted '{orig}'; "
                    "the consumer must promote values on read"
                )
                if _is_temporal_widening(p_canon, c):
                    reason += (
                        " (finer temporal unit narrows the representable int64 range; "
                        "out-of-range values would fail conversion)"
                    )
                return _check("permitted_types", ConsumerCompatibilityStatus.CONDITIONALLY_COMPATIBLE, reason)

    reason = f"Producer type '{field.type}' is not in declared permitted types: {req.permitted_types}"
    narrower = [orig for c, orig in permitted if is_safe_widening(c, p_canon)]
    if narrower:
        reason += (
            f"; producer type is wider than permitted '{narrower[0]}', so reading requires a narrowing "
            "cast that can overflow or lose precision (value range is not provable from metadata)"
        )
    if policy == "strict" and any(is_safe_widening(p_canon, c) for c, _ in permitted):
        reason += "; strict policy forbids type promotion"
    invalid = [orig for c, orig in permitted if not is_recognized_type(c)]
    if invalid:
        reason += f"; unrecognized permitted type declaration(s): {invalid}"
    return _check("permitted_types", ConsumerCompatibilityStatus.INCOMPATIBLE, reason)


def _producer_list_size(field: FieldDefinition) -> Optional[int]:
    if field.list_size is not None:
        return int(field.list_size)
    m = re.search(r"\[(\d+)\]\s*$", field.type.strip())
    return int(m.group(1)) if m else None


def _check_list_size(req: ConsumerFieldRequirement, field: FieldDefinition) -> ConstraintCheck:
    assert req.list_size is not None
    if get_container_kind(field.type) != "fixed_size_list":
        return _check(
            "list_size",
            ConsumerCompatibilityStatus.INCOMPATIBLE,
            f"FixedSizeList dimension mismatch: consumer requires FixedSizeList [{req.list_size}], "
            f"producer provides non-fixed type '{field.type}'",
        )
    p_size = _producer_list_size(field)
    if p_size is None:
        return _check(
            "list_size",
            ConsumerCompatibilityStatus.UNCERTAIN,
            f"Producer FixedSizeList '{field.type}' does not declare a dimension",
        )
    if p_size != req.list_size:
        return _check(
            "list_size",
            ConsumerCompatibilityStatus.INCOMPATIBLE,
            f"FixedSizeList dimension mismatch: consumer requires [{req.list_size}], producer provides [{p_size}]",
        )
    return _check("list_size", ConsumerCompatibilityStatus.COMPATIBLE)


def _check_decimal(req: ConsumerFieldRequirement, field: FieldDefinition) -> List[ConstraintCheck]:
    dec = _parse_decimal_params(field.type)
    if not dec:
        return [
            _check(
                "decimal",
                ConsumerCompatibilityStatus.INCOMPATIBLE,
                f"Consumer requires a decimal (precision<={req.precision}, scale={req.scale}), "
                f"but producer type is '{field.type}'",
            )
        ]
    _, p_prec, p_scale = dec
    checks: List[ConstraintCheck] = []
    if req.scale is not None:
        if p_scale != req.scale:
            checks.append(
                _check(
                    "decimal_scale",
                    ConsumerCompatibilityStatus.INCOMPATIBLE,
                    f"Decimal scale mismatch: consumer requires scale={req.scale}, producer has scale={p_scale} "
                    "(scale changes alter the numeric interpretation of stored values)",
                )
            )
        else:
            checks.append(_check("decimal_scale", ConsumerCompatibilityStatus.COMPATIBLE))
    if req.precision is not None:
        if p_prec > req.precision:
            checks.append(
                _check(
                    "decimal_precision",
                    ConsumerCompatibilityStatus.CONDITIONALLY_COMPATIBLE,
                    f"Decimal precision: producer precision ({p_prec}) exceeds consumer maximum "
                    f"({req.precision}); values with more digits would overflow and the value range "
                    "cannot be proven from metadata",
                )
            )
        else:
            checks.append(_check("decimal_precision", ConsumerCompatibilityStatus.COMPATIBLE))
    return checks


_UNITS = ["s", "ms", "us", "ns"]


def _check_temporal(
    req: ConsumerFieldRequirement, field: FieldDefinition, policy: str
) -> List[ConstraintCheck]:
    raw = field.type.strip()
    ts = _parse_timestamp_params(raw)
    dur = re.match(r"^duration\[(s|ms|us|ns)\]$", raw, re.IGNORECASE)
    if ts is None and dur is None:
        return [
            _check(
                "temporal",
                ConsumerCompatibilityStatus.INCOMPATIBLE,
                f"Consumer requires timestamp[{req.time_unit or '*'}, tz={req.timezone or '*'}], "
                f"but producer type is '{field.type}'",
            )
        ]
    checks: List[ConstraintCheck] = []
    if ts is not None:
        p_unit, p_tz = ts
    else:
        assert dur is not None
        p_unit, p_tz = dur.group(1).lower(), None
    if req.timezone is not None:
        if ts is None:
            checks.append(
                _check(
                    "timezone",
                    ConsumerCompatibilityStatus.INCOMPATIBLE,
                    f"Timestamp timezone mismatch: consumer requires timezone '{req.timezone}', "
                    f"producer type '{field.type}' carries no timezone",
                )
            )
        elif p_tz != req.timezone:
            checks.append(
                _check(
                    "timezone",
                    ConsumerCompatibilityStatus.INCOMPATIBLE,
                    f"Timestamp timezone mismatch: consumer requires timezone '{req.timezone}', producer has '{p_tz}'",
                )
            )
        else:
            checks.append(_check("timezone", ConsumerCompatibilityStatus.COMPATIBLE))
    if req.time_unit is not None:
        if p_unit == req.time_unit:
            checks.append(_check("time_unit", ConsumerCompatibilityStatus.COMPATIBLE))
        elif policy != "permissive":
            checks.append(
                _check(
                    "time_unit",
                    ConsumerCompatibilityStatus.INCOMPATIBLE,
                    f"Timestamp unit mismatch: consumer requires '{req.time_unit}', producer has '{p_unit}' "
                    "(strict policy forbids unit conversion)",
                )
            )
        elif _UNITS.index(p_unit) > _UNITS.index(req.time_unit):
            checks.append(
                _check(
                    "time_unit",
                    ConsumerCompatibilityStatus.CONDITIONALLY_COMPATIBLE,
                    f"Timestamp unit '{p_unit}' is finer than consumer requirement '{req.time_unit}'; "
                    f"conversion truncates sub-'{req.time_unit}' precision unless values are aligned",
                )
            )
        else:
            checks.append(
                _check(
                    "time_unit",
                    ConsumerCompatibilityStatus.CONDITIONALLY_COMPATIBLE,
                    f"Timestamp unit '{p_unit}' is coarser than consumer requirement '{req.time_unit}'; "
                    "conversion to the finer unit can overflow int64 for out-of-range values "
                    "(temporal overflow risk)",
                )
            )
    return checks


def _check_nullability(
    req: ConsumerFieldRequirement, effective_nullable: bool, strict_nullability: bool, nested: bool
) -> Optional[ConstraintCheck]:
    if req.allow_null or not effective_nullable:
        return None
    where = "it or one of its ancestors is nullable" if nested else "it is nullable"
    if strict_nullability:
        return _check(
            "nullability",
            ConsumerCompatibilityStatus.INCOMPATIBLE,
            f"Field '{req.name}' may contain nulls in producer ({where}), "
            "but consumer requires strictly non-null values",
        )
    return _check(
        "nullability",
        ConsumerCompatibilityStatus.CONDITIONALLY_COMPATIBLE,
        f"Field '{req.name}' may contain nulls in producer ({where}); runtime null checks required",
    )


def evaluate_requirement_constraints(
    req: ConsumerFieldRequirement,
    producer_field: FieldDefinition,
    policy: str,
) -> List[ConstraintCheck]:
    """Evaluate every declared type constraint independently (no short-circuiting)."""
    checks: List[ConstraintCheck] = [_check_recognition(producer_field)]
    if req.permitted_types:
        checks.append(_check_permitted_types(req, producer_field, policy))
    if req.list_size is not None:
        checks.append(_check_list_size(req, producer_field))
    if req.precision is not None or req.scale is not None:
        checks.extend(_check_decimal(req, producer_field))
    if req.time_unit is not None or req.timezone is not None:
        checks.extend(_check_temporal(req, producer_field, policy))
    return checks


def _evaluate_type_compatibility(
    req: ConsumerFieldRequirement,
    producer_field: FieldDefinition,
    policy: str,
) -> Tuple[ConsumerCompatibilityStatus, Optional[str]]:
    """Backward-compatible wrapper returning the composed status and joined reasons."""
    checks = evaluate_requirement_constraints(req, producer_field, policy)
    status = compose_statuses(c.status for c in checks)
    reasons = [c.reason for c in checks if c.reason]
    return status, ("; ".join(reasons) if reasons else None)


_TYPE_CONSTRAINTS = frozenset(
    {"permitted_types", "decimal", "decimal_scale", "decimal_precision", "temporal", "timezone", "time_unit"}
)


def evaluate_single_consumer(
    schema: SchemaContract,
    consumer: ConsumerContract,
) -> ConsumerImpactEvaluation:
    """Evaluate a single downstream consumer contract against a producer schema.

    ``status`` is the technical classification (most severe field status).
    ``is_blocking`` is the CI policy decision derived from requirement criticality.
    """
    field_evals: List[ConsumerFieldEvaluation] = []
    missing_fields: List[str] = []
    type_mismatches: List[str] = []
    nullability_violations: List[str] = []
    unexpected_extra: List[str] = []
    dimension_mismatches: List[str] = []
    path_errors: List[str] = []
    uncertain_fields: List[str] = []
    applied_renames: Dict[str, str] = {}

    # 1. Check each declared field requirement
    for req in consumer.fields:
        target_name = consumer.field_renames.get(req.name, req.name)
        if target_name != req.name:
            applied_renames[req.name] = target_name
        res = resolve_field_path(schema.fields, target_name)
        if res.field is None:
            if res.error_kind == "missing" and not req.required:
                field_evals.append(
                    ConsumerFieldEvaluation(
                        field_name=req.name,
                        status=ConsumerCompatibilityStatus.COMPATIBLE,
                        requirement=req,
                        violation_reason="Optional field absent in producer schema",
                        is_critical=False,
                        constraint_checks=[_check("presence", ConsumerCompatibilityStatus.COMPATIBLE)],
                    )
                )
                continue
            if res.error_kind == "missing":
                status = ConsumerCompatibilityStatus.INCOMPATIBLE
                missing_fields.append(req.name)
            elif res.error_kind == "uncertain":
                status = ConsumerCompatibilityStatus.UNCERTAIN
                uncertain_fields.append(req.name)
            else:
                status = ConsumerCompatibilityStatus.INCOMPATIBLE
                path_errors.append(req.name)
            field_evals.append(
                ConsumerFieldEvaluation(
                    field_name=req.name,
                    status=status,
                    requirement=req,
                    violation_reason=res.message,
                    is_critical=req.critical,
                    is_blocking=req.critical and status in _BLOCKING_STATUSES,
                    constraint_checks=[_check("path", status, res.message)],
                )
            )
            continue

        p_field = res.field
        checks = evaluate_requirement_constraints(req, p_field, consumer.compatibility_policy)
        null_check = _check_nullability(
            req, res.effective_nullable, consumer.strict_nullability, nested=len(res.segments) > 1
        )
        if null_check is not None:
            checks.append(null_check)

        status = compose_statuses(c.status for c in checks)
        failed = {c.constraint for c in checks if c.status == ConsumerCompatibilityStatus.INCOMPATIBLE}
        if "list_size" in failed or (
            "permitted_types" in failed
            and "fixed_size_list" in p_field.type.lower()
            and any("fixed_size_list" in pt.lower() for pt in req.permitted_types)
        ):
            dimension_mismatches.append(req.name)
        if failed & _TYPE_CONSTRAINTS:
            type_mismatches.append(req.name)
        if "nullability" in failed:
            nullability_violations.append(req.name)
        if any(c.status == ConsumerCompatibilityStatus.UNCERTAIN for c in checks):
            uncertain_fields.append(req.name)

        reasons = [
            c.reason for c in checks if c.reason and c.status != ConsumerCompatibilityStatus.COMPATIBLE
        ]
        field_evals.append(
            ConsumerFieldEvaluation(
                field_name=req.name,
                status=status,
                requirement=req,
                producer_type=p_field.type,
                producer_nullable=res.effective_nullable,
                violation_reason="; ".join(reasons) if reasons else None,
                is_critical=req.critical,
                is_blocking=req.critical and status in _BLOCKING_STATUSES,
                constraint_checks=checks,
            )
        )

    # 2. Unexpected extra top-level fields (declared consumer-level requirement)
    requested_top = {split_field_path(r.name)[0] for r in consumer.fields}
    if not consumer.allow_extra_fields:
        for p in schema.fields:
            if p.name not in requested_top and p.name not in unexpected_extra:
                unexpected_extra.append(p.name)
                reason = f"Extra field '{p.name}' not permitted by consumer policy (allow_extra_fields=false)"
                field_evals.append(
                    ConsumerFieldEvaluation(
                        field_name=p.name,
                        status=ConsumerCompatibilityStatus.INCOMPATIBLE,
                        producer_type=p.type,
                        producer_nullable=p.nullable,
                        violation_reason=reason,
                        is_critical=True,
                        is_blocking=True,
                        constraint_checks=[
                            _check("allow_extra_fields", ConsumerCompatibilityStatus.INCOMPATIBLE, reason)
                        ],
                    )
                )

    # 3. Strict top-level column ordering (declared consumer-level requirement)
    ordering_violation = False
    if consumer.strict_ordering:
        expected_order: List[str] = []
        for r in consumer.fields:
            top = split_field_path(r.name)[0]
            if top not in expected_order and any(f.name == top for f in schema.fields):
                expected_order.append(top)
        producer_order = [f.name for f in schema.fields if f.name in expected_order]
        if len(expected_order) > 1 and producer_order != expected_order:
            ordering_violation = True
            reason = f"Column order {producer_order} differs from declared order {expected_order}"
            field_evals.append(
                ConsumerFieldEvaluation(
                    field_name="<column_order>",
                    status=ConsumerCompatibilityStatus.INCOMPATIBLE,
                    violation_reason=reason,
                    is_critical=True,
                    is_blocking=True,
                    constraint_checks=[
                        _check("strict_ordering", ConsumerCompatibilityStatus.INCOMPATIBLE, reason)
                    ],
                )
            )

    # 4. Overall technical status and CI policy decision (kept separate)
    overall_status = compose_statuses(fe.status for fe in field_evals)
    blocking_fields = [fe.field_name for fe in field_evals if fe.is_blocking]
    non_blocking_violations = [
        fe.field_name for fe in field_evals if not fe.is_blocking and fe.status in _BLOCKING_STATUSES
    ]
    is_blocking = len(blocking_fields) > 0

    recs: List[str] = []
    if missing_fields:
        recs.append(f"Restore required fields: {missing_fields}")
    if path_errors:
        recs.append(f"Fix ambiguous or non-traversable nested paths: {path_errors}")
    if type_mismatches:
        recs.append(f"Reconcile type mismatches: {type_mismatches}")
    if dimension_mismatches:
        recs.append(f"Reconcile vector dimension constraints: {dimension_mismatches}")
    if nullability_violations:
        recs.append(
            f"Enforce non-null constraints or update consumer to handle nulls: {nullability_violations}"
        )
    if uncertain_fields:
        recs.append(f"Provide complete schema information or verify manually: {uncertain_fields}")
    if unexpected_extra:
        recs.append(f"Filter out extra fields before emitting to consumer: {unexpected_extra}")
    if ordering_violation:
        recs.append("Restore declared column order for strict positional consumer")

    summary = (
        f"{consumer.consumer_name} ({consumer.consumer_id}): "
        f"Status={overall_status.value}, Blocking={is_blocking}. "
        f"Missing={len(missing_fields)}, TypeMismatches={len(type_mismatches)}, "
        f"DimensionMismatches={len(dimension_mismatches)}, NullViolations={len(nullability_violations)}, "
        f"PathErrors={len(path_errors)}, Uncertain={len(uncertain_fields)}"
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
        path_resolution_errors=path_errors,
        uncertain_fields=uncertain_fields,
        ordering_violation=ordering_violation,
        blocking_fields=blocking_fields,
        non_blocking_violations=non_blocking_violations,
        contract_direction=consumer.contract_direction,
        criticality=consumer.criticality,
        explicit_renames_applied=applied_renames,
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
        matrix[c.consumer_id] = {fe.field_name: fe.status.value for fe in res.field_evaluations}

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
        ImpactGraphEdge(source="producer", target="dataset", relationship="produces")
    ]

    counts: Dict[ConsumerCompatibilityStatus, int] = {s: 0 for s in ConsumerCompatibilityStatus}
    for ev in evaluations:
        counts[ev.status] += 1
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
            + ev.path_resolution_errors
            + ev.unexpected_extra_fields
            + ev.uncertain_fields
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

    overall = compose_statuses(ev.status for ev in evaluations)
    blocking_cnt = sum(1 for ev in evaluations if ev.is_blocking)
    graph = ConsumerImpactGraph(nodes=nodes, edges=edges)

    summary = (
        f"Evaluated {len(consumer_list)} consumers against '{dataset_identifier}': "
        f"{counts[ConsumerCompatibilityStatus.COMPATIBLE]} compatible, "
        f"{counts[ConsumerCompatibilityStatus.CONDITIONALLY_COMPATIBLE]} conditional, "
        f"{counts[ConsumerCompatibilityStatus.POTENTIALLY_BREAKING]} potentially breaking, "
        f"{counts[ConsumerCompatibilityStatus.UNCERTAIN]} uncertain, "
        f"{counts[ConsumerCompatibilityStatus.INCOMPATIBLE]} incompatible; "
        f"{blocking_cnt} blocking under declared criticality. Overall verdict: {overall.value}"
    )

    return ConsumerImpactReport(
        dataset_identifier=dataset_identifier,
        overall_status=overall,
        total_consumers=len(consumer_list),
        compatible_consumers=counts[ConsumerCompatibilityStatus.COMPATIBLE],
        conditionally_compatible_consumers=counts[ConsumerCompatibilityStatus.CONDITIONALLY_COMPATIBLE],
        potentially_breaking_consumers=counts[ConsumerCompatibilityStatus.POTENTIALLY_BREAKING],
        incompatible_consumers=counts[ConsumerCompatibilityStatus.INCOMPATIBLE],
        uncertain_consumers=counts[ConsumerCompatibilityStatus.UNCERTAIN],
        blocking_consumers=blocking_cnt,
        is_blocking=blocking_cnt > 0,
        evaluations=evaluations,
        impact_graph=graph,
        compatibility_matrix=matrix,
        summary=summary,
    )
