"""Data models, enumerations, and contract schemas for parquet-guard."""

from __future__ import annotations

import re
from enum import Enum
from typing import Any, ClassVar, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class Severity(str, Enum):
    """Classification of schema change severity."""

    SAFE = "SAFE"
    WARNING = "WARNING"
    BREAKING = "BREAKING"


class ResultState(str, Enum):
    """Formal status of a schema comparison, contract check, or dataset audit."""

    VALID = "VALID"
    INVALID = "INVALID"
    DRIFTED = "DRIFTED"
    COMPATIBLE = "COMPATIBLE"
    INCOMPATIBLE = "INCOMPATIBLE"
    INDETERMINATE = "INDETERMINATE"
    INSPECTION_ERROR = "INSPECTION_ERROR"


class ErrorCode(str, Enum):
    """Machine-readable, stable diagnostic error codes."""

    # Contract errors
    CONTRACT_UNSUPPORTED_VERSION = "CONTRACT_UNSUPPORTED_VERSION"
    CONTRACT_INVALID_SCHEMA = "CONTRACT_INVALID_SCHEMA"
    CONTRACT_EMPTY_FIELDS = "CONTRACT_EMPTY_FIELDS"
    CONTRACT_DUPLICATE_FIELDS = "CONTRACT_DUPLICATE_FIELDS"

    # Schema structural & type discrepancies
    FIELD_DROPPED = "FIELD_DROPPED"
    FIELD_ADDED = "FIELD_ADDED"
    FIELD_ORDER_CHANGED = "FIELD_ORDER_CHANGED"
    TYPE_MISMATCH = "TYPE_MISMATCH"
    TYPE_WIDENED = "TYPE_WIDENED"
    TYPE_NARROWED = "TYPE_NARROWED"
    NULLABILITY_CHANGED = "NULLABILITY_CHANGED"
    METADATA_CHANGED = "METADATA_CHANGED"
    CONTAINER_MISMATCH = "CONTAINER_MISMATCH"
    CONTAINER_PARAM_CHANGED = "CONTAINER_PARAM_CHANGED"
    CONTAINER_PARAM_MISMATCH = "CONTAINER_PARAM_CHANGED"
    ORDERED_FLAG_CHANGED = "ORDERED_FLAG_CHANGED"
    DICTIONARY_ORDERED_MISMATCH = "ORDERED_FLAG_CHANGED"
    KEYS_SORTED_CHANGED = "KEYS_SORTED_CHANGED"
    MAP_SORTED_MISMATCH = "KEYS_SORTED_CHANGED"
    DECIMAL_SCALE_MISMATCH = "DECIMAL_SCALE_MISMATCH"
    TIMESTAMP_TIMEZONE_MISMATCH = "TIMESTAMP_TIMEZONE_MISMATCH"
    TEMPORAL_OVERFLOW_RISK = "TEMPORAL_OVERFLOW_RISK"

    # Audit & IO errors
    NO_FILES_FOUND = "NO_FILES_FOUND"
    CORRUPTED_FILE = "CORRUPTED_FILE"
    DATASET_INCOMPATIBILITY = "DATASET_INCOMPATIBILITY"
    CACHE_INTEGRITY_ERROR = "CACHE_INTEGRITY_ERROR"


class ChangeType(str, Enum):
    """Specific category of change detected."""

    FIELD_ADDED = "FIELD_ADDED"
    FIELD_REMOVED = "FIELD_REMOVED"
    FIELD_ORDER_CHANGED = "FIELD_ORDER_CHANGED"
    TYPE_MISMATCH = "TYPE_MISMATCH"
    TYPE_WIDENED = "TYPE_WIDENED"
    TYPE_NARROWED = "TYPE_NARROWED"
    NULLABILITY_CHANGED = "NULLABILITY_CHANGED"
    METADATA_CHANGED = "METADATA_CHANGED"
    CONTAINER_PARAM_CHANGED = "CONTAINER_PARAM_CHANGED"
    ORDERED_FLAG_CHANGED = "ORDERED_FLAG_CHANGED"
    KEYS_SORTED_CHANGED = "KEYS_SORTED_CHANGED"


class FieldDefinition(BaseModel):
    """Specification of a single Parquet/Arrow column field."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(description="Column field name")
    type: str = Field(description="Canonical Arrow data type string representation")
    nullable: bool = Field(default=True, description="Whether the column permits null values")
    metadata: Dict[str, str] = Field(default_factory=dict, description="Arbitrary field metadata")
    children: List[FieldDefinition] = Field(
        default_factory=list, description="Nested child fields for Struct, List, Map, etc."
    )

    # Typed Arrow container and scalar parameters
    list_size: Optional[int] = Field(default=None, description="Size constraint for FixedSizeList")
    ordered: Optional[bool] = Field(default=None, description="Ordered flag for Dictionary type")
    keys_sorted: Optional[bool] = Field(default=None, description="Keys sorted flag for Map type")
    precision: Optional[int] = Field(default=None, description="Precision for Decimal128/Decimal256")
    scale: Optional[int] = Field(default=None, description="Scale for Decimal128/Decimal256")
    time_unit: Optional[str] = Field(
        default=None, description="Unit for Timestamp or Duration (s, ms, us, ns)"
    )
    timezone: Optional[str] = Field(default=None, description="Timezone string for Timestamp")
    index_type: Optional[str] = Field(default=None, description="Index type name for Dictionary")
    value_type: Optional[str] = Field(default=None, description="Value type name for Dictionary")

    @model_validator(mode="after")
    def populate_type_parameters(self) -> FieldDefinition:
        """Infer missing type parameters from type string if not explicitly supplied."""
        t_str = self.type.strip()

        # FixedSizeList length inference: e.g. fixed_size_list<item: int32>[4]
        if self.list_size is None and "fixed_size_list" in t_str.lower():
            m_len = re.search(r"\[(\d+)\]$", t_str)
            if m_len:
                self.list_size = int(m_len.group(1))

        # Dictionary ordered flag inference: e.g. dictionary<..., ordered=1>
        if self.ordered is None and "dictionary" in t_str.lower():
            if re.search(r"ordered=(1|true)", t_str, re.IGNORECASE):
                self.ordered = True
            elif re.search(r"ordered=(0|false)", t_str, re.IGNORECASE):
                self.ordered = False

        # Map keys_sorted flag inference: e.g. map<..., keys_sorted>
        if self.keys_sorted is None and t_str.lower().startswith("map"):
            if "keys_sorted" in t_str.lower():
                self.keys_sorted = True
            else:
                self.keys_sorted = False

        # Decimal precision and scale inference: e.g. decimal128(10, 2)
        if (self.precision is None or self.scale is None) and "decimal" in t_str.lower():
            m_dec = re.search(r"decimal(?:128|256)\((\d+),\s*(-?\d+)\)", t_str, re.IGNORECASE)
            if m_dec:
                if self.precision is None:
                    self.precision = int(m_dec.group(1))
                if self.scale is None:
                    self.scale = int(m_dec.group(2))

        # Timestamp parameters: e.g. timestamp[ms, tz=UTC]
        if self.time_unit is None and t_str.lower().startswith("timestamp"):
            m_ts = re.search(r"timestamp\[(s|ms|us|ns)(?:,\s*tz=([^\]]+))?\]", t_str, re.IGNORECASE)
            if m_ts:
                self.time_unit = m_ts.group(1).lower()
                if self.timezone is None and m_ts.group(2):
                    self.timezone = m_ts.group(2).strip()

        # Duration parameters: e.g. duration[s], duration[ms], duration[us], duration[ns]
        if self.time_unit is None and t_str.lower().startswith("duration"):
            m_dur = re.search(r"duration\[(s|ms|us|ns)\]", t_str, re.IGNORECASE)
            if m_dur:
                self.time_unit = m_dur.group(1).lower()

        return self

    def find_duplicate_paths(self, prefix: str = "", depth: int = 0, max_depth: int = 100) -> List[str]:
        """Recursively search for duplicate child field names within this field."""
        if depth > max_depth:
            curr_target = f"{prefix}.{self.name}" if prefix else self.name
            raise ValueError(
                f"Schema nesting depth exceeded maximum supported depth ({max_depth}) at '{curr_target}'"
            )

        curr_path = f"{prefix}.{self.name}" if prefix else self.name
        dups: List[str] = []
        if self.children:
            seen: set[str] = set()
            child_dups: set[str] = set()
            for child in self.children:
                if child.name in seen:
                    child_dups.add(child.name)
                seen.add(child.name)
            for cd in sorted(list(child_dups)):
                dups.append(f"{curr_path}.{cd}")
            for child in self.children:
                dups.extend(
                    child.find_duplicate_paths(prefix=curr_path, depth=depth + 1, max_depth=max_depth)
                )
        return dups

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, FieldDefinition):
            return False
        return (
            self.name == other.name
            and self.type == other.type
            and self.nullable == other.nullable
            and self.metadata == other.metadata
            and self.children == other.children
            and self.list_size == other.list_size
            and self.ordered == other.ordered
            and self.keys_sorted == other.keys_sorted
            and self.precision == other.precision
            and self.scale == other.scale
            and self.time_unit == other.time_unit
            and self.timezone == other.timezone
        )


class SchemaContract(BaseModel):
    """Declarative golden contract defining expected Parquet schema layout."""

    model_config = ConfigDict(extra="forbid")

    SUPPORTED_VERSIONS: ClassVar[set[str]] = {"1.0"}

    version: str = Field(default="1.0", description="Contract format version")
    name: Optional[str] = Field(default=None, description="Human readable dataset or contract name")
    fields: List[FieldDefinition] = Field(description="Ordered list of fields")
    metadata: Dict[str, str] = Field(default_factory=dict, description="Top-level schema metadata")

    @field_validator("version")
    @classmethod
    def validate_version(cls, v: str) -> str:
        """Enforce strict supported contract versioning."""
        if v not in cls.SUPPORTED_VERSIONS:
            raise ValueError(
                f"Unsupported contract version '{v}'. Supported contract versions: {sorted(cls.SUPPORTED_VERSIONS)}"
            )
        return v

    @field_validator("fields")
    @classmethod
    def validate_fields_not_empty(cls, v: List[FieldDefinition]) -> List[FieldDefinition]:
        """Prevent invalid empty contracts from silently matching arbitrary datasets."""
        if not v:
            raise ValueError(
                "SchemaContract cannot be empty: must define at least one field column definition."
            )
        return v

    def get_field(self, name: str) -> Optional[FieldDefinition]:
        """Lookup field definition by case-sensitive name."""
        for field in self.fields:
            if field.name == name:
                return field
        return None

    def field_names(self) -> List[str]:
        """Return list of all field names in order."""
        return [f.name for f in self.fields]

    def find_all_duplicate_paths(self) -> List[str]:
        """Recursively return all duplicated field paths across all hierarchy levels."""
        dups: List[str] = []
        # 1. Top-level duplicate fields
        seen: set[str] = set()
        top_dups: set[str] = set()
        for f in self.fields:
            if f.name in seen:
                top_dups.add(f.name)
            seen.add(f.name)
        for td in sorted(list(top_dups)):
            dups.append(td)
        # 2. Nested duplicate fields in children
        for f in self.fields:
            dups.extend(f.find_duplicate_paths(prefix=""))
        return dups

    def has_duplicate_fields(self) -> bool:
        """True if schema has duplicate field names at top-level or any nested level."""
        return len(self.find_all_duplicate_paths()) > 0

    def duplicate_field_names(self) -> List[str]:
        """Return list of duplicated field names or paths in deterministic sorted order."""
        return self.find_all_duplicate_paths()


class FieldDiff(BaseModel):
    """Detailed record of a discrepancy between baseline and target schema."""

    field_name: str
    change_type: ChangeType
    severity: Severity
    error_code: Optional[ErrorCode] = None
    field_path: Optional[str] = None
    old_value: Optional[str] = None
    new_value: Optional[str] = None
    description: str
    recommendation: Optional[str] = None


class DiffReport(BaseModel):
    """Aggregated comparison report between two schemas or schema vs contract."""

    target_name: str
    baseline_name: str
    mode: str = Field(default="permissive", description="'strict' or 'permissive'")
    diffs: List[FieldDiff] = Field(default_factory=list)
    result_state: ResultState = Field(default=ResultState.VALID)

    @property
    def has_breaking_changes(self) -> bool:
        """True if any diff has BREAKING severity."""
        return any(d.severity == Severity.BREAKING for d in self.diffs)

    @property
    def has_warnings(self) -> bool:
        """True if any diff has WARNING severity."""
        return any(d.severity == Severity.WARNING for d in self.diffs)

    @property
    def is_compatible(self) -> bool:
        """Compatible if no breaking changes exist."""
        return not self.has_breaking_changes

    @property
    def stats(self) -> Dict[str, int]:
        """Count breakdown of diffs by severity."""
        return {
            "total_diffs": len(self.diffs),
            "breaking": sum(1 for d in self.diffs if d.severity == Severity.BREAKING),
            "warnings": sum(1 for d in self.diffs if d.severity == Severity.WARNING),
            "safe": sum(1 for d in self.diffs if d.severity == Severity.SAFE),
        }


class PartitionFileReport(BaseModel):
    """Metadata inspection summary for a single Parquet file in a dataset."""

    file_path: str
    file_size_bytes: int
    row_count: int
    row_groups: int
    column_count: int
    schema_fingerprint: str
    is_divergent: bool = False
    divergence_reason: Optional[str] = None
    warnings: List[str] = Field(default_factory=list)


class PartitionAuditResult(BaseModel):
    """Audit outcome across all files in a multi-partition Parquet dataset."""

    root_path: str
    total_files_scanned: int
    total_rows: int
    total_bytes: int
    distinct_schemas_count: int
    is_consistent: bool
    is_compatible: bool = True
    has_drift: bool = False
    result_state: ResultState = Field(default=ResultState.VALID)
    warnings_count: int = 0
    breaking_count: int = 0
    canonical_schema: Optional[SchemaContract] = None
    divergent_files_count: int = 0
    files: List[PartitionFileReport] = Field(default_factory=list)
    divergence_details: List[str] = Field(default_factory=list)
    drifts: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    cache_hits: int = Field(default=0, description="Number of files reused from incremental audit cache")
    cache_misses: int = Field(default=0, description="Number of files newly inspected")
    incremental: bool = Field(default=False, description="Whether incremental caching was used")
