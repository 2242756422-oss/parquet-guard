"""Dataset topology, schema grouping, and drift concentration analysis.

Enables understanding partitioned Parquet datasets by:
1. Grouping files by canonical schema fingerprint.
2. Calculating partition distribution percentages.
3. Quantifying divergence concentration across fields.
4. Aggregating downstream consumer impact across distinct schema variants.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from parquet_guard.core.consumer import (
    ConsumerCompatibilityStatus,
    ConsumerContract,
    evaluate_single_consumer,
)
from parquet_guard.core.inspector import inspect_parquet_schema
from parquet_guard.core.models import (
    PartitionAuditResult,
    PartitionFileReport,
    SchemaContract,
)


class FieldDivergenceSummary(BaseModel):
    """Summary of how a single field varies across schema groups in a dataset."""

    model_config = ConfigDict(extra="forbid")

    field_path: str = Field(description="Column name or dotted path")
    variants: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="List of observed type/attribute variants with file counts and percentages",
    )
    is_breaking_divergence: bool = Field(
        default=False, description="True if variants cannot be safely unified without migration"
    )
    affected_files_count: int = Field(description="Total number of files exhibiting divergent definitions")


class SchemaGroup(BaseModel):
    """A distinct structural schema group observed across dataset partition files."""

    model_config = ConfigDict(extra="forbid")

    group_id: str = Field(description="Group identifier, e.g. 'G01'")
    schema_fingerprint: str = Field(description="SHA-256 canonical schema fingerprint")
    file_count: int = Field(description="Number of files sharing this exact schema")
    percentage: float = Field(description="Percentage of total dataset files in this group (0-100)")
    row_count: int = Field(description="Total rows in this schema group")
    byte_size: int = Field(description="Total byte size of files in this group")
    sample_files: List[str] = Field(
        default_factory=list, description="Up to 5 representative file paths in this group"
    )
    schema_contract: SchemaContract = Field(description="Extracted SchemaContract for this group")
    consumer_status: Dict[str, ConsumerCompatibilityStatus] = Field(
        default_factory=dict,
        description="Compatibility status with each declared consumer (consumer_id -> status)",
    )


class DatasetTopologyReport(BaseModel):
    """Comprehensive topology report showing schema distribution and divergence concentration."""

    model_config = ConfigDict(extra="forbid")

    root_path: str = Field(description="Scanned dataset root directory")
    total_files: int = Field(description="Total Parquet files scanned")
    total_rows: int = Field(description="Total rows across all files")
    total_bytes: int = Field(description="Total size in bytes across all files")
    distinct_schemas_count: int = Field(description="Number of distinct schema groups detected")
    canonical_group_id: str = Field(description="ID of the majority canonical schema group")
    groups: List[SchemaGroup] = Field(
        default_factory=list, description="List of all schema groups sorted by file count descending"
    )
    divergence_concentration: List[FieldDivergenceSummary] = Field(
        default_factory=list, description="Field-level divergence concentration across groups"
    )
    summary: str = Field(description="Human-readable executive summary")


def analyze_dataset_topology(
    audit_result: PartitionAuditResult,
    consumers: Optional[List[ConsumerContract]] = None,
) -> DatasetTopologyReport:
    """Analyze schema grouping and drift concentration from an existing audit result."""
    if not audit_result.files:
        return DatasetTopologyReport(
            root_path=audit_result.root_path,
            total_files=0,
            total_rows=0,
            total_bytes=0,
            distinct_schemas_count=0,
            canonical_group_id="NONE",
            groups=[],
            divergence_concentration=[],
            summary="Empty dataset: zero Parquet files found.",
        )

    # Group files by schema fingerprint
    files_by_fingerprint: Dict[str, List[PartitionFileReport]] = {}
    for f in audit_result.files:
        fp = f.schema_fingerprint
        if fp not in files_by_fingerprint:
            files_by_fingerprint[fp] = []
        files_by_fingerprint[fp].append(f)

    # Sort fingerprints by file count descending (majority group first)
    sorted_fps = sorted(
        files_by_fingerprint.keys(),
        key=lambda fp: len(files_by_fingerprint[fp]),
        reverse=True,
    )

    total_files = len(audit_result.files)
    total_rows = sum(f.row_count for f in audit_result.files)
    total_bytes = sum(f.file_size_bytes for f in audit_result.files)

    groups: List[SchemaGroup] = []
    schemas_by_group: Dict[str, SchemaContract] = {}

    for idx, fp in enumerate(sorted_fps, start=1):
        group_id = f"G{idx:02d}"
        file_list = files_by_fingerprint[fp]
        count = len(file_list)
        pct = round((count / total_files) * 100.0, 2)
        group_rows = sum(f.row_count for f in file_list)
        group_bytes = sum(f.file_size_bytes for f in file_list)
        samples = [f.file_path for f in file_list[:5]]

        # Read schema of representative file
        sample_path = file_list[0].file_path
        group_schema = inspect_parquet_schema(sample_path)
        schemas_by_group[group_id] = group_schema

        # Evaluate consumers against this group if provided
        group_consumer_status: Dict[str, ConsumerCompatibilityStatus] = {}
        if consumers:
            for c in consumers:
                ev = evaluate_single_consumer(group_schema, c)
                group_consumer_status[c.consumer_id] = ev.status

        groups.append(
            SchemaGroup(
                group_id=group_id,
                schema_fingerprint=fp,
                file_count=count,
                percentage=pct,
                row_count=group_rows,
                byte_size=group_bytes,
                sample_files=samples,
                schema_contract=group_schema,
                consumer_status=group_consumer_status,
            )
        )

    canonical_group_id = groups[0].group_id if groups else "NONE"

    # Analyze field divergence concentration across groups
    divergence_concentration: List[FieldDivergenceSummary] = []
    if len(groups) > 1:
        # Collect all unique field names across all groups
        all_field_names: set[str] = set()
        for g in groups:
            all_field_names.update(g.schema_contract.field_names())

        for field_name in sorted(list(all_field_names)):
            variants: List[Dict[str, Any]] = []
            seen_defs: set[str] = set()

            for g in groups:
                f_def = g.schema_contract.get_field(field_name)
                type_repr = f_def.type if f_def else "<MISSING>"
                nullable_repr = f"nullable={f_def.nullable}" if f_def else ""
                key = f"{type_repr} ({nullable_repr})" if nullable_repr else type_repr

                # Check if this variant is already recorded
                existing = next((v for v in variants if v["variant_key"] == key), None)
                if existing:
                    existing["file_count"] += g.file_count
                    existing["groups"].append(g.group_id)
                else:
                    variants.append(
                        {
                            "variant_key": key,
                            "type": type_repr,
                            "nullable": f_def.nullable if f_def else None,
                            "file_count": g.file_count,
                            "groups": [g.group_id],
                        }
                    )
                seen_defs.add(key)

            # If more than one definition exists, this field exhibits divergence
            if len(seen_defs) > 1:
                # Calculate file percentages for each variant
                for v in variants:
                    v["percentage"] = round((v["file_count"] / total_files) * 100.0, 2)

                # Total divergent files is count of files not in majority variant
                sorted_vars = sorted(variants, key=lambda x: x["file_count"], reverse=True)
                divergent_files = total_files - sorted_vars[0]["file_count"]

                divergence_concentration.append(
                    FieldDivergenceSummary(
                        field_path=field_name,
                        variants=variants,
                        is_breaking_divergence=True,
                        affected_files_count=divergent_files,
                    )
                )

    summary = (
        f"Dataset topology for '{audit_result.root_path}': "
        f"{total_files} files across {len(groups)} distinct schema groups. "
        f"Majority canonical group {canonical_group_id} contains {groups[0].percentage}% of files. "
        f"Divergent fields: {len(divergence_concentration)}."
    )

    return DatasetTopologyReport(
        root_path=audit_result.root_path,
        total_files=total_files,
        total_rows=total_rows,
        total_bytes=total_bytes,
        distinct_schemas_count=len(groups),
        canonical_group_id=canonical_group_id,
        groups=groups,
        divergence_concentration=divergence_concentration,
        summary=summary,
    )
