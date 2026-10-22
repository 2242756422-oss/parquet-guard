"""Formatting and report generation for terminal, GitHub Markdown, JSON, and SARIF."""

from __future__ import annotations

import json
from typing import Any, Dict, List

from parquet_guard.core.models import (
    DiffReport,
    PartitionAuditResult,
    Severity,
)

SARIF_SCHEMA_URI = (
    "https://raw.githubusercontent.com/oasis-tcs/sarif-spec/master/Schemata/sarif-schema-2.1.0.json"
)

SARIF_RULES: List[Dict[str, Any]] = [
    {
        "id": "TYPE_MISMATCH",
        "name": "TypeMismatch",
        "shortDescription": {"text": "Incompatible Parquet column type change"},
    },
    {
        "id": "TYPE_WIDENED",
        "name": "TypeWidened",
        "shortDescription": {"text": "Parquet column type widened"},
    },
    {
        "id": "TYPE_NARROWED",
        "name": "TypeNarrowed",
        "shortDescription": {"text": "Incompatible Parquet column type narrowing"},
    },
    {
        "id": "FIELD_REMOVED",
        "name": "FieldRemoved",
        "shortDescription": {"text": "Required Parquet column missing"},
    },
    {
        "id": "FIELD_ADDED",
        "name": "FieldAdded",
        "shortDescription": {"text": "New Parquet column added"},
    },
    {
        "id": "FIELD_ORDER_CHANGED",
        "name": "FieldOrderChanged",
        "shortDescription": {"text": "Parquet column ordering differs from contract"},
    },
    {
        "id": "NULLABILITY_CHANGED",
        "name": "NullabilityChanged",
        "shortDescription": {"text": "Column nullability constraint violation"},
    },
    {
        "id": "METADATA_CHANGED",
        "name": "MetadataChanged",
        "shortDescription": {"text": "Schema or field metadata attributes modified"},
    },
    {
        "id": "CONTAINER_PARAM_CHANGED",
        "name": "ContainerParamChanged",
        "shortDescription": {"text": "Nested container structural parameter mismatch"},
    },
    {
        "id": "ORDERED_FLAG_CHANGED",
        "name": "OrderedFlagChanged",
        "shortDescription": {"text": "Dictionary ordered sorting flag modified"},
    },
    {
        "id": "KEYS_SORTED_CHANGED",
        "name": "KeysSortedChanged",
        "shortDescription": {"text": "Map keys_sorted attribute modified"},
    },
    {
        "id": "DECIMAL_SCALE_MISMATCH",
        "name": "DecimalScaleMismatch",
        "shortDescription": {"text": "Decimal scale shifted altering numeric values"},
    },
    {
        "id": "TIMESTAMP_TIMEZONE_MISMATCH",
        "name": "TimestampTimezoneMismatch",
        "shortDescription": {"text": "Timestamp timezone attribute mismatch"},
    },
    {
        "id": "SCHEMA_DIVERGENT",
        "name": "SchemaDivergent",
        "shortDescription": {"text": "Partition schema incompatible with baseline contract"},
    },
    {
        "id": "SCHEMA_DRIFT_WARNING",
        "name": "SchemaDriftWarning",
        "shortDescription": {"text": "Non-breaking schema drift or warning detected"},
    },
]


def format_diff_terminal(report: DiffReport) -> str:
    """Format DiffReport for human-readable ANSI terminal output."""
    lines = []
    lines.append("=" * 70)
    lines.append(" PARQUET-GUARD SCHEMA COMPARISON REPORT")
    lines.append("=" * 70)
    lines.append(f" Target   : {report.target_name}")
    lines.append(f" Baseline : {report.baseline_name}")
    lines.append(f" Mode     : {report.mode.upper()}")
    lines.append(f" State    : {report.result_state.value}")

    stats = report.stats
    lines.append(
        f" Summary  : {stats['total_diffs']} diff(s) | "
        f"{stats['breaking']} BREAKING | "
        f"{stats['warnings']} WARNING | "
        f"{stats['safe']} SAFE"
    )
    lines.append("-" * 70)

    if not report.diffs:
        lines.append(" [PASS] Schemas are perfectly identical. No differences found.")
        lines.append("=" * 70)
        return "\n".join(lines)

    for idx, diff in enumerate(report.diffs, 1):
        sev = diff.severity
        tag = f"[{sev.value}]"
        lines.append(f" {idx:2d}. {tag:<10} Field: {diff.field_name} ({diff.change_type.value})")
        lines.append(f"     Details  : {diff.description}")
        if diff.old_value or diff.new_value:
            lines.append(f"     Old Type : {diff.old_value or '<none>'}")
            lines.append(f"     New Type : {diff.new_value or '<none>'}")
        if diff.recommendation:
            lines.append(f"     Action   : {diff.recommendation}")
        lines.append("")

    lines.append("-" * 70)
    if report.has_breaking_changes:
        lines.append(" [FAIL] Breaking schema regressions detected!")
    elif report.has_warnings:
        lines.append(" [PASS with WARNINGS] Backward-compatible, but schema warnings detected.")
    else:
        lines.append(" [PASS] Backward-compatible additive schema evolution.")
    lines.append("=" * 70)

    return "\n".join(lines)


def format_diff_markdown(report: DiffReport) -> str:
    """Format DiffReport as GitHub-Flavored Markdown for PR comments and CI summaries."""
    stats = report.stats
    if report.has_breaking_changes:
        status_badge = "❌ **BREAKING REGRESSION DETECTED**"
    elif report.has_warnings:
        status_badge = "⚠️ **PASS WITH WARNINGS**"
    elif report.diffs:
        status_badge = "✅ **SAFE ADDITIVE EVOLUTION**"
    else:
        status_badge = "✅ **100% IDENTICAL**"

    md = []
    md.append("## 🛡️ Parquet-Guard Schema Validation Report")
    md.append("")
    md.append(f"**Status**: {status_badge} | **State**: `{report.result_state.value}`")
    md.append("")
    md.append("| Target | Baseline | Mode | Breaking | Warnings | Safe |")
    md.append("| :--- | :--- | :---: | :---: | :---: | :---: |")
    md.append(
        f"| `{report.target_name}` | `{report.baseline_name}` | `{report.mode}` | "
        f"**{stats['breaking']}** | {stats['warnings']} | {stats['safe']} |"
    )
    md.append("")

    if report.has_breaking_changes:
        md.append("> [!CAUTION]")
        md.append("> **Breaking schema changes detected!** Downstream data consumers will fail if merged.")
        md.append("")
    else:
        md.append("> [!NOTE]")
        md.append(
            "> All columns and types strictly conform to contract or represent safe, backward-compatible widening."
        )
        md.append("")

    if report.diffs:
        md.append("### Detailed Schema Discrepancies")
        md.append("")
        md.append("| Severity | Field | Change Type | Description | Recommendation |")
        md.append("| :---: | :--- | :---: | :--- | :--- |")
        for diff in report.diffs:
            sev_icon = (
                "🔴"
                if diff.severity == Severity.BREAKING
                else ("🟡" if diff.severity == Severity.WARNING else "🟢")
            )
            md.append(
                f"| {sev_icon} `{diff.severity.value}` | `{diff.field_name}` | `{diff.change_type.value}` | "
                f"{diff.description} | {diff.recommendation or 'N/A'} |"
            )
        md.append("")

    md.append("---")
    md.append("*Generated by [parquet-guard](https://github.com/2242756422-oss/parquet-guard)*")
    return "\n".join(md)


def format_diff_json(report: DiffReport) -> str:
    """Serialize DiffReport to formatted JSON string."""
    return json.dumps(report.model_dump(), indent=2)


def format_diff_sarif(report: DiffReport) -> str:
    """Format DiffReport into SARIF 2.1.0 for GitHub Code Scanning."""
    results: List[Dict[str, Any]] = []

    for diff in report.diffs:
        level = (
            "error"
            if diff.severity == Severity.BREAKING
            else ("warning" if diff.severity == Severity.WARNING else "note")
        )
        rule_id = diff.error_code.value if diff.error_code else diff.change_type.value

        results.append(
            {
                "ruleId": rule_id,
                "level": level,
                "message": {
                    "text": f"[{diff.severity.value}] {diff.description} Recommendation: {diff.recommendation or 'None'}"
                },
                "locations": [
                    {
                        "physicalLocation": {
                            "artifactLocation": {"uri": report.target_name},
                            "region": {"startLine": 1},
                        }
                    }
                ],
                "properties": {
                    "fieldName": diff.field_name,
                    "changeType": diff.change_type.value,
                    "severity": diff.severity.value,
                },
            }
        )

    sarif = {
        "$schema": SARIF_SCHEMA_URI,
        "version": "2.1.0",
        "runs": [
            {
                "tool": {
                    "driver": {
                        "name": "parquet-guard",
                        "informationUri": "https://github.com/2242756422-oss/parquet-guard",
                        "rules": SARIF_RULES,
                    }
                },
                "results": results,
            }
        ],
    }
    return json.dumps(sarif, indent=2)


def format_audit_terminal(result: PartitionAuditResult) -> str:
    """Format PartitionAuditResult for terminal output."""
    lines = []
    lines.append("=" * 70)
    lines.append(" PARQUET-GUARD PARTITION DATASET AUDIT REPORT")
    lines.append("=" * 70)
    lines.append(f" Target Path  : {result.root_path}")
    lines.append(f" Total Files  : {result.total_files_scanned}")
    lines.append(f" Total Rows   : {result.total_rows:,}")
    lines.append(f" Total Size   : {result.total_bytes / (1024 * 1024):.2f} MB")
    lines.append(f" Schemas Found: {result.distinct_schemas_count} distinct schema(s)")
    lines.append(f" Result State : {result.result_state.value}")
    lines.append(
        f" Compatibility: {'PASSED' if result.is_compatible else 'FAILED'} (Breaking changes: {result.breaking_count})"
    )
    lines.append(
        f" Schema Drift : {'DETECTED' if result.has_drift else 'NONE'} (Warnings/Drifts: {result.warnings_count})"
    )
    if result.incremental:
        lines.append(
            f" Cache Stats  : {result.cache_hits} hit(s), {result.cache_misses} miss(es) (Incremental)"
        )
    lines.append(f" Divergent    : {result.divergent_files_count} file(s)")
    lines.append("-" * 70)

    if result.is_consistent:
        if result.has_drift:
            lines.append(
                " [PASS with WARNINGS] Dataset is backward-compatible, but schema drift was detected."
            )
        else:
            lines.append(" [PASS] Dataset schema is 100% consistent across all partitions.")
    else:
        lines.append(" [FAIL] Schema drift detected across dataset partitions!")

    if result.divergence_details:
        lines.append("")
        lines.append(" Audit Details:")
        for idx, det in enumerate(result.divergence_details, 1):
            lines.append(f"  {idx}. {det}")

    lines.append("=" * 70)
    return "\n".join(lines)


def format_audit_markdown(result: PartitionAuditResult) -> str:
    """Format PartitionAuditResult as GitHub-Flavored Markdown."""
    if result.is_consistent and not result.has_drift:
        status_badge = "✅ **100% Consistent**"
    elif result.is_compatible:
        status_badge = f"🟡 **Compatible with Drift** ({result.warnings_count} warnings)"
    else:
        status_badge = f"❌ **Incompatible** ({result.breaking_count} breaking)"

    md = []
    md.append("## 📦 Parquet-Guard Dataset Partition Audit")
    md.append("")
    md.append(
        "| Target Path | Files Scanned | Total Rows | Total Size | Distinct Schemas | Compatibility | Schema Drift | State | Status |"
    )
    md.append("| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |")
    md.append(
        f"| `{result.root_path}` | {result.total_files_scanned} | {result.total_rows:,} | "
        f"{result.total_bytes / (1024 * 1024):.2f} MB | {result.distinct_schemas_count} | "
        f"{'PASSED' if result.is_compatible else 'FAILED'} | {'DETECTED' if result.has_drift else 'NONE'} | "
        f"`{result.result_state.value}` | {status_badge} |"
    )
    md.append("")

    if result.incremental:
        md.append(f"**Incremental Cache**: {result.cache_hits} hits, {result.cache_misses} misses.")
        md.append("")

    if not result.is_compatible:
        md.append("> [!CAUTION]")
        md.append(f"> **{result.divergent_files_count} incompatible file(s)** found in the dataset tree.")
        md.append("")
    elif result.has_drift:
        md.append("> [!WARNING]")
        md.append(
            f"> Dataset partitions are backward-compatible, but schema drift or warnings were detected ({result.warnings_count} warning(s))."
        )
        md.append("")

    if result.divergence_details:
        md.append("### Divergence and Drift Log")
        for det in result.divergence_details:
            icon = "🔴" if "[BREAKING]" in det else ("🟡" if "[WARNING]" in det else "⚠️")
            md.append(f"- {icon} {det}")
        md.append("")

    return "\n".join(md)


def format_audit_json(result: PartitionAuditResult) -> str:
    """Serialize PartitionAuditResult to formatted JSON string."""
    return json.dumps(result.model_dump(), indent=2)


def format_audit_sarif(result: PartitionAuditResult) -> str:
    """Format PartitionAuditResult into SARIF 2.1.0 format for GitHub Code Scanning."""
    sarif_results: List[Dict[str, Any]] = []

    for file_rep in result.files:
        if file_rep.is_divergent:
            sarif_results.append(
                {
                    "ruleId": "SCHEMA_DIVERGENT",
                    "level": "error",
                    "message": {
                        "text": file_rep.divergence_reason or "Partition schema incompatible with baseline"
                    },
                    "locations": [
                        {
                            "physicalLocation": {
                                "artifactLocation": {"uri": file_rep.file_path},
                                "region": {"startLine": 1},
                            }
                        }
                    ],
                }
            )
        for warn in file_rep.warnings:
            sarif_results.append(
                {
                    "ruleId": "SCHEMA_DRIFT_WARNING",
                    "level": "warning",
                    "message": {"text": warn},
                    "locations": [
                        {
                            "physicalLocation": {
                                "artifactLocation": {"uri": file_rep.file_path},
                                "region": {"startLine": 1},
                            }
                        }
                    ],
                }
            )

    sarif = {
        "$schema": SARIF_SCHEMA_URI,
        "version": "2.1.0",
        "runs": [
            {
                "tool": {
                    "driver": {
                        "name": "parquet-guard",
                        "informationUri": "https://github.com/2242756422-oss/parquet-guard",
                        "rules": SARIF_RULES,
                    }
                },
                "results": sarif_results,
            }
        ],
    }
    return json.dumps(sarif, indent=2)
