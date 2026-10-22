"""Evidence normalizer for Parquet Guard AI subsystem.

Transforms deterministic DiffReports and SchemaContracts into structured,
sanitized NormalizedEvidence payloads with stable finding identifiers.

CRITICAL PRIVACY GUARANTEE:
Zero raw row data is ever accessed, inspected, or converted into evidence.
Only structural schema ASTs and metadata attributes are normalized.
"""

from __future__ import annotations

import html
from typing import Optional, Set

from parquet_guard.ai.models import NormalizedEvidence, NormalizedFinding
from parquet_guard.core.models import DiffReport, PartitionAuditResult


def normalize_diff_report(
    report: DiffReport,
    user_context: Optional[str] = None,
) -> NormalizedEvidence:
    """Normalize a deterministic DiffReport into a structured NormalizedEvidence payload."""
    findings: list[NormalizedFinding] = []

    for idx, diff in enumerate(report.diffs, start=1):
        f_id = f"F{idx:03d}"
        path = diff.field_path or diff.field_name
        findings.append(
            NormalizedFinding(
                finding_id=f_id,
                field_name=diff.field_name,
                field_path=path,
                change_type=diff.change_type,
                severity=diff.severity,
                error_code=diff.error_code,
                old_type=diff.old_value,
                new_type=diff.new_value,
                old_value=diff.old_value,
                new_value=diff.new_value,
                description=diff.description,
                recommendation=diff.recommendation,
            )
        )

    return NormalizedEvidence(
        target_name=report.target_name,
        baseline_name=report.baseline_name,
        comparison_mode=report.mode,
        deterministic_verdict=report.result_state,
        is_breaking=report.has_breaking_changes,
        findings=findings,
        metadata_changes=[],
        user_context=user_context,
    )


def normalize_audit_result(
    audit: PartitionAuditResult,
    user_context: Optional[str] = None,
) -> NormalizedEvidence:
    """Normalize a multi-partition PartitionAuditResult into NormalizedEvidence."""
    findings: list[NormalizedFinding] = []

    # Map partition divergence details into normalized findings
    idx = 1
    for div in audit.divergence_details:
        f_id = f"F{idx:03d}"
        findings.append(
            NormalizedFinding(
                finding_id=f_id,
                field_name="partition_dataset",
                field_path="dataset",
                change_type="METADATA_CHANGED",  # type: ignore[arg-type]
                severity=audit.result_state,  # type: ignore[arg-type]
                error_code=None,
                description=div,
                recommendation="Reconcile partition schemas across dataset tree.",
            )
        )
        idx += 1

    for drift in audit.drifts:
        f_id = f"F{idx:03d}"
        findings.append(
            NormalizedFinding(
                finding_id=f_id,
                field_name="schema_drift",
                field_path="dataset.drift",
                change_type="TYPE_MISMATCH",  # type: ignore[arg-type]
                severity="WARNING",  # type: ignore[arg-type]
                description=drift,
                recommendation="Investigate schema drift across partitions.",
            )
        )
        idx += 1

    return NormalizedEvidence(
        target_name=audit.root_path,
        baseline_name="CanonicalDatasetBaseline",
        comparison_mode="audit",
        deterministic_verdict=audit.result_state,
        is_breaking=not audit.is_consistent,
        findings=findings,
        metadata_changes=audit.warnings,
        user_context=user_context,
    )


def sanitize_token(text: str, max_length: int = 500) -> str:
    """Sanitize strings for secure XML/JSON inclusion, neutralizing delimiter injection."""
    if not text:
        return ""
    # Strip dangerous control chars and escape XML entities
    cleaned = text.replace("\x00", "").replace("\r", "")
    if len(cleaned) > max_length:
        cleaned = cleaned[:max_length] + " [TRUNCATED]"
    return html.escape(cleaned)


def format_evidence_for_prompt(evidence: NormalizedEvidence) -> str:
    """Serialize normalized evidence into structured XML/JSON data boundaries for the LLM."""
    lines: list[str] = [
        "<evidence_data>",
        f"  <target_name>{sanitize_token(evidence.target_name)}</target_name>",
        f"  <baseline_name>{sanitize_token(evidence.baseline_name)}</baseline_name>",
        f"  <comparison_mode>{evidence.comparison_mode}</comparison_mode>",
        f"  <deterministic_verdict>{evidence.deterministic_verdict.value}</deterministic_verdict>",
        f"  <is_breaking>{str(evidence.is_breaking).lower()}</is_breaking>",
        f"  <findings_count>{len(evidence.findings)}</findings_count>",
        "  <findings>",
    ]

    for f in evidence.findings:
        err_str = f.error_code.value if f.error_code else "NONE"
        lines.extend(
            [
                f'    <finding id="{f.finding_id}">',
                f"      <field_path>{sanitize_token(f.field_path)}</field_path>",
                f"      <change_type>{f.change_type.value}</change_type>",
                f"      <severity>{f.severity.value}</severity>",
                f"      <error_code>{err_str}</error_code>",
                f"      <old_type>{sanitize_token(f.old_type or 'None')}</old_type>",
                f"      <new_type>{sanitize_token(f.new_type or 'None')}</new_type>",
                f"      <description>{sanitize_token(f.description)}</description>",
                f"      <recommendation>{sanitize_token(f.recommendation or 'None')}</recommendation>",
                "    </finding>",
            ]
        )

    lines.append("  </findings>")

    if evidence.user_context:
        lines.extend(
            [
                "  <user_provided_context>",
                f"    {sanitize_token(evidence.user_context, max_length=1000)}",
                "  </user_provided_context>",
            ]
        )

    lines.append("</evidence_data>")
    return "\n".join(lines)


def get_valid_finding_ids(evidence: NormalizedEvidence) -> Set[str]:
    """Return the set of valid finding_id strings defined in the evidence."""
    return {f.finding_id for f in evidence.findings}


def get_valid_field_paths(evidence: NormalizedEvidence) -> Set[str]:
    """Return the set of valid field_path strings defined in the evidence."""
    return {f.field_path for f in evidence.findings}
