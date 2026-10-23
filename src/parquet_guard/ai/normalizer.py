"""Evidence normalizer for Parquet Guard AI subsystem.

Transforms deterministic DiffReports, SchemaContracts, and ConsumerImpactReports
into structured, sanitized NormalizedEvidence payloads with stable finding identifiers.

CRITICAL PRIVACY GUARANTEE & METADATA BOUNDARY:
- Raw Data Rows: Zero record values, row contents, or data payloads are ever
  accessed, read, or transmitted.
- Metadata Disclosed: When remote AI providers (e.g., OpenAICompatibleProvider)
  are configured and invoked, schema structural metadata (field names, Arrow
  data types, nullabilities, field/schema metadata keys and values, deterministic
  error codes, diff descriptions, and optional user-supplied context) is serialized
  into prompts and transmitted over HTTPS to the configured provider endpoint.
  When using the default offline deterministic engine or MockAIProvider, zero
  network requests are made.
"""

from __future__ import annotations

import html
from typing import Optional, Set

from parquet_guard.ai.models import NormalizedEvidence, NormalizedFinding
from parquet_guard.core.consumer import (
    ConsumerCompatibilityStatus,
    ConsumerImpactReport,
)
from parquet_guard.core.models import (
    ChangeType,
    DiffReport,
    ErrorCode,
    PartitionAuditResult,
    ResultState,
    Severity,
)


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
        consumer_impacts=[],
        user_context=user_context,
    )


def normalize_audit_result(
    audit: PartitionAuditResult,
    user_context: Optional[str] = None,
) -> NormalizedEvidence:
    """Normalize a multi-partition PartitionAuditResult into NormalizedEvidence."""
    findings: list[NormalizedFinding] = []

    idx = 1
    for div in audit.divergence_details:
        f_id = f"F{idx:03d}"
        findings.append(
            NormalizedFinding(
                finding_id=f_id,
                field_name="partition_dataset",
                field_path="dataset",
                change_type=ChangeType.METADATA_CHANGED,
                severity=Severity.BREAKING if audit.is_consistent is False else Severity.WARNING,
                error_code=ErrorCode.DATASET_INCOMPATIBILITY,
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
                change_type=ChangeType.TYPE_MISMATCH,
                severity=Severity.WARNING,
                error_code=ErrorCode.TYPE_MISMATCH,
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
        consumer_impacts=[],
        user_context=user_context,
    )


def normalize_consumer_impact_report(
    report: ConsumerImpactReport,
    user_context: Optional[str] = None,
) -> NormalizedEvidence:
    """Normalize a ConsumerImpactReport into structured NormalizedEvidence."""
    findings: list[NormalizedFinding] = []
    consumer_summaries: list[dict[str, object]] = []

    idx = 1
    for ev in report.evaluations:
        consumer_summaries.append(
            {
                "consumer_id": ev.consumer_id,
                "consumer_name": ev.consumer_name,
                "status": ev.status.value,
                "is_blocking": ev.is_blocking,
                "missing_fields": ev.missing_required_fields,
                "type_mismatches": ev.type_mismatches,
                "nullability_violations": ev.nullability_violations,
                "dimension_mismatches": ev.dimension_mismatches,
            }
        )

        for fe in ev.field_evaluations:
            if fe.status in (
                ConsumerCompatibilityStatus.INCOMPATIBLE,
                ConsumerCompatibilityStatus.CONDITIONALLY_COMPATIBLE,
                ConsumerCompatibilityStatus.POTENTIALLY_BREAKING,
            ):
                f_id = f"C{idx:03d}"
                fe.linked_finding_id = f_id
                severity = (
                    Severity.BREAKING
                    if fe.status == ConsumerCompatibilityStatus.INCOMPATIBLE and fe.is_critical
                    else Severity.WARNING
                )

                ch_type = ChangeType.TYPE_MISMATCH
                if fe.requirement and fe.field_name in ev.missing_required_fields:
                    ch_type = ChangeType.FIELD_REMOVED
                elif fe.field_name in ev.nullability_violations:
                    ch_type = ChangeType.NULLABILITY_CHANGED
                elif fe.field_name in ev.dimension_mismatches:
                    ch_type = ChangeType.CONTAINER_PARAM_CHANGED

                findings.append(
                    NormalizedFinding(
                        finding_id=f_id,
                        field_name=fe.field_name,
                        field_path=f"{ev.consumer_id}.{fe.field_name}",
                        change_type=ch_type,
                        severity=severity,
                        error_code=ErrorCode.DATASET_INCOMPATIBILITY,
                        old_type=str(fe.requirement.permitted_types)
                        if fe.requirement and fe.requirement.permitted_types
                        else None,
                        new_type=fe.producer_type,
                        description=f"Consumer '{ev.consumer_name}' ({ev.consumer_id}): {fe.violation_reason or 'Requirement violated'}",
                        recommendation="; ".join(ev.recommendations) if ev.recommendations else None,
                    )
                )
                idx += 1

    det_verdict = ResultState.VALID
    if report.overall_status == ConsumerCompatibilityStatus.INCOMPATIBLE:
        det_verdict = ResultState.INCOMPATIBLE
    elif report.overall_status in (
        ConsumerCompatibilityStatus.CONDITIONALLY_COMPATIBLE,
        ConsumerCompatibilityStatus.POTENTIALLY_BREAKING,
    ):
        det_verdict = ResultState.DRIFTED

    return NormalizedEvidence(
        target_name=report.dataset_identifier,
        baseline_name="ConsumerContractsRegistry",
        comparison_mode="consumer_aware",
        deterministic_verdict=det_verdict,
        is_breaking=(report.overall_status == ConsumerCompatibilityStatus.INCOMPATIBLE),
        findings=findings,
        metadata_changes=[],
        consumer_impacts=consumer_summaries,
        user_context=user_context,
    )


def sanitize_token(text: str, max_length: int = 500) -> str:
    """Sanitize strings for secure XML/JSON inclusion, neutralizing delimiter injection."""
    if not text:
        return ""
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

    if evidence.consumer_impacts:
        lines.append("  <consumer_impacts>")
        for ci in evidence.consumer_impacts:
            lines.append(
                f'    <consumer id="{sanitize_token(str(ci.get("consumer_id")))}" status="{sanitize_token(str(ci.get("status")))}">'
            )
            lines.append(f"      <name>{sanitize_token(str(ci.get('consumer_name')))}</name>")
            lines.append(f"      <blocking>{str(ci.get('is_blocking')).lower()}</blocking>")
            lines.append("    </consumer>")
        lines.append("  </consumer_impacts>")

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
