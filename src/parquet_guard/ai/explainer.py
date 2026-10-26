"""Evidence-grounded Schema Drift Diagnostic Engine.

Executes:
1. Evidence normalization and injection detection.
2. AI model invocation with structured JSON output enforcement.
3. Rigid validation against deterministic ground-truth findings.
4. Guaranteed deterministic fallback if provider is unavailable or fails.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Optional

from parquet_guard.ai.models import (
    CandidateRemediation,
    ConfirmedChange,
    DriftDiagnosisReport,
    FindingImpactCategory,
    NormalizedEvidence,
    PotentialConsequence,
    ProviderMetadata,
)
from parquet_guard.ai.normalizer import (
    format_evidence_for_prompt,
    get_valid_finding_ids,
    normalize_audit_result,
    normalize_diff_report,
)
from parquet_guard.ai.prompts import (
    DIAGNOSIS_SYSTEM_PROMPT,
    PROMPT_VERSION,
    detect_suspicious_injection,
)
from parquet_guard.ai.providers import BaseAIProvider, ProviderError, get_provider
from parquet_guard.core.models import (
    DiffReport,
    ErrorCode,
    PartitionAuditResult,
    ResultState,
    Severity,
)

logger = logging.getLogger(__name__)


def generate_deterministic_diagnosis(
    evidence: NormalizedEvidence,
    reason_for_fallback: str = "Deterministic rules applied.",
) -> DriftDiagnosisReport:
    """Generate a 100% deterministic, rule-based diagnostic report without calling an LLM."""
    confirmed: list[ConfirmedChange] = []
    consequences: list[PotentialConsequence] = []
    remediations: list[CandidateRemediation] = []
    all_cited: list[str] = []

    for f in evidence.findings:
        all_cited.append(f.finding_id)
        confirmed.append(
            ConfirmedChange(
                finding_id=f.finding_id,
                field_path=f.field_path,
                nature_of_change=f.description,
                severity=f.severity,
                evidence_quote=f"{f.old_type or 'None'} -> {f.new_type or 'None'}",
            )
        )

        # Map error codes to concrete operational consequences
        if f.error_code == ErrorCode.TEMPORAL_OVERFLOW_RISK:
            consequences.append(
                PotentialConsequence(
                    category=FindingImpactCategory.TEMPORAL_OVERFLOW,
                    affected_engines=["PyArrow", "DuckDB", "Spark"],
                    description=(
                        f"Field '{f.field_path}' precision widened to finer temporal unit. "
                        "Signed 64-bit nanosecond representation is physically limited to 1677–2262 AD. "
                        "Values outside this boundary will cause ArrowInvalid casting exceptions."
                    ),
                    linked_finding_ids=[f.finding_id],
                )
            )
            remediations.append(
                CandidateRemediation(
                    strategy_name="Verify Temporal Range & Add Cast Guard",
                    action_type="DATA_SCAN",
                    description=(
                        f"Verify upstream values for '{f.field_path}' fall within 1677-2262 AD. "
                        "In permissive mode this is a warning; in strict mode keep units identical."
                    ),
                    feasibility="HIGH",
                    tradeoffs="Requires one-off validation of date boundaries.",
                )
            )
        elif f.error_code == ErrorCode.DECIMAL_SCALE_MISMATCH:
            consequences.append(
                PotentialConsequence(
                    category=FindingImpactCategory.PRECISION_LOSS,
                    affected_engines=["DuckDB", "Spark", "Polars"],
                    description=(
                        f"Field '{f.field_path}' decimal scale changed. In Parquet physical storage, "
                        "scale shifts alter integer value interpretation, causing massive numeric distortion."
                    ),
                    linked_finding_ids=[f.finding_id],
                )
            )
            remediations.append(
                CandidateRemediation(
                    strategy_name="Revert Decimal Scale",
                    action_type="REVERT_PIPELINE",
                    description="Keep decimal scale identical between baseline and target partitions.",
                    feasibility="HIGH",
                    tradeoffs="Preserves numeric fidelity without data corruption.",
                )
            )
        elif f.severity == Severity.BREAKING:
            consequences.append(
                PotentialConsequence(
                    category=FindingImpactCategory.QUERY_FAILURE,
                    affected_engines=["DuckDB", "Spark", "PyArrow"],
                    description=(
                        f"Field '{f.field_path}' introduced a breaking schema regression: {f.description}. "
                        "Downstream readers expecting baseline layout will fail at runtime."
                    ),
                    linked_finding_ids=[f.finding_id],
                )
            )
            remediations.append(
                CandidateRemediation(
                    strategy_name="Update Contract or Revert Incompatible Column",
                    action_type="UPDATE_CONTRACT",
                    description=(
                        f"If the change to '{f.field_path}' is intentional, update golden contract; "
                        "otherwise align upstream generator with baseline."
                    ),
                    feasibility="HIGH",
                    tradeoffs="Updating contract allows evolution but requires reader verification.",
                )
            )
        else:
            consequences.append(
                PotentialConsequence(
                    category=FindingImpactCategory.SAFE_ADDITION,
                    affected_engines=["All"],
                    description=f"Field '{f.field_path}' change is safe and backward-compatible.",
                    linked_finding_ids=[f.finding_id],
                )
            )

    if not evidence.findings:
        summary_text = (
            f"Dataset '{evidence.target_name}' is fully compatible with '{evidence.baseline_name}'."
        )
    elif evidence.is_breaking:
        summary_text = (
            f"Breaking schema drift detected in '{evidence.target_name}': "
            f"{len(evidence.findings)} finding(s) violate contract under '{evidence.comparison_mode}' mode."
        )
    else:
        summary_text = (
            f"Permissive schema evolution detected in '{evidence.target_name}': "
            f"{len(evidence.findings)} safe or warning finding(s)."
        )

    return DriftDiagnosisReport(
        schema_version="1.0",
        summary=summary_text,
        deterministic_verdict=evidence.deterministic_verdict,
        is_blocking_ci=evidence.is_breaking,
        findings_count=len(evidence.findings),
        confirmed_changes=confirmed,
        potential_consequences=consequences,
        all_cited_finding_ids=all_cited,
        confidence_assessment="HIGH",
        assumptions=["Baseline contract defines true expected schema layout."],
        hypotheses=["Schema divergence is caused by modified serialization in upstream producer."],
        unknowns_and_limitations=[
            "Zero data rows were read. Value bounds, null percentages, and cardinalities require row-level scan."
        ],
        recommended_verification_steps=[
            "Run automated schema check in CI: parquet-guard check <file> -c <contract>",
            "Execute sample reader queries in downstream query engines.",
        ],
        candidate_remediations=remediations
        or [
            CandidateRemediation(
                strategy_name="No Action Required",
                action_type="UPDATE_CONTRACT",
                description="Schemas are compatible.",
                feasibility="HIGH",
                tradeoffs="None.",
            )
        ],
        suspicious_input_detected=detect_suspicious_injection(evidence),
        provider_metadata=ProviderMetadata(
            provider_name="deterministic_rule_engine",
            model_id="rule-engine-v1",
            prompt_version=PROMPT_VERSION,
            tokens_prompt=0,
            tokens_completion=0,
            latency_ms=0.5,
            fallback_used=True,
        ),
    )


def diagnose_schema_drift(
    report: DiffReport | PartitionAuditResult,
    provider: Optional[BaseAIProvider] = None,
    user_context: Optional[str] = None,
    allow_fallback: bool = True,
) -> DriftDiagnosisReport:
    """Diagnose schema drift using evidence-grounded AI with guaranteed deterministic fallback."""
    # 1. Normalize input report
    if isinstance(report, DiffReport):
        evidence = normalize_diff_report(report, user_context=user_context)
    else:
        evidence = normalize_audit_result(report, user_context=user_context)

    # 2. Check for adversarial injection signatures
    is_suspicious = detect_suspicious_injection(evidence)

    # 3. If no provider supplied, resolve default provider
    active_provider = provider or get_provider(provider_type="auto")

    # 4. Invoke provider
    prompt = format_evidence_for_prompt(evidence)

    try:
        response = active_provider.complete(
            prompt=prompt,
            system_prompt=DIAGNOSIS_SYSTEM_PROMPT,
            response_format_json=True,
        )

        # Parse JSON from model
        cleaned_content = response.content.strip()
        # Handle optional markdown code block wrapping
        if cleaned_content.startswith("```"):
            cleaned_content = re.sub(r"^```(?:json)?\n?", "", cleaned_content)
            cleaned_content = re.sub(r"\n?```$", "", cleaned_content)

        raw_dict = json.loads(cleaned_content)

        # Construct and validate Pydantic model
        diagnosis = DriftDiagnosisReport.model_validate(raw_dict)

        # Capture raw pre-sanitization model audit telemetry for evaluator fidelity grading
        raw_verdict_ok = (
            diagnosis.deterministic_verdict == evidence.deterministic_verdict
            and diagnosis.is_blocking_ci == evidence.is_breaking
        )
        valid_ids = get_valid_finding_ids(evidence)
        raw_cited = list(diagnosis.all_cited_finding_ids)
        for c in diagnosis.confirmed_changes:
            if c.finding_id not in raw_cited:
                raw_cited.append(c.finding_id)
        for cq in diagnosis.potential_consequences:
            for fid in cq.linked_finding_ids:
                if fid not in raw_cited:
                    raw_cited.append(fid)
        raw_invalid = [fid for fid in raw_cited if fid not in valid_ids]
        raw_suspicious = diagnosis.suspicious_input_detected

        # INVARIANT 1: Force deterministic verdict preservation (LLM cannot override CI verdict!)
        diagnosis.deterministic_verdict = evidence.deterministic_verdict
        diagnosis.is_blocking_ci = evidence.is_breaking

        # INVARIANT 2: Validate cited finding IDs against real evidence
        sanitized_cited = [fid for fid in diagnosis.all_cited_finding_ids if fid in valid_ids]
        diagnosis.all_cited_finding_ids = sanitized_cited or list(valid_ids)

        if is_suspicious:
            diagnosis.suspicious_input_detected = True

        diagnosis.provider_metadata = ProviderMetadata(
            provider_name=response.provider_name,
            model_id=response.model_id,
            prompt_version=PROMPT_VERSION,
            tokens_prompt=response.tokens_prompt,
            tokens_completion=response.tokens_completion,
            latency_ms=response.latency_ms,
            fallback_used=False,
            raw_verdict_preserved=raw_verdict_ok,
            raw_cited_finding_ids=raw_cited,
            raw_invalid_citations=raw_invalid,
            raw_suspicious_input_detected=raw_suspicious,
        )
        return diagnosis

    except (ProviderError, json.JSONDecodeError, Exception) as err:
        logger.warning("AI provider failed (%s); evaluating fallback...", err)
        if allow_fallback:
            fallback = generate_deterministic_diagnosis(
                evidence, reason_for_fallback=f"Fallback triggered due to provider failure: {err}"
            )
            return fallback
        raise


def format_diagnosis_terminal(diagnosis: DriftDiagnosisReport) -> str:
    """Format diagnosis report for terminal display with Rich styling."""
    import io

    from rich.box import ROUNDED
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table

    buf = io.StringIO()
    console = Console(file=buf, force_terminal=True, width=95)

    status_color = (
        "red" if diagnosis.is_blocking_ci else ("yellow" if diagnosis.findings_count > 0 else "green")
    )
    verdict_badge = f"[{status_color} bold]{diagnosis.deterministic_verdict.value}[/{status_color} bold]"

    console.print()
    console.print(
        Panel(
            f"[bold cyan]PARQUET GUARD AI — SCHEMA DRIFT COPILOT[/bold cyan]\n"
            f"Deterministic Verdict : {verdict_badge} (Blocking CI: {diagnosis.is_blocking_ci})\n"
            f"Confidence            : [bold]{diagnosis.confidence_assessment}[/bold] | "
            f"Suspicious Input: [bold]{diagnosis.suspicious_input_detected}[/bold]\n"
            f"Summary               : {diagnosis.summary}",
            title="🛡️ AI Schema Evolution Diagnosis",
            box=ROUNDED,
            border_style=status_color,
        )
    )

    if diagnosis.confirmed_changes:
        tbl = Table(title="Confirmed Schema Changes (Grounded Findings)", box=ROUNDED)
        tbl.add_column("ID", style="bold cyan", width=6)
        tbl.add_column("Field Path", style="magenta", width=24)
        tbl.add_column("Severity", width=10)
        tbl.add_column("Nature of Change", style="white")

        for c in diagnosis.confirmed_changes:
            sev_color = (
                "red"
                if c.severity == Severity.BREAKING
                else ("yellow" if c.severity == Severity.WARNING else "green")
            )
            tbl.add_row(
                c.finding_id,
                c.field_path,
                f"[{sev_color}]{c.severity.value}[/{sev_color}]",
                c.nature_of_change,
            )
        console.print(tbl)

    if diagnosis.potential_consequences:
        console.print("\n[bold yellow]⚡ Potential Downstream Consequences:[/bold yellow]")
        for cq in diagnosis.potential_consequences:
            engines = ", ".join(cq.affected_engines) if cq.affected_engines else "All Readers"
            console.print(f"  • [[bold cyan]{cq.category.value}[/bold cyan]] ({engines}): {cq.description}")

    if diagnosis.candidate_remediations:
        console.print("\n[bold green]🛠️ Candidate Remediation Strategies:[/bold green]")
        for r in diagnosis.candidate_remediations:
            console.print(f"  • [bold]{r.strategy_name}[/bold] ({r.action_type}): {r.description}")
            console.print(f"    [dim]Tradeoffs: {r.tradeoffs}[/dim]")

    if diagnosis.unknowns_and_limitations:
        console.print("\n[bold dim]ℹ️ Unknowns & Value-Level Limitations (Metadata-Only Boundary):[/bold dim]")
        for u in diagnosis.unknowns_and_limitations:
            console.print(f"  • [dim]{u}[/dim]")

    console.print()
    return buf.getvalue()


def format_diagnosis_markdown(diagnosis: DriftDiagnosisReport) -> str:
    """Format diagnosis report as Markdown suitable for PR comments or CI artifacts."""
    badge = "🚨 **BREAKING / INCOMPATIBLE**" if diagnosis.is_blocking_ci else "⚠️ **DRIFTED (WARNINGS)**"
    if diagnosis.deterministic_verdict == ResultState.VALID:
        badge = "✅ **COMPATIBLE / VALID**"

    lines: list[str] = [
        "## 🛡️ Parquet Guard AI — Schema Drift Diagnosis",
        "",
        f"> **Deterministic Verdict**: {badge}  ",
        f"> **Blocking CI**: `{diagnosis.is_blocking_ci}` | **Confidence**: `{diagnosis.confidence_assessment}`  ",
        f"> **Findings Analyzed**: `{diagnosis.findings_count}` | **Suspicious Input**: `{diagnosis.suspicious_input_detected}`  ",
        "",
        "### Summary",
        f"{diagnosis.summary}",
        "",
    ]

    if diagnosis.confirmed_changes:
        lines.extend(
            [
                "### Confirmed Changes (Citing Deterministic Findings)",
                "| Finding ID | Field Path | Severity | Nature of Change | Evidence |",
                "| :---: | :--- | :---: | :--- | :--- |",
            ]
        )
        for c in diagnosis.confirmed_changes:
            lines.append(
                f"| `{c.finding_id}` | `{c.field_path}` | `{c.severity.value}` | {c.nature_of_change} | `{c.evidence_quote}` |"
            )
        lines.append("")

    if diagnosis.potential_consequences:
        lines.extend(["### Potential Downstream Consequences", ""])
        for con in diagnosis.potential_consequences:
            engines = ", ".join(con.affected_engines)
            lines.append(f"- **`{con.category.value}`** ({engines}): {con.description}")
        lines.append("")

    if diagnosis.candidate_remediations:
        lines.extend(["### Candidate Remediation Options", ""])
        for r in diagnosis.candidate_remediations:
            lines.append(f"1. **{r.strategy_name}** (`{r.action_type}`, Feasibility: `{r.feasibility}`)")
            lines.append(f"   - {r.description}")
            lines.append(f"   - *Tradeoffs*: {r.tradeoffs}")
        lines.append("")

    if diagnosis.unknowns_and_limitations:
        lines.extend(
            [
                "### Known Limitations & Unknowns (Zero-Data-Scan Boundary)",
                "",
                "> The deterministic engine inspects Parquet footers exclusively. The following questions cannot be resolved without scanning raw row payloads:",
                "",
            ]
        )
        for u in diagnosis.unknowns_and_limitations:
            lines.append(f"- {u}")
        lines.append("")

    return "\n".join(lines)


def format_diagnosis_json(diagnosis: DriftDiagnosisReport) -> str:
    """Format diagnosis report as indented JSON."""
    return str(diagnosis.model_dump_json(indent=2))
