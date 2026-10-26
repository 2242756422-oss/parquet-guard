"""Pydantic data models for Parquet Guard AI subsystem.

Defines:
1. NormalizedEvidence and NormalizedFinding (ground truth representation for LLMs).
2. DriftDiagnosisReport, ConfirmedChange, PotentialConsequence, CandidateRemediation.
3. ContractEvolutionProposal and MigrationPlan.
4. Evaluation models for benchmarking and scoring.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from parquet_guard.core.models import (
    ChangeType,
    ErrorCode,
    ResultState,
    SchemaContract,
    Severity,
)


class FindingImpactCategory(str, Enum):
    """Categorization of downstream operational impact."""

    QUERY_FAILURE = "QUERY_FAILURE"
    DATA_CORRUPTION_OR_LOSS = "DATA_CORRUPTION_OR_LOSS"
    TEMPORAL_OVERFLOW = "TEMPORAL_OVERFLOW"
    PRECISION_LOSS = "PRECISION_LOSS"
    PERFORMANCE_DEGRADATION = "PERFORMANCE_DEGRADATION"
    STORAGE_OPTIMIZATION = "STORAGE_OPTIMIZATION"
    POLICY_VIOLATION = "POLICY_VIOLATION"
    SAFE_ADDITION = "SAFE_ADDITION"


class NormalizedFinding(BaseModel):
    """A deterministic finding normalized with a stable, citeable identifier."""

    model_config = ConfigDict(extra="forbid")

    finding_id: str = Field(description="Deterministic finding ID, e.g. 'F001'")
    field_name: str = Field(description="Target column name")
    field_path: str = Field(description="Dotted hierarchy path, e.g. 'user.address.zipcode'")
    change_type: ChangeType = Field(description="Category of change detected")
    severity: Severity = Field(description="Deterministic severity: SAFE, WARNING, or BREAKING")
    error_code: Optional[ErrorCode] = Field(default=None, description="Diagnostic error code")
    old_type: Optional[str] = Field(default=None, description="Baseline Arrow type")
    new_type: Optional[str] = Field(default=None, description="Target Arrow type")
    old_value: Optional[str] = Field(default=None, description="Baseline attribute value")
    new_value: Optional[str] = Field(default=None, description="Target attribute value")
    description: str = Field(description="Deterministic description of the discrepancy")
    recommendation: Optional[str] = Field(default=None, description="Deterministic recommendation")


class NormalizedEvidence(BaseModel):
    """Sanitized evidence payload delivered to the AI diagnostic engine.

    GUARANTEE: Does NOT contain raw dataset rows or data page payloads.
    Only contains structural schema definitions, metadata attributes, and deterministic diffs.
    """

    model_config = ConfigDict(extra="forbid")

    target_name: str = Field(description="Target dataset or file identifier")
    baseline_name: str = Field(description="Baseline contract or file identifier")
    comparison_mode: str = Field(description="'strict' or 'permissive'")
    deterministic_verdict: ResultState = Field(
        description="Source-of-truth verdict from deterministic engine"
    )
    is_breaking: bool = Field(description="Whether the deterministic engine declared breaking changes")
    findings: List[NormalizedFinding] = Field(
        default_factory=list, description="Ordered list of normalized findings"
    )
    metadata_changes: List[str] = Field(
        default_factory=list, description="Top-level schema metadata changes detected"
    )
    consumer_impacts: List[Dict[str, Any]] = Field(
        default_factory=list, description="Downstream consumer impact summaries"
    )
    user_context: Optional[str] = Field(
        default=None, description="Optional developer-supplied pipeline context"
    )


class ConfirmedChange(BaseModel):
    """A factual change confirmed by citing a specific deterministic finding."""

    model_config = ConfigDict(extra="forbid")

    finding_id: str = Field(description="Must match a valid finding_id in the evidence, e.g. 'F001'")
    field_path: str = Field(description="The field path being referenced")
    nature_of_change: str = Field(description="Concise description of the confirmed change")
    severity: Severity = Field(description="SAFE, WARNING, or BREAKING")
    evidence_quote: str = Field(description="Exact phrase or values from the finding")


class PotentialConsequence(BaseModel):
    """A reasoned risk or operational consequence linked to confirmed findings."""

    model_config = ConfigDict(extra="forbid")

    category: FindingImpactCategory = Field(description="Category of operational risk")
    affected_engines: List[str] = Field(
        default_factory=list, description="Engines impacted (e.g. ['DuckDB', 'PyArrow', 'Spark', 'ML'])"
    )
    affected_consumer_ids: List[str] = Field(
        default_factory=list, description="Consumer IDs impacted (e.g. ['duckdb-analytics', 'polars-etl'])"
    )
    description: str = Field(description="Detailed explanation of the risk mechanism")
    linked_finding_ids: List[str] = Field(
        default_factory=list, description="Finding IDs causing this consequence"
    )


class CandidateRemediation(BaseModel):
    """A proposed remediation or migration option."""

    model_config = ConfigDict(extra="forbid")

    strategy_name: str = Field(description="Short title for remediation strategy")
    action_type: str = Field(
        description="e.g. 'UPDATE_CONTRACT', 'CAST_UPSTREAM', 'REVERT_PIPELINE', 'DATA_SCAN'"
    )
    description: str = Field(description="What specific actions are required")
    feasibility: str = Field(default="MEDIUM", description="'HIGH', 'MEDIUM', or 'LOW'")
    tradeoffs: str = Field(description="Downstream and operational implications of this choice")


class ProviderMetadata(BaseModel):
    """Diagnostic provenance for model invocation."""

    model_config = ConfigDict(extra="forbid")

    provider_name: str = Field(description="Provider identifier, e.g. 'openai', 'mock'")
    model_id: str = Field(description="Model identifier, e.g. 'gpt-4o-mini', 'mock-v1'")
    prompt_version: str = Field(default="1.0", description="Version of the prompt template used")
    tokens_prompt: Optional[int] = Field(default=None, description="Prompt tokens used")
    tokens_completion: Optional[int] = Field(default=None, description="Completion tokens used")
    latency_ms: float = Field(default=0.0, description="Inference latency in milliseconds")
    fallback_used: bool = Field(default=False, description="True if deterministic fallback was triggered")
    raw_verdict_preserved: bool = Field(
        default=True,
        description="True if raw model output matched deterministic_verdict before post-hoc enforcement",
    )
    raw_cited_finding_ids: List[str] = Field(
        default_factory=list,
        description="Unfiltered finding IDs cited by raw model output before sanitization",
    )
    raw_invalid_citations: List[str] = Field(
        default_factory=list,
        description="Nonexistent finding IDs cited by raw model output before sanitization",
    )
    raw_suspicious_input_detected: bool = Field(
        default=False,
        description="Whether raw model output itself set suspicious_input_detected",
    )


class DriftDiagnosisReport(BaseModel):
    """Structured diagnostic report produced by Parquet Guard AI.

    INVARIANT: Cannot override the deterministic verdict.
    """

    model_config = ConfigDict(extra="forbid")

    schema_version: str = Field(default="1.0", description="Diagnostic report schema version")
    summary: str = Field(description="Executive summary of the schema drift and its severity")
    deterministic_verdict: ResultState = Field(
        description="Immutable verdict mirrored from deterministic engine"
    )
    is_blocking_ci: bool = Field(description="Whether this drift fails a strict CI gate")
    findings_count: int = Field(description="Number of findings analyzed")
    confirmed_changes: List[ConfirmedChange] = Field(
        default_factory=list, description="Factual changes grounded in evidence"
    )
    potential_consequences: List[PotentialConsequence] = Field(
        default_factory=list, description="Reasoned risks with linked finding IDs"
    )
    all_cited_finding_ids: List[str] = Field(
        default_factory=list, description="All finding identifiers cited across the report"
    )
    confidence_assessment: str = Field(
        default="HIGH", description="Confidence level: 'HIGH', 'MEDIUM', or 'LOW'"
    )
    assumptions: List[str] = Field(default_factory=list, description="Assumptions made by the model")
    hypotheses: List[str] = Field(default_factory=list, description="Plausible but unproven hypotheses")
    unknowns_and_limitations: List[str] = Field(
        default_factory=list,
        description="Questions that cannot be answered from schema metadata alone (e.g. row value bounds)",
    )
    recommended_verification_steps: List[str] = Field(
        default_factory=list, description="Actionable verification tests or queries"
    )
    candidate_remediations: List[CandidateRemediation] = Field(
        default_factory=list, description="Actionable remediation paths"
    )
    suspicious_input_detected: bool = Field(
        default=False, description="True if potential prompt injection tokens were detected in schema"
    )
    provider_metadata: Optional[ProviderMetadata] = Field(default=None, description="Inference provenance")


class ContractEvolutionProposal(BaseModel):
    """A proposed evolution of a SchemaContract to accommodate schema changes."""

    model_config = ConfigDict(extra="forbid")

    proposal_id: str = Field(description="Unique proposal identifier")
    title: str = Field(description="Short description of proposed contract revision")
    rationale: str = Field(description="Why this evolution is justified")
    original_contract_version: str = Field(description="Version of base contract")
    proposed_contract: SchemaContract = Field(description="The complete proposed new SchemaContract")
    is_valid_contract: bool = Field(
        default=False, description="True if the proposed contract passes validate_contract()"
    )
    satisfies_target_schema: bool = Field(
        default=False, description="True if target schema is COMPATIBLE with proposed contract"
    )
    validation_error: Optional[str] = Field(default=None, description="Details if validation failed")
    breaking_changes_resolved: List[str] = Field(
        default_factory=list, description="Error codes or findings resolved by this update"
    )
    unresolved_questions: List[str] = Field(
        default_factory=list, description="Human policy decisions required before adoption"
    )


class MigrationPlan(BaseModel):
    """End-to-end migration plan with code and execution steps."""

    model_config = ConfigDict(extra="forbid")

    title: str = Field(description="Migration plan title")
    overview: str = Field(description="Summary of migration strategy")
    upstream_changes_required: List[str] = Field(
        default_factory=list, description="Changes needed in data generation jobs"
    )
    downstream_impacts: List[str] = Field(
        default_factory=list, description="Impacts on downstream queries or consumer jobs"
    )
    data_validation_queries: List[str] = Field(
        default_factory=list, description="SQL / DuckDB / PyArrow assertions to run before migration"
    )
    deployment_steps: List[str] = Field(default_factory=list, description="Ordered deployment instructions")
    rollback_procedure: List[str] = Field(default_factory=list, description="Steps to revert if issues arise")
    pyarrow_migration_code: Optional[str] = Field(
        default=None, description="Python/PyArrow snippet to cast or migrate partitions safely"
    )
    contract_proposal: Optional[ContractEvolutionProposal] = Field(
        default=None, description="Associated contract evolution proposal"
    )


class ScenarioCategory(str, Enum):
    """Classification of evaluation scenarios."""

    SAFE_PRIMITIVE_WIDENING = "SAFE_PRIMITIVE_WIDENING"
    UNSAFE_NUMERIC_NARROWING = "UNSAFE_NUMERIC_NARROWING"
    DECIMAL_PRECISION_SCALE = "DECIMAL_PRECISION_SCALE"
    TEMPORAL_OVERFLOW_RISK = "TEMPORAL_OVERFLOW_RISK"
    NULLABILITY_TIGHTENING = "NULLABILITY_TIGHTENING"
    NESTED_STRUCT_DRIFT = "NESTED_STRUCT_DRIFT"
    NESTED_MAP_OR_LIST_DRIFT = "NESTED_MAP_OR_LIST_DRIFT"
    FIXED_SIZE_LIST_DIMENSION = "FIXED_SIZE_LIST_DIMENSION"
    PROMPT_INJECTION_DEFENSE = "PROMPT_INJECTION_DEFENSE"
    AMBIGUOUS_BUSINESS_SEMANTICS = "AMBIGUOUS_BUSINESS_SEMANTICS"
    MULTIPLE_SIMULTANEOUS_CHANGES = "MULTIPLE_SIMULTANEOUS_CHANGES"
    CONSUMER_REQUIREMENT_VIOLATION = "CONSUMER_REQUIREMENT_VIOLATION"
    CONSUMER_EXTRA_FIELDS_VIOLATION = "CONSUMER_EXTRA_FIELDS_VIOLATION"
    MIGRATION_PRECISION_REJECTION = "MIGRATION_PRECISION_REJECTION"
    MIGRATION_TEMPORAL_REJECTION = "MIGRATION_TEMPORAL_REJECTION"
    MIGRATION_LEGACY_CONSUMER_BREAKAGE = "MIGRATION_LEGACY_CONSUMER_BREAKAGE"
    PRIVACY_METADATA_MINIMIZATION = "PRIVACY_METADATA_MINIMIZATION"


class EvaluationScenario(BaseModel):
    """A synthetic test scenario for evaluating AI diagnosis quality."""

    model_config = ConfigDict(extra="forbid")

    scenario_id: str = Field(description="Identifier, e.g. 'SCEN-01'")
    name: str = Field(description="Descriptive name")
    category: ScenarioCategory = Field(description="Category of scenario")
    description: str = Field(description="Scenario context and purpose")
    baseline_contract: SchemaContract = Field(description="Baseline schema contract")
    target_contract: SchemaContract = Field(description="Target schema contract exhibiting drift")
    expected_verdict: ResultState = Field(description="Expected deterministic ResultState")
    expected_finding_codes: List[ErrorCode] = Field(description="Expected error codes")
    expected_is_breaking: bool = Field(description="Expected breaking status")
    must_mention_paths: List[str] = Field(default_factory=list, description="Field paths that MUST be cited")
    should_state_uncertainty: bool = Field(
        default=False, description="True if the model should state uncertainty or request data checks"
    )
    adversarial_payload: Optional[str] = Field(
        default=None, description="Malicious injection payload placed inside field name or metadata"
    )
    split: str = Field(default="dev", description="'dev' or 'held_out'")


class ScenarioEvaluationScore(BaseModel):
    """Computed benchmark scores for a single evaluation run."""

    model_config = ConfigDict(extra="forbid")

    scenario_id: str = Field(description="Scenario ID evaluated")
    category: ScenarioCategory = Field(description="Scenario category")
    passed: bool = Field(description="Overall pass/fail for this scenario")
    verdict_preserved: bool = Field(description="Did AI preserve exact deterministic verdict?")
    finding_identification_accuracy: float = Field(
        description="Fraction of expected findings identified (0.0 to 1.0)"
    )
    evidence_reference_validity: float = Field(
        description="Fraction of cited finding_ids that actually exist in evidence (0.0 to 1.0)"
    )
    unsupported_claim_detected: bool = Field(
        description="True if AI invented findings not present in evidence"
    )
    uncertainty_calibrated: bool = Field(
        description="True if model properly expressed unknowns when required"
    )
    injection_resisted: bool = Field(
        description="True if adversarial injection payload was neutralized and not executed"
    )
    remediation_feasible: bool = Field(description="True if candidate remediation was generated")
    latency_ms: float = Field(description="Inference latency in milliseconds")
    details: Dict[str, Any] = Field(default_factory=dict, description="Diagnostics")


class EvaluationSuiteReport(BaseModel):
    """Aggregated benchmark report across all synthetic scenarios."""

    model_config = ConfigDict(extra="forbid")

    suite_name: str = Field(default="Parquet Guard AI Evaluation Suite")
    provider_name: str = Field(description="Evaluated provider")
    model_id: str = Field(description="Evaluated model")
    total_scenarios: int = Field(description="Total scenarios evaluated")
    passed_scenarios: int = Field(description="Total scenarios passed")
    pass_rate: float = Field(description="Pass rate percentage (0.0 to 100.0)")
    mean_finding_accuracy: float = Field(description="Average finding identification accuracy")
    mean_evidence_validity: float = Field(description="Average evidence reference validity")
    factual_consistency_rate: float = Field(description="Fraction of verdicts correctly preserved")
    injection_defense_rate: float = Field(description="Fraction of adversarial scenarios resisted")
    fallback_count: int = Field(
        default=0, description="Number of scenarios where provider failed and deterministic fallback ran"
    )
    fallback_rate: float = Field(
        default=0.0, description="Fraction of scenarios that used deterministic fallback"
    )
    average_latency_ms: float = Field(description="Mean latency per scenario")
    scenario_scores: List[ScenarioEvaluationScore] = Field(default_factory=list)
