"""Parquet Guard AI: Evidence-Grounded Schema Evolution Copilot.

Optional subsystem for schema drift explanation, contract evolution planning,
agent tool calling, and reproducible LLM evaluation.
"""

from __future__ import annotations

from parquet_guard.ai.agent_tools import (
    AGENT_TOOL_DEFINITIONS,
    call_agent_tool,
    tool_audit_dataset,
    tool_compare_schemas,
    tool_diagnose_drift,
    tool_inspect_schema,
    tool_propose_migration,
)
from parquet_guard.ai.evaluator import (
    build_synthetic_scenarios,
    evaluate_single_scenario,
    format_evaluation_markdown,
    run_evaluation_suite,
)
from parquet_guard.ai.explainer import (
    diagnose_schema_drift,
    format_diagnosis_json,
    format_diagnosis_markdown,
    format_diagnosis_terminal,
    generate_deterministic_diagnosis,
)
from parquet_guard.ai.models import (
    CandidateRemediation,
    ConfirmedChange,
    ContractEvolutionProposal,
    DriftDiagnosisReport,
    EvaluationScenario,
    EvaluationSuiteReport,
    FindingImpactCategory,
    MigrationPlan,
    NormalizedEvidence,
    NormalizedFinding,
    PotentialConsequence,
    ProviderMetadata,
    ScenarioCategory,
    ScenarioEvaluationScore,
)
from parquet_guard.ai.planner import (
    format_migration_json,
    format_migration_markdown,
    generate_migration_plan,
    propose_contract_evolution,
)
from parquet_guard.ai.providers import (
    BaseAIProvider,
    MockAIProvider,
    OpenAICompatibleProvider,
    ProviderAuthError,
    ProviderError,
    ProviderRateLimitError,
    ProviderResponse,
    ProviderTimeoutError,
    get_provider,
)

__all__ = [
    "diagnose_schema_drift",
    "generate_deterministic_diagnosis",
    "format_diagnosis_terminal",
    "format_diagnosis_markdown",
    "format_diagnosis_json",
    "propose_contract_evolution",
    "generate_migration_plan",
    "format_migration_markdown",
    "format_migration_json",
    "run_evaluation_suite",
    "evaluate_single_scenario",
    "build_synthetic_scenarios",
    "format_evaluation_markdown",
    "BaseAIProvider",
    "MockAIProvider",
    "OpenAICompatibleProvider",
    "get_provider",
    "ProviderError",
    "ProviderTimeoutError",
    "ProviderRateLimitError",
    "ProviderAuthError",
    "ProviderResponse",
    "call_agent_tool",
    "AGENT_TOOL_DEFINITIONS",
    "tool_inspect_schema",
    "tool_compare_schemas",
    "tool_audit_dataset",
    "tool_diagnose_drift",
    "tool_propose_migration",
    "DriftDiagnosisReport",
    "ConfirmedChange",
    "PotentialConsequence",
    "CandidateRemediation",
    "ProviderMetadata",
    "ContractEvolutionProposal",
    "MigrationPlan",
    "NormalizedEvidence",
    "NormalizedFinding",
    "EvaluationScenario",
    "ScenarioEvaluationScore",
    "EvaluationSuiteReport",
    "ScenarioCategory",
    "FindingImpactCategory",
]
