"""Agent Interoperability & Tool-Use Interfaces for Parquet Guard.

Exposes deterministic and AI-assisted capabilities as structured tools
for AI coding agents, OpenAI Function Calling, Anthropic Tools, and MCP servers.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from parquet_guard.ai.explainer import diagnose_schema_drift
from parquet_guard.ai.planner import generate_migration_plan
from parquet_guard.ai.providers import get_provider
from parquet_guard.core.auditor import audit_dataset
from parquet_guard.core.compatibility import compare_schemas
from parquet_guard.core.inspector import inspect_schema, load_contract


def tool_inspect_schema(file_path: str) -> Dict[str, Any]:
    """Inspect Parquet schema and metadata without reading row data."""
    contract = inspect_schema(file_path)
    return {
        "status": "success",
        "file_path": file_path,
        "fields_count": len(contract.fields),
        "fields": [
            {
                "name": f.name,
                "type": f.type,
                "nullable": f.nullable,
                "children_count": len(f.children),
            }
            for f in contract.fields
        ],
        "metadata": contract.metadata,
    }


def tool_compare_schemas(
    target_path: str,
    baseline_path: str,
    mode: str = "permissive",
) -> Dict[str, Any]:
    """Compare schema between two Parquet files or between a Parquet file and a contract."""
    target_contract = inspect_schema(target_path)
    if baseline_path.endswith(".json"):
        baseline_contract = load_contract(baseline_path)
    else:
        baseline_contract = inspect_schema(baseline_path)

    report = compare_schemas(target_contract, baseline_contract, mode=mode)
    return {
        "status": "success",
        "verdict": report.result_state.value,
        "is_compatible": report.is_compatible,
        "has_breaking_changes": report.has_breaking_changes,
        "has_warnings": report.has_warnings,
        "diff_count": len(report.diffs),
        "diffs": [
            {
                "field_name": d.field_name,
                "field_path": d.field_path,
                "severity": d.severity.value,
                "change_type": d.change_type.value,
                "error_code": d.error_code.value if d.error_code else None,
                "old_value": d.old_value,
                "new_value": d.new_value,
                "description": d.description,
            }
            for d in report.diffs
        ],
    }


def tool_audit_dataset(
    target_path: str,
    mode: str = "strict",
    contract_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Audit schema consistency across all Parquet files in a partitioned directory."""
    baseline_contract = load_contract(contract_path) if contract_path else None
    result = audit_dataset(target_path, mode=mode, baseline_contract=baseline_contract)
    return {
        "status": "success",
        "verdict": result.result_state.value,
        "is_consistent": result.is_consistent,
        "total_files_scanned": result.total_files_scanned,
        "total_rows": result.total_rows,
        "distinct_schemas_count": result.distinct_schemas_count,
        "warnings_count": result.warnings_count,
        "breaking_count": result.breaking_count,
        "divergence_details": result.divergence_details,
    }


def tool_diagnose_drift(
    target_path: str,
    baseline_path: str,
    provider: str = "auto",
) -> Dict[str, Any]:
    """Perform evidence-grounded AI diagnosis on schema drift."""
    target_contract = inspect_schema(target_path)
    if baseline_path.endswith(".json"):
        baseline_contract = load_contract(baseline_path)
    else:
        baseline_contract = inspect_schema(baseline_path)

    diff_report = compare_schemas(target_contract, baseline_contract, mode="permissive")
    ai_provider = get_provider(provider)
    diagnosis = diagnose_schema_drift(diff_report, provider=ai_provider)

    return {
        "status": "success",
        "verdict": diagnosis.deterministic_verdict.value,
        "is_blocking_ci": diagnosis.is_blocking_ci,
        "summary": diagnosis.summary,
        "confirmed_changes": [c.model_dump() for c in diagnosis.confirmed_changes],
        "potential_consequences": [c.model_dump() for c in diagnosis.potential_consequences],
        "candidate_remediations": [r.model_dump() for r in diagnosis.candidate_remediations],
        "unknowns_and_limitations": diagnosis.unknowns_and_limitations,
    }


def tool_propose_migration(
    target_path: str,
    baseline_path: str,
) -> Dict[str, Any]:
    """Generate contract evolution proposal and executable PyArrow migration code."""
    target_contract = inspect_schema(target_path)
    if baseline_path.endswith(".json"):
        baseline_contract = load_contract(baseline_path)
    else:
        baseline_contract = inspect_schema(baseline_path)

    plan = generate_migration_plan(baseline_contract, target_contract)
    return {
        "status": "success",
        "title": plan.title,
        "overview": plan.overview,
        "upstream_changes": plan.upstream_changes_required,
        "downstream_impacts": plan.downstream_impacts,
        "deployment_steps": plan.deployment_steps,
        "rollback_procedure": plan.rollback_procedure,
        "pyarrow_code": plan.pyarrow_migration_code,
        "proposed_contract": (
            plan.contract_proposal.proposed_contract.model_dump() if plan.contract_proposal else None
        ),
        "proposal_satisfies_target": (
            plan.contract_proposal.satisfies_target_schema if plan.contract_proposal else False
        ),
    }


AGENT_TOOL_DEFINITIONS: List[Dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "parquet_guard_inspect_schema",
            "description": "Inspect Parquet metadata and schema structure without reading row data.",
            "parameters": {
                "type": "object",
                "properties": {"file_path": {"type": "string", "description": "Path to Parquet file"}},
                "required": ["file_path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "parquet_guard_compare_schemas",
            "description": "Compare schema compatibility between two files or file vs contract.",
            "parameters": {
                "type": "object",
                "properties": {
                    "target_path": {"type": "string", "description": "Target Parquet file"},
                    "baseline_path": {
                        "type": "string",
                        "description": "Baseline Parquet file or SchemaContract JSON",
                    },
                    "mode": {
                        "type": "string",
                        "enum": ["permissive", "strict"],
                        "default": "permissive",
                    },
                },
                "required": ["target_path", "baseline_path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "parquet_guard_audit_dataset",
            "description": "Audit multi-partition dataset consistency across files.",
            "parameters": {
                "type": "object",
                "properties": {
                    "target_path": {"type": "string", "description": "Partitioned directory or glob"},
                    "mode": {"type": "string", "enum": ["strict", "permissive"], "default": "strict"},
                    "contract_path": {"type": "string", "description": "Optional baseline contract JSON"},
                },
                "required": ["target_path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "parquet_guard_diagnose_drift",
            "description": "Run AI diagnostic on schema drift to evaluate risks and candidate remediations.",
            "parameters": {
                "type": "object",
                "properties": {
                    "target_path": {"type": "string", "description": "Target Parquet file"},
                    "baseline_path": {"type": "string", "description": "Baseline Parquet or contract JSON"},
                    "provider": {"type": "string", "enum": ["auto", "mock", "openai"], "default": "auto"},
                },
                "required": ["target_path", "baseline_path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "parquet_guard_propose_migration",
            "description": "Generate an evolved contract proposal and executable PyArrow migration code.",
            "parameters": {
                "type": "object",
                "properties": {
                    "target_path": {"type": "string", "description": "Target Parquet file with new schema"},
                    "baseline_path": {"type": "string", "description": "Existing baseline contract JSON"},
                },
                "required": ["target_path", "baseline_path"],
            },
        },
    },
]


def call_agent_tool(tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Safe dispatcher for agent tool calls."""
    try:
        if tool_name == "parquet_guard_inspect_schema":
            return tool_inspect_schema(arguments["file_path"])
        elif tool_name == "parquet_guard_compare_schemas":
            return tool_compare_schemas(
                arguments["target_path"],
                arguments["baseline_path"],
                mode=arguments.get("mode", "permissive"),
            )
        elif tool_name == "parquet_guard_audit_dataset":
            return tool_audit_dataset(
                arguments["target_path"],
                mode=arguments.get("mode", "strict"),
                contract_path=arguments.get("contract_path"),
            )
        elif tool_name == "parquet_guard_diagnose_drift":
            return tool_diagnose_drift(
                arguments["target_path"],
                arguments["baseline_path"],
                provider=arguments.get("provider", "auto"),
            )
        elif tool_name == "parquet_guard_propose_migration":
            return tool_propose_migration(
                arguments["target_path"],
                arguments["baseline_path"],
            )
        else:
            return {"status": "error", "message": f"Unknown tool name '{tool_name}'"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}
