"""Multi-engine runtime probe runner, discrepancy detector, and reporter."""

from __future__ import annotations

import glob
import os
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Union

from parquet_guard.core.inspector import inspect_schema
from parquet_guard.core.models import SchemaContract
from parquet_guard.verification.adapters import BaseEngineAdapter, validate_row_limit
from parquet_guard.verification.duckdb_engine import DuckDBEngineAdapter
from parquet_guard.verification.models import (
    DisagreementClassification,
    EngineProbeOperation,
    EngineProbeResult,
    EngineVerificationReport,
    MetadataVerdict,
    RuleVsRuntimeDisagreement,
    RuntimeVerdict,
    ValidationScope,
)
from parquet_guard.verification.polars_engine import PolarsEngineAdapter
from parquet_guard.verification.pyarrow_engine import PyArrowEngineAdapter

# Built-in engine adapters
_ADAPTERS: Dict[str, BaseEngineAdapter] = {
    "pyarrow": PyArrowEngineAdapter(),
    "duckdb": DuckDBEngineAdapter(),
    "polars": PolarsEngineAdapter(),
}


def register_engine_adapter(adapter: BaseEngineAdapter) -> None:
    """Register a custom engine verification adapter."""
    _ADAPTERS[adapter.name.lower()] = adapter


def get_engine_adapter(name: str) -> Optional[BaseEngineAdapter]:
    """Retrieve an engine adapter by name (case-insensitive)."""
    return _ADAPTERS.get(name.lower())


def list_available_engines() -> List[str]:
    """List names of engines currently available and installed."""
    return [name for name, adapter in _ADAPTERS.items() if adapter.is_available()]


def resolve_file_paths(target: Union[str, Path, Sequence[Union[str, Path]]]) -> List[str]:
    """Safely resolve single file, directory, or glob pattern to existing file paths."""
    if isinstance(target, (list, tuple)):
        return [str(p) for p in target if os.path.isfile(str(p))]

    target_str = str(target)
    if os.path.isfile(target_str):
        return [target_str]
    if os.path.isdir(target_str):
        files = sorted(glob.glob(os.path.join(target_str, "**", "*.parquet"), recursive=True))
        return files

    # Try glob expansion
    expanded = sorted(glob.glob(target_str, recursive=True))
    return [f for f in expanded if os.path.isfile(f)]


def classify_disagreement(
    meta_verdict: MetadataVerdict,
    runtime_verdict: RuntimeVerdict,
) -> DisagreementClassification:
    """Classify the relationship between static metadata prediction and runtime reality."""
    if runtime_verdict in (
        RuntimeVerdict.UNSUPPORTED,
        RuntimeVerdict.NOT_RUN,
        RuntimeVerdict.INCONCLUSIVE,
        RuntimeVerdict.ERROR,
    ):
        return DisagreementClassification.INCONCLUSIVE

    if meta_verdict in (MetadataVerdict.ERROR, MetadataVerdict.UNKNOWN):
        return DisagreementClassification.INCONCLUSIVE

    is_meta_safe = meta_verdict in (MetadataVerdict.COMPATIBLE, MetadataVerdict.WARNING)
    is_runtime_pass = runtime_verdict == RuntimeVerdict.PASS

    if is_meta_safe and is_runtime_pass:
        return DisagreementClassification.AGREEMENT
    if not is_meta_safe and not is_runtime_pass:
        return DisagreementClassification.AGREEMENT
    if not is_meta_safe and is_runtime_pass:
        return DisagreementClassification.RULE_CONSERVATIVE
    # is_meta_safe and not is_runtime_pass:
    return DisagreementClassification.RULE_OPTIMISTIC


def verify_engine_compatibility(
    target: Optional[Union[str, Path, Sequence[Union[str, Path]]]] = None,
    expected_contract: Optional[SchemaContract] = None,
    engines: Optional[List[str]] = None,
    operations: Optional[List[EngineProbeOperation]] = None,
    row_limit: int = 10,
    scope: ValidationScope = ValidationScope.SYNTHETIC_DATA,
    *,
    files: Optional[Union[str, Path, Sequence[Union[str, Path]]]] = None,
    contract: Optional[SchemaContract] = None,
    allow_row_probe: bool = False,
) -> EngineVerificationReport:
    """Execute real query engine verification probes across PyArrow, DuckDB, and Polars.

    Separates deterministic metadata findings from runtime execution observations,
    detecting any rule-vs-runtime disagreements without leaking raw row data.
    """
    raw_target = target if target is not None else files
    if raw_target is None:
        raise ValueError("Either 'target' or 'files' must be provided.")
    if expected_contract is None and contract is not None:
        expected_contract = contract

    resolved_files = resolve_file_paths(raw_target)
    target_display = (
        str(raw_target) if not isinstance(raw_target, (list, tuple)) else f"{len(raw_target)} files"
    )

    # Validate row_limit before accessing data
    validated_row_limit = validate_row_limit(row_limit)

    report = EngineVerificationReport(
        target_path=target_display,
        validation_scope=ValidationScope.USER_AUTHORIZED_DATA
        if allow_row_probe
        else ValidationScope.METADATA_ONLY,
        engine_results=[],
        disagreements=[],
        finding_ids=[],
        tested_invariants=[],
        limitations=[],
    )

    if not resolved_files:
        report.metadata_verdict = MetadataVerdict.ERROR
        report.runtime_verdict = RuntimeVerdict.ERROR
        report.limitations.append(f"No Parquet files found matching '{target_display}'.")
        return report

    # 1. Evaluate Static Metadata Verdict
    expected_cols = None
    target_field_types: Dict[str, str] = {}
    if expected_contract is not None:
        leaf_cols: List[str] = []
        for f in expected_contract.fields:
            if not f.children:
                leaf_cols.append(f.name)
            else:

                def _collect_leaf(fld, prefix=""):
                    p = f"{prefix}.{fld.name}" if prefix else fld.name
                    if not fld.children:
                        leaf_cols.append(p)
                    else:
                        for ch in fld.children:
                            _collect_leaf(ch, p)

                _collect_leaf(f)
        expected_cols = leaf_cols
        target_field_types = {f.name: f.type for f in expected_contract.fields}
        try:
            from parquet_guard.core.compatibility import compare_schemas

            has_break = False
            has_warn = False
            all_findings = []
            for f_path in resolved_files:
                f_schema = inspect_schema(f_path)
                diff_rep = compare_schemas(f_schema, expected_contract)
                if diff_rep.has_breaking_changes:
                    has_break = True
                elif diff_rep.has_warnings:
                    has_warn = True
                all_findings.extend([d.error_code.value for d in diff_rep.diffs if d.error_code])

            if has_break:
                report.metadata_verdict = MetadataVerdict.INCOMPATIBLE
            elif has_warn:
                report.metadata_verdict = MetadataVerdict.WARNING
            else:
                report.metadata_verdict = MetadataVerdict.COMPATIBLE
            report.finding_ids = sorted(list(set(all_findings)))
        except Exception as exc:
            report.metadata_verdict = MetadataVerdict.ERROR
            report.limitations.append(f"Metadata inspection failed: {exc}")
    else:
        try:
            _ = inspect_schema(resolved_files[0])
            report.metadata_verdict = MetadataVerdict.COMPATIBLE
        except Exception as exc:
            report.metadata_verdict = MetadataVerdict.ERROR
            report.limitations.append(f"Inspection of target file failed: {exc}")

    # 2. Authorization Capability Gate:
    # If operator has not authorized row data access, skip all row-level probes immediately
    if not allow_row_probe:
        report.limitations.append(
            "Row-level engine execution probes were skipped because allow_row_probe=False. "
            "Explicit authorization is required to read row data."
        )
        report.runtime_verdict = RuntimeVerdict.NOT_RUN
        report.requested_engines = list(engines) if engines is not None else ["pyarrow", "duckdb", "polars"]
        report.requested_operations = (
            list(operations) if operations is not None else [EngineProbeOperation.READ]
        )
        report.executed_engines = []
        report.skipped_engines = list(report.requested_engines)
        report.executed_operations = []
        return report

    # 3. Select target engines
    if engines is None:
        selected_engines = ["pyarrow", "duckdb", "polars"]
    else:
        selected_engines = list(engines)

    if operations is None:
        probed_operations = [EngineProbeOperation.READ]
        if len(resolved_files) > 1:
            probed_operations.append(EngineProbeOperation.SCHEMA_UNION)
        elif target_field_types:
            probed_operations.append(EngineProbeOperation.CAST)
    else:
        probed_operations = list(operations)

    report.requested_engines = list(selected_engines)
    report.requested_operations = list(probed_operations)

    if not selected_engines:
        report.runtime_verdict = RuntimeVerdict.NOT_RUN
        report.limitations.append("Explicitly empty engines list provided; no engine probes requested.")
        return report

    if not probed_operations:
        report.runtime_verdict = RuntimeVerdict.NOT_RUN
        report.limitations.append("Explicitly empty operations list provided; no operations executed.")
        return report

    all_runtime_verdicts = []
    executed_engines: List[str] = []
    skipped_engines: List[str] = []
    executed_ops: List[EngineProbeOperation] = []

    # 4. Execute Engine Probes
    for eng_name in selected_engines:
        adapter = get_engine_adapter(eng_name)
        if adapter is None or not adapter.is_available():
            skipped_engines.append(eng_name)
            res = (
                adapter.make_unsupported_result(EngineProbeOperation.READ)
                if adapter
                else EngineProbeResult(
                    engine_name=eng_name,
                    operation=EngineProbeOperation.READ,
                    runtime_verdict=RuntimeVerdict.UNSUPPORTED,
                    error_message=f"Unknown or unavailable engine '{eng_name}'.",
                )
            )
            report.engine_results.append(res)
            all_runtime_verdicts.append(res.runtime_verdict)
            continue

        executed_engines.append(eng_name)

        # Execute READ probe
        if EngineProbeOperation.READ in probed_operations:
            read_res = adapter.probe_read(
                resolved_files, expected_columns=expected_cols, row_limit=validated_row_limit
            )
            report.engine_results.append(read_res)
            all_runtime_verdicts.append(read_res.runtime_verdict)
            report.tested_invariants.extend(read_res.tested_invariants)
            if EngineProbeOperation.READ not in executed_ops:
                executed_ops.append(EngineProbeOperation.READ)

            # Check Disagreement for Read
            diag = classify_disagreement(report.metadata_verdict, read_res.runtime_verdict)
            if (
                diag != DisagreementClassification.AGREEMENT
                and diag != DisagreementClassification.INCONCLUSIVE
            ):
                explanation = (
                    f"Engine {eng_name} yielded {read_res.runtime_verdict.value} for READ "
                    f"while static rules predicted {report.metadata_verdict.value}."
                )
                if read_res.error_message:
                    explanation += f" Reason: {read_res.error_message}"
                report.disagreements.append(
                    RuleVsRuntimeDisagreement(
                        engine_name=eng_name,
                        operation=EngineProbeOperation.READ,
                        rule_predicted_verdict=report.metadata_verdict,
                        runtime_verdict=read_res.runtime_verdict,
                        classification=diag,
                        explanation=explanation,
                        remediation_advice=(
                            "Review engine-specific casting behavior"
                            if diag == DisagreementClassification.RULE_OPTIMISTIC
                            else "Rule flagged potential break, but engine successfully tolerated widening."
                        ),
                    )
                )

        # Execute SCHEMA_UNION probe
        if EngineProbeOperation.SCHEMA_UNION in probed_operations:
            if len(resolved_files) > 1:
                union_res = adapter.probe_schema_union(resolved_files, row_limit=validated_row_limit)
                report.engine_results.append(union_res)
                all_runtime_verdicts.append(union_res.runtime_verdict)
                report.tested_invariants.extend(union_res.tested_invariants)
                if EngineProbeOperation.SCHEMA_UNION not in executed_ops:
                    executed_ops.append(EngineProbeOperation.SCHEMA_UNION)

                diag_union = classify_disagreement(report.metadata_verdict, union_res.runtime_verdict)
                if (
                    diag_union != DisagreementClassification.AGREEMENT
                    and diag_union != DisagreementClassification.INCONCLUSIVE
                ):
                    report.disagreements.append(
                        RuleVsRuntimeDisagreement(
                            engine_name=eng_name,
                            operation=EngineProbeOperation.SCHEMA_UNION,
                            rule_predicted_verdict=report.metadata_verdict,
                            runtime_verdict=union_res.runtime_verdict,
                            classification=diag_union,
                            explanation=f"Engine {eng_name} yielded {union_res.runtime_verdict.value} for SCHEMA_UNION while static rules predicted {report.metadata_verdict.value}.",
                            remediation_advice="Reconcile multi-partition schema divergence across partition files.",
                        )
                    )
            else:
                # Single file union cannot verify multi-file partition union
                union_inconc = EngineProbeResult(
                    engine_name=eng_name,
                    operation=EngineProbeOperation.SCHEMA_UNION,
                    runtime_verdict=RuntimeVerdict.INCONCLUSIVE,
                    error_message="SCHEMA_UNION probe requested but only a single file provided (minimum 2 files required for union).",
                )
                report.engine_results.append(union_inconc)
                all_runtime_verdicts.append(union_inconc.runtime_verdict)

        # Execute CAST probe
        if EngineProbeOperation.CAST in probed_operations:
            if target_field_types:
                cast_res = adapter.probe_cast(
                    resolved_files[0], target_field_types, row_limit=validated_row_limit
                )
                report.engine_results.append(cast_res)
                all_runtime_verdicts.append(cast_res.runtime_verdict)
                report.tested_invariants.extend(cast_res.tested_invariants)
                if EngineProbeOperation.CAST not in executed_ops:
                    executed_ops.append(EngineProbeOperation.CAST)

                diag_cast = classify_disagreement(report.metadata_verdict, cast_res.runtime_verdict)
                if (
                    diag_cast != DisagreementClassification.AGREEMENT
                    and diag_cast != DisagreementClassification.INCONCLUSIVE
                ):
                    report.disagreements.append(
                        RuleVsRuntimeDisagreement(
                            engine_name=eng_name,
                            operation=EngineProbeOperation.CAST,
                            rule_predicted_verdict=report.metadata_verdict,
                            runtime_verdict=cast_res.runtime_verdict,
                            classification=diag_cast,
                            explanation=f"Engine {eng_name} yielded {cast_res.runtime_verdict.value} for CAST while static rules predicted {report.metadata_verdict.value}.",
                            remediation_advice="Review type casting constraints or nullability in target schema.",
                        )
                    )
            else:
                # CAST requested without contract types
                cast_inconc = EngineProbeResult(
                    engine_name=eng_name,
                    operation=EngineProbeOperation.CAST,
                    runtime_verdict=RuntimeVerdict.INCONCLUSIVE,
                    error_message="CAST probe requested but no target schema/contract types provided.",
                )
                report.engine_results.append(cast_inconc)
                all_runtime_verdicts.append(cast_inconc.runtime_verdict)

    report.executed_engines = executed_engines
    report.skipped_engines = skipped_engines
    report.executed_operations = executed_ops

    # 5. Synthesize Aggregate Runtime Verdict
    if not all_runtime_verdicts or not probed_operations:
        report.runtime_verdict = RuntimeVerdict.NOT_RUN
    elif any(v == RuntimeVerdict.FAIL for v in all_runtime_verdicts):
        report.runtime_verdict = RuntimeVerdict.FAIL
    elif any(v == RuntimeVerdict.ERROR for v in all_runtime_verdicts):
        report.runtime_verdict = RuntimeVerdict.ERROR
    elif any(v == RuntimeVerdict.INCONCLUSIVE for v in all_runtime_verdicts):
        report.runtime_verdict = RuntimeVerdict.INCONCLUSIVE
    elif any(v == RuntimeVerdict.UNSUPPORTED for v in all_runtime_verdicts):
        report.runtime_verdict = (
            RuntimeVerdict.UNSUPPORTED
            if all(v == RuntimeVerdict.UNSUPPORTED for v in all_runtime_verdicts)
            else RuntimeVerdict.INCONCLUSIVE
        )
    elif all(v == RuntimeVerdict.PASS for v in all_runtime_verdicts):
        report.runtime_verdict = RuntimeVerdict.PASS
    else:
        report.runtime_verdict = RuntimeVerdict.INCONCLUSIVE

    # De-duplicate invariants
    report.tested_invariants = sorted(list(set(report.tested_invariants)))

    return report

    return report


def format_engine_verification_terminal(report: EngineVerificationReport) -> str:
    """Format an EngineVerificationReport for human-readable terminal output."""
    lines = [
        "=" * 70,
        " ENGINE RUNTIME VERIFICATION REPORT",
        "=" * 70,
        f" Target Path       : {report.target_path}",
        f" Validation Scope  : {report.validation_scope.value}",
        f" Metadata Verdict  : {report.metadata_verdict.value}",
        f" Runtime Verdict   : {report.runtime_verdict.value}",
        f" Overall Outcome   : {'COMPATIBLE' if report.overall_compatible else 'POTENTIAL INCOMPATIBILITY'}",
        "-" * 70,
        f" {'Engine':<12} | {'Operation':<14} | {'Verdict':<10} | {'Latency':<8} | {'Error'}",
        "-" * 70,
    ]
    for r in report.engine_results:
        err = r.error_category or ("OK" if r.runtime_verdict == RuntimeVerdict.PASS else "N/A")
        lines.append(
            f" {r.engine_name:<12} | {r.operation.value:<14} | {r.runtime_verdict.value:<10} | {r.duration_ms:6.2f}ms | {err}"
        )
    lines.append("=" * 70)

    if report.disagreements:
        lines.append("\n RULE VS RUNTIME DISAGREEMENTS:")
        for d in report.disagreements:
            lines.append(f" - [{d.classification.value}] {d.engine_name}: {d.explanation}")
            if d.remediation_advice:
                lines.append(f"   Advice: {d.remediation_advice}")
        lines.append("=" * 70)

    return "\n".join(lines)


def format_engine_verification_json(report: EngineVerificationReport) -> str:
    """Serialize an EngineVerificationReport to JSON."""
    return str(report.model_dump_json(indent=2))


def format_engine_verification_markdown(report: EngineVerificationReport) -> str:
    """Format an EngineVerificationReport as GitHub-compatible Markdown."""
    lines = [
        "## 🔍 Parquet Guard Engine Verification Report",
        "",
        f"- **Target**: `{report.target_path}`",
        f"- **Validation Scope**: `{report.validation_scope.value}`",
        f"- **Metadata Verdict**: `{report.metadata_verdict.value}`",
        f"- **Runtime Verdict**: `{report.runtime_verdict.value}`",
        f"- **Overall Compatible**: `{'YES' if report.overall_compatible else 'NO'}`",
        "",
        "### Engine Probe Matrix",
        "",
        "| Engine | Version | Operation | Verdict | Latency | Status |",
        "| :--- | :--- | :--- | :---: | :---: | :--- |",
    ]
    for r in report.engine_results:
        status_icon = (
            "✅ PASS" if r.runtime_verdict == RuntimeVerdict.PASS else ("⚠️ " + r.runtime_verdict.value)
        )
        lines.append(
            f"| `{r.engine_name}` | `{r.engine_version or 'N/A'}` | `{r.operation.value}` | "
            f"{status_icon} | {r.duration_ms:.2f}ms | {r.error_category or 'OK'} |"
        )

    if report.disagreements:
        lines.append("")
        lines.append("### ⚠️ Rule vs. Runtime Disagreements")
        lines.append("")
        for d in report.disagreements:
            lines.append(f"- **{d.engine_name}** (`{d.classification.value}`): {d.explanation}")
            if d.remediation_advice:
                lines.append(f"  - *Advice*: {d.remediation_advice}")

    return "\n".join(lines)
