"""Command Line Interface for parquet-guard."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

import click

from parquet_guard import (
    __version__,
    audit_dataset,
    compare_schemas,
    extract_contract,
    extract_file_metadata,
    inspect_schema,
    load_contract,
    save_contract,
)
from parquet_guard.core.inspector import validate_contract
from parquet_guard.core.reporter import (
    format_audit_json,
    format_audit_markdown,
    format_audit_sarif,
    format_audit_terminal,
    format_diff_json,
    format_diff_markdown,
    format_diff_sarif,
    format_diff_terminal,
)


@click.group()
@click.version_option(version=__version__, prog_name="parquet-guard")
def main() -> None:
    """parquet-guard: High-performance, zero-data-scan Parquet schema contract & drift guard."""
    pass


@main.command("inspect")
@click.argument("file_path", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "-f",
    "--format",
    "fmt",
    type=click.Choice(["text", "json"]),
    default="text",
    show_default=True,
    help="Output format.",
)
def inspect_cmd(file_path: str, fmt: str) -> None:
    """Inspect and display Parquet schema, metadata, and column nullabilities without reading rows."""
    try:
        contract = inspect_schema(file_path)
        meta = extract_file_metadata(file_path)

        if fmt == "json":
            out = {
                "file_metadata": meta,
                "schema_contract": contract.model_dump(),
            }
            click.echo(json.dumps(out, indent=2))
        else:
            click.echo("=" * 70)
            click.echo(" PARQUET FILE METADATA & SCHEMA INSPECTOR")
            click.echo("=" * 70)
            click.echo(f" File        : {meta['file_name']}")
            click.echo(f" File Size   : {meta['file_size_bytes'] / (1024 * 1024):.3f} MB")
            click.echo(f" Rows        : {meta['num_rows']:,}")
            click.echo(f" Columns     : {meta['num_columns']}")
            click.echo(f" Row Groups  : {meta['num_row_groups']}")
            click.echo(f" Codecs      : {', '.join(meta['compression_codecs'])}")
            click.echo(f" Format Ver  : {meta['format_version']}")
            click.echo("-" * 70)
            click.echo(f" {'Field Name':<28} | {'Type':<26} | Nullable")
            click.echo("-" * 70)
            for f in contract.fields:
                click.echo(f" {f.name:<28} | {f.type:<26} | {f.nullable}")
            click.echo("=" * 70)

    except Exception as exc:
        click.secho(f"Error inspecting Parquet file: {exc}", fg="red", err=True)
        sys.exit(2)


@main.command("init-contract")
@click.argument("file_path", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "-o",
    "--output",
    "output_path",
    required=True,
    type=click.Path(dir_okay=False),
    help="Destination path for the golden contract JSON file.",
)
@click.option("-n", "--name", "contract_name", default=None, help="Optional contract identifier name.")
def init_contract_cmd(file_path: str, output_path: str, contract_name: Optional[str]) -> None:
    """Export a golden SchemaContract JSON from an existing Parquet file."""
    try:
        contract = extract_contract(file_path, name=contract_name)
        save_contract(contract, output_path)
        click.secho(
            f"Successfully generated contract at '{output_path}' ({len(contract.fields)} fields).", fg="green"
        )
    except Exception as exc:
        click.secho(f"Error exporting contract: {exc}", fg="red", err=True)
        sys.exit(2)


@main.command("validate-contract")
@click.argument("contract_path", type=click.Path(exists=True, dir_okay=False))
def validate_contract_cmd(contract_path: str) -> None:
    """Validate the schema and structural integrity of a golden contract JSON file."""
    is_valid, err_msg = validate_contract(contract_path)
    if is_valid:
        click.secho(
            f"Contract '{contract_path}' is valid (100% compliant with SchemaContract specification).",
            fg="green",
        )
        sys.exit(0)
    else:
        click.secho(f"Contract validation error in '{contract_path}':\n{err_msg}", fg="red", err=True)
        sys.exit(2)


@main.command("check")
@click.argument("file_path", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "-c",
    "--contract",
    "contract_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to baseline SchemaContract JSON file.",
)
@click.option(
    "-m",
    "--mode",
    type=click.Choice(["permissive", "strict"]),
    default="permissive",
    show_default=True,
    help="Permissive allows safe additive fields; strict forbids any discrepancy (including safe widening).",
)
@click.option(
    "-f",
    "--format",
    "fmt",
    type=click.Choice(["text", "markdown", "json", "sarif"]),
    default="text",
    show_default=True,
    help="Report output format.",
)
@click.option(
    "--check-metadata/--no-check-metadata",
    default=True,
    show_default=True,
    help="Check for schema and field-level metadata changes.",
)
@click.option(
    "--check-field-order/--no-check-field-order",
    default=False,
    show_default=True,
    help="Enforce identical field ordering (automatically enabled in strict mode).",
)
@click.option("-o", "--output", "out_file", type=click.Path(dir_okay=False), help="Save report to file.")
def check_cmd(
    file_path: str,
    contract_path: str,
    mode: str,
    fmt: str,
    check_metadata: bool,
    check_field_order: bool,
    out_file: Optional[str],
) -> None:
    """Validate a Parquet file's schema against a golden SchemaContract."""
    try:
        target_schema = inspect_schema(file_path)
        baseline_schema = load_contract(contract_path)
        report = compare_schemas(
            target_schema,
            baseline_schema,
            mode=mode,
            check_metadata=check_metadata,
            check_field_order=check_field_order,
        )

        if fmt == "markdown":
            output_str = format_diff_markdown(report)
        elif fmt == "json":
            output_str = format_diff_json(report)
        elif fmt == "sarif":
            output_str = format_diff_sarif(report)
        else:
            output_str = format_diff_terminal(report)

        if out_file:
            Path(out_file).write_text(output_str, encoding="utf-8")
            click.echo(f"Report written to '{out_file}'.")
        else:
            click.echo(output_str)

        sys.exit(1 if report.has_breaking_changes else 0)

    except Exception as exc:
        click.secho(f"Error running contract check: {exc}", fg="red", err=True)
        sys.exit(2)


@main.command("diff")
@click.argument("file1", type=click.Path(exists=True, dir_okay=False))
@click.argument("file2", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "-m",
    "--mode",
    type=click.Choice(["permissive", "strict"]),
    default="permissive",
    show_default=True,
    help="Permissive allows safe additive fields; strict forbids any discrepancy.",
)
@click.option(
    "-f",
    "--format",
    "fmt",
    type=click.Choice(["text", "markdown", "json", "sarif"]),
    default="text",
    show_default=True,
    help="Report output format.",
)
@click.option(
    "--check-metadata/--no-check-metadata",
    default=True,
    show_default=True,
    help="Check for schema and field-level metadata changes.",
)
@click.option(
    "--check-field-order/--no-check-field-order",
    default=False,
    show_default=True,
    help="Enforce identical field ordering (automatically enabled in strict mode).",
)
@click.option("-o", "--output", "out_file", type=click.Path(dir_okay=False), help="Save report to file.")
def diff_cmd(
    file1: str,
    file2: str,
    mode: str,
    fmt: str,
    check_metadata: bool,
    check_field_order: bool,
    out_file: Optional[str],
) -> None:
    """Directly compare and diff schemas between two Parquet files."""
    try:
        schema1 = inspect_schema(file1)
        schema2 = inspect_schema(file2)
        report = compare_schemas(
            schema2,
            schema1,
            mode=mode,
            check_metadata=check_metadata,
            check_field_order=check_field_order,
        )
        report.target_name = Path(file2).name
        report.baseline_name = Path(file1).name

        if fmt == "markdown":
            output_str = format_diff_markdown(report)
        elif fmt == "json":
            output_str = format_diff_json(report)
        elif fmt == "sarif":
            output_str = format_diff_sarif(report)
        else:
            output_str = format_diff_terminal(report)

        if out_file:
            Path(out_file).write_text(output_str, encoding="utf-8")
            click.echo(f"Diff report written to '{out_file}'.")
        else:
            click.echo(output_str)

        sys.exit(1 if report.has_breaking_changes else 0)

    except Exception as exc:
        click.secho(f"Error running schema diff: {exc}", fg="red", err=True)
        sys.exit(2)


@main.command("audit")
@click.argument("target_path", type=str)
@click.option(
    "-m",
    "--mode",
    type=click.Choice(["strict", "permissive"]),
    default="strict",
    show_default=True,
    help="Audit mode: strict (enforces identical order, metadata, types) or permissive (permits safe widening/additions).",
)
@click.option(
    "-f",
    "--format",
    "fmt",
    type=click.Choice(["text", "markdown", "json", "sarif"]),
    default="text",
    show_default=True,
    help="Report output format.",
)
@click.option(
    "-r/-nr",
    "--recursive/--no-recursive",
    default=True,
    show_default=True,
    help="Recursively scan subdirectories for Parquet files.",
)
@click.option(
    "--check-metadata/--no-check-metadata",
    default=True,
    show_default=True,
    help="Check for schema and field-level metadata changes across partitions.",
)
@click.option(
    "--check-field-order/--no-check-field-order",
    default=True,
    show_default=True,
    help="Enforce identical field ordering across partition files.",
)
@click.option(
    "-c",
    "--contract",
    "--baseline",
    "contract_file",
    type=click.Path(exists=True, dir_okay=False),
    help="Optional explicit golden contract file to audit partitions against.",
)
@click.option(
    "--fail-on-warnings",
    is_flag=True,
    default=False,
    help="Fail with exit code 1 if any warnings or non-breaking schema drifts are detected.",
)
@click.option(
    "--incremental/--no-incremental",
    default=False,
    show_default=True,
    help="Enable incremental caching to skip unchanged partition files.",
)
@click.option(
    "--force-full",
    is_flag=True,
    default=False,
    help="Ignore existing incremental audit cache and force full re-audit.",
)
@click.option(
    "--cache-path",
    type=click.Path(dir_okay=False),
    default=None,
    help="Custom path for the incremental audit cache JSON manifest.",
)
@click.option("-o", "--output", "out_file", type=click.Path(dir_okay=False), help="Save report to file.")
def audit_cmd(
    target_path: str,
    mode: str,
    fmt: str,
    recursive: bool,
    check_metadata: bool,
    check_field_order: bool,
    contract_file: Optional[str],
    fail_on_warnings: bool,
    incremental: bool,
    force_full: bool,
    cache_path: Optional[str],
    out_file: Optional[str],
) -> None:
    """Audit schema consistency across all Parquet files in a partitioned dataset tree."""
    try:
        baseline_contract = load_contract(contract_file) if contract_file else None
        result = audit_dataset(
            target_path,
            mode=mode,
            recursive=recursive,
            check_metadata=check_metadata,
            check_field_order=check_field_order,
            baseline_contract=baseline_contract,
            incremental=incremental,
            cache_path=cache_path,
            force_full=force_full,
        )

        if fmt == "markdown":
            output_str = format_audit_markdown(result)
        elif fmt == "json":
            output_str = format_audit_json(result)
        elif fmt == "sarif":
            output_str = format_audit_sarif(result)
        else:
            output_str = format_audit_terminal(result)

        if out_file:
            Path(out_file).write_text(output_str, encoding="utf-8")
            click.echo(f"Audit report written to '{out_file}'.")
        else:
            click.echo(output_str)

        if result.total_files_scanned == 0:
            click.secho(
                f"Error: No Parquet files found matching path/pattern: '{target_path}'.", fg="red", err=True
            )
            sys.exit(2)

        if fail_on_warnings and result.warnings_count > 0:
            sys.exit(1)

        sys.exit(0 if result.is_consistent else 1)

    except Exception as exc:
        click.secho(f"Error running partition audit: {exc}", fg="red", err=True)
        sys.exit(2)


@main.command("explain")
@click.argument("target_path", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "-c",
    "--contract",
    "contract_path",
    type=click.Path(exists=True, dir_okay=False),
    help="Path to baseline SchemaContract JSON file.",
)
@click.option(
    "-b",
    "--baseline",
    "baseline_path",
    type=click.Path(exists=True, dir_okay=False),
    help="Alternative baseline Parquet file path.",
)
@click.option(
    "-m",
    "--mode",
    type=click.Choice(["permissive", "strict"]),
    default="permissive",
    show_default=True,
    help="Validation mode: permissive allows safe widening; strict enforces exact identity.",
)
@click.option(
    "-p",
    "--provider",
    type=click.Choice(["auto", "mock", "openai"]),
    default="auto",
    show_default=True,
    help="AI inference provider.",
)
@click.option("--model", default=None, help="Custom model ID (e.g. 'gpt-4o-mini').")
@click.option(
    "-f",
    "--format",
    "fmt",
    type=click.Choice(["text", "markdown", "json"]),
    default="text",
    show_default=True,
    help="Output format.",
)
@click.option("--context", default=None, help="Optional pipeline context to guide diagnosis.")
@click.option(
    "--fallback/--no-fallback",
    default=True,
    show_default=True,
    help="Fall back to deterministic rule engine if AI provider is unavailable.",
)
@click.option("-o", "--output", "out_file", type=click.Path(dir_okay=False), help="Save report to file.")
def explain_cmd(
    target_path: str,
    contract_path: Optional[str],
    baseline_path: Optional[str],
    mode: str,
    provider: str,
    model: Optional[str],
    fmt: str,
    context: Optional[str],
    fallback: bool,
    out_file: Optional[str],
) -> None:
    """Diagnose schema drift with evidence-grounded AI assistance."""
    if not contract_path and not baseline_path:
        click.secho("Error: Must provide either --contract (-c) or --baseline (-b).", fg="red", err=True)
        sys.exit(2)

    try:
        from parquet_guard.ai.explainer import (
            diagnose_schema_drift,
            format_diagnosis_json,
            format_diagnosis_markdown,
            format_diagnosis_terminal,
        )
        from parquet_guard.ai.providers import get_provider

        target_schema = inspect_schema(target_path)
        if contract_path:
            baseline_schema = load_contract(contract_path)
        else:
            baseline_schema = inspect_schema(str(baseline_path))

        diff_report = compare_schemas(target_schema, baseline_schema, mode=mode)
        ai_provider = get_provider(provider_type=provider, model_id=model)

        diagnosis = diagnose_schema_drift(
            diff_report,
            provider=ai_provider,
            user_context=context,
            allow_fallback=fallback,
        )

        if fmt == "markdown":
            output_str = format_diagnosis_markdown(diagnosis)
        elif fmt == "json":
            output_str = format_diagnosis_json(diagnosis)
        else:
            output_str = format_diagnosis_terminal(diagnosis)

        if out_file:
            Path(out_file).write_text(output_str, encoding="utf-8")
            click.echo(f"Diagnosis written to '{out_file}'.")
        else:
            click.echo(output_str)

        sys.exit(1 if diagnosis.is_blocking_ci else 0)

    except Exception as exc:
        click.secho(f"Error executing AI diagnosis: {exc}", fg="red", err=True)
        sys.exit(2)


@main.command("propose-migration")
@click.argument("target_path", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "-c",
    "--contract",
    "contract_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to existing baseline SchemaContract JSON file.",
)
@click.option(
    "-m",
    "--mode",
    type=click.Choice(["permissive", "strict"]),
    default="permissive",
    show_default=True,
    help="Compatibility mode for proposed contract.",
)
@click.option(
    "-f",
    "--format",
    "fmt",
    type=click.Choice(["text", "markdown", "json"]),
    default="text",
    show_default=True,
    help="Report output format.",
)
@click.option(
    "--save-contract",
    type=click.Path(dir_okay=False),
    default=None,
    help="Optional path to save the proposed new SchemaContract JSON.",
)
@click.option(
    "-o", "--output", "out_file", type=click.Path(dir_okay=False), help="Save migration plan to file."
)
def propose_migration_cmd(
    target_path: str,
    contract_path: str,
    mode: str,
    fmt: str,
    save_contract: Optional[str],
    out_file: Optional[str],
) -> None:
    """Generate contract evolution proposal and executable PyArrow migration plan."""
    try:
        from parquet_guard.ai.planner import (
            format_migration_json,
            format_migration_markdown,
            generate_migration_plan,
        )

        target_schema = inspect_schema(target_path)
        baseline_schema = load_contract(contract_path)

        plan = generate_migration_plan(baseline_schema, target_schema)

        if save_contract and plan.contract_proposal:
            from parquet_guard.core.inspector import save_contract as persist_contract

            persist_contract(plan.contract_proposal.proposed_contract, save_contract)
            click.secho(f"Saved proposed contract to '{save_contract}'.", fg="green")

        if fmt == "markdown":
            output_str = format_migration_markdown(plan)
        elif fmt == "json":
            output_str = format_migration_json(plan)
        else:
            output_str = format_migration_markdown(plan)

        if out_file:
            Path(out_file).write_text(output_str, encoding="utf-8")
            click.echo(f"Migration plan written to '{out_file}'.")
        else:
            click.echo(output_str)

        sys.exit(0)

    except Exception as exc:
        click.secho(f"Error generating migration plan: {exc}", fg="red", err=True)
        sys.exit(2)


@main.command("eval-ai")
@click.option(
    "-p",
    "--provider",
    type=click.Choice(["auto", "mock", "openai"]),
    default="auto",
    show_default=True,
    help="AI inference provider to evaluate.",
)
@click.option("--model", default=None, help="Custom model ID.")
@click.option(
    "-f",
    "--format",
    "fmt",
    type=click.Choice(["text", "markdown", "json"]),
    default="text",
    show_default=True,
    help="Output report format.",
)
@click.option(
    "-o", "--output", "out_file", type=click.Path(dir_okay=False), help="Save evaluation report to file."
)
@click.option(
    "--split",
    type=click.Choice(["all", "dev", "held_out"]),
    default="all",
    show_default=True,
    help="Evaluation split to run ('all', 'dev', or 'held_out').",
)
def eval_ai_cmd(
    provider: str,
    model: Optional[str],
    fmt: str,
    out_file: Optional[str],
    split: str,
) -> None:
    """Run reproducible benchmark evaluation across synthetic schema evolution scenarios."""
    try:
        from parquet_guard.ai.evaluator import (
            format_evaluation_markdown,
            run_evaluation_suite,
        )
        from parquet_guard.ai.providers import get_provider

        ai_provider = get_provider(provider_type=provider, model_id=model)
        split_filter = None if split == "all" else split
        click.secho(
            f"Evaluating provider: {provider} (model: {model or 'default'}, split: {split})...",
            fg="cyan",
        )

        report = run_evaluation_suite(
            provider=ai_provider,
            progress_callback=lambda name, cur, tot: click.echo(f"  [{cur}/{tot}] Evaluating: {name}"),
            split=split_filter,
        )

        if fmt == "json":
            output_str = report.model_dump_json(indent=2)
        else:
            output_str = format_evaluation_markdown(report)

        if out_file:
            Path(out_file).write_text(output_str, encoding="utf-8")
            click.echo(f"Evaluation report written to '{out_file}'.")
        else:
            click.echo(output_str)

        click.secho(
            f"\nEvaluation Complete: {report.passed_scenarios}/{report.total_scenarios} passed "
            f"({report.pass_rate}%). Factual consistency: {report.factual_consistency_rate * 100:.1f}%.",
            fg="green" if report.pass_rate >= 80.0 else "yellow",
        )
        if provider == "mock":
            click.secho(
                "Notice: Mock provider scores verify test harness, parsing, and contract consistency (offline simulation, not live LLM reasoning).",
                dim=True,
            )
        sys.exit(0)

    except Exception as exc:
        click.secho(f"Error running AI evaluation: {exc}", fg="red", err=True)
        sys.exit(2)


@main.command("impact")
@click.argument("target_path", type=click.Path(exists=True))
@click.option(
    "-c",
    "--consumers",
    "consumers_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to ConsumerContract or ConsumerGroupSpecification JSON file.",
)
@click.option(
    "-f",
    "--format",
    "fmt",
    type=click.Choice(["text", "json"]),
    default="text",
    show_default=True,
    help="Output format.",
)
@click.option(
    "-o",
    "--output",
    "out_file",
    type=click.Path(dir_okay=False),
    help="Optional output destination file.",
)
def impact_cmd(target_path: str, consumers_path: str, fmt: str, out_file: Optional[str]) -> None:
    """Analyze upstream Parquet schema drift against declared downstream consumer contracts."""
    try:
        from parquet_guard.core.consumer import (
            ConsumerContract,
            ConsumerGroupSpecification,
            evaluate_consumer_impact,
        )

        p = Path(target_path)
        if p.is_dir():
            audit_res = audit_dataset(str(p))
            if not audit_res.canonical_schema:
                click.secho(f"No valid Parquet files found in directory: {target_path}", fg="red", err=True)
                sys.exit(2)
            schema = audit_res.canonical_schema
        else:
            schema = inspect_schema(str(p))

        c_data = json.loads(Path(consumers_path).read_text(encoding="utf-8"))
        if "consumers" in c_data:
            group = ConsumerGroupSpecification.model_validate(c_data)
            consumers = group.consumers
        else:
            consumers = [ConsumerContract.model_validate(c_data)]

        report = evaluate_consumer_impact(schema, consumers, dataset_identifier=p.name)

        if fmt == "json":
            out_str = report.model_dump_json(indent=2)
        else:
            lines = [
                "=" * 70,
                f" CONSUMER IMPACT ANALYSIS: {p.name}",
                "=" * 70,
                f" Overall Verdict : {report.overall_status.value}",
                f" Total Consumers : {report.total_consumers}",
                f" Compatible      : {report.compatible_consumers}",
                f" Conditional     : {report.conditionally_compatible_consumers}",
                f" Incompatible    : {report.incompatible_consumers}",
                "-" * 70,
            ]
            for ev in report.evaluations:
                lines.append(f" Consumer: {ev.consumer_name} ({ev.consumer_id}) -> {ev.status.value}")
                if ev.missing_required_fields:
                    lines.append(f"   Missing Fields     : {ev.missing_required_fields}")
                if ev.type_mismatches:
                    lines.append(f"   Type Mismatches    : {ev.type_mismatches}")
                if ev.nullability_violations:
                    lines.append(f"   Null Violations    : {ev.nullability_violations}")
                if ev.dimension_mismatches:
                    lines.append(f"   Dimension Mismatch : {ev.dimension_mismatches}")
                if ev.unexpected_extra_fields:
                    lines.append(f"   Extra Fields       : {ev.unexpected_extra_fields}")
            lines.append("=" * 70)
            out_str = "\n".join(lines)

        if out_file:
            Path(out_file).write_text(out_str, encoding="utf-8")
            click.echo(f"Consumer impact report saved to: {out_file}")
        else:
            click.echo(out_str)

        if report.is_blocking:
            sys.exit(1)
        sys.exit(0)

    except Exception as exc:
        click.secho(f"Error evaluating consumer impact: {exc}", fg="red", err=True)
        sys.exit(2)


@main.command("verify-migration")
@click.argument("plan_path", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "-c",
    "--consumers",
    "consumers_path",
    type=click.Path(exists=True, dir_okay=False),
    help="Optional consumer contracts JSON to verify against migrated output.",
)
@click.option(
    "-n",
    "--rows",
    "sample_rows",
    default=50,
    show_default=True,
    type=int,
    help="Number of synthetic rows to generate and test.",
)
@click.option(
    "-f",
    "--format",
    "fmt",
    type=click.Choice(["text", "json"]),
    default="text",
    show_default=True,
    help="Output format.",
)
@click.option(
    "-o",
    "--output",
    "out_file",
    type=click.Path(dir_okay=False),
    help="Optional output destination file.",
)
def verify_migration_cmd(
    plan_path: str,
    consumers_path: Optional[str],
    sample_rows: int,
    fmt: str,
    out_file: Optional[str],
) -> None:
    """Execute candidate migration plan in an isolated synthetic sandbox to verify row count and value invariants."""
    try:
        from parquet_guard.core.consumer import (
            ConsumerContract,
            ConsumerGroupSpecification,
        )
        from parquet_guard.core.migration_verifier import (
            ExecutableMigrationPlan,
            verify_migration_plan,
        )

        plan_data = json.loads(Path(plan_path).read_text(encoding="utf-8"))
        plan = ExecutableMigrationPlan.model_validate(plan_data)

        consumers = None
        if consumers_path:
            c_data = json.loads(Path(consumers_path).read_text(encoding="utf-8"))
            if "consumers" in c_data:
                consumers = ConsumerGroupSpecification.model_validate(c_data).consumers
            else:
                consumers = [ConsumerContract.model_validate(c_data)]

        click.secho(f"Executing migration sandbox verification: {plan.title}...", fg="cyan")
        res = verify_migration_plan(plan, consumers=consumers, sample_rows=sample_rows)

        if fmt == "json":
            out_str = res.model_dump_json(indent=2)
        else:
            lines = [
                "=" * 70,
                f" MIGRATION SANDBOX VERIFICATION: {plan.plan_id}",
                "=" * 70,
                f" Result           : {'PASSED' if res.success else 'FAILED'}",
                f" Rows Tested      : {res.rows_tested}",
                f" Rows Preserved   : {res.row_count_preserved}",
                f" Schema Matched   : {res.schema_matched}",
                f" Value Invariants : {res.value_invariants_passed}",
                f" Execution Time   : {res.execution_latency_ms} ms",
                "-" * 70,
            ]
            if res.incompatible_consumers:
                lines.append(f" Incompatible Consumers : {res.incompatible_consumers}")
            if res.errors:
                lines.append(" Errors:")
                for err in res.errors:
                    lines.append(f"   - {err}")
            if res.warnings:
                lines.append(" Warnings:")
                for w in res.warnings:
                    lines.append(f"   - {w}")
            lines.append("=" * 70)
            out_str = "\n".join(lines)

        if out_file:
            Path(out_file).write_text(out_str, encoding="utf-8")
            click.echo(f"Verification report saved to: {out_file}")
        else:
            click.echo(out_str)

        sys.exit(0 if res.success else 1)

    except Exception as exc:
        click.secho(f"Error verifying migration: {exc}", fg="red", err=True)
        sys.exit(2)


@main.command("report")
@click.argument("dataset_dir", type=click.Path(exists=True, file_okay=False))
@click.option(
    "-o",
    "--output",
    "output_html",
    required=True,
    type=click.Path(dir_okay=False),
    help="Output destination path for generated standalone HTML report.",
)
@click.option(
    "-c",
    "--consumers",
    "consumers_path",
    type=click.Path(exists=True, dir_okay=False),
    help="Optional path to consumer contracts JSON.",
)
@click.option(
    "-m",
    "--migration-plan",
    "plan_path",
    type=click.Path(exists=True, dir_okay=False),
    help="Optional ExecutableMigrationPlan JSON to verify in sandbox.",
)
@click.option(
    "--title",
    "title",
    default=None,
    help="Dataset or pipeline title for report header.",
)
def report_cmd(
    dataset_dir: str,
    output_html: str,
    consumers_path: Optional[str],
    plan_path: Optional[str],
    title: Optional[str],
) -> None:
    """Generate a self-contained standalone HTML diagnostic report for a dataset."""
    try:
        from parquet_guard.core.consumer import (
            ConsumerContract,
            ConsumerGroupSpecification,
            evaluate_consumer_impact,
        )
        from parquet_guard.core.grouping import analyze_dataset_topology
        from parquet_guard.core.html_reporter import generate_html_diagnostic_report
        from parquet_guard.core.migration_verifier import (
            ExecutableMigrationPlan,
            verify_migration_plan,
        )

        d_path = Path(dataset_dir)
        report_title = title or d_path.name

        click.secho(f"Auditing dataset '{d_path}'...", fg="cyan")
        audit_res = audit_dataset(str(d_path))

        consumers = None
        if consumers_path:
            c_data = json.loads(Path(consumers_path).read_text(encoding="utf-8"))
            if "consumers" in c_data:
                consumers = ConsumerGroupSpecification.model_validate(c_data).consumers
            else:
                consumers = [ConsumerContract.model_validate(c_data)]

        topology = analyze_dataset_topology(audit_res, consumers=consumers)

        impact_rep = None
        if consumers and audit_res.canonical_schema:
            impact_rep = evaluate_consumer_impact(
                audit_res.canonical_schema, consumers, dataset_identifier=report_title
            )

        mig_res = None
        if plan_path:
            p_data = json.loads(Path(plan_path).read_text(encoding="utf-8"))
            plan = ExecutableMigrationPlan.model_validate(p_data)
            mig_res = verify_migration_plan(plan, consumers=consumers)

        generate_html_diagnostic_report(
            output_path=output_html,
            audit_result=audit_res,
            impact_report=impact_rep,
            topology_report=topology,
            migration_result=mig_res,
            dataset_title=report_title,
        )
        click.secho(f"Diagnostic report successfully written to '{output_html}'.", fg="green")
        sys.exit(0)

    except Exception as exc:
        click.secho(f"Error generating diagnostic report: {exc}", fg="red", err=True)
        sys.exit(2)


@main.command("serve-mcp")
@click.option(
    "--allowed-dir",
    "allowed_dirs",
    multiple=True,
    type=click.Path(exists=True, file_okay=False),
    help="Allowed directory boundary for file access (can be specified multiple times).",
)
def serve_mcp_cmd(allowed_dirs: tuple[str, ...]) -> None:
    """Start the Model Context Protocol (MCP) server over standard I/O (stdio)."""
    try:
        from parquet_guard.mcp.server import MCPServer

        dirs = list(allowed_dirs) if allowed_dirs else None
        server = MCPServer(allowed_dirs=dirs)
        server.run_stdio()
    except Exception as exc:
        click.secho(f"MCP server terminated with error: {exc}", fg="red", err=True)
        sys.exit(2)


@main.command("init-consumer")
@click.argument("file_path", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "-o",
    "--output",
    "output_path",
    required=True,
    type=click.Path(dir_okay=False),
    help="Destination path for consumer contract JSON file.",
)
@click.option(
    "-id",
    "--consumer-id",
    "consumer_id",
    required=True,
    help="Unique machine identifier for consumer (e.g. 'duckdb-analytics').",
)
@click.option(
    "-n",
    "--name",
    "consumer_name",
    default=None,
    help="Human-readable name for consumer.",
)
def init_consumer_cmd(
    file_path: str, output_path: str, consumer_id: str, consumer_name: Optional[str]
) -> None:
    """Export a starter ConsumerContract JSON inferred from an existing Parquet file."""
    try:
        from parquet_guard.core.consumer import (
            ConsumerContract,
            ConsumerFieldRequirement,
        )

        schema = inspect_schema(file_path)
        reqs = []
        for f in schema.fields:
            reqs.append(
                ConsumerFieldRequirement(
                    name=f.name,
                    required=True,
                    permitted_types=[f.type],
                    allow_null=f.nullable,
                    list_size=f.list_size,
                    scale=f.scale,
                    time_unit=f.time_unit,
                    timezone=f.timezone,
                    critical=True,
                )
            )

        c = ConsumerContract(
            consumer_id=consumer_id,
            consumer_name=consumer_name or consumer_id,
            fields=reqs,
            allow_extra_fields=True,
        )
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(output_path).write_text(c.model_dump_json(indent=2), encoding="utf-8")
        click.secho(f"Consumer contract successfully exported to '{output_path}'.", fg="green")
        sys.exit(0)

    except Exception as exc:
        click.secho(f"Error creating consumer contract: {exc}", fg="red", err=True)
        sys.exit(2)


if __name__ == "__main__":
    main()
