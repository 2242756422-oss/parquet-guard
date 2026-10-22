"""Command Line Interface for parquet-guard."""

from __future__ import annotations

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
            click.echo(format_diff_json(out))  # type: ignore[arg-type]
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


if __name__ == "__main__":
    main()
