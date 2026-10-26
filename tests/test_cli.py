"""End-to-end tests for the Click CLI interface."""

from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from parquet_guard.cli import main


def test_cli_version() -> None:
    from parquet_guard import __version__

    runner = CliRunner()
    result = runner.invoke(main, ["--version"])
    assert result.exit_code == 0
    assert __version__ in result.output


def test_cli_inspect(sample_parquet_file: Path) -> None:
    runner = CliRunner()
    # 1. Text format
    result = runner.invoke(main, ["inspect", str(sample_parquet_file)])
    assert result.exit_code == 0
    assert "PARQUET FILE METADATA" in result.output
    assert "event_id" in result.output
    assert "user_id" in result.output
    assert "Rows" in result.output

    # 2. JSON format
    result_json = runner.invoke(main, ["inspect", str(sample_parquet_file), "--format", "json"])
    assert result_json.exit_code == 0
    import json

    data = json.loads(result_json.output)
    assert "file_metadata" in data
    assert "schema_contract" in data
    assert data["file_metadata"]["num_rows"] == 3


def test_cli_init_contract_and_check(
    sample_parquet_file: Path, widened_parquet_file: Path, breaking_parquet_file: Path, temp_dir: Path
) -> None:
    runner = CliRunner()
    contract_file = temp_dir / "golden_contract.json"

    # 1. Export contract
    init_res = runner.invoke(main, ["init-contract", str(sample_parquet_file), "-o", str(contract_file)])
    assert init_res.exit_code == 0
    assert contract_file.is_file()

    # 2. Check identical sample against contract (expect PASS, exit 0)
    check_pass = runner.invoke(main, ["check", str(sample_parquet_file), "-c", str(contract_file)])
    assert check_pass.exit_code == 0
    assert "[PASS]" in check_pass.output

    # 3. Check widened file against contract in permissive mode (expect PASS with SAFE, exit 0)
    check_wide = runner.invoke(
        main, ["check", str(widened_parquet_file), "-c", str(contract_file), "-m", "permissive"]
    )
    assert check_wide.exit_code == 0

    # 4. Check widened file in strict mode (expect FAIL because of added field, exit 1)
    check_strict = runner.invoke(
        main, ["check", str(widened_parquet_file), "-c", str(contract_file), "-m", "strict"]
    )
    assert check_strict.exit_code == 1

    # 5. Check breaking file against contract (expect FAIL, exit 1)
    check_break = runner.invoke(main, ["check", str(breaking_parquet_file), "-c", str(contract_file)])
    assert check_break.exit_code == 1
    assert "BREAKING" in check_break.output


def test_cli_diff(sample_parquet_file: Path, breaking_parquet_file: Path) -> None:
    runner = CliRunner()
    diff_res = runner.invoke(
        main, ["diff", str(sample_parquet_file), str(breaking_parquet_file), "-f", "markdown"]
    )
    assert diff_res.exit_code == 1
    assert "Parquet-Guard Schema Validation Report" in diff_res.output
    assert "BREAKING" in diff_res.output


def test_cli_audit(partitioned_dataset: Path, divergent_dataset: Path) -> None:
    runner = CliRunner()

    # Audit consistent dataset -> exit 0
    audit_clean = runner.invoke(main, ["audit", str(partitioned_dataset)])
    assert audit_clean.exit_code == 0
    assert "[PASS] Dataset schema is 100% consistent" in audit_clean.output

    # Audit divergent dataset -> exit 1
    audit_drift = runner.invoke(main, ["audit", str(divergent_dataset)])
    assert audit_drift.exit_code == 1
    assert "[FAIL] Schema drift detected" in audit_drift.output


def test_cli_init_consumer_and_impact(
    sample_parquet_file: Path, breaking_parquet_file: Path, temp_dir: Path
) -> None:
    """Test init-consumer and impact CLI commands."""
    runner = CliRunner()
    consumer_file = temp_dir / "consumer.json"

    # 1. init-consumer
    r_init = runner.invoke(
        main,
        [
            "init-consumer",
            str(sample_parquet_file),
            "-o",
            str(consumer_file),
            "-id",
            "test-consumer",
            "-n",
            "Test Consumer",
        ],
    )
    assert r_init.exit_code == 0
    assert consumer_file.is_file()

    # 2. impact against same file (compatible, exit 0)
    r_impact_pass = runner.invoke(
        main,
        ["impact", str(sample_parquet_file), "-c", str(consumer_file), "-f", "json"],
    )
    assert r_impact_pass.exit_code == 0
    import json

    data = json.loads(r_impact_pass.output)
    assert data["overall_status"] == "COMPATIBLE"

    # 3. impact against breaking file (breaking, exit 1)
    r_impact_fail = runner.invoke(
        main,
        ["impact", str(breaking_parquet_file), "-c", str(consumer_file)],
    )
    assert r_impact_fail.exit_code == 1
    assert "CONSUMER IMPACT ANALYSIS" in r_impact_fail.output


def test_cli_verify_migration(sample_parquet_file: Path, temp_dir: Path) -> None:
    """Test verify-migration CLI command."""
    runner = CliRunner()
    from parquet_guard.core.inspector import inspect_schema
    from parquet_guard.core.migration_verifier import (
        ExecutableMigrationPlan,
    )

    schema = inspect_schema(sample_parquet_file)
    plan = ExecutableMigrationPlan(
        plan_id="CLI-MIG-01",
        title="Test CLI Migration",
        description="Safe cast test",
        source_contract=schema,
        target_contract=schema,
        operations=[],
    )
    plan_file = temp_dir / "plan.json"
    plan_file.write_text(plan.model_dump_json(indent=2), encoding="utf-8")

    res = runner.invoke(main, ["verify-migration", str(plan_file), "-n", "20"])
    assert res.exit_code == 0
    assert "MIGRATION SANDBOX VERIFICATION" in res.output
    assert "PASSED" in res.output


def test_cli_report(partitioned_dataset: Path, temp_dir: Path) -> None:
    """Test report CLI command generating standalone HTML."""
    runner = CliRunner()
    report_file = temp_dir / "report.html"

    res = runner.invoke(
        main,
        ["report", str(partitioned_dataset), "-o", str(report_file), "--title", "CLI Test Lake"],
    )
    assert res.exit_code == 0
    assert report_file.is_file()
    content = report_file.read_text(encoding="utf-8")
    assert "<!DOCTYPE html>" in content
    assert "CLI Test Lake" in content
