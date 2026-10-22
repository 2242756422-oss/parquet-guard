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
