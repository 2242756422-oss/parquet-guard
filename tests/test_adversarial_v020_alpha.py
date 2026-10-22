"""Adversarial and metamorphic functional tests for parquet-guard v0.2.0 Alpha release."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from parquet_guard.cli import main
from parquet_guard.core.auditor import audit_dataset
from parquet_guard.core.compatibility import (
    _parse_list_inner,
    _parse_map_params,
    is_safe_widening,
)
from parquet_guard.core.inspector import (
    ContractValidationError,
    load_contract,
    validate_contract,
)
from parquet_guard.core.models import (
    ResultState,
)


def test_adversarial_incremental_cache_after_file_deletion(tmp_path: Path) -> None:
    """Metamorphic test: deleting an incompatible file must restore consistent status under incremental caching.

    Previous bug: CachedFileInfo leaked `is_divergent=True` per-file, causing surviving files to falsely
    remain marked divergent after offending files were removed.
    """
    ds_dir = tmp_path / "dataset"
    ds_dir.mkdir(parents=True, exist_ok=True)
    cache_file = ds_dir / ".parquet_guard_cache.json"

    file_a = ds_dir / "part_a.parquet"
    file_b = ds_dir / "part_b.parquet"

    # File A: int32
    t_a = pa.Table.from_arrays([pa.array([1, 2, 3], type=pa.int32())], names=["col"])
    pq.write_table(t_a, str(file_a))

    # File B: string (incompatible with int32)
    t_b = pa.Table.from_arrays([pa.array(["x", "y", "z"], type=pa.string())], names=["col"])
    pq.write_table(t_b, str(file_b))

    # Run 1: Cold audit with incremental caching -> Must detect incompatibility
    res1 = audit_dataset(ds_dir, incremental=True, cache_path=cache_file)
    assert res1.is_consistent is False
    assert res1.is_compatible is False
    assert res1.result_state in (ResultState.INCOMPATIBLE, ResultState.INDETERMINATE)
    assert cache_file.exists()

    # Now delete File A! Only File B (string) remains.
    file_a.unlink()

    # Run 2: Incremental audit on surviving dataset -> Must evaluate to 100% consistent!
    res2 = audit_dataset(ds_dir, incremental=True, cache_path=cache_file)
    assert res2.is_consistent is True
    assert res2.is_compatible is True
    assert res2.distinct_schemas_count == 1
    assert res2.total_files_scanned == 1
    assert res2.cache_hits == 1  # File B was reused from cache without false divergence!
    assert res2.result_state == ResultState.VALID


def test_adversarial_map_keys_sorted_and_complex_nested_parsing() -> None:
    """Verify bracket-aware Map parsing with keys_sorted and nested inner types."""
    # 1. Parsing with keys_sorted
    params = _parse_map_params("map<string, int32, keys_sorted>")
    assert params == ("string", "int32")

    # 2. Parsing nested Map value
    params_nested = _parse_map_params("map<string, list<int32>, keys_sorted>")
    assert params_nested == ("string", "list<int32>")

    # 3. Safe widening inside map with keys_sorted
    assert is_safe_widening("map<string, int32, keys_sorted>", "map<string, int64, keys_sorted>") is True
    assert is_safe_widening("map<string, int64, keys_sorted>", "map<string, int32, keys_sorted>") is False
    assert (
        is_safe_widening("map<string, list<int32>, keys_sorted>", "map<string, list<int64>, keys_sorted>")
        is True
    )


def test_adversarial_decimal_scale_invariance_in_safe_widening() -> None:
    """Verify that decimal scale changes are strictly forbidden in is_safe_widening.

    In Parquet physical storage, values are stored as unscaled integers.
    Any scale modification alters the numeric interpretation of stored data.
    """
    # Scale changes must NOT be safe widening
    assert is_safe_widening("decimal128(10, 2)", "decimal128(12, 3)") is False
    assert is_safe_widening("decimal128(10, 2)", "decimal128(10, 1)") is False

    # Precision expansion with identical scale is safe
    assert is_safe_widening("decimal128(10, 2)", "decimal128(18, 2)") is True
    assert is_safe_widening("decimal128(10, 2)", "decimal256(38, 2)") is True


def test_adversarial_list_inner_parsing_across_container_variants() -> None:
    """Verify inner type extraction across List, LargeList, and FixedSizeList."""
    assert _parse_list_inner("list<int32>") == "int32"
    assert _parse_list_inner("list<item: int32>") == "int32"
    assert _parse_list_inner("large_list<item: string>") == "string"
    assert _parse_list_inner("fixed_size_list<item: float32>[128]") == "float32"
    assert _parse_list_inner("fixed_size_list<float64>[512]") == "float64"


def test_adversarial_contract_validation_rejections(tmp_path: Path) -> None:
    """Ensure invalid, malformed, or malicious contract schemas are strictly rejected."""
    # 1. Invalid JSON
    bad_json = tmp_path / "bad_syntax.json"
    bad_json.write_text("{not valid json", encoding="utf-8")
    with pytest.raises(ContractValidationError, match="Invalid JSON syntax"):
        load_contract(bad_json)
    is_valid, err = validate_contract(bad_json)
    assert is_valid is False
    assert "Invalid JSON syntax" in str(err)

    # 2. Unsupported contract version
    unsupported_ver = tmp_path / "bad_version.json"
    unsupported_ver.write_text(
        json.dumps({"version": "99.0", "fields": [{"name": "x", "type": "int32"}]}),
        encoding="utf-8",
    )
    with pytest.raises(ContractValidationError, match="Unsupported contract version"):
        load_contract(unsupported_ver)

    # 3. Extra unknown keys (forbid extra)
    extra_keys = tmp_path / "extra_keys.json"
    extra_keys.write_text(
        json.dumps(
            {
                "version": "1.0",
                "fields": [{"name": "x", "type": "int32"}],
                "unknown_injected_key": "malicious",
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ContractValidationError):
        load_contract(extra_keys)

    # 4. Empty fields list
    empty_fields = tmp_path / "empty_fields.json"
    empty_fields.write_text(
        json.dumps({"version": "1.0", "fields": []}),
        encoding="utf-8",
    )
    with pytest.raises(ContractValidationError, match="SchemaContract cannot be empty"):
        load_contract(empty_fields)

    # 5. Duplicate columns at nested level
    dup_nested = tmp_path / "dup_nested.json"
    dup_nested.write_text(
        json.dumps(
            {
                "version": "1.0",
                "fields": [
                    {
                        "name": "user",
                        "type": "struct",
                        "children": [
                            {"name": "email", "type": "string"},
                            {"name": "email", "type": "string"},
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ContractValidationError, match="duplicate column path"):
        load_contract(dup_nested)


def test_adversarial_cli_exit_codes_and_sarif_export(tmp_path: Path) -> None:
    """Test CLI exit codes for success (0), regression (1), and error/usage (2) with SARIF output."""
    runner = CliRunner()

    # Create baseline and drifting parquet files
    f_ok = tmp_path / "ok.parquet"
    f_drift = tmp_path / "drift.parquet"

    t_ok = pa.Table.from_arrays([pa.array([1, 2, 3], type=pa.int64())], names=["user_id"])
    pq.write_table(t_ok, str(f_ok))

    t_drift = pa.Table.from_arrays([pa.array([1, 2, 3], type=pa.int32())], names=["user_id"])
    pq.write_table(t_drift, str(f_drift))

    contract_path = tmp_path / "contract.json"
    runner.invoke(main, ["init-contract", str(f_ok), "-o", str(contract_path)])
    assert contract_path.exists()

    # 1. Check matching file -> exit code 0
    res_ok = runner.invoke(main, ["check", str(f_ok), "-c", str(contract_path)])
    assert res_ok.exit_code == 0

    # 2. Check breaking file in permissive mode (int64 -> int32 narrowing is breaking) -> exit code 1
    sarif_out = tmp_path / "report.sarif"
    res_drift = runner.invoke(
        main,
        ["check", str(f_drift), "-c", str(contract_path), "--format", "sarif", "-o", str(sarif_out)],
    )
    assert res_drift.exit_code == 1
    assert sarif_out.exists()

    # Validate SARIF JSON structure
    sarif_data = json.loads(sarif_out.read_text(encoding="utf-8"))
    assert sarif_data["version"] == "2.1.0"
    assert len(sarif_data["runs"]) == 1
    assert sarif_data["runs"][0]["tool"]["driver"]["name"] == "parquet-guard"
    assert len(sarif_data["runs"][0]["results"]) >= 1

    # 3. Missing file or non-existent path -> exit code 2
    res_missing = runner.invoke(main, ["check", "non_existent.parquet", "-c", str(contract_path)])
    assert res_missing.exit_code == 2

    # 4. Validate contract command
    res_val = runner.invoke(main, ["validate-contract", str(contract_path)])
    assert res_val.exit_code == 0
