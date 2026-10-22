"""Comprehensive verification and regression test suite for parquet-guard v0.2.0:
1. P0-1: FixedSizeList length mismatch detection (strict structural equality).
2. P0-2: Dictionary ordered attribute differentiation (strict vs permissive).
3. P0-3: Map keys_sorted attribute differentiation (strict vs permissive).
4. P0-4: Strict contract validation (rejecting unsupported versions, extra fields, empty fields).
5. Contract round-trip invariant: extract -> save -> load -> compare.
6. Incremental auditing: cold run, cache hits, partial invalidation, cache integrity.
7. SARIF 2.1.0 report generation for CI/CD Code Scanning.
8. Distinct ResultState and ErrorCode classification.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from click.testing import CliRunner

from parquet_guard import (
    ChangeType,
    ErrorCode,
    FieldDefinition,
    ResultState,
    SchemaContract,
    Severity,
    audit_dataset,
    compare_schemas,
    extract_contract,
    format_audit_sarif,
    format_diff_sarif,
    inspect_schema,
    load_contract,
    save_contract,
    validate_contract,
)
from parquet_guard.cli import main
from parquet_guard.core.cache import load_audit_cache


def test_p0_1_fixed_size_list_structural_mismatch(tmp_path: Path):
    """P0-1 Regression: FixedSizeList length mismatch must be strictly rejected as BREAKING."""
    f1 = pa.field("coords", pa.list_(pa.float32(), list_size=2))
    f2 = pa.field("coords", pa.list_(pa.float32(), list_size=3))

    c1 = SchemaContract(fields=[FieldDefinition(name="coords", type=str(f1.type), list_size=2)])
    c2 = SchemaContract(fields=[FieldDefinition(name="coords", type=str(f2.type), list_size=3)])

    # Strict mode must fail compatibility with CONTAINER_PARAM_CHANGED
    diff_strict = compare_schemas(c2, c1, mode="strict")
    assert diff_strict.is_compatible is False
    assert diff_strict.has_breaking_changes is True
    assert diff_strict.result_state == ResultState.INCOMPATIBLE
    assert any(
        d.change_type == ChangeType.CONTAINER_PARAM_CHANGED
        and d.severity == Severity.BREAKING
        and d.error_code == ErrorCode.CONTAINER_PARAM_CHANGED
        for d in diff_strict.diffs
    )

    # Permissive mode must ALSO reject fixed-size length change because memory layout differs
    diff_perm = compare_schemas(c2, c1, mode="permissive")
    assert diff_perm.is_compatible is False
    assert diff_perm.has_breaking_changes is True

    # Real Parquet end-to-end check
    f_path1 = tmp_path / "fsl2.parquet"
    f_path2 = tmp_path / "fsl3.parquet"
    t1 = pa.Table.from_arrays([pa.array([[1.0, 2.0]], type=f1.type)], schema=pa.schema([f1]))
    t2 = pa.Table.from_arrays([pa.array([[1.0, 2.0, 3.0]], type=f2.type)], schema=pa.schema([f2]))
    pq.write_table(t1, str(f_path1))
    pq.write_table(t2, str(f_path2))

    s1 = inspect_schema(f_path1)
    s2 = inspect_schema(f_path2)
    assert s1.fields[0].list_size == 2
    assert s2.fields[0].list_size == 3

    diff_real = compare_schemas(s2, s1, mode="strict")
    assert diff_real.is_compatible is False
    assert diff_real.has_breaking_changes is True


def test_p0_2_dictionary_ordered_attribute(tmp_path: Path):
    """P0-2 Regression: Dictionary ordered attribute must be strictly checked."""
    d_unordered = pa.dictionary(pa.int32(), pa.string(), ordered=False)
    d_ordered = pa.dictionary(pa.int32(), pa.string(), ordered=True)

    f_un = pa.field("category", d_unordered)
    f_ord = pa.field("category", d_ordered)

    c_un = SchemaContract(fields=[FieldDefinition(name="category", type=str(f_un.type), ordered=False)])
    c_ord = SchemaContract(fields=[FieldDefinition(name="category", type=str(f_ord.type), ordered=True)])

    # Strict mode diff: changing ordered flag is BREAKING
    strict_diff = compare_schemas(c_ord, c_un, mode="strict")
    assert strict_diff.is_compatible is False
    assert strict_diff.has_breaking_changes is True
    assert any(
        d.change_type == ChangeType.ORDERED_FLAG_CHANGED
        and d.severity == Severity.BREAKING
        and d.error_code == ErrorCode.ORDERED_FLAG_CHANGED
        for d in strict_diff.diffs
    )

    # Permissive mode diff: changing ordered flag produces a WARNING but is compatible
    perm_diff = compare_schemas(c_ord, c_un, mode="permissive")
    assert perm_diff.is_compatible is True
    assert perm_diff.has_warnings is True
    assert perm_diff.result_state == ResultState.DRIFTED
    assert any(
        d.change_type == ChangeType.ORDERED_FLAG_CHANGED and d.severity == Severity.WARNING
        for d in perm_diff.diffs
    )


def test_p0_3_map_keys_sorted_attribute():
    """P0-3 Regression: Map keys_sorted attribute must be differentiated."""
    m_unsorted = pa.map_(pa.string(), pa.int64(), keys_sorted=False)
    m_sorted = pa.map_(pa.string(), pa.int64(), keys_sorted=True)

    c_unsorted = SchemaContract(
        fields=[FieldDefinition(name="metrics", type=str(m_unsorted), keys_sorted=False)]
    )
    c_sorted = SchemaContract(fields=[FieldDefinition(name="metrics", type=str(m_sorted), keys_sorted=True)])

    # Strict mode diff: keys_sorted change is BREAKING
    strict_diff = compare_schemas(c_sorted, c_unsorted, mode="strict")
    assert strict_diff.is_compatible is False
    assert strict_diff.has_breaking_changes is True
    assert any(
        d.change_type == ChangeType.KEYS_SORTED_CHANGED
        and d.severity == Severity.BREAKING
        and d.error_code == ErrorCode.KEYS_SORTED_CHANGED
        for d in strict_diff.diffs
    )

    # Permissive mode diff: keys_sorted change produces WARNING
    perm_diff = compare_schemas(c_sorted, c_unsorted, mode="permissive")
    assert perm_diff.is_compatible is True
    assert perm_diff.has_warnings is True
    assert perm_diff.result_state == ResultState.DRIFTED
    assert any(
        d.change_type == ChangeType.KEYS_SORTED_CHANGED and d.severity == Severity.WARNING
        for d in perm_diff.diffs
    )


def test_p0_4_strict_contract_validation(tmp_path: Path):
    """P0-4 Regression: Reject unsupported contract versions, extra fields, and empty contracts."""
    # 1. Unsupported version
    bad_ver_path = tmp_path / "bad_version.json"
    bad_ver_path.write_text(json.dumps({"version": "999.0", "fields": [{"name": "id", "type": "int32"}]}))
    is_valid, err = validate_contract(bad_ver_path)
    assert is_valid is False
    assert "Unsupported contract version '999.0'" in str(err)

    # 2. Unknown properties (extra="forbid")
    extra_field_path = tmp_path / "extra_field.json"
    extra_field_path.write_text(
        json.dumps(
            {
                "version": "1.0",
                "unknown_policy": "strict",
                "fields": [{"name": "id", "type": "int32"}],
            }
        )
    )
    is_valid, err = validate_contract(extra_field_path)
    assert is_valid is False
    assert "Extra inputs are not permitted" in str(err)

    # 3. Empty fields list
    empty_fields_path = tmp_path / "empty_contract.json"
    empty_fields_path.write_text(json.dumps({"version": "1.0", "fields": []}))
    is_valid, err = validate_contract(empty_fields_path)
    assert is_valid is False
    assert "cannot be empty" in str(err)

    # 4. Valid contract passes
    valid_path = tmp_path / "valid_contract.json"
    valid_path.write_text(
        json.dumps(
            {
                "version": "1.0",
                "name": "events_contract",
                "fields": [{"name": "id", "type": "int64"}],
            }
        )
    )
    is_valid, err = validate_contract(valid_path)
    assert is_valid is True
    assert err is None

    # CLI validate-contract command
    runner = CliRunner()
    res_cli_bad = runner.invoke(main, ["validate-contract", str(bad_ver_path)])
    assert res_cli_bad.exit_code == 2

    res_cli_good = runner.invoke(main, ["validate-contract", str(valid_path)])
    assert res_cli_good.exit_code == 0


def test_contract_round_trip_invariant(tmp_path: Path):
    """Extract -> Save -> Load -> Compare round-trip invariant."""
    sample_file = tmp_path / "sample.parquet"
    st = pa.struct([pa.field("sub_a", pa.int32()), pa.field("sub_b", pa.string())])
    s = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("payload", st),
            pa.field("tags", pa.list_(pa.string())),
        ],
        metadata={b"author": b"test_suite"},
    )
    tbl = pa.Table.from_arrays(
        [
            pa.array([1]),
            pa.array([{"sub_a": 10, "sub_b": "ok"}], type=st),
            pa.array([["tag1", "tag2"]]),
        ],
        schema=s,
    )
    pq.write_table(tbl, str(sample_file))

    # 1. Extract contract
    extracted = extract_contract(sample_file, name="round_trip_test")

    # 2. Save contract to JSON
    json_path = tmp_path / "exported_contract.json"
    save_contract(extracted, json_path)

    # 3. Load contract from JSON
    loaded = load_contract(json_path)

    # 4. Invariant: extracted == loaded and compare_schemas yields zero diffs
    assert len(loaded.fields) == len(extracted.fields)
    assert loaded.metadata == extracted.metadata
    diff = compare_schemas(loaded, extracted, mode="strict", check_metadata=True, check_field_order=True)
    assert diff.is_compatible is True
    assert len(diff.diffs) == 0
    assert diff.result_state == ResultState.VALID


def test_incremental_audit_system(tmp_path: Path):
    """Verify incremental audit caching: cold run, cache hits, file modification, and force-full."""
    ds_dir = tmp_path / "dataset"
    ds_dir.mkdir()
    schema = pa.schema([pa.field("val", pa.int32())])

    # Create 5 partition files
    file_paths = []
    for i in range(5):
        fp = ds_dir / f"part_{i}.parquet"
        pq.write_table(pa.Table.from_arrays([[i]], schema=schema), str(fp))
        file_paths.append(fp)

    cache_file = ds_dir / ".parquet_guard_cache.json"

    # Run 1: Cold run with incremental enabled (all cache misses)
    res_cold = audit_dataset(ds_dir, mode="strict", incremental=True)
    assert res_cold.is_consistent is True
    assert res_cold.incremental is True
    assert res_cold.cache_hits == 0
    assert res_cold.cache_misses == 5
    assert cache_file.is_file()

    # Verify cache file structure on disk
    loaded_cache = load_audit_cache(cache_file)
    assert loaded_cache is not None
    assert len(loaded_cache.files) == 5

    # Run 2: Warm run with no changes (100% cache hits)
    res_warm = audit_dataset(ds_dir, mode="strict", incremental=True)
    assert res_warm.is_consistent is True
    assert res_warm.cache_hits == 5
    assert res_warm.cache_misses == 0

    # Modify exactly 1 file
    pq.write_table(pa.Table.from_arrays([[999]], schema=schema), str(file_paths[2]))

    # Run 3: Partially invalidated run (4 hits, 1 miss)
    res_partial = audit_dataset(ds_dir, mode="strict", incremental=True)
    assert res_partial.is_consistent is True
    assert res_partial.cache_hits == 4
    assert res_partial.cache_misses == 1

    # Run 4: Force full re-audit ignoring cache
    res_forced = audit_dataset(ds_dir, mode="strict", incremental=True, force_full=True)
    assert res_forced.is_consistent is True
    assert res_forced.cache_hits == 0
    assert res_forced.cache_misses == 5

    # CLI audit with --incremental and --force-full flags
    runner = CliRunner()
    cli_inc = runner.invoke(main, ["audit", str(ds_dir), "--incremental"])
    assert cli_inc.exit_code == 0
    assert "Incremental" in cli_inc.output


def test_sarif_report_generation(tmp_path: Path):
    """Verify SARIF 2.1.0 output formatting for diff and audit."""
    c_base = SchemaContract(fields=[FieldDefinition(name="id", type="int32")])
    c_target = SchemaContract(fields=[FieldDefinition(name="id", type="string")])

    report = compare_schemas(c_target, c_base, mode="permissive")
    sarif_str = format_diff_sarif(report)
    sarif_data = json.loads(sarif_str)

    assert sarif_data["version"] == "2.1.0"
    assert sarif_data["runs"][0]["tool"]["driver"]["name"] == "parquet-guard"
    assert len(sarif_data["runs"][0]["results"]) == 1
    assert sarif_data["runs"][0]["results"][0]["level"] == "error"

    # Audit SARIF check
    f = tmp_path / "t.parquet"
    pq.write_table(pa.Table.from_arrays([[1]], schema=pa.schema([pa.field("x", pa.int32())])), str(f))
    audit_res = audit_dataset(f, baseline_contract=c_base)
    audit_sarif_str = format_audit_sarif(audit_res)
    audit_sarif_data = json.loads(audit_sarif_str)
    assert audit_sarif_data["version"] == "2.1.0"
    assert len(audit_sarif_data["runs"][0]["results"]) >= 1
