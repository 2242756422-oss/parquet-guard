"""Adversarial reliability hardening and pre-merge validation tests for Project Atlas PR #6.

Covers the four critical engineering areas required for engineering acceptance:
1. Part I (P0): Strict Python API Authorization Validation
   - Rejection of all non-boolean values for allow_row_probe with TypeError before any row reads
   - Verification of zero row reads on unauthorized calls via engine spies
   - Legitimate row probes permitted only with explicit boolean True
2. Part II (P1): Elimination of False-Positive CAST Results Across All Engines
   - Missing target columns (all or subset) fail closed with RuntimeVerdict.FAIL across PyArrow, DuckDB, Polars
   - Detailed execution accounting (requested, attempted, successful, failed_or_skipped)
   - Runtime conversion failure -> FAIL
   - Unsupported type mapping -> UNSUPPORTED
   - Empty dataset -> distinguishes schema castability with explicit empty_dataset notice
   - Overall runner prevents PASS when any CAST probe fails
3. Part III (P1): Aggregate Row-Probe Budgets
   - validate_total_row_budget rejection of invalid types, 0, negative values
   - Single-invocation budget tracking across multiple engines and operations
   - Budget exhaustion causes subsequent probes to be NOT_RUN with ROW_BUDGET_EXHAUSTED
   - Aggregate compatibility is False when budget exhaustion leaves probes unverified
4. Part IV (P1): Honest Multi-Partition SCHEMA_UNION Coverage
   - Partition 1 satisfying LIMIT cannot mask data defects or corruption in partition 2
   - Explicit partition coverage accounting: files_requested, files_metadata_inspected, files_row_probed, files_skipped, coverage_complete, coverage_limitations
   - Partial partition coverage cannot claim overall compatibility
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from parquet_guard.core.models import FieldDefinition, SchemaContract
from parquet_guard.verification import (
    DuckDBEngineAdapter,
    EngineProbeOperation,
    PolarsEngineAdapter,
    PyArrowEngineAdapter,
    RuntimeVerdict,
    verify_engine_compatibility,
)
from parquet_guard.verification.adapters import validate_total_row_budget


@pytest.fixture
def sample_file(tmp_path: Path) -> Path:
    """Create a standard small Parquet file for testing."""
    p = tmp_path / "sample.parquet"
    tbl = pa.table(
        {
            "id": [1, 2, 3],
            "name": ["alice", "bob", "charlie"],
            "score": [10.5, 20.0, 30.2],
        }
    )
    pq.write_table(tbl, str(p))
    return p


@pytest.fixture
def empty_parquet_file(tmp_path: Path) -> Path:
    """Create an empty Parquet file with schema but 0 rows."""
    p = tmp_path / "empty.parquet"
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
            ("score", pa.float64()),
        ]
    )
    tbl = schema.empty_table()
    pq.write_table(tbl, str(p))
    return p


# ===========================================================================
# PART I (P0): Strict Python API Authorization Validation
# ===========================================================================


@pytest.mark.parametrize(
    "invalid_auth",
    [
        "false",
        "true",
        "0",
        "1",
        1,
        0,
        None,
        [],
        {},
        1.0,
        object(),
    ],
)
def test_p0_api_auth_rejects_non_boolean_types(sample_file: Path, invalid_auth: object) -> None:
    """P0 Invariant: allow_row_probe must strictly be a bool. Any other type raises TypeError.

    Ensures zero row-level reads occur across pyarrow, duckdb, and polars when invalid auth is passed.
    """
    with (
        patch("pyarrow.parquet.ParquetFile.iter_batches") as spy_arrow,
        patch("duckdb.connect") as spy_duck,
        patch("polars.scan_parquet") as spy_polars,
    ):
        with pytest.raises(TypeError, match="allow_row_probe must be an explicit boolean"):
            verify_engine_compatibility(
                files=sample_file,
                allow_row_probe=invalid_auth,  # type: ignore[arg-type]
            )

        # Absolutely zero engine row-probe entry points may be touched
        assert spy_arrow.call_count == 0
        assert spy_duck.call_count == 0
        assert spy_polars.call_count == 0


def test_p0_api_auth_explicit_false_denies_row_reads(sample_file: Path) -> None:
    """P0 Invariant: Explicit allow_row_probe=False performs metadata inspection but zero row reads."""
    with (
        patch("pyarrow.parquet.ParquetFile.iter_batches") as spy_arrow,
        patch("duckdb.connect") as spy_duck,
        patch("polars.scan_parquet") as spy_polars,
    ):
        report = verify_engine_compatibility(
            files=sample_file,
            allow_row_probe=False,
        )

        assert spy_arrow.call_count == 0
        assert spy_duck.call_count == 0
        assert spy_polars.call_count == 0
        assert report.runtime_verdict == RuntimeVerdict.NOT_RUN
        assert report.overall_compatible is False
        assert any("allow_row_probe=False" in lim for lim in report.limitations)


def test_p0_api_auth_explicit_true_permits_bounded_probes(sample_file: Path) -> None:
    """P0 Invariant: Explicit allow_row_probe=True authorizes bounded engine execution."""
    report = verify_engine_compatibility(
        files=sample_file,
        allow_row_probe=True,
        row_limit=5,
    )
    assert report.runtime_verdict == RuntimeVerdict.PASS
    assert report.overall_compatible is True
    assert report.total_rows_probed > 0


# ===========================================================================
# PART II (P1): Elimination of False-Positive CAST Results Across All Engines
# ===========================================================================


@pytest.mark.parametrize(
    "engine_cls",
    [PyArrowEngineAdapter, DuckDBEngineAdapter, PolarsEngineAdapter],
)
def test_p1_cast_all_target_fields_missing_fails_closed(
    sample_file: Path,
    engine_cls: type,
) -> None:
    """P1: When all requested target fields are absent, adapter must return FAIL (not PASS)."""
    adapter = engine_cls()
    if not adapter.is_available():
        pytest.skip(f"Engine {adapter.name} unavailable.")

    missing_types: Dict[str, str] = {
        "absent_column_a": "int64",
        "absent_column_b": "string",
    }
    res = adapter.probe_cast(str(sample_file), missing_types, row_limit=5)

    assert res.runtime_verdict == RuntimeVerdict.FAIL
    assert res.error_category == "MISSING_TARGET_COLUMNS"
    assert "absent_column_a" in (res.error_message or "")
    assert res.details.get("requested_cast_fields") == ["absent_column_a", "absent_column_b"]
    assert res.details.get("attempted_cast_fields") == []
    assert res.details.get("successful_cast_fields") == []
    assert res.details.get("failed_or_skipped_cast_fields") == ["absent_column_a", "absent_column_b"]


@pytest.mark.parametrize(
    "engine_cls",
    [PyArrowEngineAdapter, DuckDBEngineAdapter, PolarsEngineAdapter],
)
def test_p1_cast_partial_target_fields_missing_fails_closed(
    sample_file: Path,
    engine_cls: type,
) -> None:
    """P1: When some requested fields exist but others are missing, adapter must return FAIL."""
    adapter = engine_cls()
    if not adapter.is_available():
        pytest.skip(f"Engine {adapter.name} unavailable.")

    partial_types: Dict[str, str] = {
        "id": "int64",  # exists
        "absent_col": "string",  # missing!
    }
    res = adapter.probe_cast(str(sample_file), partial_types, row_limit=5)

    assert res.runtime_verdict == RuntimeVerdict.FAIL
    assert res.error_category == "MISSING_TARGET_COLUMNS"
    assert "absent_col" in res.details.get("failed_or_skipped_cast_fields", [])


@pytest.mark.parametrize(
    "engine_cls",
    [PyArrowEngineAdapter, DuckDBEngineAdapter, PolarsEngineAdapter],
)
def test_p1_cast_unsupported_type_mapping(
    sample_file: Path,
    engine_cls: type,
) -> None:
    """P1: Unmappable target types must yield UNSUPPORTED, not PASS or unhandled error."""
    adapter = engine_cls()
    if not adapter.is_available():
        pytest.skip(f"Engine {adapter.name} unavailable.")

    unsupported_types: Dict[str, str] = {
        "id": "unsupported_exotic_type_42",
    }
    res = adapter.probe_cast(str(sample_file), unsupported_types, row_limit=5)

    assert res.runtime_verdict == RuntimeVerdict.UNSUPPORTED
    assert res.error_category == "UNSUPPORTED_TYPE_MAPPING"
    assert "id" in res.details.get("failed_or_skipped_cast_fields", [])


@pytest.mark.parametrize(
    "engine_cls",
    [PyArrowEngineAdapter, DuckDBEngineAdapter, PolarsEngineAdapter],
)
def test_p1_cast_conversion_failure_yields_fail(
    sample_file: Path,
    engine_cls: type,
) -> None:
    """P1: Actual conversion error (e.g. string to int64) must return FAIL."""
    adapter = engine_cls()
    if not adapter.is_available():
        pytest.skip(f"Engine {adapter.name} unavailable.")

    bad_cast_types: Dict[str, str] = {
        "name": "int64",  # 'alice' cannot be converted to int64
    }
    res = adapter.probe_cast(str(sample_file), bad_cast_types, row_limit=5)

    assert res.runtime_verdict == RuntimeVerdict.FAIL
    assert "name" in res.details.get("failed_or_skipped_cast_fields", [])


@pytest.mark.parametrize(
    "engine_cls",
    [PyArrowEngineAdapter, DuckDBEngineAdapter, PolarsEngineAdapter],
)
def test_p1_cast_empty_target_types_dict_inconclusive(
    sample_file: Path,
    engine_cls: type,
) -> None:
    """P1: Empty target_field_types dict passed directly to probe_cast yields INCONCLUSIVE."""
    adapter = engine_cls()
    if not adapter.is_available():
        pytest.skip(f"Engine {adapter.name} unavailable.")

    res = adapter.probe_cast(str(sample_file), {}, row_limit=5)
    assert res.runtime_verdict == RuntimeVerdict.INCONCLUSIVE
    assert res.error_category == "EMPTY_TARGET_TYPES"


@pytest.mark.parametrize(
    "engine_cls",
    [PyArrowEngineAdapter, DuckDBEngineAdapter, PolarsEngineAdapter],
)
def test_p1_cast_empty_dataset_distinguishes_schema_level(
    empty_parquet_file: Path,
    engine_cls: type,
) -> None:
    """P1: Empty dataset validates schema castability without claiming data value conversion."""
    adapter = engine_cls()
    if not adapter.is_available():
        pytest.skip(f"Engine {adapter.name} unavailable.")

    valid_types: Dict[str, str] = {
        "id": "int64",
        "score": "float64",
    }
    res = adapter.probe_cast(str(empty_parquet_file), valid_types, row_limit=5)

    assert res.runtime_verdict == RuntimeVerdict.PASS
    assert res.details.get("empty_dataset") is True
    assert "empty_dataset_schema_cast" in res.tested_invariants


def test_p1_runner_cast_failure_prevents_overall_pass(sample_file: Path) -> None:
    """P1: When READ passes but CAST fails due to missing column, overall compatibility must be False."""
    contract = SchemaContract(
        name="test_contract",
        version="1.0",
        fields=[
            FieldDefinition(name="id", type="int64", nullable=False),
            FieldDefinition(name="missing_expected_col", type="string", nullable=False),
        ],
    )
    report = verify_engine_compatibility(
        files=sample_file,
        contract=contract,
        operations=[EngineProbeOperation.READ, EngineProbeOperation.CAST],
        allow_row_probe=True,
    )

    assert report.runtime_verdict == RuntimeVerdict.FAIL
    assert report.overall_compatible is False
    cast_results = [r for r in report.engine_results if r.operation == EngineProbeOperation.CAST]
    assert all(r.runtime_verdict == RuntimeVerdict.FAIL for r in cast_results)


# ===========================================================================
# PART III (P1): Aggregate Row-Probe Budgets
# ===========================================================================


def test_p1_budget_validator() -> None:
    """P1 Invariant: validate_total_row_budget enforces valid bounds."""
    assert validate_total_row_budget(None) is None
    assert validate_total_row_budget(100) == 100
    assert validate_total_row_budget(1) == 1

    with pytest.raises(ValueError):
        validate_total_row_budget(0)
    with pytest.raises(ValueError):
        validate_total_row_budget(-10)
    with pytest.raises(ValueError):
        validate_total_row_budget(True)
    with pytest.raises(ValueError):
        validate_total_row_budget(False)
    with pytest.raises(ValueError):
        validate_total_row_budget("100")


def test_p1_aggregate_row_budget_exhaustion(sample_file: Path) -> None:
    """P1 Invariant: Invocation-scoped row budget halts subsequent probes when exhausted."""
    # Set a tiny budget of 3 rows across multiple engines and operations
    report = verify_engine_compatibility(
        files=sample_file,
        engines=["pyarrow", "duckdb", "polars"],
        operations=[EngineProbeOperation.READ, EngineProbeOperation.CAST],
        contract=SchemaContract(
            name="test",
            version="1.0",
            fields=[FieldDefinition(name="id", type="int64", nullable=False)],
        ),
        row_limit=3,
        max_total_rows=3,  # Budget only allows first probe to read
        allow_row_probe=True,
    )

    # Some probes must have been skipped due to exhaustion
    exhausted_probes = [r for r in report.engine_results if r.error_category == "ROW_BUDGET_EXHAUSTED"]
    assert len(exhausted_probes) > 0
    assert all(r.runtime_verdict == RuntimeVerdict.NOT_RUN for r in exhausted_probes)
    # Total runtime verdict cannot be PASS when required probes were NOT_RUN
    assert report.runtime_verdict != RuntimeVerdict.PASS
    assert report.overall_compatible is False
    assert report.total_rows_budget == 3
    assert report.total_rows_probed >= 3
    assert any("budget exhaustion" in lim for lim in report.limitations)


# ===========================================================================
# PART IV (P1): Multi-Partition Coverage in SCHEMA_UNION
# ===========================================================================


def test_p1_schema_union_downstream_partition_defect_detected(tmp_path: Path) -> None:
    """P1: A first partition large enough to satisfy row_limit CANNOT mask defects in partition 2."""
    p1 = tmp_path / "part_1.parquet"
    p2 = tmp_path / "part_2.parquet"

    # Partition 1 has 50 valid rows (sufficient to satisfy row_limit=5)
    tbl1 = pa.table({"id": list(range(50)), "val": [{"a": i} for i in range(50)]})
    pq.write_table(tbl1, str(p1))

    # Partition 2 has incompatible type (int vs struct for 'val')
    tbl2 = pa.table({"id": [100, 101], "val": [999, 888]})
    pq.write_table(tbl2, str(p2))

    for engine_cls in (PyArrowEngineAdapter, DuckDBEngineAdapter, PolarsEngineAdapter):
        adapter = engine_cls()
        if not adapter.is_available():
            continue

        res = adapter.probe_schema_union([str(p1), str(p2)], row_limit=5)
        # Because partition 2 is defective, union probe must detect the issue and fail
        assert res.runtime_verdict in (RuntimeVerdict.FAIL, RuntimeVerdict.INCONCLUSIVE), (
            f"Engine {adapter.name} masked downstream partition defect!"
        )


def test_p1_schema_union_complete_coverage_accounting(tmp_path: Path) -> None:
    """P1: Full partition coverage is verified and accurately recorded in probe details."""
    p1 = tmp_path / "p1.parquet"
    p2 = tmp_path / "p2.parquet"

    tbl1 = pa.table({"id": [1, 2], "val": ["a", "b"]})
    tbl2 = pa.table({"id": [3, 4], "val": ["c", "d"]})
    pq.write_table(tbl1, str(p1))
    pq.write_table(tbl2, str(p2))

    for engine_cls in (PyArrowEngineAdapter, DuckDBEngineAdapter, PolarsEngineAdapter):
        adapter = engine_cls()
        if not adapter.is_available():
            continue

        res = adapter.probe_schema_union([str(p1), str(p2)], row_limit=5)
        assert res.runtime_verdict == RuntimeVerdict.PASS
        assert res.details.get("files_requested") == 2
        assert res.details.get("files_metadata_inspected") == 2
        assert res.details.get("files_row_probed") == 2
        assert res.details.get("files_skipped") == 0
        assert res.details.get("coverage_complete") is True
        assert res.details.get("coverage_limitations") == []


def test_p1_schema_union_single_file_reports_incomplete_coverage(sample_file: Path) -> None:
    """P1: Single-file SCHEMA_UNION must report coverage_complete=False and INCONCLUSIVE verdict."""
    for engine_cls in (PyArrowEngineAdapter, DuckDBEngineAdapter, PolarsEngineAdapter):
        adapter = engine_cls()
        if not adapter.is_available():
            continue

        res = adapter.probe_schema_union([str(sample_file)], row_limit=5)
        assert res.runtime_verdict == RuntimeVerdict.INCONCLUSIVE
        assert res.details.get("coverage_complete") is False
        assert len(res.details.get("coverage_limitations", [])) > 0
