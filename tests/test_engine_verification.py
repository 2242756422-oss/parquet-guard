"""Comprehensive test suite for the engine verification layer across PyArrow, DuckDB, and Polars."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from parquet_guard.core.inspector import extract_contract
from parquet_guard.verification import (
    DuckDBEngineAdapter,
    EngineProbeOperation,
    PolarsEngineAdapter,
    PyArrowEngineAdapter,
    RuntimeVerdict,
    ValidationScope,
    classify_disagreement,
    format_engine_verification_json,
    format_engine_verification_markdown,
    format_engine_verification_terminal,
    list_available_engines,
    quote_sql_identifier,
    verify_engine_compatibility,
)
from parquet_guard.verification.models import (
    DisagreementClassification,
    MetadataVerdict,
)


@pytest.fixture
def sample_parquet(tmp_path: Path) -> Path:
    p = tmp_path / "sample.parquet"
    tbl = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "name": pa.array(["alice", "bob", "carol"], type=pa.string()),
            "score": pa.array([10.5, 20.0, 30.5], type=pa.float64()),
        }
    )
    pq.write_table(tbl, p)
    return p


def test_list_available_engines() -> None:
    engines = list_available_engines()
    assert "pyarrow" in engines
    assert "duckdb" in engines
    assert "polars" in engines


def test_quote_sql_identifier_sanitization() -> None:
    assert quote_sql_identifier("normal_col") == '"normal_col"'
    assert quote_sql_identifier('col"with"quotes') == '"col""with""quotes"'
    assert quote_sql_identifier("col'; DROP TABLE users; --") == '"col\'; DROP TABLE users; --"'
    with pytest.raises(TypeError):
        quote_sql_identifier(123)  # type: ignore[arg-type]


def test_pyarrow_engine_read_and_cast(sample_parquet: Path) -> None:
    adapter = PyArrowEngineAdapter()
    assert adapter.is_available()
    assert adapter.version() is not None

    # Read probe
    res_read = adapter.probe_read([str(sample_parquet)], expected_columns=["id", "name"])
    assert res_read.runtime_verdict == RuntimeVerdict.PASS
    assert res_read.duration_ms >= 0

    # Read missing column
    res_missing = adapter.probe_read([str(sample_parquet)], expected_columns=["missing_col"])
    assert res_missing.runtime_verdict == RuntimeVerdict.FAIL
    assert res_missing.error_category in ("SCHEMA_INCOMPATIBILITY", "ENGINE_RUNTIME_ERROR")

    # Cast probe
    res_cast = adapter.probe_cast(str(sample_parquet), {"id": "int32", "score": "float32"})
    assert res_cast.runtime_verdict == RuntimeVerdict.PASS


def test_duckdb_engine_read_and_union(sample_parquet: Path, tmp_path: Path) -> None:
    adapter = DuckDBEngineAdapter()
    assert adapter.is_available()
    assert adapter.version() is not None

    # Read probe
    res_read = adapter.probe_read([str(sample_parquet)], expected_columns=["id", "score"])
    assert res_read.runtime_verdict == RuntimeVerdict.PASS
    assert 'SELECT "id", "score"' in (res_read.executed_expression or "")

    # Multi-file union probe
    p2 = tmp_path / "part2.parquet"
    tbl2 = pa.table(
        {
            "id": pa.array([4, 5], type=pa.int64()),
            "name": pa.array(["dan", "erin"], type=pa.string()),
            "extra_col": pa.array(["x", "y"], type=pa.string()),
        }
    )
    pq.write_table(tbl2, p2)

    res_union = adapter.probe_schema_union([str(sample_parquet), str(p2)])
    assert res_union.runtime_verdict == RuntimeVerdict.PASS
    assert res_union.details.get("files_unified") == 2


def test_polars_engine_read_and_cast(sample_parquet: Path) -> None:
    adapter = PolarsEngineAdapter()
    assert adapter.is_available()
    assert adapter.version() is not None

    # Read probe
    res_read = adapter.probe_read([str(sample_parquet)], expected_columns=["name", "score"])
    assert res_read.runtime_verdict == RuntimeVerdict.PASS

    # Cast probe
    res_cast = adapter.probe_cast(str(sample_parquet), {"score": "float32"})
    assert res_cast.runtime_verdict == RuntimeVerdict.PASS


def test_classify_disagreement_matrix() -> None:
    # Both agree on safety
    assert (
        classify_disagreement(MetadataVerdict.COMPATIBLE, RuntimeVerdict.PASS)
        == DisagreementClassification.AGREEMENT
    )
    # Both agree on failure
    assert (
        classify_disagreement(MetadataVerdict.INCOMPATIBLE, RuntimeVerdict.FAIL)
        == DisagreementClassification.AGREEMENT
    )
    # Rule flagged incompatibility, but engine succeeded
    assert (
        classify_disagreement(MetadataVerdict.INCOMPATIBLE, RuntimeVerdict.PASS)
        == DisagreementClassification.RULE_CONSERVATIVE
    )
    # Rule said safe, but engine runtime failed -> HAZARDOUS
    assert (
        classify_disagreement(MetadataVerdict.COMPATIBLE, RuntimeVerdict.FAIL)
        == DisagreementClassification.RULE_OPTIMISTIC
    )
    # Engine not run or unsupported
    assert (
        classify_disagreement(MetadataVerdict.COMPATIBLE, RuntimeVerdict.NOT_RUN)
        == DisagreementClassification.INCONCLUSIVE
    )


def test_verify_engine_compatibility_runner(sample_parquet: Path) -> None:
    contract = extract_contract(sample_parquet)
    report = verify_engine_compatibility(
        str(sample_parquet),
        expected_contract=contract,
        engines=["pyarrow", "duckdb", "polars"],
        operations=[EngineProbeOperation.READ, EngineProbeOperation.CAST],
        scope=ValidationScope.SYNTHETIC_DATA,
    )
    assert report.overall_compatible is True
    assert report.metadata_verdict == MetadataVerdict.COMPATIBLE
    assert report.runtime_verdict == RuntimeVerdict.PASS
    assert len(report.engine_results) >= 3

    # Formatters
    term = format_engine_verification_terminal(report)
    assert "ENGINE RUNTIME VERIFICATION REPORT" in term
    assert "pyarrow" in term
    assert "duckdb" in term
    assert "polars" in term

    md = format_engine_verification_markdown(report)
    assert "## 🔍 Parquet Guard Engine Verification Report" in md
    assert "| `pyarrow` |" in md

    js = format_engine_verification_json(report)
    data = json.loads(js)
    assert data["target_path"] == str(sample_parquet)
    assert data["overall_compatible"] is True
