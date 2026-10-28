"""Data-driven test suite verifying the real-world schema drift corpus and rule-vs-runtime behavior."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from parquet_guard.core.inspector import extract_contract
from parquet_guard.verification import (
    DuckDBEngineAdapter,
    EngineProbeOperation,
    PolarsEngineAdapter,
    verify_engine_compatibility,
)

CORPUS_DIR = Path(__file__).parent / "corpus"
MANIFEST_PATH = CORPUS_DIR / "corpus_manifest.json"


def load_manifest():
    if not MANIFEST_PATH.exists():
        pytest.skip("Corpus manifest does not exist. Run generate_corpus.py first.")
    with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.mark.parametrize("case", load_manifest(), ids=lambda c: c["case_id"])
def test_corpus_case_engine_verification(case: dict) -> None:
    baseline_path = CORPUS_DIR / case["baseline_file"]
    target_path = CORPUS_DIR / case["target_file"]

    assert baseline_path.exists(), f"Baseline file missing for {case['case_id']}"
    assert target_path.exists(), f"Target file missing for {case['case_id']}"

    contract = None
    if case["expected_metadata_verdict"] != "ERROR":
        try:
            contract = extract_contract(baseline_path)
        except Exception:
            contract = None

    op = EngineProbeOperation(case.get("probe_operation", "READ"))

    if op == EngineProbeOperation.SCHEMA_UNION:
        probe_targets = [str(CORPUS_DIR / f) for f in case["probe_files"]]
        report = verify_engine_compatibility(
            target=probe_targets,
            expected_contract=contract,
            engines=["pyarrow", "duckdb", "polars"],
            operations=[EngineProbeOperation.SCHEMA_UNION],
            allow_row_probe=True,
        )
    elif op == EngineProbeOperation.CAST:
        report = verify_engine_compatibility(
            target=str(target_path),
            expected_contract=contract,
            engines=["pyarrow", "duckdb", "polars"],
            operations=[EngineProbeOperation.CAST],
            allow_row_probe=True,
        )
    else:  # READ
        report = verify_engine_compatibility(
            target=str(target_path),
            expected_contract=contract,
            engines=["pyarrow", "duckdb", "polars"],
            operations=[EngineProbeOperation.READ],
            allow_row_probe=True,
        )

    # 1. Assert metadata verdict matches expected
    assert report.metadata_verdict.value == case["expected_metadata_verdict"], (
        f"Metadata verdict mismatch in {case['case_id']}: "
        f"got {report.metadata_verdict.value}, expected {case['expected_metadata_verdict']}"
    )

    # 2. Check individual engine runtime results
    engine_results_by_name = {r.engine_name: r for r in report.engine_results}

    # PyArrow
    pa_res = engine_results_by_name.get("pyarrow")
    assert pa_res is not None
    assert pa_res.runtime_verdict.value == case["expected_pyarrow_verdict"], (
        f"PyArrow mismatch in {case['case_id']}: got {pa_res.runtime_verdict.value}, "
        f"expected {case['expected_pyarrow_verdict']}. Error: {pa_res.error_message}"
    )

    # DuckDB
    ddb_res = engine_results_by_name.get("duckdb")
    if DuckDBEngineAdapter().is_available() and ddb_res:
        assert ddb_res.runtime_verdict.value == case["expected_duckdb_verdict"], (
            f"DuckDB mismatch in {case['case_id']}: got {ddb_res.runtime_verdict.value}, "
            f"expected {case['expected_duckdb_verdict']}. Error: {ddb_res.error_message}"
        )

    # Polars
    pl_res = engine_results_by_name.get("polars")
    if PolarsEngineAdapter().is_available() and pl_res:
        assert pl_res.runtime_verdict.value == case["expected_polars_verdict"], (
            f"Polars mismatch in {case['case_id']}: got {pl_res.runtime_verdict.value}, "
            f"expected {case['expected_polars_verdict']}. Error: {pl_res.error_message}"
        )

    # 3. Ensure no undetected optimistic false-safety bugs!
    assert not report.has_rule_optimistic_disagreement, (
        f"Hazardous RULE_OPTIMISTIC disagreement detected in {case['case_id']}! "
        f"Static rule claimed safety, but engine execution failed. Disagreements: {report.disagreements}"
    )
