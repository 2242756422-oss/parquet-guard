"""Pytest integration fixtures and assertions for Parquet Guard."""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Union

import pytest

from parquet_guard.core.compatibility import compare_schemas
from parquet_guard.core.inspector import inspect_schema, load_contract
from parquet_guard.core.models import SchemaContract, Severity
from parquet_guard.verification import (
    EngineProbeOperation,
    EngineVerificationReport,
    verify_engine_compatibility,
)
from parquet_guard.verification.models import RuntimeVerdict


def assert_schema_compatible(
    target: Union[str, Path, SchemaContract],
    baseline: Union[str, Path, SchemaContract],
    mode: str = "permissive",
    check_metadata: bool = True,
    check_field_order: bool = False,
) -> None:
    """Assert that a target Parquet file or schema contract is compatible with baseline contract.

    Raises AssertionError with structured diff description if breaking changes are detected.
    """
    target_schema = target if isinstance(target, SchemaContract) else inspect_schema(target)
    if isinstance(baseline, SchemaContract):
        baseline_schema = baseline
    elif str(baseline).endswith(".json"):
        baseline_schema = load_contract(baseline)
    else:
        baseline_schema = inspect_schema(baseline)

    diff = compare_schemas(
        target_schema,
        baseline_schema,
        mode=mode,
        check_metadata=check_metadata,
        check_field_order=check_field_order,
    )

    breaking = [d for d in diff.diffs if d.severity == Severity.BREAKING]
    if breaking:
        reasons = "\n".join(f"  - [{d.change_type.value}] {d.description}" for d in breaking)
        raise AssertionError(
            f"Schema compatibility assertion failed ({len(breaking)} breaking changes):\n{reasons}"
        )


def assert_engine_verified(
    files: Union[str, Path, List[Union[str, Path]]],
    contract: Optional[Union[str, Path, SchemaContract]] = None,
    engines: Optional[List[str]] = None,
    operations: Optional[List[EngineProbeOperation]] = None,
    fail_on_disagreement: bool = True,
    row_limit: int = 10,
) -> EngineVerificationReport:
    """Assert that Parquet file(s) execute successfully across target runtime query engines.

    Args:
        files: Target Parquet file(s) or directory.
        contract: Optional baseline contract.
        engines: List of engines to probe (defaults to all installed: pyarrow, duckdb, polars).
        operations: List of probe operations (READ, CAST, SCHEMA_UNION).
        fail_on_disagreement: If True, raises AssertionError when rule optimistic disagreement occurs.
        row_limit: Maximum rows to probe.

    Returns:
        EngineVerificationReport on success.
    """
    contract_obj: Optional[SchemaContract] = None
    if contract is not None:
        if isinstance(contract, SchemaContract):
            contract_obj = contract
        elif str(contract).endswith(".json"):
            contract_obj = load_contract(contract)
        else:
            contract_obj = inspect_schema(contract)

    report = verify_engine_compatibility(
        files=files,
        contract=contract_obj,
        engines=engines,
        operations=operations,
        row_limit=row_limit,
    )

    if report.runtime_verdict == RuntimeVerdict.FAIL:
        failures = [
            f"  - [{r.engine_name}] {r.operation.value}: {r.error_message}"
            for r in report.engine_results
            if r.runtime_verdict == RuntimeVerdict.FAIL
        ]
        raise AssertionError("Runtime query engine verification failed:\n" + "\n".join(failures))

    if fail_on_disagreement and report.has_rule_optimistic_disagreement:
        disagreements = [
            f"  - [{d.engine_name}] {d.classification.value}: {d.explanation}" for d in report.disagreements
        ]
        raise AssertionError("Rule vs Runtime optimistic disagreement detected:\n" + "\n".join(disagreements))

    return report


class ParquetGuardTester:
    """Helper test fixture class providing concise testing assertions."""

    assert_schema_compatible = staticmethod(assert_schema_compatible)
    assert_engine_verified = staticmethod(assert_engine_verified)


@pytest.fixture
def parquet_guard() -> ParquetGuardTester:
    """Pytest fixture providing Parquet Guard assertion helpers."""
    return ParquetGuardTester()
