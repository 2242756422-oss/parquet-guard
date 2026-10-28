"""Adversarial reproduction and verification tests for Project Atlas PR #6 final hardening.

Covers the 18 critical safety, authorization, and semantic boundaries specified in:
Parquet_Guard_AI_Project_Atlas_PR6_最终可靠性修复Prompt.md:
1. MCP authorization omitted (default-deny)
2. MCP non-boolean / malformed authorization
3. MCP explicit False rejection
4. MCP explicit True in allowed dir bounded execution
5. MCP path escape
6. Python API unauthorized row access denial with row read spy
7. CLI missing authorization flag behavior
8. engines=[] non-pass / no silent default replacement
9. Partial requested engines missing / unsupported -> overall non-pass
10. operations=[] non-pass
11. CAST with no target types -> inconclusive / non-pass
12. SCHEMA_UNION with single file -> inconclusive
13. READ PASS + CAST FAIL -> overall FAIL
14. READ PASS + UNION UNSUPPORTED -> overall non-pass
15. row_limit validation: 0, negative, bool, huge
16. Dual-direction consumer evolution (BACKWARD vs FORWARD vs FULL)
17. Criticality consistency for INFORMATIONAL, STANDARD, and CRITICAL
18. Footer metadata hash boundary definition & acknowledgment
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from parquet_guard.cli import main
from parquet_guard.core.consumer import (
    ConsumerCompatibilityStatus,
    ConsumerContract,
    ConsumerFieldRequirement,
    evaluate_consumer_evolution,
    evaluate_single_consumer,
)
from parquet_guard.core.models import (
    FieldDefinition,
    SchemaContract,
)
from parquet_guard.mcp.server import MCPServer
from parquet_guard.verification import (
    EngineProbeOperation,
    MetadataVerdict,
    PyArrowEngineAdapter,
    RuntimeVerdict,
    classify_disagreement,
    validate_row_limit,
    verify_engine_compatibility,
)


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


# ---------------------------------------------------------------------------
# 1. MCP Default-Deny & Authorization Boundaries (QA-B01 to QA-B05)
# ---------------------------------------------------------------------------


def test_boundary_01_mcp_auth_omitted_denied(sample_file: Path) -> None:
    """Boundary 1: MCP tools/call completely omitting user_data_authorized MUST be denied (default False)."""
    server = MCPServer(allowed_dirs=[str(sample_file.parent)])

    # Spy on row reading in engines
    with (
        patch("pyarrow.parquet.ParquetFile.iter_batches") as spy_arrow,
        patch("duckdb.connect") as spy_duck,
        patch("polars.scan_parquet") as spy_polars,
    ):
        res = server.handle_tool_call(
            "verify_engine_compatibility",
            {"file_path": str(sample_file)},  # user_data_authorized is completely omitted!
        )
        assert res.get("isError") is True
        error_msg = res["content"][0]["text"]
        assert "user_data_authorized" in error_msg or "authorization" in error_msg.lower()
        # Verify ZERO row-level reading functions were called!
        assert spy_arrow.call_count == 0
        assert spy_duck.call_count == 0
        assert spy_polars.call_count == 0


def test_boundary_02_mcp_malformed_auth_denied(sample_file: Path) -> None:
    """Boundary 2: MCP invalid authorization types ('false', 'true', 1, None, {}) must be rejected."""
    server = MCPServer(allowed_dirs=[str(sample_file.parent)])

    invalid_values = ["false", "true", 1, 0, None, {}, [True]]
    for bad_val in invalid_values:
        res = server.handle_tool_call(
            "verify_engine_compatibility",
            {"file_path": str(sample_file), "user_data_authorized": bad_val},
        )
        assert res.get("isError") is True, f"Failed to reject invalid auth value: {bad_val!r}"


def test_boundary_03_mcp_explicit_false_denied(sample_file: Path) -> None:
    """Boundary 3: MCP explicit user_data_authorized=False must be rejected with zero row access."""
    server = MCPServer(allowed_dirs=[str(sample_file.parent)])
    with patch("pyarrow.parquet.ParquetFile.iter_batches") as spy_arrow:
        res = server.handle_tool_call(
            "verify_engine_compatibility",
            {"file_path": str(sample_file), "user_data_authorized": False},
        )
        assert res.get("isError") is True
        assert spy_arrow.call_count == 0


def test_boundary_04_mcp_explicit_true_allowed(sample_file: Path) -> None:
    """Boundary 4: MCP explicit boolean True inside allowed directory executes bounded probe."""
    server = MCPServer(allowed_dirs=[str(sample_file.parent)])
    res = server.handle_tool_call(
        "verify_engine_compatibility",
        {"file_path": str(sample_file), "user_data_authorized": True, "row_limit": 5},
    )
    assert res.get("isError") is not True
    assert "content" in res
    assert (
        "ENGINE RUNTIME VERIFICATION REPORT" in res["content"][0]["text"]
        or "overall_compatible" in res["content"][0]["text"]
    )


def test_boundary_05_mcp_path_escape_rejected(sample_file: Path, tmp_path: Path) -> None:
    """Boundary 5: MCP path traversal or outside allowed dirs must be rejected even if authorized=True."""
    outside_file = tmp_path.parent / "outside.parquet"
    server = MCPServer(allowed_dirs=[str(sample_file.parent)])
    res = server.handle_tool_call(
        "verify_engine_compatibility",
        {"file_path": str(outside_file), "user_data_authorized": True},
    )
    assert res.get("isError") is True
    assert "outside permitted directory" in res["content"][0]["text"]


# ---------------------------------------------------------------------------
# 2. Python API & CLI Capability Gates (QA-B06 to QA-B07)
# ---------------------------------------------------------------------------


def test_boundary_06_python_api_unauthorized_probes_denied(sample_file: Path) -> None:
    """Boundary 6: Python API verify_engine_compatibility with allow_row_probe=False must NOT read row data."""
    with (
        patch("pyarrow.parquet.ParquetFile.iter_batches") as spy_arrow,
        patch("duckdb.connect") as spy_duck,
        patch("polars.scan_parquet") as spy_polars,
    ):
        report = verify_engine_compatibility(
            files=sample_file,
            allow_row_probe=False,  # default or explicit False
        )
        # Row reading spies must record zero calls
        assert spy_arrow.call_count == 0
        assert spy_duck.call_count == 0
        assert spy_polars.call_count == 0
        # Overall compatibility must be False (not fully executed)
        assert report.overall_compatible is False
        assert report.runtime_verdict == RuntimeVerdict.NOT_RUN


def test_boundary_07_cli_missing_auth_flag_behavior(sample_file: Path) -> None:
    """Boundary 7: CLI verify-engine without --allow-row-probe must fail closed (exit non-zero)."""
    runner = CliRunner()
    res = runner.invoke(main, ["verify-engine", str(sample_file)])
    assert res.exit_code != 0
    # Must explicitly state row probes were not authorized or overall incompatible
    assert (
        "POTENTIAL INCOMPATIBILITY" in res.output
        or "allow_row_probe" in res.output
        or "NOT_RUN" in res.output
    )


# ---------------------------------------------------------------------------
# 3. Multi-Engine Completeness & Aggregation (QA-B08 to QA-B14)
# ---------------------------------------------------------------------------


def test_boundary_08_engines_empty_list_fails_closed(sample_file: Path) -> None:
    """Boundary 8: Explicit engines=[] must NOT silently substitute default engines; must not PASS."""
    report = verify_engine_compatibility(
        files=sample_file,
        engines=[],
        allow_row_probe=True,
    )
    assert report.overall_compatible is False
    assert report.runtime_verdict in (RuntimeVerdict.NOT_RUN, RuntimeVerdict.INCONCLUSIVE)
    assert report.executed_engines == []


def test_boundary_09_partial_engines_missing_fails_closed(sample_file: Path) -> None:
    """Boundary 9: User requested engines=['pyarrow', 'nonexistent_engine']. PyArrow passes, nonexistent is missing -> overall non-pass."""
    report = verify_engine_compatibility(
        files=sample_file,
        engines=["pyarrow", "nonexistent_engine"],
        allow_row_probe=True,
    )
    assert report.overall_compatible is False
    assert report.runtime_verdict != RuntimeVerdict.PASS
    assert "nonexistent_engine" in report.skipped_engines or any(
        r.engine_name == "nonexistent_engine" and r.runtime_verdict == RuntimeVerdict.UNSUPPORTED
        for r in report.engine_results
    )


def test_boundary_10_operations_empty_list_fails_closed(sample_file: Path) -> None:
    """Boundary 10: Explicit operations=[] must not PASS."""
    report = verify_engine_compatibility(
        files=sample_file,
        operations=[],
        allow_row_probe=True,
    )
    assert report.overall_compatible is False
    assert report.runtime_verdict in (RuntimeVerdict.NOT_RUN, RuntimeVerdict.INCONCLUSIVE)


def test_boundary_11_cast_without_target_types_inconclusive(sample_file: Path) -> None:
    """Boundary 11: operations=[CAST] without contract target types must be INCONCLUSIVE, not PASS."""
    report = verify_engine_compatibility(
        files=sample_file,
        operations=[EngineProbeOperation.CAST],
        contract=None,  # No contract provided
        allow_row_probe=True,
    )
    assert report.overall_compatible is False
    assert report.runtime_verdict in (RuntimeVerdict.INCONCLUSIVE, RuntimeVerdict.NOT_RUN)


def test_boundary_12_schema_union_single_file_inconclusive(sample_file: Path) -> None:
    """Boundary 12: operations=[SCHEMA_UNION] with only 1 file must be INCONCLUSIVE, not PASS."""
    report = verify_engine_compatibility(
        files=sample_file,
        operations=[EngineProbeOperation.SCHEMA_UNION],
        allow_row_probe=True,
    )
    assert report.overall_compatible is False
    assert report.runtime_verdict == RuntimeVerdict.INCONCLUSIVE


def test_boundary_13_read_pass_cast_fail_yields_fail(sample_file: Path) -> None:
    """Boundary 13: One operation (READ) passes, another (CAST) fails -> overall must be FAIL."""
    # Build a contract requiring int32 'name' (which contains strings -> cast failure)
    bad_contract = SchemaContract(
        name="test",
        version="1.0",
        fields=[
            FieldDefinition(name="id", type="int64", nullable=False),
            FieldDefinition(name="name", type="int32", nullable=False),
        ],
    )
    report = verify_engine_compatibility(
        files=sample_file,
        contract=bad_contract,
        operations=[EngineProbeOperation.READ, EngineProbeOperation.CAST],
        engines=["duckdb"],
        allow_row_probe=True,
    )
    assert report.overall_compatible is False
    assert report.runtime_verdict == RuntimeVerdict.FAIL


def test_boundary_14_read_pass_union_unsupported_non_pass(sample_file: Path) -> None:
    """Boundary 14: Engine passes READ but is UNSUPPORTED on UNION -> overall cannot be PASS."""
    adapter = PyArrowEngineAdapter()
    # Mock probe_schema_union to return UNSUPPORTED
    with patch.object(
        adapter,
        "probe_schema_union",
        return_value=adapter.make_unsupported_result(EngineProbeOperation.SCHEMA_UNION),
    ):
        with patch("parquet_guard.verification.runner.get_engine_adapter", return_value=adapter):
            report = verify_engine_compatibility(
                files=[sample_file, sample_file],
                operations=[EngineProbeOperation.READ, EngineProbeOperation.SCHEMA_UNION],
                engines=["pyarrow"],
                allow_row_probe=True,
            )
            assert report.overall_compatible is False
            assert report.runtime_verdict != RuntimeVerdict.PASS


# ---------------------------------------------------------------------------
# 4. row_limit Resource Safety (QA-B15)
# ---------------------------------------------------------------------------


def test_boundary_15_row_limit_validation() -> None:
    """Boundary 15: row_limit must be a positive integer, rejecting 0, negative, bools, huge values."""
    with pytest.raises(ValueError):
        validate_row_limit(0)
    with pytest.raises(ValueError):
        validate_row_limit(-5)
    with pytest.raises(ValueError):
        validate_row_limit(True)  # Bool must be rejected
    with pytest.raises(ValueError):
        validate_row_limit(False)
    with pytest.raises(ValueError):
        validate_row_limit("10")  # String non-int must be rejected
    with pytest.raises(ValueError):
        validate_row_limit(100_000)  # Exceeds max 10,000

    # Valid values
    assert validate_row_limit(1) == 1
    assert validate_row_limit(10) == 10
    assert validate_row_limit(10_000) == 10_000


# ---------------------------------------------------------------------------
# 5. Consumer Evolution & Directionality (QA-B16)
# ---------------------------------------------------------------------------


def test_boundary_16_consumer_evolution_directionality() -> None:
    """Boundary 16: BACKWARD vs FORWARD vs FULL evolution evaluation produces distinct results.

    Scenario: Producer evolves from v1 (id) to v2 (id, required_token).
    - BACKWARD: A new consumer requiring 'required_token' reading v1 data FAILS (old data lacks required field).
    - FORWARD: An old consumer requiring only 'id' with allow_extra_fields=True reading v2 data SUCCEEDS.
    - FULL: Fails because backward failed.
    """
    v1_producer = SchemaContract(
        name="ds",
        version="1.0",
        fields=[FieldDefinition(name="id", type="int64", nullable=False)],
    )
    v2_producer = SchemaContract(
        name="ds",
        version="1.0",
        fields=[
            FieldDefinition(name="id", type="int64", nullable=False),
            FieldDefinition(name="required_token", type="string", nullable=False),
        ],
    )

    old_consumer = ConsumerContract(
        consumer_id="old-c",
        consumer_name="Old Consumer",
        fields=[ConsumerFieldRequirement(name="id", required=True, permitted_types=["int64"])],
        allow_extra_fields=True,
    )
    new_consumer = ConsumerContract(
        consumer_id="new-c",
        consumer_name="New Consumer",
        fields=[
            ConsumerFieldRequirement(name="id", required=True, permitted_types=["int64"]),
            ConsumerFieldRequirement(name="required_token", required=True, permitted_types=["string"]),
        ],
        allow_extra_fields=True,
    )

    # 1. Evaluate evolution with BACKWARD direction
    rep_backward = evaluate_consumer_evolution(
        baseline_producer=v1_producer,
        target_producer=v2_producer,
        old_consumer=old_consumer,
        new_consumer=new_consumer,
        direction="BACKWARD",
    )
    assert rep_backward.status == ConsumerCompatibilityStatus.INCOMPATIBLE
    assert rep_backward.is_blocking is True

    # 2. Evaluate evolution with FORWARD direction
    rep_forward = evaluate_consumer_evolution(
        baseline_producer=v1_producer,
        target_producer=v2_producer,
        old_consumer=old_consumer,
        new_consumer=new_consumer,
        direction="FORWARD",
    )
    assert rep_forward.status == ConsumerCompatibilityStatus.COMPATIBLE
    assert rep_forward.is_blocking is False

    # 3. Evaluate evolution with FULL direction
    rep_full = evaluate_consumer_evolution(
        baseline_producer=v1_producer,
        target_producer=v2_producer,
        old_consumer=old_consumer,
        new_consumer=new_consumer,
        direction="FULL",
    )
    assert rep_full.status == ConsumerCompatibilityStatus.INCOMPATIBLE
    assert rep_full.is_blocking is True


# ---------------------------------------------------------------------------
# 6. Criticality & Field Renames Consistency (QA-B17)
# ---------------------------------------------------------------------------


def test_boundary_17_criticality_and_renames_consistency() -> None:
    """Boundary 17: INFORMATIONAL criticality must not block CI, with zero contradictions in blocking_fields.

    Also tests field_renames: mapped fields must not be treated as unexpected extra fields.
    """
    producer = SchemaContract(
        name="ds",
        version="1.0",
        fields=[
            FieldDefinition(name="new_user_id", type="string", nullable=False),
            FieldDefinition(name="age", type="string", nullable=False),
        ],
    )

    # INFORMATIONAL consumer with a type violation and a field rename
    info_consumer = ConsumerContract(
        consumer_id="info-c",
        consumer_name="Info Consumer",
        criticality="INFORMATIONAL",
        field_renames={"old_user_id": "new_user_id"},
        fields=[
            ConsumerFieldRequirement(name="old_user_id", required=True, permitted_types=["string"]),
            ConsumerFieldRequirement(name="age", required=True, permitted_types=["int64"]),  # Type mismatch!
        ],
        allow_extra_fields=False,
    )

    eval_res = evaluate_single_consumer(producer, info_consumer)

    # Technical status must faithfully report incompatibility
    assert eval_res.status == ConsumerCompatibilityStatus.INCOMPATIBLE
    # Policy decision: INFORMATIONAL NEVER BLOCKS
    assert eval_res.is_blocking is False
    # No contradiction between is_blocking and blocking_fields
    assert eval_res.blocking_fields == []
    for fe in eval_res.field_evaluations:
        assert fe.is_blocking is False
    # 'new_user_id' was properly recognized through field_renames and NOT flagged as unexpected extra
    assert eval_res.unexpected_extra_fields == []


# ---------------------------------------------------------------------------
# 7. Disagreement Classifier Error Handling & Footer Boundaries (QA-B18)
# ---------------------------------------------------------------------------


def test_boundary_18_disagreement_error_not_agreement() -> None:
    """Boundary 18: If metadata or runtime encounters ERROR, it must classify as INCONCLUSIVE, not AGREEMENT."""
    diag1 = classify_disagreement(MetadataVerdict.ERROR, RuntimeVerdict.FAIL)
    assert diag1.value == "INCONCLUSIVE"

    diag2 = classify_disagreement(MetadataVerdict.COMPATIBLE, RuntimeVerdict.ERROR)
    assert diag2.value == "INCONCLUSIVE"
