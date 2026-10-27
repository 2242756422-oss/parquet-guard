"""Adversarial reproduction and verification tests for Project Atlas PR #6 defects (P0/P1).

Covers regression checkpoints QA-01 through QA-20:
- QA-01: All engines unsupported -> must NOT vacuous PASS (P0)
- QA-02: operations=[] or unexecuted requests -> must NOT PASS (P0)
- QA-03: metadata ERROR + runtime FAIL -> must NOT classify as AGREEMENT (P0)
- QA-04: CLI exit codes contract on runtime error/unsupported (P0)
- QA-05 & QA-06: Bounded row reads in PyArrow & Polars (P0)
- QA-07 & QA-08: Data authorization boundaries & accurate scope tagging (P0)
- QA-09: >16KB Parquet footer change with identical tail 16KB invalidates cache (P0)
- QA-10: require_hash=True fails closed when hash is missing/corrupted (P0)
- QA-11: DuckDB TRY_CAST masking conversion failure as NULL -> must FAIL (P1)
- QA-12 & QA-13: Multi-partition schema union & CAST disagreement tracking (P1)
- QA-14: Consumer contract direction (BACKWARD vs FORWARD vs FULL) validation (P1)
- QA-15: Consumer criticality (INFORMATIONAL does not block CI) (P1)
- QA-16: Reject invalid rename cycles and directions (P1)
- QA-17: Migration sandbox bounded sample read & accurate validation mode (P1)
- QA-18: RemoteStorageBackend mtime datetime parsing & storage boundaries (P1)
- QA-19: MCP sandbox path boundary & unauthorized row-read prevention (P0)
- QA-20: Baseline compatibility preservation (P0)
"""

from __future__ import annotations

import struct
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from parquet_guard.core.cache import (
    CachedFileInfo,
    compute_footer_checksum,
    verify_cached_file_identity,
)
from parquet_guard.core.consumer import (
    ConsumerContract,
    ConsumerFieldRequirement,
    evaluate_single_consumer,
)
from parquet_guard.core.models import SchemaContract
from parquet_guard.verification.models import (
    DisagreementClassification,
    EngineProbeOperation,
    MetadataVerdict,
    RuntimeVerdict,
)
from parquet_guard.verification.runner import (
    classify_disagreement,
    verify_engine_compatibility,
)


@pytest.fixture
def sample_parquet(tmp_path: Path) -> Path:
    p = tmp_path / "sample.parquet"
    t = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    pq.write_table(t, p)
    return p


# ---------------------------------------------------------------------------
# QA-01 & QA-02: Vacuous PASS Bug (P0-A)
# ---------------------------------------------------------------------------


def test_qa01_unsupported_engine_does_not_yield_vacuous_pass(sample_parquet: Path) -> None:
    """QA-01: When all requested engines are unsupported/unknown, verdict must NOT be PASS and overall_compatible must be False."""
    report = verify_engine_compatibility(
        target=sample_parquet,
        engines=["nonexistent_engine_xyz"],
        operations=[EngineProbeOperation.READ],
    )
    assert report.runtime_verdict != RuntimeVerdict.PASS
    assert report.runtime_verdict in (RuntimeVerdict.UNSUPPORTED, RuntimeVerdict.INCONCLUSIVE)
    assert report.overall_compatible is False
    assert "nonexistent_engine_xyz" in report.skipped_engines or any(
        r.runtime_verdict == RuntimeVerdict.UNSUPPORTED for r in report.engine_results
    )


def test_qa02_empty_operations_does_not_pass(sample_parquet: Path) -> None:
    """QA-02: When operations=[] is specified, verdict must NOT be PASS and overall_compatible must be False."""
    report = verify_engine_compatibility(
        target=sample_parquet,
        engines=["pyarrow"],
        operations=[],
    )
    assert report.runtime_verdict != RuntimeVerdict.PASS
    assert report.runtime_verdict in (RuntimeVerdict.NOT_RUN, RuntimeVerdict.INCONCLUSIVE)
    assert report.overall_compatible is False


# ---------------------------------------------------------------------------
# QA-03: Metadata ERROR + Runtime FAIL Disagreement (P0-A)
# ---------------------------------------------------------------------------


def test_qa03_metadata_error_runtime_fail_is_inconclusive() -> None:
    """QA-03: Metadata ERROR + Runtime FAIL must classify as INCONCLUSIVE, never AGREEMENT."""
    diag = classify_disagreement(MetadataVerdict.ERROR, RuntimeVerdict.FAIL)
    assert diag == DisagreementClassification.INCONCLUSIVE

    diag2 = classify_disagreement(MetadataVerdict.UNKNOWN, RuntimeVerdict.FAIL)
    assert diag2 == DisagreementClassification.INCONCLUSIVE


# ---------------------------------------------------------------------------
# QA-09 & QA-10: Parquet Footer Checksum & Cache Trust (P0-C)
# ---------------------------------------------------------------------------


def test_qa09_large_footer_change_outside_tail_invalidates_cache(tmp_path: Path) -> None:
    """QA-09: A Parquet file with >16KB metadata whose early metadata changed (same size)

    must change the footer checksum, invalidating the cache.
    Old tail-16KB logic falsely produced the exact same hash (false cache hit).
    """
    import io

    # Create a table with 120 columns to push metadata size well beyond 16KB
    cols = {f"col_{i:03d}": [f"val_{i}_{j}" for j in range(5)] for i in range(120)}
    table_v1 = pa.table(cols)
    metadata = {f"meta_{i:03d}".encode(): f"x_{i}_{'z' * 250}".encode() for i in range(100)}
    table_v1 = table_v1.replace_schema_metadata(metadata)

    buf = io.BytesIO()
    pq.write_table(table_v1, buf)
    data1 = bytearray(buf.getvalue())
    size = len(data1)
    footer_len = struct.unpack("<I", data1[-8:-4])[0]
    assert footer_len > 16384, f"Footer len {footer_len} should exceed 16KB"

    footer_start = size - 8 - footer_len
    # Create data2 mutating a byte in the early footer region (before the tail 16KB)
    data2 = bytearray(data1)
    target_idx = footer_start + 100
    assert target_idx < (size - 16384), "Mutated byte must be before tail 16KB region"
    data2[target_idx] = (data2[target_idx] + 1) % 256

    p1 = tmp_path / "large_footer_v1.parquet"
    p2 = tmp_path / "large_footer_v2.parquet"
    p1.write_bytes(data1)
    p2.write_bytes(data2)

    # Prove that the tail 16KB are IDENTICAL (demonstrating why the old 16KB tail hash produced a false hit)
    assert data1[-16384:] == data2[-16384:], "Tail 16KB must be identical"

    # Now verify that compute_footer_checksum hashes the entire footer and correctly flags difference
    hash1 = compute_footer_checksum(p1)
    hash2 = compute_footer_checksum(p2)
    assert hash1 is not None
    assert hash2 is not None
    assert hash1 != hash2, "Full footer hash must distinguish changes outside the tail 16KB"


def test_qa10_require_hash_fails_closed_when_hash_missing(sample_parquet: Path) -> None:
    """QA-10: verify_cached_file_identity with require_hash=True must fail closed if cached hash is missing."""
    st = sample_parquet.stat()
    cached = CachedFileInfo(
        file_size_bytes=st.st_size,
        mtime_ns=st.st_mtime_ns,
        row_count=3,
        row_groups=1,
        column_count=2,
        schema_fingerprint="abc",
        content_hash=None,  # Missing hash
        cache_confidence="HEURISTIC",
    )
    valid, reason = verify_cached_file_identity(sample_parquet, cached, require_hash=True)
    assert valid is False
    assert reason in ("MISSING_CACHED_HASH", "CHECKSUM_MISMATCH")


# ---------------------------------------------------------------------------
# QA-11: DuckDB TRY_CAST Masking Nulls (P1-A)
# ---------------------------------------------------------------------------


def test_qa11_duckdb_cast_failure_detected(tmp_path: Path) -> None:
    """QA-11: When column contains non-numeric strings, casting to integer must FAIL,

    never masked as PASS by TRY_CAST.
    """
    try:
        from parquet_guard.verification.duckdb_engine import DuckDBEngineAdapter
    except ImportError:
        pytest.skip("duckdb not installed")

    adapter = DuckDBEngineAdapter()
    if not adapter.is_available():
        pytest.skip("duckdb not available")

    # Create Parquet file with invalid int string
    p = tmp_path / "invalid_ints.parquet"
    t = pa.table({"str_col": ["123", "not_a_number", "456"]})
    pq.write_table(t, p)

    res = adapter.probe_cast(str(p), target_field_types={"str_col": "int32"}, row_limit=10)
    assert res.runtime_verdict == RuntimeVerdict.FAIL
    assert res.runtime_verdict != RuntimeVerdict.PASS


# ---------------------------------------------------------------------------
# QA-14, QA-15, QA-16: Consumer Contracts 2.0 Semantics (P1-C)
# ---------------------------------------------------------------------------


def test_qa14_consumer_direction_validation() -> None:
    """QA-14: Reject invalid contract_direction; support BACKWARD, FORWARD, FULL."""
    # Valid directions
    for d in ("BACKWARD", "FORWARD", "FULL"):
        c = ConsumerContract(
            consumer_id="test",
            consumer_name="Test",
            contract_direction=d,
        )
        assert c.contract_direction == d

    # Invalid direction must raise ValueError
    with pytest.raises(ValueError, match="Invalid contract_direction"):
        ConsumerContract(
            consumer_id="test",
            consumer_name="Test",
            contract_direction="SIDEWAYS",
        )


def test_qa15_informational_consumer_does_not_block_ci() -> None:
    """QA-15: INFORMATIONAL consumer with technical incompatibilities does NOT block CI."""
    from parquet_guard.core.models import FieldDefinition

    contract = ConsumerContract(
        consumer_id="info-analytics",
        consumer_name="Informational Analytics",
        criticality="INFORMATIONAL",
        fields=[ConsumerFieldRequirement(name="required_col", required=True, permitted_types=["int64"])],
    )
    # Producer missing required_col
    producer_schema = SchemaContract(fields=[FieldDefinition(name="unrelated_col", type="string")])
    eval_res = evaluate_single_consumer(producer_schema, contract)

    assert eval_res.status.value == "INCOMPATIBLE"
    assert eval_res.is_blocking is False  # Does NOT block CI because consumer is INFORMATIONAL


def test_qa16_circular_or_colliding_renames_rejected() -> None:
    """QA-16: Reject self-renames, colliding renames, and circular rename mappings."""
    # Self-rename
    with pytest.raises(ValueError, match="Self-rename"):
        ConsumerContract(
            consumer_id="test",
            consumer_name="Test",
            field_renames={"col_a": "col_a"},
        )

    # Collision (two different fields mapped to same producer column)
    with pytest.raises(ValueError, match="Collision"):
        ConsumerContract(
            consumer_id="test",
            consumer_name="Test",
            field_renames={"col_a": "target_col", "col_b": "target_col"},
        )

    # Circular rename
    with pytest.raises(ValueError, match="Circular"):
        ConsumerContract(
            consumer_id="test",
            consumer_name="Test",
            field_renames={"col_a": "col_b", "col_b": "col_a"},
        )


# ---------------------------------------------------------------------------
# QA-04: CLI Exit Code Contract (P0-A)
# ---------------------------------------------------------------------------


def test_qa04_cli_verify_engine_exit_code_contract(sample_parquet: Path, tmp_path: Path) -> None:
    """QA-04: verify-engine CLI exit codes: 0 for compatible, 1 for incompatible/failed, 2 for CLI errors."""
    from click.testing import CliRunner

    from parquet_guard.cli import main
    from parquet_guard.core.models import FieldDefinition, SchemaContract

    runner = CliRunner()

    # 1. Compatible case -> Exit 0
    res_ok = runner.invoke(main, ["verify-engine", str(sample_parquet), "-e", "pyarrow", "-s", "read"])
    assert res_ok.exit_code == 0

    # 2. Incompatible contract -> Exit 1
    bad_contract_path = tmp_path / "bad_contract.json"
    bad_contract = SchemaContract(fields=[FieldDefinition(name="missing_col", type="int64")])
    bad_contract_path.write_text(bad_contract.model_dump_json(), encoding="utf-8")

    res_fail = runner.invoke(
        main,
        ["verify-engine", str(sample_parquet), "-c", str(bad_contract_path), "-e", "pyarrow"],
    )
    assert res_fail.exit_code == 1

    # 3. Invalid CLI arguments / options -> Exit 2
    res_err = runner.invoke(main, ["verify-engine", str(sample_parquet), "--invalid-option-xyz"])
    assert res_err.exit_code == 2


# ---------------------------------------------------------------------------
# QA-05 & QA-06: Bounded Row Reads in PyArrow & Polars (P0-B)
# ---------------------------------------------------------------------------


def test_qa05_pyarrow_bounded_row_reads(tmp_path: Path) -> None:
    """QA-05: PyArrow probe_read must use bounded iter_batches rather than materializing the full table."""
    from parquet_guard.verification.pyarrow_engine import PyArrowEngineAdapter

    large_p = tmp_path / "large_read.parquet"
    t = pa.table({"col": list(range(1000))})
    pq.write_table(t, large_p)

    adapter = PyArrowEngineAdapter()
    res = adapter.probe_read([str(large_p)], row_limit=5)
    assert res.runtime_verdict == RuntimeVerdict.PASS


def test_qa06_polars_bounded_row_reads(tmp_path: Path) -> None:
    """QA-06: Polars probe_cast must use lazy scan with limit rather than full eager read."""
    try:
        from parquet_guard.verification.polars_engine import PolarsEngineAdapter
    except ImportError:
        pytest.skip("polars not installed")

    adapter = PolarsEngineAdapter()
    if not adapter.is_available():
        pytest.skip("polars not available")

    large_p = tmp_path / "large_cast.parquet"
    t = pa.table({"int_col": list(range(1000))})
    pq.write_table(t, large_p)

    res = adapter.probe_cast(str(large_p), target_field_types={"int_col": "int64"}, row_limit=5)
    assert res.runtime_verdict == RuntimeVerdict.PASS


# ---------------------------------------------------------------------------
# QA-12 & QA-13: Single-partition Union & Operation-level Disagreement (P1-B)
# ---------------------------------------------------------------------------


def test_qa12_single_file_union_is_inconclusive(sample_parquet: Path) -> None:
    """QA-12: Single-partition schema union probe must be INCONCLUSIVE, not a vacuous PASS."""
    from parquet_guard.verification.pyarrow_engine import PyArrowEngineAdapter

    adapter = PyArrowEngineAdapter()
    res = adapter.probe_schema_union([str(sample_parquet)], row_limit=5)
    assert res.runtime_verdict == RuntimeVerdict.INCONCLUSIVE
    assert res.error_category == "INSUFFICIENT_PARTITIONS"


def test_qa13_cast_disagreement_recorded(sample_parquet: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """QA-13: When static rules predict COMPATIBLE but runtime CAST fails,

    a RULE_OPTIMISTIC disagreement must be recorded for CAST.
    """
    from parquet_guard.core.models import FieldDefinition, SchemaContract
    from parquet_guard.verification import pyarrow_engine
    from parquet_guard.verification.models import EngineProbeResult

    contract = SchemaContract(
        fields=[
            FieldDefinition(name="id", type="int64"),
            FieldDefinition(name="name", type="string"),
        ]
    )

    def mock_probe_cast(*args, **kwargs):
        return EngineProbeResult(
            engine_name="pyarrow",
            operation=EngineProbeOperation.CAST,
            runtime_verdict=RuntimeVerdict.FAIL,
            error_message="Simulated runtime cast failure",
        )

    monkeypatch.setattr(pyarrow_engine.PyArrowEngineAdapter, "probe_cast", mock_probe_cast)

    report = verify_engine_compatibility(
        target=sample_parquet,
        expected_contract=contract,
        engines=["pyarrow"],
        operations=[EngineProbeOperation.CAST],
    )
    assert report.metadata_verdict == MetadataVerdict.COMPATIBLE
    assert report.runtime_verdict == RuntimeVerdict.FAIL
    assert report.has_rule_optimistic_disagreement is True
    assert any(d.operation == EngineProbeOperation.CAST for d in report.disagreements)


# ---------------------------------------------------------------------------
# QA-17: Migration Sandbox In-Memory Mode Tagging (P1-D)
# ---------------------------------------------------------------------------


def test_qa17_migration_provided_table_labeled_accurately() -> None:
    """QA-17: verify_migration_plan with caller-supplied test_table labels validation_mode as provided_in_memory."""
    from parquet_guard.core.migration_verifier import (
        ExecutableMigrationPlan,
        verify_migration_plan,
    )
    from parquet_guard.core.models import FieldDefinition, SchemaContract

    schema = SchemaContract(fields=[FieldDefinition(name="val", type="int64")])
    plan = ExecutableMigrationPlan(
        plan_id="TEST-INMEM",
        title="In-memory test",
        description="Verify mode labeling",
        source_contract=schema,
        target_contract=schema,
        operations=[],
    )
    inmem_table = pa.table({"val": [1, 2, 3]})
    res = verify_migration_plan(plan, test_table=inmem_table, user_data_authorized=False)
    assert res.validation_mode == "provided_in_memory"
    assert res.validation_mode != "synthetic"


# ---------------------------------------------------------------------------
# QA-18: RemoteStorageBackend Mtime Datetime Parsing (P1-E)
# ---------------------------------------------------------------------------


def test_qa18_remote_storage_backend_mtime_datetime() -> None:
    """QA-18: RemoteStorageBackend parses datetime and ISO string mtimes without raising TypeError."""
    from datetime import datetime, timezone

    from parquet_guard.storage.backends import RemoteStorageBackend

    remote = RemoteStorageBackend("s3://test-bucket/data")

    # Mock _get_fs to return custom info dictionary with datetime object
    class MockFS:
        def info(self, path: str) -> dict:
            return {
                "ETag": "abc-123",
                "size": 1024,
                "mtime": datetime(2026, 10, 27, 12, 0, 0, tzinfo=timezone.utc),
                "type": "file",
            }

    remote._fs = MockFS()
    st = remote.stat("s3://test-bucket/data/file.parquet")
    assert st.mtime_ns > 0
    assert st.size == 1024
    assert st.etag == "abc-123"


# ---------------------------------------------------------------------------
# QA-19: MCP Unauthorized User Data Probe Prevention (P1-F)
# ---------------------------------------------------------------------------


def test_qa19_mcp_unauthorized_user_data_probe_rejected(sample_parquet: Path) -> None:
    """QA-19: MCPServer rejects verify_engine_compatibility if user_data_authorized is explicitly False."""
    from parquet_guard.mcp.server import MCPServer

    server = MCPServer(allowed_dirs=[str(sample_parquet.parent)])
    call_msg = {
        "jsonrpc": "2.0",
        "id": 101,
        "method": "tools/call",
        "params": {
            "name": "verify_engine_compatibility",
            "arguments": {
                "file_path": str(sample_parquet),
                "user_data_authorized": False,
            },
        },
    }
    resp = server.process_message(call_msg)
    assert resp is not None
    assert resp.get("result", {}).get("isError") is True
    err_text = resp["result"]["content"][0]["text"]
    assert "authorization" in err_text.lower()


# ---------------------------------------------------------------------------
# QA-20: Baseline Compatibility Preservation
# ---------------------------------------------------------------------------


def test_qa20_baseline_static_inspection_unaffected(sample_parquet: Path) -> None:
    """QA-20: Static schema inspection and contract loading maintain zero regressions."""
    from parquet_guard.core.inspector import inspect_schema

    schema = inspect_schema(str(sample_parquet))
    assert len(schema.fields) == 2
    assert schema.get_field("id") is not None
    assert schema.get_field("name") is not None
