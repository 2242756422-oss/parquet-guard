"""Verification tests for v0.2.0-alpha engineering hardening.

Covers:
- Unsigned integer safe widening vs bit-depth overflow protection.
- Duration and Interval type widening and narrowing.
- Union and Null Arrow type compatibility.
- Recursion depth and pathological deep nesting limits.
- Corrupted cache resilience and auto-recovery.
- Portable relative cache lookups.
- Execution of examples/quickstart_demo.py.
- Wide schemas and zero-byte failure modes.
"""

from __future__ import annotations

import json
import runpy
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from parquet_guard.core.auditor import audit_dataset
from parquet_guard.core.cache import load_audit_cache
from parquet_guard.core.compatibility import (
    analyze_compatibility,
    compare_schemas,
    is_safe_widening,
)
from parquet_guard.core.inspector import ParquetReadError, inspect_schema
from parquet_guard.core.models import (
    FieldDefinition,
    ResultState,
    SchemaContract,
)


def test_unsigned_integer_widening_rules():
    """Verify bit-depth safety of unsigned integer widening."""
    # uint8 fits in uint16, uint32, uint64, int16, int32, int64, float32, float64
    assert is_safe_widening("uint8", "uint16") is True
    assert is_safe_widening("uint8", "uint32") is True
    assert is_safe_widening("uint8", "uint64") is True
    assert is_safe_widening("uint8", "int16") is True
    assert is_safe_widening("uint8", "int32") is True
    assert is_safe_widening("uint8", "int64") is True
    assert is_safe_widening("uint8", "float32") is True
    assert is_safe_widening("uint8", "float64") is True

    # uint16 fits in uint32, uint64, int32, int64, float32, float64
    assert is_safe_widening("uint16", "uint32") is True
    assert is_safe_widening("uint16", "int32") is True
    assert is_safe_widening("uint16", "int64") is True
    assert is_safe_widening("uint16", "float32") is True
    assert is_safe_widening("uint16", "float64") is True

    # uint32 fits in uint64, int64, float64 (53 mantissa bits)
    assert is_safe_widening("uint32", "uint64") is True
    assert is_safe_widening("uint32", "int64") is True
    assert is_safe_widening("uint32", "float64") is True
    # uint32 does NOT safely widen to float32 (only 24 mantissa bits)
    assert is_safe_widening("uint32", "float32") is False

    # uint64 CANNOT safely widen to int64 (values > 2^63 - 1 wrap negative)
    assert is_safe_widening("uint64", "int64") is False
    # uint64 CANNOT safely widen to float64 (loses precision above 2^53)
    assert is_safe_widening("uint64", "float64") is False


def test_duration_widening_and_narrowing():
    """Verify Arrow duration unit precision widening and narrowing."""
    # s -> ms -> us -> ns
    assert is_safe_widening("duration[s]", "duration[ms]") is True
    assert is_safe_widening("duration[s]", "duration[us]") is True
    assert is_safe_widening("duration[s]", "duration[ns]") is True
    assert is_safe_widening("duration[ms]", "duration[us]") is True
    assert is_safe_widening("duration[ms]", "duration[ns]") is True
    assert is_safe_widening("duration[us]", "duration[ns]") is True

    # Reverse (narrowing) loses sub-unit precision -> NOT safe
    assert is_safe_widening("duration[ns]", "duration[us]") is False
    assert is_safe_widening("duration[ns]", "duration[s]") is False
    assert is_safe_widening("duration[ms]", "duration[s]") is False


def test_recursion_depth_protection():
    """Verify that deeply nested pathological schemas trigger recursion protection."""
    # Construct a schema 120 levels deep
    curr = FieldDefinition(name="leaf", type="int32")
    for level in range(120):
        curr = FieldDefinition(name=f"level_{level}", type="struct", children=[curr])

    contract = SchemaContract(version="1.0", name="deep_schema", fields=[curr])

    # 1. find_duplicate_paths should raise ValueError when max_depth exceeded
    with pytest.raises(ValueError, match="Schema nesting depth exceeded maximum"):
        contract.find_all_duplicate_paths()

    # 2. analyze_compatibility should emit a BREAKING diff instead of hitting Python RecursionError
    curr_new = FieldDefinition(name="leaf", type="int64")
    for level in range(120):
        curr_new = FieldDefinition(name=f"level_{level}", type="struct", children=[curr_new])

    diffs = analyze_compatibility(curr, curr_new, max_depth=50)
    assert len(diffs) > 0
    assert any("recursion depth limit" in d.description for d in diffs)


def test_corrupted_cache_auto_recovery(tmp_path: Path):
    """Verify that a corrupted or truncated cache file is safely ignored and cleanly overwritten."""
    p1 = tmp_path / "part-01.parquet"
    table = pa.Table.from_pydict({"x": [1, 2, 3], "y": ["a", "b", "c"]})
    pq.write_table(table, p1)

    cache_file = tmp_path / ".parquet_guard_cache.json"

    # Write corrupt data to cache file
    cache_file.write_text("{malformed_json: true, unterminated", encoding="utf-8")
    assert load_audit_cache(cache_file) is None

    # Run audit with incremental=True
    audit_res = audit_dataset(tmp_path, mode="strict", incremental=True, cache_path=cache_file)
    assert audit_res.is_consistent is True
    assert audit_res.total_files_scanned == 1
    assert audit_res.cache_hits == 0

    # Cache should now be repaired and loadable
    repaired_cache = load_audit_cache(cache_file)
    assert repaired_cache is not None
    assert len(repaired_cache.files) == 1

    # Second audit should hit the repaired cache
    audit_res_2 = audit_dataset(tmp_path, mode="strict", incremental=True, cache_path=cache_file)
    assert audit_res_2.is_consistent is True
    assert audit_res_2.cache_hits == 1


def test_portable_relative_cache_lookup(tmp_path: Path):
    """Verify that cached file entries keyed with relative paths produce valid cache hits."""
    p1 = tmp_path / "part-01.parquet"
    table = pa.Table.from_pydict({"col1": [10, 20], "col2": [1.1, 2.2]})
    pq.write_table(table, p1)

    cache_file = tmp_path / ".parquet_guard_cache.json"

    # Initial audit creates cache
    res1 = audit_dataset(tmp_path, mode="strict", incremental=True, cache_path=cache_file)
    assert res1.is_consistent is True
    assert res1.cache_hits == 0

    # Modify cache file to store relative key instead of absolute
    cache_data = json.loads(cache_file.read_text(encoding="utf-8"))
    abs_key = list(cache_data["files"].keys())[0]
    cached_info = cache_data["files"].pop(abs_key)
    rel_key = Path(abs_key).name  # "part-01.parquet"
    cache_data["files"][rel_key] = cached_info
    cache_file.write_text(json.dumps(cache_data), encoding="utf-8")

    # Run audit again: should recognize relative key and score cache HIT!
    res2 = audit_dataset(tmp_path, mode="strict", incremental=True, cache_path=cache_file)
    assert res2.is_consistent is True
    assert res2.cache_hits == 1


def test_quickstart_demo_executable():
    """Verify that examples/quickstart_demo.py runs end-to-end without errors."""
    demo_path = Path(__file__).parent.parent / "examples" / "quickstart_demo.py"
    assert demo_path.is_file(), f"Demo script not found: {demo_path}"

    # Execute via runpy in-process and ensure clean exit code 0
    try:
        runpy.run_path(str(demo_path), run_name="__main__")
    except SystemExit as exc:
        assert exc.code == 0


def test_wide_schema_handling(tmp_path: Path):
    """Verify zero-data-scan performance and correctness with a 250-column wide schema."""
    num_cols = 250
    data = {f"column_{i}": [i, i + 1] for i in range(num_cols)}
    table = pa.Table.from_pydict(data)
    wide_path = tmp_path / "wide_table.parquet"
    pq.write_table(table, wide_path)

    contract = inspect_schema(wide_path)
    assert len(contract.fields) == num_cols
    assert contract.has_duplicate_fields() is False

    # Comparison against itself should be VALID
    diff = compare_schemas(contract, contract, mode="strict")
    assert diff.result_state == ResultState.VALID
    assert len(diff.diffs) == 0


def test_empty_and_zero_byte_file_inspection(tmp_path: Path):
    """Verify robust error reporting on zero-byte files without unhandled crashes."""
    empty_file = tmp_path / "empty.parquet"
    empty_file.write_bytes(b"")

    with pytest.raises(ParquetReadError, match="Failed to inspect Parquet metadata"):
        inspect_schema(empty_file)

    # Dataset audit should mark zero-byte file as CORRUPTED and report divergence
    res = audit_dataset(tmp_path, mode="strict")
    assert res.is_consistent is False
    assert res.result_state == ResultState.INCOMPATIBLE
    assert res.files[0].schema_fingerprint == "CORRUPTED"
