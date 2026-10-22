"""Boundary-oriented regression tests for temporal type widening (Timestamp & Duration).

Demonstrates and verifies:
1. Physical Arrow int64 representation limits:
   - Nanosecond timestamps are bounded between 1677-09-21 and 2262-04-11.
   - Timestamps outside this range (e.g. year 2300) fail with ArrowInvalid when cast to nanoseconds.
   - Nanosecond durations are bounded to ~292 years (exceeding values fail with ArrowInvalid).
2. Schema-only comparison policies:
   - Strict mode rejects temporal widening as BREAKING.
   - Permissive mode issues a WARNING with TEMPORAL_OVERFLOW_RISK detailing the range constraint.
   - Reverse temporal conversions (narrowing) are always BREAKING.
"""

from __future__ import annotations

from datetime import datetime

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from parquet_guard.core.compatibility import (
    analyze_compatibility,
    compare_schemas,
)
from parquet_guard.core.models import (
    ChangeType,
    ErrorCode,
    FieldDefinition,
    ResultState,
    SchemaContract,
    Severity,
)


def test_arrow_nanosecond_timestamp_physical_overflow_boundary():
    """Prove empirically that timestamps outside 1677-2262 AD overflow int64 nanoseconds."""
    # 1. Valid timestamp (2026 AD) converts cleanly to nanoseconds
    valid_ts = pa.array([datetime(2026, 10, 22, 12, 0, 0)], type=pa.timestamp("s"))
    casted_valid = pc.cast(valid_ts, pa.timestamp("ns"))
    assert casted_valid[0].as_py() == datetime(2026, 10, 22, 12, 0, 0)

    # 2. Maximum representable timestamp in int64 nanoseconds: 2262-04-11 23:47:16 UTC
    edge_inside_ts = pa.array([datetime(2262, 4, 11, 23, 47, 16)], type=pa.timestamp("s"))
    casted_edge = pc.cast(edge_inside_ts, pa.timestamp("ns"))
    assert casted_edge[0].as_py() == datetime(2262, 4, 11, 23, 47, 16)

    # 3. Immediate boundary overflow: 2262-04-12 00:00:00 UTC exceeds int64 nanoseconds
    edge_outside_ts = pa.array([datetime(2262, 4, 12, 0, 0, 0)], type=pa.timestamp("s"))
    with pytest.raises(pa.ArrowInvalid, match="out of bounds timestamp"):
        pc.cast(edge_outside_ts, pa.timestamp("ns"))

    # 4. Distant future timestamp (2300 AD) overflows int64 nanoseconds and raises ArrowInvalid
    future_ts = pa.array([datetime(2300, 1, 1, 0, 0, 0)], type=pa.timestamp("s"))
    with pytest.raises(pa.ArrowInvalid, match="out of bounds timestamp"):
        pc.cast(future_ts, pa.timestamp("ns"))

    # 5. Distant past timestamp (1500 AD) overflows int64 nanoseconds and raises ArrowInvalid
    past_ts = pa.array([datetime(1500, 1, 1, 0, 0, 0)], type=pa.timestamp("s"))
    with pytest.raises(pa.ArrowInvalid, match="out of bounds timestamp"):
        pc.cast(past_ts, pa.timestamp("ns"))


def test_arrow_nanosecond_duration_physical_overflow_boundary():
    """Prove empirically that durations exceeding ~292.27 years overflow int64 nanoseconds."""
    # 1. Small duration (10,000 seconds) casts cleanly to nanoseconds
    small_dur = pa.array([10_000], type=pa.duration("s"))
    casted_dur = pc.cast(small_dur, pa.duration("ns"))
    assert casted_dur[0].as_py().total_seconds() == 10_000

    # 2. Maximum representable duration in int64 nanoseconds: 9,223,372,036 seconds (~292.27 years)
    edge_inside_dur = pa.array([9_223_372_036], type=pa.duration("s"))
    casted_edge_dur = pc.cast(edge_inside_dur, pa.duration("ns"))
    assert casted_edge_dur[0].as_py().total_seconds() == 9_223_372_036

    # 3. Immediate boundary overflow: 9,223,372,037 seconds (> 2^63 - 1 nanoseconds)
    edge_outside_dur = pa.array([9_223_372_037], type=pa.duration("s"))
    with pytest.raises(pa.ArrowInvalid, match="out of bounds timestamp"):
        pc.cast(edge_outside_dur, pa.duration("ns"))

    # 4. Large duration (10 billion seconds = ~317 years) overflows int64 nanoseconds
    large_dur = pa.array([10_000_000_000], type=pa.duration("s"))
    with pytest.raises(pa.ArrowInvalid, match="out of bounds"):
        pc.cast(large_dur, pa.duration("ns"))


def test_timestamp_widening_strict_mode_is_breaking():
    """Verify that strict mode treats timestamp precision widening as BREAKING."""
    f_old = FieldDefinition(name="ts", type="timestamp[s, tz=UTC]")
    f_new = FieldDefinition(name="ts", type="timestamp[ns, tz=UTC]")

    diffs = analyze_compatibility(f_old, f_new, mode="strict")
    assert len(diffs) == 1
    assert diffs[0].severity == Severity.BREAKING
    assert diffs[0].change_type == ChangeType.TYPE_WIDENED


def test_timestamp_widening_permissive_mode_surfaces_overflow_risk():
    """Verify that permissive mode flags temporal widening as WARNING with TEMPORAL_OVERFLOW_RISK."""
    f_old = FieldDefinition(name="ts", type="timestamp[s, tz=UTC]")
    f_new = FieldDefinition(name="ts", type="timestamp[ns, tz=UTC]")

    diffs = analyze_compatibility(f_old, f_new, mode="permissive")
    assert len(diffs) == 1
    assert diffs[0].severity == Severity.WARNING
    assert diffs[0].error_code == ErrorCode.TEMPORAL_OVERFLOW_RISK
    assert "1677-2262 AD" in diffs[0].description
    assert "ArrowInvalid" in diffs[0].description

    # In compare_schemas: report state should be DRIFTED (not breaking, but with warning)
    c_old = SchemaContract(version="1.0", name="base", fields=[f_old])
    c_new = SchemaContract(version="1.0", name="target", fields=[f_new])
    report = compare_schemas(c_new, c_old, mode="permissive")
    assert report.result_state == ResultState.DRIFTED
    assert report.has_warnings is True
    assert report.has_breaking_changes is False


def test_duration_widening_permissive_mode_surfaces_overflow_risk():
    """Verify duration widening generates WARNING with TEMPORAL_OVERFLOW_RISK."""
    f_old = FieldDefinition(name="elapsed", type="duration[s]")
    f_new = FieldDefinition(name="elapsed", type="duration[ns]")

    diffs = analyze_compatibility(f_old, f_new, mode="permissive")
    assert len(diffs) == 1
    assert diffs[0].severity == Severity.WARNING
    assert diffs[0].error_code == ErrorCode.TEMPORAL_OVERFLOW_RISK
    assert "±292 years" in diffs[0].description


def test_temporal_narrowing_is_always_breaking():
    """Verify that temporal precision narrowing (ns -> s) is always BREAKING."""
    f_old = FieldDefinition(name="ts", type="timestamp[ns, tz=UTC]")
    f_new = FieldDefinition(name="ts", type="timestamp[s, tz=UTC]")

    diffs_strict = analyze_compatibility(f_old, f_new, mode="strict")
    assert any(
        d.severity == Severity.BREAKING and d.change_type == ChangeType.TYPE_NARROWED for d in diffs_strict
    )

    diffs_perm = analyze_compatibility(f_old, f_new, mode="permissive")
    assert any(
        d.severity == Severity.BREAKING and d.change_type == ChangeType.TYPE_NARROWED for d in diffs_perm
    )


def test_non_temporal_widening_remains_safe_in_permissive_mode():
    """Ensure non-temporal widening (e.g. int32 -> int64) is unaffected and remains SAFE."""
    f_old = FieldDefinition(name="num", type="int32")
    f_new = FieldDefinition(name="num", type="int64")

    diffs = analyze_compatibility(f_old, f_new, mode="permissive")
    assert len(diffs) == 1
    assert diffs[0].severity == Severity.SAFE
    assert diffs[0].error_code == ErrorCode.TYPE_WIDENED
