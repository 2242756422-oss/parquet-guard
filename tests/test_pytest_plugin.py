"""Tests for pytest integration assertions and fixtures."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from parquet_guard.testing import (
    ParquetGuardTester,
    assert_engine_verified,
    assert_schema_compatible,
)


def test_assert_schema_compatible_success(tmp_path: Path) -> None:
    f1 = tmp_path / "base.parquet"
    f2 = tmp_path / "target.parquet"

    t1 = pa.Table.from_arrays([pa.array([1, 2])], names=["id"])
    # Target adds an optional column (permissive backward-compatible)
    t2 = pa.Table.from_arrays([pa.array([1, 2]), pa.array(["a", "b"])], names=["id", "name"])

    pq.write_table(t1, f1)
    pq.write_table(t2, f2)

    assert_schema_compatible(f2, f1, mode="permissive")


def test_assert_schema_compatible_failure(tmp_path: Path) -> None:
    f1 = tmp_path / "base.parquet"
    f2 = tmp_path / "target.parquet"

    t1 = pa.Table.from_arrays([pa.array([1, 2]), pa.array(["a", "b"])], names=["id", "name"])
    # Target drops required column 'name'
    t2 = pa.Table.from_arrays([pa.array([1, 2])], names=["id"])

    pq.write_table(t1, f1)
    pq.write_table(t2, f2)

    with pytest.raises(AssertionError) as exc_info:
        assert_schema_compatible(f2, f1, mode="permissive")
    assert "breaking changes" in str(exc_info.value)


def test_assert_engine_verified_success(tmp_path: Path) -> None:
    f = tmp_path / "sample.parquet"
    t = pa.Table.from_arrays([pa.array([10, 20])], names=["count"])
    pq.write_table(t, f)

    rep = assert_engine_verified(f, engines=["pyarrow"])
    assert rep.runtime_verdict.value == "PASS"


def test_parquet_guard_fixture(parquet_guard: ParquetGuardTester, tmp_path: Path) -> None:
    f = tmp_path / "sample.parquet"
    t = pa.Table.from_arrays([pa.array(["ok"])], names=["status"])
    pq.write_table(t, f)

    parquet_guard.assert_engine_verified(f, engines=["pyarrow"])
    parquet_guard.assert_schema_compatible(f, f)
