"""Test fixtures and synthetic Parquet file generators for parquet-guard tests."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


@pytest.fixture
def temp_dir(tmp_path: Path) -> Path:
    """Provide clean temporary directory."""
    return tmp_path


@pytest.fixture
def sample_parquet_file(tmp_path: Path) -> Path:
    """Generate a standard Parquet file with common primitive types."""
    file_path = tmp_path / "baseline_sample.parquet"
    schema = pa.schema(
        [
            ("event_id", pa.string(), False),
            ("user_id", pa.int64(), False),
            ("score", pa.float64(), True),
            ("is_active", pa.bool_(), True),
            ("timestamp", pa.timestamp("us", tz="UTC"), False),
        ]
    )
    table = pa.Table.from_arrays(
        [
            pa.array(["evt_001", "evt_002", "evt_003"]),
            pa.array([101, 102, 103]),
            pa.array([9.5, 8.2, None]),
            pa.array([True, False, True]),
            pa.array(
                [1700000000000000, 1700000001000000, 1700000002000000], type=pa.timestamp("us", tz="UTC")
            ),
        ],
        schema=schema,
    )

    pq.write_table(table, str(file_path), compression="snappy")
    return file_path


@pytest.fixture
def widened_parquet_file(tmp_path: Path) -> Path:
    """Generate Parquet file with safe type widening and additive nullable column."""
    file_path = tmp_path / "widened_sample.parquet"
    schema = pa.schema(
        [
            ("event_id", pa.large_string(), False),
            ("user_id", pa.int64(), False),
            ("score", pa.float64(), True),
            ("is_active", pa.bool_(), True),
            ("timestamp", pa.timestamp("ns", tz="UTC"), False),
            ("extra_tag", pa.string(), True),  # Added nullable column
        ]
    )
    table = pa.Table.from_arrays(
        [
            pa.array(["evt_001", "evt_002"]),
            pa.array([101, 102]),
            pa.array([9.5, 8.2]),
            pa.array([True, False]),
            pa.array([1700000000000000000, 1700000001000000000], type=pa.timestamp("ns", tz="UTC")),
            pa.array(["tag_a", "tag_b"]),
        ],
        schema=schema,
    )

    pq.write_table(table, str(file_path))
    return file_path


@pytest.fixture
def breaking_parquet_file(tmp_path: Path) -> Path:
    """Generate Parquet file with breaking changes: narrowed type, dropped column, relaxed nullability."""
    file_path = tmp_path / "breaking_sample.parquet"
    schema = pa.schema(
        [
            ("event_id", pa.string(), True),  # Relaxed nullability (NOT NULL -> NULLABLE)
            ("user_id", pa.int32(), False),  # Narrowed type (int64 -> int32)
            # "score" column dropped
            ("is_active", pa.bool_(), True),
            ("timestamp", pa.timestamp("us", tz="UTC"), False),
        ]
    )
    table = pa.Table.from_arrays(
        [
            pa.array(["evt_001", None]),
            pa.array([101, 102], type=pa.int32()),
            pa.array([True, False]),
            pa.array([1700000000000000, 1700000001000000], type=pa.timestamp("us", tz="UTC")),
        ],
        schema=schema,
    )

    pq.write_table(table, str(file_path))
    return file_path


@pytest.fixture
def partitioned_dataset(tmp_path: Path) -> Path:
    """Generate a clean 3-partition Parquet dataset directory."""
    root = tmp_path / "dataset"
    schema = pa.schema(
        [
            ("id", pa.int64(), False),
            ("value", pa.string(), True),
        ]
    )
    for day in ["2026-10-01", "2026-10-02", "2026-10-03"]:
        part_dir = root / f"date={day}"
        part_dir.mkdir(parents=True)
        table = pa.Table.from_arrays(
            [
                pa.array([1, 2, 3]),
                pa.array(["a", "b", "c"]),
            ],
            schema=schema,
        )
        pq.write_table(table, str(part_dir / "part-0.parquet"))
    return root


@pytest.fixture
def divergent_dataset(tmp_path: Path) -> Path:
    """Generate a partitioned dataset where one partition drifts in column type."""
    root = tmp_path / "divergent_dataset"
    schema_clean = pa.schema(
        [
            ("id", pa.int64(), False),
            ("value", pa.string(), True),
        ]
    )
    schema_drift = pa.schema(
        [
            ("id", pa.string(), False),  # Type drift int64 -> string
            ("value", pa.string(), True),
        ]
    )

    for day in ["2026-10-01", "2026-10-02"]:
        part_dir = root / f"date={day}"
        part_dir.mkdir(parents=True)
        table = pa.Table.from_arrays(
            [
                pa.array([1, 2]),
                pa.array(["a", "b"]),
            ],
            schema=schema_clean,
        )
        pq.write_table(table, str(part_dir / "part-0.parquet"))

    # Drifted partition
    drift_dir = root / "date=2026-10-03"
    drift_dir.mkdir(parents=True)
    table_drift = pa.Table.from_arrays(
        [
            pa.array(["id_1", "id_2"]),
            pa.array(["c", "d"]),
        ],
        schema=schema_drift,
    )
    pq.write_table(table_drift, str(drift_dir / "part-0.parquet"))

    return root
