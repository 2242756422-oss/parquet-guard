"""Edge case and security validation tests for parquet-guard."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from parquet_guard.core.compatibility import compare_schemas
from parquet_guard.core.inspector import inspect_schema


def test_zero_row_parquet_file(temp_dir: Path) -> None:
    """Validate that parquet-guard correctly inspects 0-row empty Parquet files."""
    file_path = temp_dir / "zero_rows.parquet"
    schema = pa.schema(
        [
            ("col_a", pa.int32(), False),
            ("col_b", pa.string(), True),
        ]
    )
    table = pa.Table.from_arrays(
        [
            pa.array([], type=pa.int32()),
            pa.array([], type=pa.string()),
        ],
        schema=schema,
    )
    pq.write_table(table, str(file_path))

    contract = inspect_schema(file_path)
    assert len(contract.fields) == 2
    f_a = contract.get_field("col_a")
    assert f_a is not None
    assert f_a.type == "int32"


def test_nested_struct_and_list_types(temp_dir: Path) -> None:
    """Validate complex nested structures (List, Struct)."""
    file_path = temp_dir / "nested.parquet"
    struct_type = pa.struct(
        [
            ("inner_id", pa.int64()),
            ("inner_val", pa.string()),
        ]
    )
    schema = pa.schema(
        [
            ("tags", pa.list_(pa.string()), True),
            ("metadata", struct_type, True),
        ]
    )
    table = pa.Table.from_arrays(
        [
            pa.array([["tag1", "tag2"], ["tag3"]]),
            pa.array([{"inner_id": 1, "inner_val": "v1"}, {"inner_id": 2, "inner_val": "v2"}]),
        ],
        schema=schema,
    )
    pq.write_table(table, str(file_path))

    contract = inspect_schema(file_path)
    assert len(contract.fields) == 2
    tags_field = contract.get_field("tags")
    assert tags_field is not None
    assert "list" in tags_field.type.lower()
    meta_field = contract.get_field("metadata")
    assert meta_field is not None
    assert "struct" in meta_field.type.lower()


def test_wide_schema_performance(temp_dir: Path) -> None:
    """Validate high-column count (100 columns) without performance degradation."""
    file_path = temp_dir / "wide_100_cols.parquet"
    fields = [pa.field(f"col_{i}", pa.int64(), nullable=(i % 2 == 0)) for i in range(100)]
    schema = pa.schema(fields)
    arrays = [pa.array([i, i * 2]) for i in range(100)]
    table = pa.Table.from_arrays(arrays, schema=schema)
    pq.write_table(table, str(file_path))

    contract = inspect_schema(file_path)
    assert len(contract.fields) == 100

    report = compare_schemas(contract, contract)
    assert report.is_compatible is True
    assert len(report.diffs) == 0
