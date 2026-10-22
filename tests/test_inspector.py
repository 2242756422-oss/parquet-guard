"""Unit tests for Parquet metadata inspector and contract extractor."""

from __future__ import annotations

from pathlib import Path

import pytest

from parquet_guard.core.inspector import (
    ParquetReadError,
    extract_file_metadata,
    inspect_schema,
    load_contract,
    save_contract,
)
from parquet_guard.core.models import SchemaContract


def test_inspect_schema_basic(sample_parquet_file: Path) -> None:
    contract = inspect_schema(sample_parquet_file)
    assert isinstance(contract, SchemaContract)
    assert len(contract.fields) == 5

    names = contract.field_names()
    assert "event_id" in names
    assert "user_id" in names
    assert "score" in names
    assert "is_active" in names
    assert "timestamp" in names

    event_id = contract.get_field("event_id")
    assert event_id is not None
    assert event_id.type == "string"
    assert event_id.nullable is False

    score = contract.get_field("score")
    assert score is not None
    assert score.type == "double" or score.type == "float64"
    assert score.nullable is True


def test_extract_file_metadata(sample_parquet_file: Path) -> None:
    meta = extract_file_metadata(sample_parquet_file)
    assert meta["num_rows"] == 3
    assert meta["num_columns"] == 5
    assert meta["num_row_groups"] >= 1
    assert "SNAPPY" in meta["compression_codecs"]
    assert meta["file_size_bytes"] > 0


def test_save_and_load_contract(sample_parquet_file: Path, temp_dir: Path) -> None:
    contract = inspect_schema(sample_parquet_file)
    contract.name = "test_events"
    out_json = temp_dir / "contract.json"

    save_contract(contract, out_json)
    assert out_json.is_file()

    loaded = load_contract(out_json)
    assert loaded.name == "test_events"
    assert len(loaded.fields) == len(contract.fields)
    assert loaded.fields[0] == contract.fields[0]


def test_inspect_nonexistent_file(temp_dir: Path) -> None:
    fake_path = temp_dir / "does_not_exist.parquet"
    with pytest.raises(ParquetReadError, match="does not exist"):
        inspect_schema(fake_path)


def test_inspect_corrupted_file(temp_dir: Path) -> None:
    corrupted_path = temp_dir / "corrupted.parquet"
    corrupted_path.write_bytes(b"NOT_A_VALID_PARQUET_FILE_HEADER")
    with pytest.raises(ParquetReadError, match="Failed to inspect Parquet metadata"):
        inspect_schema(corrupted_path)
