"""Unit tests for dataset partition auditor."""

from __future__ import annotations

from pathlib import Path

from parquet_guard.core.auditor import audit_dataset


def test_audit_consistent_dataset(partitioned_dataset: Path) -> None:
    result = audit_dataset(partitioned_dataset)
    assert result.is_consistent is True
    assert result.total_files_scanned == 3
    assert result.distinct_schemas_count == 1
    assert result.divergent_files_count == 0
    assert len(result.divergence_details) == 0
    assert result.total_rows == 9


def test_audit_divergent_dataset(divergent_dataset: Path) -> None:
    result = audit_dataset(divergent_dataset)
    assert result.is_consistent is False
    assert result.total_files_scanned == 3
    assert result.distinct_schemas_count == 2
    assert result.divergent_files_count == 1
    assert len(result.divergence_details) == 1
    # Check that divergence detail notes the type change
    detail = result.divergence_details[0]
    assert "date=2026-10-03" in detail
    assert "id" in detail


def test_audit_empty_directory(temp_dir: Path) -> None:
    empty_dir = temp_dir / "empty"
    empty_dir.mkdir()
    result = audit_dataset(empty_dir)
    assert result.total_files_scanned == 0
    assert result.is_consistent is False
    assert len(result.divergence_details) == 1
    assert "No Parquet files found" in result.divergence_details[0]
