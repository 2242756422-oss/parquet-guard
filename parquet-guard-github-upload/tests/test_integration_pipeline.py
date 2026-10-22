"""Integration test verifying data pipeline guard with real ingestion lifecycle."""

from pathlib import Path

from examples.data_pipeline_guard.test_pipeline import test_guarded_pipeline_lifecycle


def test_integration_pipeline_e2e(tmp_path: Path) -> None:
    """Run full guarded ingestion pipeline lifecycle test."""
    test_guarded_pipeline_lifecycle(tmp_path)
