"""Example: Resilient Parquet Ingestion Pipeline guarded by parquet-guard.

Prevents downstream query failures by validating schema contracts before ingestion.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, Dict, Tuple

import pyarrow.parquet as pq

from parquet_guard import compare_schemas, inspect_schema, load_contract
from parquet_guard.core.reporter import format_diff_markdown


class GuardedIngestionPipeline:
    """Ingestion pipeline that validates Parquet schema contracts before loading."""

    def __init__(self, contract_path: Path, storage_dir: Path, quarantine_dir: Path) -> None:
        self.contract_path = Path(contract_path)
        self.storage_dir = Path(storage_dir)
        self.quarantine_dir = Path(quarantine_dir)

        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)
        self.contract = load_contract(self.contract_path)

    def process_batch(self, incoming_parquet_path: Path, partition_key: str) -> Tuple[bool, str]:
        """Validate and ingest an incoming daily batch of Parquet data.

        Returns:
            Tuple of (success: bool, message: str)
        """
        incoming_file = Path(incoming_parquet_path)
        if not incoming_file.is_file():
            return False, f"File not found: {incoming_file}"

        # 1. Zero-data-scan inspect and compare against contract
        batch_schema = inspect_schema(incoming_file)
        diff_report = compare_schemas(batch_schema, self.contract, mode="permissive")

        # 2. Breaking regression gate
        if diff_report.has_breaking_changes:
            quarantine_dest = self.quarantine_dir / f"{partition_key}_{incoming_file.name}"
            shutil.copy(incoming_file, quarantine_dest)

            # Generate markdown alert report
            md_alert = format_diff_markdown(diff_report)
            alert_file = self.quarantine_dir / f"{partition_key}_alert.md"
            alert_file.write_text(md_alert, encoding="utf-8")

            return (
                False,
                f"REJECTED: Breaking schema regression detected in batch '{incoming_file.name}'. "
                f"Quarantined to '{quarantine_dest}'. Alert report written to '{alert_file}'.",
            )

        # 3. Safe ingestion into production partition store
        dest_partition_dir = self.storage_dir / f"partition={partition_key}"
        dest_partition_dir.mkdir(parents=True, exist_ok=True)
        dest_file = dest_partition_dir / "data.parquet"
        shutil.copy(incoming_file, dest_file)

        return True, f"ACCEPTED: Batch '{incoming_file.name}' safely ingested to '{dest_file}'."

    def run_downstream_aggregation(self) -> Dict[str, Any]:
        """Simulate downstream analytical aggregation over all ingested partitions."""
        parquet_files = list(self.storage_dir.glob("**/data.parquet"))
        if not parquet_files:
            return {"total_rows": 0, "partitions": 0}

        total_rows = 0
        for f in parquet_files:
            meta = pq.read_metadata(str(f))
            total_rows += meta.num_rows

        return {
            "total_rows": total_rows,
            "partitions": len(parquet_files),
            "status": "HEALTHY",
        }
