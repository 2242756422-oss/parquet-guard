"""Persistent, incremental schema auditing cache for large-scale partitioned Parquet datasets."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


class CachedFileInfo(BaseModel):
    """Cached metadata and audit evaluation for an individual Parquet partition file."""

    model_config = ConfigDict(extra="forbid")

    file_size_bytes: int
    mtime_ns: int
    row_count: int
    row_groups: int
    column_count: int
    schema_fingerprint: str
    is_divergent: bool = False
    divergence_reason: Optional[str] = None
    warnings: List[str] = Field(default_factory=list)


class DatasetAuditCache(BaseModel):
    """Manifest file persisting audit evaluations across unchanged partition files."""

    model_config = ConfigDict(extra="forbid")

    version: str = "1.0"
    dataset_root: str
    contract_fingerprint: str
    mode: str
    check_metadata: bool
    check_field_order: bool
    updated_at: str
    files: Dict[str, CachedFileInfo] = Field(default_factory=dict)

    def is_cache_valid(
        self,
        contract_fp: str,
        mode: str,
        check_metadata: bool,
        check_field_order: bool,
    ) -> bool:
        """Verify whether cached evaluation matches current contract and audit parameters."""
        return (
            self.contract_fingerprint == contract_fp
            and self.mode == mode
            and self.check_metadata == check_metadata
            and self.check_field_order == check_field_order
        )


def load_audit_cache(cache_path: Union[str, Path]) -> Optional[DatasetAuditCache]:
    """Load audit cache from disk, returning None if absent or corrupted."""
    path = Path(cache_path)
    if not path.is_file():
        return None

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return DatasetAuditCache.model_validate(data)
    except Exception:
        # Gracefully handle corrupted or incompatible cache by returning None to trigger full audit
        return None


def save_audit_cache(cache: DatasetAuditCache, cache_path: Union[str, Path]) -> None:
    """Atomically write audit cache to disk to prevent corruptions during aborted runs."""
    path = Path(cache_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    cache.updated_at = datetime.now(timezone.utc).isoformat()
    json_bytes = json.dumps(cache.model_dump(), indent=2, ensure_ascii=True).encode("utf-8")

    # Atomic write pattern: write to temporary file in same directory, then atomic rename
    temp_fd, temp_file_path = tempfile.mkstemp(dir=path.parent, prefix=".pg_cache_tmp_")
    try:
        with os.fdopen(temp_fd, "wb") as f:
            f.write(json_bytes)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_file_path, path)
    except Exception:
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        raise
