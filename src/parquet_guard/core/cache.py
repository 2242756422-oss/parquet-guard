"""Persistent, incremental schema auditing cache for large-scale partitioned Parquet datasets."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


class CachedFileInfo(BaseModel):
    """Cached intrinsic metadata and schema fingerprint for an individual Parquet partition file.

    Note: Consistency and divergence are relational properties evaluated dynamically across
    the current dataset, ensuring no stale divergence state leaks when partitions are deleted or modified.
    """

    model_config = ConfigDict(extra="forbid")

    file_size_bytes: int
    mtime_ns: int
    row_count: int
    row_groups: int
    column_count: int
    schema_fingerprint: str
    is_corrupted: bool = False
    has_duplicate_columns: bool = False
    error_message: Optional[str] = None


class DatasetAuditCache(BaseModel):
    """Manifest file persisting partition metadata and schema ASTs across unchanged files."""

    model_config = ConfigDict(extra="forbid")

    version: str = "1.0"
    dataset_root: str
    updated_at: str
    files: Dict[str, CachedFileInfo] = Field(default_factory=dict)
    schemas: Dict[str, Dict[str, Any]] = Field(
        default_factory=dict, description="Mapping of schema fingerprint to serialized SchemaContract"
    )

    def is_cache_valid(self, version: str = "1.0") -> bool:
        """Verify whether cached manifest matches supported cache version."""
        return self.version == version


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
