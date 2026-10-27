"""Persistent, incremental schema auditing cache for large-scale partitioned Parquet datasets."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union

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
    content_hash: Optional[str] = Field(
        default=None,
        description="Cryptographic SHA-256 checksum of Parquet footer for high-assurance validation",
    )
    etag: Optional[str] = Field(
        default=None, description="Object storage entity tag or version ID when audited from object store"
    )
    cache_confidence: str = Field(
        default="HEURISTIC", description="'HEURISTIC' (mtime+size) or 'HIGH_RELIABLE' (content_hash/etag)"
    )
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


def compute_footer_checksum(file_path: Union[str, Path], chunk_size: int = 16384) -> Optional[str]:
    """Compute a fast SHA-256 checksum of the Parquet footer region.

    Reads up to chunk_size (default 16KB) from the tail of the file where the Parquet
    metadata footer and magic bytes reside. O(1) disk I/O, independent of row count.
    """
    import hashlib

    p = Path(file_path)
    if not p.is_file():
        return None
    try:
        size = p.stat().st_size
        if size == 0:
            return None
        with open(p, "rb") as fh:
            if size <= chunk_size:
                data = fh.read()
            else:
                fh.seek(size - chunk_size)
                data = fh.read(chunk_size)
        return hashlib.sha256(data).hexdigest()
    except Exception:
        return None


def verify_cached_file_identity(
    file_path: Union[str, Path],
    cached: CachedFileInfo,
    require_hash: bool = False,
) -> Tuple[bool, str]:
    """Verify partition file identity against cached entry distinguishing heuristic vs high-assurance.

    Returns (is_valid: bool, verification_reason: str).
    """
    p = Path(file_path)
    if not p.is_file():
        return False, "FILE_NOT_FOUND"

    try:
        st = p.stat()
    except Exception:
        return False, "STAT_FAILED"

    if cached.file_size_bytes != st.st_size:
        return False, "SIZE_CHANGED"

    if cached.mtime_ns != st.st_mtime_ns:
        return False, "MTIME_CHANGED"

    if require_hash or cached.content_hash is not None:
        actual_hash = compute_footer_checksum(p)
        if actual_hash is None or actual_hash != cached.content_hash:
            return False, "CHECKSUM_MISMATCH"
        return True, "MATCH_HIGH_RELIABLE"

    return True, "MATCH_HEURISTIC"
