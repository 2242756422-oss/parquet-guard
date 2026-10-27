"""Storage abstraction backends for local, in-memory, and remote object store filesystems."""

from __future__ import annotations

import fnmatch
import hashlib
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple


class StorageBackendError(Exception):
    """Raised when storage operations fail or required storage driver is missing."""


@dataclass(frozen=True)
class FileStat:
    """Metadata describing a file or object in a storage backend."""

    path: str
    size: int
    mtime_ns: int
    etag: Optional[str] = None
    is_dir: bool = False


class StorageBackend(ABC):
    """Abstract base storage backend providing zero-data-scan metadata & slice reads."""

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Return True if file or directory exists."""

    @abstractmethod
    def stat(self, path: str) -> FileStat:
        """Return metadata stat for a given path."""

    @abstractmethod
    def read_bytes(self, path: str, offset: Optional[int] = None, length: Optional[int] = None) -> bytes:
        """Read arbitrary slice of bytes from file without loading entire file into memory."""

    @abstractmethod
    def list_files(self, prefix: str, pattern: Optional[str] = None, recursive: bool = True) -> List[str]:
        """List files under prefix, optionally filtered by glob pattern (e.g. '*.parquet')."""

    def compute_footer_checksum(self, path: str, chunk_size: int = 16384) -> Optional[str]:
        """Compute SHA-256 checksum of file tail / footer region (O(1) I/O)."""
        try:
            st = self.stat(path)
            if st.size == 0 or st.is_dir:
                return None
            if st.size <= chunk_size:
                data = self.read_bytes(path, offset=0, length=st.size)
            else:
                data = self.read_bytes(path, offset=st.size - chunk_size, length=chunk_size)
            return hashlib.sha256(data).hexdigest()
        except Exception:
            return None


class LocalStorageBackend(StorageBackend):
    """Local filesystem storage backend based on standard library os/Path."""

    def exists(self, path: str) -> bool:
        return Path(path).exists()

    def stat(self, path: str) -> FileStat:
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"File or path does not exist: {path}")
        st = p.stat()
        return FileStat(
            path=str(p.resolve()),
            size=st.st_size,
            mtime_ns=st.st_mtime_ns,
            etag=None,
            is_dir=p.is_dir(),
        )

    def read_bytes(self, path: str, offset: Optional[int] = None, length: Optional[int] = None) -> bytes:
        p = Path(path)
        with open(p, "rb") as f:
            if offset is not None:
                f.seek(offset)
            if length is not None:
                return f.read(length)
            return f.read()

    def list_files(self, prefix: str, pattern: Optional[str] = None, recursive: bool = True) -> List[str]:
        p = Path(prefix)
        if not p.exists():
            return []
        if p.is_file():
            return [str(p.resolve())]

        glob_pattern = "**/" + (pattern or "*") if recursive else (pattern or "*")
        matched: List[str] = []
        for item in p.glob(glob_pattern):
            if item.is_file():
                matched.append(str(item.resolve()))
        return sorted(matched)


class MemoryStorageBackend(StorageBackend):
    """In-memory storage backend for hermetic unit testing, faking S3/GCS etags & latencies."""

    def __init__(self) -> None:
        self._files: Dict[str, bytes] = {}
        self._etags: Dict[str, str] = {}
        self._mtimes: Dict[str, int] = {}

    def write_file(self, path: str, data: bytes, etag: Optional[str] = None, mtime_ns: int = 1000) -> None:
        norm = path.replace("\\", "/")
        self._files[norm] = data
        self._etags[norm] = etag or hashlib.sha256(data).hexdigest()[:16]
        self._mtimes[norm] = mtime_ns

    def exists(self, path: str) -> bool:
        norm = path.replace("\\", "/")
        return norm in self._files or any(k.startswith(norm + "/") for k in self._files)

    def stat(self, path: str) -> FileStat:
        norm = path.replace("\\", "/")
        if norm in self._files:
            data = self._files[norm]
            return FileStat(
                path=norm,
                size=len(data),
                mtime_ns=self._mtimes[norm],
                etag=self._etags[norm],
                is_dir=False,
            )
        if any(k.startswith(norm + "/") for k in self._files):
            return FileStat(path=norm, size=0, mtime_ns=0, is_dir=True)
        raise FileNotFoundError(f"In-memory file not found: {path}")

    def read_bytes(self, path: str, offset: Optional[int] = None, length: Optional[int] = None) -> bytes:
        norm = path.replace("\\", "/")
        if norm not in self._files:
            raise FileNotFoundError(f"In-memory file not found: {path}")
        data = self._files[norm]
        start = offset or 0
        end = (start + length) if length is not None else len(data)
        return data[start:end]

    def list_files(self, prefix: str, pattern: Optional[str] = None, recursive: bool = True) -> List[str]:
        norm_prefix = prefix.replace("\\", "/").rstrip("/")
        matched: List[str] = []
        pat = pattern or "*"
        for k in self._files:
            if k == norm_prefix or k.startswith(norm_prefix + "/"):
                filename = k.split("/")[-1]
                if not recursive:
                    parent = "/".join(k.split("/")[:-1])
                    if parent != norm_prefix:
                        continue
                if fnmatch.fnmatch(filename, pat) or fnmatch.fnmatch(k, pat):
                    matched.append(k)
        return sorted(matched)


class RemoteStorageBackend(StorageBackend):
    """Storage backend delegating to fsspec for cloud object stores (s3, gs, az)."""

    def __init__(self, url_or_protocol: str, **storage_options: object) -> None:
        self.url = url_or_protocol
        self.storage_options = storage_options
        self._fs = None

    def _get_fs(self):  # type: ignore[no-untyped-def]
        if self._fs is not None:
            return self._fs
        try:
            import fsspec  # type: ignore[import-untyped]

            fs, path = fsspec.core.url_to_fs(self.url, **self.storage_options)
            self._fs = fs
            return self._fs
        except ImportError as err:
            raise StorageBackendError(
                "Cloud object store support requires 'fsspec'. "
                "Install optional dependencies using: pip install 'parquet-guard[engines]' or pip install fsspec s3fs"
            ) from err

    def exists(self, path: str) -> bool:
        fs = self._get_fs()
        return bool(fs.exists(path))

    def stat(self, path: str) -> FileStat:
        fs = self._get_fs()
        info = fs.info(path)
        etag = info.get("ETag") or info.get("etag") or info.get("VersionId")
        size = info.get("size", 0)
        mtime = int(info.get("mtime", 0) * 1e9) if "mtime" in info else 0
        is_dir = info.get("type") == "directory"
        return FileStat(
            path=path,
            size=size,
            mtime_ns=mtime,
            etag=etag,
            is_dir=is_dir,
        )

    def read_bytes(self, path: str, offset: Optional[int] = None, length: Optional[int] = None) -> bytes:
        fs = self._get_fs()
        start = offset or 0
        end = (start + length) if length is not None else None
        return bytes(fs.read_bytes(path, start=start, end=end))

    def list_files(self, prefix: str, pattern: Optional[str] = None, recursive: bool = True) -> List[str]:
        fs = self._get_fs()
        found = fs.find(prefix, detail=False) if recursive else fs.ls(prefix, detail=False)
        pat = pattern or "*.parquet"
        matched = [f for f in found if fnmatch.fnmatch(f, pat)]
        return sorted(matched)


def resolve_backend(path_or_url: str) -> Tuple[StorageBackend, str]:
    """Parse a path or URL string and return the appropriate StorageBackend and normalized path."""
    for scheme in ("s3://", "gs://", "gcs://", "az://", "abfss://", "hdfs://"):
        if path_or_url.startswith(scheme):
            return RemoteStorageBackend(path_or_url), path_or_url

    if path_or_url.startswith("file://"):
        return LocalStorageBackend(), path_or_url[len("file://") :]

    return LocalStorageBackend(), path_or_url
