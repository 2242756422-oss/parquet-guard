"""Tests for storage backends and footer checksum cache verification."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from parquet_guard.core.auditor import audit_dataset
from parquet_guard.core.cache import (
    CachedFileInfo,
    compute_footer_checksum,
    verify_cached_file_identity,
)
from parquet_guard.storage import (
    FileStat,
    LocalStorageBackend,
    MemoryStorageBackend,
    RemoteStorageBackend,
    StorageBackendError,
    resolve_backend,
)


def test_local_storage_backend(tmp_path: Path) -> None:
    test_file = tmp_path / "sample.parquet"
    table = pa.Table.from_arrays([pa.array([1, 2, 3])], names=["id"])
    pq.write_table(table, test_file)

    backend = LocalStorageBackend()
    assert backend.exists(str(test_file))
    stat = backend.stat(str(test_file))
    assert isinstance(stat, FileStat)
    assert stat.size > 0
    assert not stat.is_dir

    # Test reading magic bytes (first 4 bytes: PAR1)
    magic = backend.read_bytes(str(test_file), offset=0, length=4)
    assert magic == b"PAR1"

    # Test reading tail magic bytes
    tail = backend.read_bytes(str(test_file), offset=stat.size - 4, length=4)
    assert tail == b"PAR1"

    checksum = backend.compute_footer_checksum(str(test_file))
    assert checksum is not None
    assert len(checksum) == 64

    # Test list files
    files = backend.list_files(str(tmp_path), pattern="*.parquet")
    assert len(files) == 1
    assert files[0] == str(test_file.resolve())


def test_memory_storage_backend() -> None:
    backend = MemoryStorageBackend()
    sample_data = b"PAR1" + b"some_metadata" + b"PAR1"
    backend.write_file("s3://fake-bucket/data/part1.parquet", sample_data, etag="etag-123")

    assert backend.exists("s3://fake-bucket/data/part1.parquet")
    stat = backend.stat("s3://fake-bucket/data/part1.parquet")
    assert stat.size == len(sample_data)
    assert stat.etag == "etag-123"

    magic = backend.read_bytes("s3://fake-bucket/data/part1.parquet", offset=0, length=4)
    assert magic == b"PAR1"

    tail = backend.read_bytes("s3://fake-bucket/data/part1.parquet", offset=stat.size - 4, length=4)
    assert tail == b"PAR1"

    checksum = backend.compute_footer_checksum("s3://fake-bucket/data/part1.parquet", chunk_size=8)
    assert checksum is not None
    assert len(checksum) == 64

    listed = backend.list_files("s3://fake-bucket/data", pattern="*.parquet")
    assert len(listed) == 1
    assert listed[0] == "s3://fake-bucket/data/part1.parquet"


def test_resolve_backend_routing() -> None:
    backend, path = resolve_backend("s3://my-bucket/dataset")
    assert isinstance(backend, RemoteStorageBackend)
    assert path == "s3://my-bucket/dataset"

    backend, path = resolve_backend("gs://google-bucket/data")
    assert isinstance(backend, RemoteStorageBackend)

    backend, path = resolve_backend("file:///tmp/data.parquet")
    assert isinstance(backend, LocalStorageBackend)
    assert path == "/tmp/data.parquet"

    backend, path = resolve_backend("/var/log/data.parquet")
    assert isinstance(backend, LocalStorageBackend)
    assert path == "/var/log/data.parquet"


def test_remote_storage_backend_missing_fsspec_message(monkeypatch: pytest.MonkeyPatch) -> None:
    import sys

    # Simulate fsspec unimportable
    monkeypatch.setitem(sys.modules, "fsspec", None)
    remote = RemoteStorageBackend("s3://bucket/test")
    with pytest.raises(StorageBackendError) as exc_info:
        remote.exists("s3://bucket/test/file.parquet")
    assert "fsspec" in str(exc_info.value)


def test_footer_checksum_and_cache_invalidation(tmp_path: Path) -> None:
    file_path = tmp_path / "part_v1.parquet"
    table1 = pa.Table.from_arrays([pa.array([10, 20])], names=["val"])
    pq.write_table(table1, file_path)

    st1 = file_path.stat()
    hash1 = compute_footer_checksum(file_path)
    assert hash1 is not None

    cached_entry = CachedFileInfo(
        file_size_bytes=st1.st_size,
        mtime_ns=st1.st_mtime_ns,
        row_count=2,
        row_groups=1,
        column_count=1,
        schema_fingerprint="fp1",
        content_hash=hash1,
        cache_confidence="HIGH_RELIABLE",
    )

    # 1. Matching check
    valid, reason = verify_cached_file_identity(file_path, cached_entry, require_hash=True)
    assert valid is True
    assert reason == "MATCH_HIGH_RELIABLE"

    # 2. Simulate silent data mutation with artificial same size
    # Overwrite file with different schema
    table2 = pa.Table.from_arrays([pa.array(["abc", "xyz"])], names=["str_col"])
    pq.write_table(table2, file_path)

    # If mtime and size were somehow matched or we force hash check:
    valid, reason = verify_cached_file_identity(file_path, cached_entry, require_hash=True)
    assert valid is False
    assert reason in ("CHECKSUM_MISMATCH", "SIZE_CHANGED", "MTIME_CHANGED")


def test_audit_dataset_with_footer_checksum_incremental(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    file_a = data_dir / "a.parquet"
    file_b = data_dir / "b.parquet"

    table_a = pa.Table.from_arrays([pa.array([1, 2, 3])], names=["x"])
    table_b = pa.Table.from_arrays([pa.array([4, 5, 6])], names=["x"])
    pq.write_table(table_a, file_a)
    pq.write_table(table_b, file_b)

    cache_file = tmp_path / "cache.json"

    # Run 1: cold audit with incremental=True and require_content_hash=True
    res1 = audit_dataset(
        data_dir,
        incremental=True,
        cache_path=cache_file,
        require_content_hash=True,
    )
    assert res1.is_consistent is True
    assert res1.total_files_scanned == 2

    # Verify cache file was written with content_hash populated
    assert cache_file.exists()
    from parquet_guard.core.cache import load_audit_cache

    cache = load_audit_cache(cache_file)
    assert cache is not None
    for item in cache.files.values():
        assert item.content_hash is not None
        assert item.cache_confidence == "HIGH_RELIABLE"

    # Run 2: warm audit - should hit cache
    res2 = audit_dataset(
        data_dir,
        incremental=True,
        cache_path=cache_file,
        require_content_hash=True,
    )
    assert res2.is_consistent is True
    assert res2.total_files_scanned == 2
