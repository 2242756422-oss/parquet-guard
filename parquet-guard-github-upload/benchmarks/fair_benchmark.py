"""Fair and reproducible benchmark comparing parquet-guard against equivalent PyArrow primitives,
full-scan approaches, and evaluating incremental cache performance.

Methodology:
- Cold run vs Hot run vs Incremental cache hits (mean across 10 iterations)
- Partition counts: 10, 50, 100 partitions
- Varied file sizes: 10,000 rows/partition
- Fair separation of:
  1. Raw Footer Metadata extraction (pq.ParquetFile.schema_arrow vs parquet-guard inspect_schema)
  2. Full Table read (pq.read_table)
  3. Multi-partition consistency audit (parquet_guard audit_dataset full vs incremental vs manual full scan)
"""

from __future__ import annotations

import gc
import platform
import shutil
import time
from pathlib import Path
from typing import Dict, List

import pyarrow as pa
import pyarrow.parquet as pq

from parquet_guard.core.auditor import audit_dataset
from parquet_guard.core.inspector import inspect_schema


def create_partitioned_files(base_dir: Path, num_partitions: int, rows_per_partition: int) -> Path:
    """Generate realistic partitioned Parquet dataset on disk."""
    root = base_dir / f"bench_{num_partitions}parts_{rows_per_partition}rows"
    if root.exists():
        # Clean up existing to ensure fresh state
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)

    schema = pa.schema(
        [
            ("transaction_id", pa.string(), False),
            ("account_id", pa.int64(), False),
            ("amount", pa.decimal128(12, 2), False),
            ("currency", pa.string(), True),
            ("timestamp", pa.timestamp("us", tz="UTC"), False),
            ("status", pa.string(), True),
            ("retry_count", pa.int32(), True),
        ]
    )

    for p in range(num_partitions):
        part_dir = root / f"date=2026-10-{p % 30 + 1:02d}" / f"shard={p:03d}"
        part_dir.mkdir(parents=True, exist_ok=True)
        file_path = part_dir / "data.parquet"

        t = pa.Table.from_arrays(
            [
                pa.array([f"tx_{i}_{p}" for i in range(rows_per_partition)]),
                pa.array([1000000 + i for i in range(rows_per_partition)]),
                pa.array([1000 + i for i in range(rows_per_partition)], type=pa.decimal128(12, 2)),
                pa.array(["USD" if i % 2 == 0 else "EUR" for i in range(rows_per_partition)]),
                pa.array(
                    [1700000000000000 + i * 1000 for i in range(rows_per_partition)],
                    type=pa.timestamp("us", tz="UTC"),
                ),
                pa.array(["COMPLETED" if i % 3 == 0 else "PENDING" for i in range(rows_per_partition)]),
                pa.array([i % 4 for i in range(rows_per_partition)], type=pa.int32()),
            ],
            schema=schema,
        )
        pq.write_table(t, str(file_path), compression="snappy")

    return root


def benchmark_single_file_inspection(file_path: Path, rounds: int = 20) -> Dict[str, float]:
    """Compare raw PyArrow metadata read vs parquet-guard metadata inspection."""
    # 1. Raw PyArrow ParquetFile.schema_arrow
    times_pyarrow: List[float] = []
    for _ in range(rounds):
        start = time.perf_counter()
        pf = pq.ParquetFile(str(file_path))
        _ = pf.schema_arrow
        times_pyarrow.append((time.perf_counter() - start) * 1000)

    # 2. parquet-guard inspect_schema (reads footer + builds Pydantic SchemaContract)
    times_pg: List[float] = []
    for _ in range(rounds):
        start = time.perf_counter()
        _ = inspect_schema(file_path)
        times_pg.append((time.perf_counter() - start) * 1000)

    # 3. Naive full table read
    times_full: List[float] = []
    for _ in range(rounds):
        start = time.perf_counter()
        _ = pq.read_table(str(file_path))
        times_full.append((time.perf_counter() - start) * 1000)

    return {
        "pyarrow_raw_ms": sum(times_pyarrow) / len(times_pyarrow),
        "parquet_guard_ms": sum(times_pg) / len(times_pg),
        "full_scan_ms": sum(times_full) / len(times_full),
    }


def benchmark_multi_file_audit(dataset_dir: Path, rounds: int = 10) -> Dict[str, float]:
    """Compare multi-file dataset audit: cold, hot full, hot incremental, and manual full-scan loop."""
    files = list(dataset_dir.glob("**/*.parquet"))
    cache_file = dataset_dir / ".parquet_guard_cache.json"

    # 1. parquet-guard full audit (no cache)
    times_pg_full: List[float] = []
    for _ in range(rounds):
        start = time.perf_counter()
        res = audit_dataset(dataset_dir, incremental=False)
        assert res.is_consistent is True
        times_pg_full.append((time.perf_counter() - start) * 1000)

    # Prime cache for incremental
    if cache_file.exists():
        cache_file.unlink()
    audit_dataset(dataset_dir, incremental=True, cache_path=cache_file)

    # 2. parquet-guard incremental audit (100% cache hits)
    times_pg_incr: List[float] = []
    for _ in range(rounds):
        start = time.perf_counter()
        res_incr = audit_dataset(dataset_dir, incremental=True, cache_path=cache_file)
        assert res_incr.is_consistent is True
        assert res_incr.cache_hits == len(files)
        times_pg_incr.append((time.perf_counter() - start) * 1000)

    # Clean up cache file after benchmark
    if cache_file.exists():
        cache_file.unlink()

    # 3. Manual full table read loop
    times_full: List[float] = []
    for _ in range(rounds):
        start = time.perf_counter()
        schemas = [pq.read_table(str(f)).schema for f in files]
        assert len(schemas) == len(files)
        times_full.append((time.perf_counter() - start) * 1000)

    return {
        "parquet_guard_full_ms": sum(times_pg_full) / len(times_pg_full),
        "parquet_guard_incremental_ms": sum(times_pg_incr) / len(times_pg_incr),
        "manual_full_read_ms": sum(times_full) / len(times_full),
    }


def main() -> None:
    bench_dir = Path("/tmp/parquet_guard_fair_benchmarks")
    bench_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 78)
    print(" PARQUET-GUARD v0.2.0 FAIR & REPRODUCIBLE COMPETITIVE BENCHMARK")
    print("=" * 78)
    print(f" OS / Arch  : {platform.platform()} ({platform.machine()})")
    print(f" Python     : {platform.python_version()}")
    print(f" PyArrow    : {pa.__version__}")
    print("=" * 78)

    # Test 1: Single file inspection
    print("\n[1] Single File Inspection (10,000 rows, 7 columns)...")
    single_dir = create_partitioned_files(bench_dir, num_partitions=1, rows_per_partition=10000)
    single_file = next(single_dir.glob("**/*.parquet"))
    single_res = benchmark_single_file_inspection(single_file, rounds=20)
    print(f"    - PyArrow Raw C++ Footer (ParquetFile.schema_arrow): {single_res['pyarrow_raw_ms']:.3f} ms")
    print(f"    - parquet-guard (Footer Read + SchemaContract AST) : {single_res['parquet_guard_ms']:.3f} ms")
    print(f"    - Naive Full-Data Scan (pq.read_table)             : {single_res['full_scan_ms']:.3f} ms")
    print(
        f"    --> Real Python contract overhead: +{single_res['parquet_guard_ms'] - single_res['pyarrow_raw_ms']:.3f} ms "
        f"over bare C++ to provide structured Pydantic models & validation."
    )

    # Test 2: Multi-file Partition Audit across partition counts (10, 50, 100)
    print("\n[2] Multi-Partition Dataset Auditing (Cold vs Hot Full vs Incremental vs Full Scan)...")
    for part_count in [10, 50, 100]:
        ds_dir = create_partitioned_files(bench_dir, num_partitions=part_count, rows_per_partition=10000)
        total_size_mb = sum(f.stat().st_size for f in ds_dir.glob("**/*.parquet")) / (1024 * 1024)

        # Cold run
        gc.collect()
        t0 = time.perf_counter()
        _ = audit_dataset(ds_dir, incremental=False)
        cold_ms = (time.perf_counter() - t0) * 1000

        # Hot runs
        audit_res = benchmark_multi_file_audit(ds_dir, rounds=10)
        hot_full_ms = audit_res["parquet_guard_full_ms"]
        hot_incr_ms = audit_res["parquet_guard_incremental_ms"]
        full_scan_ms = audit_res["manual_full_read_ms"]

        print(f"\n    * {part_count:3d} partitions ({total_size_mb:.2f} MB total across disk):")
        print(f"        parquet-guard Cold Run (I/O scan)       : {cold_ms:.2f} ms")
        print(
            f"        parquet-guard Hot Full Audit (Mean 10x) : {hot_full_ms:.2f} ms "
            f"({(hot_full_ms / part_count):.2f} ms/partition)"
        )
        print(
            f"        parquet-guard Incremental Audit (Cached): {hot_incr_ms:.2f} ms "
            f"({(hot_incr_ms / part_count):.3f} ms/partition, {hot_full_ms / max(hot_incr_ms, 0.001):.1f}x speedup vs cold)"
        )
        print(
            f"        Manual Full Table Scan (pq.read_table)  : {full_scan_ms:.2f} ms "
            f"({(full_scan_ms / part_count):.2f} ms/partition)"
        )
        print(
            f"        Speedup vs Full Data Scan               : {full_scan_ms / hot_full_ms:.1f}x (full) / "
            f"{full_scan_ms / max(hot_incr_ms, 0.001):.1f}x (incremental)"
        )

    print("\nBenchmark completed successfully.")


if __name__ == "__main__":
    main()
