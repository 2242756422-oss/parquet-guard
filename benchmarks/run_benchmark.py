"""Empirical benchmark comparing parquet-guard against full-table and naive scan methods."""

from __future__ import annotations

import gc
import platform
import time
import tracemalloc
from pathlib import Path
from typing import List, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from parquet_guard.core.auditor import audit_dataset


def create_synthetic_benchmark_dataset(
    base_dir: Path, num_partitions: int, rows_per_part: int = 10000
) -> Path:
    """Create realistic multi-partition Parquet dataset."""
    root = base_dir / f"dataset_{num_partitions}_partitions"
    root.mkdir(parents=True, exist_ok=True)

    schema = pa.schema(
        [
            ("event_id", pa.string(), False),
            ("user_id", pa.int64(), False),
            ("session_id", pa.string(), True),
            ("event_type", pa.string(), True),
            ("revenue", pa.float64(), True),
            ("timestamp", pa.timestamp("us", tz="UTC"), False),
            ("is_conversion", pa.bool_(), True),
            ("page_views", pa.int32(), True),
        ]
    )

    for p in range(num_partitions):
        part_dir = root / f"date=2026-10-{p % 30 + 1:02d}" / f"shard={p:03d}"
        part_dir.mkdir(parents=True, exist_ok=True)
        file_path = part_dir / "data.parquet"

        # Generate rows
        t = pa.Table.from_arrays(
            [
                pa.array([f"evt_{i}" for i in range(rows_per_part)]),
                pa.array([100000 + i for i in range(rows_per_part)]),
                pa.array([f"sess_{i % 50}" for i in range(rows_per_part)]),
                pa.array(["click" if i % 2 == 0 else "purchase" for i in range(rows_per_part)]),
                pa.array([float(i) * 1.5 for i in range(rows_per_part)]),
                pa.array(
                    [1700000000000000 + i * 1000 for i in range(rows_per_part)],
                    type=pa.timestamp("us", tz="UTC"),
                ),
                pa.array([i % 10 == 0 for i in range(rows_per_part)]),
                pa.array([i % 5 for i in range(rows_per_part)], type=pa.int32()),
            ],
            schema=schema,
        )
        pq.write_table(t, str(file_path), compression="snappy")

    return root


def measure_parquet_guard(dataset_path: Path, rounds: int = 5) -> Tuple[float, float]:
    """Measure latency and peak memory of parquet-guard zero-data-scan audit."""
    times: List[float] = []
    mems: List[float] = []

    for _ in range(rounds):
        gc.collect()
        tracemalloc.start()
        start = time.perf_counter()

        result = audit_dataset(dataset_path)
        assert result.is_consistent is True

        elapsed = (time.perf_counter() - start) * 1000  # ms
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        times.append(elapsed)
        mems.append(peak / 1024)  # KB

    return sum(times) / len(times), sum(mems) / len(mems)


def measure_naive_full_scan(dataset_path: Path, rounds: int = 5) -> Tuple[float, float]:
    """Measure latency and peak memory of traditional full-table data loading."""
    times: List[float] = []
    mems: List[float] = []

    files = list(dataset_path.glob("**/*.parquet"))

    for _ in range(rounds):
        gc.collect()
        tracemalloc.start()
        start = time.perf_counter()

        # Naive approach: read full tables to extract schemas
        schemas = []
        for f in files:
            table = pq.read_table(str(f))
            schemas.append(table.schema)

        elapsed = (time.perf_counter() - start) * 1000  # ms
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        times.append(elapsed)
        mems.append(peak / 1024)  # KB

    return sum(times) / len(times), sum(mems) / len(mems)


def main() -> None:
    bench_dir = Path("/tmp/parquet_guard_benchmarks")
    bench_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print(" PARQUET-GUARD REPRODUCIBLE COMPETITIVE BENCHMARK")
    print("=" * 70)
    print(f" Platform : {platform.platform()} ({platform.machine()})")
    print(f" Python   : {platform.python_version()}")
    print(f" PyArrow  : {pa.__version__}")
    print("=" * 70)

    partition_sizes = [5, 20, 50]
    results = []

    for parts in partition_sizes:
        print(f"\n[+] Preparing dataset with {parts} partitions (10,000 rows/file)...")
        ds_path = create_synthetic_benchmark_dataset(bench_dir, num_partitions=parts, rows_per_part=10000)

        total_bytes = sum(f.stat().st_size for f in ds_path.glob("**/*.parquet"))
        print(f"    Total Dataset Size on disk: {total_bytes / (1024 * 1024):.2f} MB")

        print("    Running parquet-guard (Zero-Data Footer)...")
        pg_time, pg_mem = measure_parquet_guard(ds_path, rounds=5)

        print("    Running Naive Full Table Scan (Baseline)...")
        naive_time, naive_mem = measure_naive_full_scan(ds_path, rounds=5)

        speedup = naive_time / pg_time if pg_time > 0 else 0
        mem_reduction = naive_mem / pg_mem if pg_mem > 0 else 0

        results.append(
            {
                "partitions": parts,
                "size_mb": total_bytes / (1024 * 1024),
                "pg_time_ms": pg_time,
                "naive_time_ms": naive_time,
                "speedup": speedup,
                "pg_mem_kb": pg_mem,
                "naive_mem_kb": naive_mem,
                "mem_reduction": mem_reduction,
            }
        )

        print(f"    --> parquet-guard: {pg_time:.2f} ms | Memory: {pg_mem:.1f} KB")
        print(f"    --> Naive Scan   : {naive_time:.2f} ms | Memory: {naive_mem:.1f} KB")
        print(f"    --> RESULT       : {speedup:.1f}x Faster | {mem_reduction:.1f}x Less Memory")

    print("\n" + "=" * 70)
    print(" SUMMARY TABLE FOR research/COMPETITOR_VALIDATION.md:")
    print("=" * 70)
    for r in results:
        print(
            f" {r['partitions']:2d} Partitions ({r['size_mb']:.1f}MB): "
            f"parquet-guard={r['pg_time_ms']:.2f}ms vs Naive={r['naive_time_ms']:.2f}ms "
            f"({r['speedup']:.1f}x faster, {r['mem_reduction']:.1f}x memory savings)"
        )


if __name__ == "__main__":
    main()
