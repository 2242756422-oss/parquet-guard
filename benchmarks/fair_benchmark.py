"""Fair and reproducible benchmark comparing parquet-guard against equivalent PyArrow primitives,
full-scan approaches, and evaluating incremental cache performance.

Methodology:
- Separate Warmup rounds (5 iterations) before recording measurements.
- Cold run vs Hot run vs Incremental cache hits (15 recorded repetitions).
- Statistical dispersion reporting: Mean, Median, Min, Max, and Standard Deviation.
- Partition counts: 10, 50, 100 partitions (10,000 rows/partition).
- Fair separation of:
  1. Raw Footer Metadata extraction (pq.ParquetFile.schema_arrow vs parquet-guard inspect_schema)
  2. Full Table read (pq.read_table) - illustrates payload reading vs footer inspection.
  3. Multi-partition consistency audit (parquet_guard audit_dataset full vs incremental vs manual full scan)
"""

from __future__ import annotations

import gc
import json
import math
import platform
import shutil
import time
from pathlib import Path
from typing import Any, Dict, List

import pyarrow as pa
import pyarrow.parquet as pq

from parquet_guard.core.auditor import audit_dataset
from parquet_guard.core.inspector import inspect_schema


def _calc_stats(samples: List[float]) -> Dict[str, float]:
    """Compute summary statistics (mean, median, min, max, stdev) for timing samples."""
    n = len(samples)
    if n == 0:
        return {"mean": 0.0, "median": 0.0, "min": 0.0, "max": 0.0, "stdev": 0.0}
    mean_val = sum(samples) / n
    sorted_s = sorted(samples)
    median_val = sorted_s[n // 2] if n % 2 == 1 else (sorted_s[n // 2 - 1] + sorted_s[n // 2]) / 2.0
    variance = sum((x - mean_val) ** 2 for x in samples) / (n - 1) if n > 1 else 0.0
    stdev_val = math.sqrt(variance)
    return {
        "mean": mean_val,
        "median": median_val,
        "min": sorted_s[0],
        "max": sorted_s[-1],
        "stdev": stdev_val,
    }


def create_partitioned_files(base_dir: Path, num_partitions: int, rows_per_partition: int) -> Path:
    """Generate realistic partitioned Parquet dataset on disk."""
    root = base_dir / f"bench_{num_partitions}parts_{rows_per_partition}rows"
    if root.exists():
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


def benchmark_single_file_inspection(file_path: Path, rounds: int = 30, warmup: int = 5) -> Dict[str, Any]:
    """Compare raw PyArrow metadata read vs parquet-guard metadata inspection with stats."""
    # Warmup
    for _ in range(warmup):
        _ = pq.ParquetFile(str(file_path)).schema_arrow
        _ = inspect_schema(file_path)
        _ = pq.read_table(str(file_path))

    # Alternating measurements to reduce order-bias
    times_pyarrow: List[float] = []
    times_pg: List[float] = []
    times_full: List[float] = []

    for _ in range(rounds):
        # 1. PyArrow raw footer
        t0 = time.perf_counter()
        pf = pq.ParquetFile(str(file_path))
        _ = pf.schema_arrow
        times_pyarrow.append((time.perf_counter() - t0) * 1000)

        # 2. parquet-guard inspect_schema
        t1 = time.perf_counter()
        _ = inspect_schema(file_path)
        times_pg.append((time.perf_counter() - t1) * 1000)

        # 3. Naive full table read
        t2 = time.perf_counter()
        _ = pq.read_table(str(file_path))
        times_full.append((time.perf_counter() - t2) * 1000)

    return {
        "pyarrow_raw": _calc_stats(times_pyarrow),
        "parquet_guard": _calc_stats(times_pg),
        "full_scan": _calc_stats(times_full),
        "raw_samples": {
            "pyarrow_raw": times_pyarrow,
            "parquet_guard": times_pg,
            "full_scan": times_full,
        },
    }


def benchmark_multi_file_audit(dataset_dir: Path, rounds: int = 15, warmup: int = 3) -> Dict[str, Any]:
    """Compare multi-file dataset audit: cold, hot full, hot incremental, and manual full-scan loop."""
    files = list(dataset_dir.glob("**/*.parquet"))
    cache_file = dataset_dir / ".parquet_guard_cache.json"

    # Warmup
    for _ in range(warmup):
        _ = audit_dataset(dataset_dir, incremental=False)

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

    times_pg_incr: List[float] = []
    for _ in range(rounds):
        start = time.perf_counter()
        res_incr = audit_dataset(dataset_dir, incremental=True, cache_path=cache_file)
        assert res_incr.is_consistent is True
        assert res_incr.cache_hits == len(files)
        times_pg_incr.append((time.perf_counter() - start) * 1000)

    if cache_file.exists():
        cache_file.unlink()

    times_full: List[float] = []
    for _ in range(rounds):
        start = time.perf_counter()
        schemas = [pq.read_table(str(f)).schema for f in files]
        assert len(schemas) == len(files)
        times_full.append((time.perf_counter() - start) * 1000)

    return {
        "parquet_guard_full": _calc_stats(times_pg_full),
        "parquet_guard_incremental": _calc_stats(times_pg_incr),
        "manual_full_read": _calc_stats(times_full),
        "raw_samples": {
            "parquet_guard_full": times_pg_full,
            "parquet_guard_incremental": times_pg_incr,
            "manual_full_read": times_full,
        },
    }


def main() -> None:
    bench_dir = Path("/tmp/parquet_guard_fair_benchmarks")
    bench_dir.mkdir(parents=True, exist_ok=True)

    header = [
        "=" * 78,
        " PARQUET-GUARD v0.2.0 FAIR & REPRODUCIBLE BENCHMARK",
        "=" * 78,
        f" OS / Arch  : {platform.platform()} ({platform.machine()})",
        f" Python     : {platform.python_version()}",
        f" PyArrow    : {pa.__version__}",
        "=" * 78,
    ]
    output_lines = list(header)
    for line in header:
        print(line)

    results_export: Dict[str, Any] = {
        "metadata": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "python": platform.python_version(),
            "pyarrow": pa.__version__,
            "timestamp": time.time(),
        },
        "benchmarks": {},
    }

    # Test 1: Single file inspection
    sec1 = ["\n[1] Single File Inspection (10,000 rows, 7 columns, 30 repetitions)..."]
    single_dir = create_partitioned_files(bench_dir, num_partitions=1, rows_per_partition=10000)
    single_file = next(single_dir.glob("**/*.parquet"))
    single_res = benchmark_single_file_inspection(single_file, rounds=30, warmup=5)

    py_stats = single_res["pyarrow_raw"]
    pg_stats = single_res["parquet_guard"]
    full_stats = single_res["full_scan"]

    sec1.append(
        f"    - PyArrow Raw C++ Footer (schema_arrow) : mean {py_stats['mean']:.3f} ms "
        f"[median {py_stats['median']:.3f}, min {py_stats['min']:.3f}, max {py_stats['max']:.3f}, stdev {py_stats['stdev']:.3f}]"
    )
    sec1.append(
        f"    - parquet-guard inspect_schema (AST)     : mean {pg_stats['mean']:.3f} ms "
        f"[median {pg_stats['median']:.3f}, min {pg_stats['min']:.3f}, max {pg_stats['max']:.3f}, stdev {pg_stats['stdev']:.3f}]"
    )
    sec1.append(
        f"    - Full Data Scan (pq.read_table)        : mean {full_stats['mean']:.3f} ms "
        f"[median {full_stats['median']:.3f}, min {full_stats['min']:.3f}, max {full_stats['max']:.3f}, stdev {full_stats['stdev']:.3f}]"
    )
    overhead = pg_stats["mean"] - py_stats["mean"]
    sec1.append(
        f"    --> Python SchemaContract AST construction overhead: {overhead:+.3f} ms over bare C++ footer read."
    )
    sec1.append(
        "    --> Note: Comparing metadata-only inspection against pq.read_table illustrates the architectural\n"
        "        difference between reading a ~10KB footer vs full payload ingestion; it is not a comparison against\n"
        "        other metadata-based schema validators."
    )

    for line in sec1:
        print(line)
    output_lines.extend(sec1)
    results_export["benchmarks"]["single_file"] = single_res

    # Test 2: Multi-file Partition Audit across partition counts (10, 50, 100)
    sec2 = "\n[2] Multi-Partition Dataset Auditing (15 repetitions per partition size)..."
    print(sec2)
    output_lines.append(sec2)
    results_export["benchmarks"]["multi_partition"] = {}

    for part_count in [10, 50, 100]:
        ds_dir = create_partitioned_files(bench_dir, num_partitions=part_count, rows_per_partition=10000)
        total_size_mb = sum(f.stat().st_size for f in ds_dir.glob("**/*.parquet")) / (1024 * 1024)

        gc.collect()
        t0 = time.perf_counter()
        _ = audit_dataset(ds_dir, incremental=False)
        cold_ms = (time.perf_counter() - t0) * 1000

        audit_res = benchmark_multi_file_audit(ds_dir, rounds=15, warmup=3)
        hot_full = audit_res["parquet_guard_full"]
        hot_incr = audit_res["parquet_guard_incremental"]
        full_read = audit_res["manual_full_read"]

        part_lines = [
            f"\n    * {part_count:3d} partitions ({total_size_mb:.2f} MB total across disk):",
            f"        parquet-guard Cold Run (I/O scan)       : {cold_ms:.2f} ms",
            f"        parquet-guard Hot Full Audit (mean 15x) : {hot_full['mean']:.2f} ms "
            f"[median {hot_full['median']:.2f}, stdev {hot_full['stdev']:.2f}] ({(hot_full['mean'] / part_count):.3f} ms/part)",
            f"        parquet-guard Incremental Audit (cached): {hot_incr['mean']:.2f} ms "
            f"[median {hot_incr['median']:.2f}, stdev {hot_incr['stdev']:.2f}] ({(hot_incr['mean'] / part_count):.3f} ms/part, "
            f"{hot_full['mean'] / max(hot_incr['mean'], 0.001):.1f}x speedup vs hot full)",
            f"        Manual Full Table Scan (pq.read_table)  : {full_read['mean']:.2f} ms "
            f"[median {full_read['median']:.2f}, stdev {full_read['stdev']:.2f}] ({(full_read['mean'] / part_count):.2f} ms/part)",
            f"        Ratio vs Full Payload Scan              : {full_read['mean'] / hot_full['mean']:.1f}x (hot full) / "
            f"{full_read['mean'] / max(hot_incr['mean'], 0.001):.1f}x (incremental)",
        ]
        for line in part_lines:
            print(line)
        output_lines.extend(part_lines)

        results_export["benchmarks"]["multi_partition"][str(part_count)] = {
            "partitions": part_count,
            "total_size_mb": total_size_mb,
            "cold_ms": cold_ms,
            "hot_full": hot_full,
            "hot_incremental": hot_incr,
            "manual_full_read": full_read,
        }

    footer = ["\nBenchmark completed successfully."]
    for line in footer:
        print(line)
    output_lines.extend(footer)

    # Save raw outputs
    out_dir = Path(__file__).parent
    raw_txt_path = out_dir / "raw_benchmark_output.txt"
    raw_json_path = out_dir / "benchmark_results.json"

    raw_txt_path.write_text("\n".join(output_lines) + "\n", encoding="utf-8")
    raw_json_path.write_text(json.dumps(results_export, indent=2), encoding="utf-8")
    print(f"\nRaw benchmark logs saved to: {raw_txt_path}")
    print(f"JSON benchmark metrics saved to: {raw_json_path}")


if __name__ == "__main__":
    main()
