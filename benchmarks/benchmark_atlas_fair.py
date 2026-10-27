"""Fair, rigorous statistical benchmark for Parquet Guard.

Measures latency and throughput characteristics across:
1. Zero-data-scan metadata inspection (Parquet Guard core).
2. Incremental cache hit vs cold metadata scan.
3. PyArrow runtime probe.
4. DuckDB runtime probe.
5. Polars runtime probe.

Reports statistical dispersion: mean, median, min, max, p95, and std_dev.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import tempfile
import time
from pathlib import Path
from typing import Any, Callable, Dict, List

import pyarrow as pa
import pyarrow.parquet as pq

from parquet_guard.core.auditor import audit_dataset
from parquet_guard.core.inspector import inspect_schema
from parquet_guard.verification.duckdb_engine import DuckDBEngineAdapter
from parquet_guard.verification.polars_engine import PolarsEngineAdapter
from parquet_guard.verification.pyarrow_engine import PyArrowEngineAdapter


def compute_stats(times_ms: List[float]) -> Dict[str, float]:
    """Compute comprehensive statistical dispersion metrics in milliseconds."""
    if not times_ms:
        return {}
    sorted_times = sorted(times_ms)
    n = len(sorted_times)
    mean_val = statistics.mean(sorted_times)
    median_val = statistics.median(sorted_times)
    min_val = sorted_times[0]
    max_val = sorted_times[-1]
    std_dev = statistics.stdev(sorted_times) if n > 1 else 0.0
    p95_idx = min(int(math.ceil(0.95 * n)) - 1, n - 1)
    p95_val = sorted_times[p95_idx]

    return {
        "iterations": n,
        "mean_ms": round(mean_val, 3),
        "median_ms": round(median_val, 3),
        "p95_ms": round(p95_val, 3),
        "min_ms": round(min_val, 3),
        "max_ms": round(max_val, 3),
        "std_dev_ms": round(std_dev, 3),
    }


def benchmark_function(fn: Callable[[], Any], warmup: int = 3, iterations: int = 20) -> Dict[str, float]:
    """Execute warmup cycles and collect real timing observations."""
    for _ in range(warmup):
        fn()

    times_ms: List[float] = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        fn()
        t1 = time.perf_counter()
        times_ms.append((t1 - t0) * 1000.0)

    return compute_stats(times_ms)


def setup_benchmark_dataset(target_dir: Path, num_files: int = 5, rows_per_file: int = 20_000) -> List[str]:
    """Generate realistic test Parquet partitions with 10 mixed columns."""
    files: List[str] = []
    for i in range(num_files):
        fpath = target_dir / f"partition_part_{i}.parquet"
        tbl = pa.table(
            {
                "order_id": pa.array([100000 + j for j in range(rows_per_file)], type=pa.int64()),
                "customer_id": pa.array([j % 500 for j in range(rows_per_file)], type=pa.int32()),
                "sku": pa.array([f"SKU-{j % 200}" for j in range(rows_per_file)], type=pa.string()),
                "amount": pa.array([100.50 + (j % 50) for j in range(rows_per_file)], type=pa.float64()),
                "is_fulfilled": pa.array([j % 2 == 0 for j in range(rows_per_file)], type=pa.bool_()),
                "created_at": pa.array(
                    [1700000000000000 + j * 1000 for j in range(rows_per_file)],
                    type=pa.timestamp("us", tz="UTC"),
                ),
            }
        )
        pq.write_table(tbl, str(fpath), compression="snappy")
        files.append(str(fpath))
    return files


def run_benchmarks(iterations: int = 20, warmup: int = 3) -> Dict[str, Any]:
    """Run fair benchmarks and return JSON-serializable benchmark suite summary."""
    with tempfile.TemporaryDirectory(prefix="pg_benchmark_") as tmp_dir_str:
        tmp_dir = Path(tmp_dir_str)
        files = setup_benchmark_dataset(tmp_dir, num_files=5, rows_per_file=10_000)
        cache_file = tmp_dir / ".benchmark_cache.json"

        pyarrow_adapter = PyArrowEngineAdapter()
        duckdb_adapter = DuckDBEngineAdapter()
        polars_adapter = PolarsEngineAdapter()

        results: Dict[str, Any] = {}

        # 1. Parquet Guard Zero-Data-Scan Single File Inspection
        results["zero_data_scan_single_file"] = benchmark_function(
            lambda: inspect_schema(files[0]), warmup=warmup, iterations=iterations
        )

        # 2. Cold Full Directory Audit (5 files, 50,000 rows metadata)
        results["dataset_audit_cold"] = benchmark_function(
            lambda: audit_dataset(tmp_dir, incremental=False), warmup=warmup, iterations=iterations
        )

        # 3. Incremental Cache Hit Audit
        # Pre-populate cache
        audit_dataset(tmp_dir, incremental=True, cache_path=cache_file, require_content_hash=True)
        results["dataset_audit_incremental_cache_hit"] = benchmark_function(
            lambda: audit_dataset(
                tmp_dir, incremental=True, cache_path=cache_file, require_content_hash=True
            ),
            warmup=warmup,
            iterations=iterations,
        )

        # 4. PyArrow Engine Probe (Read limit 10 rows)
        if pyarrow_adapter.is_available():
            results["engine_probe_pyarrow"] = benchmark_function(
                lambda: pyarrow_adapter.probe_read(files[:1], row_limit=10),
                warmup=warmup,
                iterations=iterations,
            )

        # 5. DuckDB Engine Probe (Read limit 10 rows)
        if duckdb_adapter.is_available():
            results["engine_probe_duckdb"] = benchmark_function(
                lambda: duckdb_adapter.probe_read(files[:1], row_limit=10),
                warmup=warmup,
                iterations=iterations,
            )

        # 6. Polars Engine Probe (Read limit 10 rows)
        if polars_adapter.is_available():
            results["engine_probe_polars"] = benchmark_function(
                lambda: polars_adapter.probe_read(files[:1], row_limit=10),
                warmup=warmup,
                iterations=iterations,
            )

        return results


def print_table(results: Dict[str, Any]) -> None:
    """Print readable text comparison table."""
    print("=" * 86)
    print(" PARQUET GUARD AI — FAIR STATISTICAL BENCHMARK (PROJECT ATLAS)")
    print("=" * 86)
    header = f"{'Benchmark Target':<38} | {'Median':<9} | {'Mean':<9} | {'P95':<9} | {'Std Dev':<9}"
    print(header)
    print("-" * 86)
    for name, stats in results.items():
        if not stats:
            continue
        line = (
            f"{name:<38} | "
            f"{stats['median_ms']:>6.2f} ms | "
            f"{stats['mean_ms']:>6.2f} ms | "
            f"{stats['p95_ms']:>6.2f} ms | "
            f"{stats['std_dev_ms']:>6.2f} ms"
        )
        print(line)
    print("=" * 86)


def main() -> None:
    parser = argparse.ArgumentParser(description="Parquet Guard Atlas Fair Benchmark Suite")
    parser.add_argument("-n", "--iterations", type=int, default=15, help="Number of benchmark iterations")
    parser.add_argument("-w", "--warmup", type=int, default=3, help="Number of warmup iterations")
    parser.add_argument("--json", action="store_true", help="Output results in JSON format")
    args = parser.parse_args()

    results = run_benchmarks(iterations=args.iterations, warmup=args.warmup)

    if args.json:
        print(json.dumps(results, indent=2))
    else:
        print_table(results)


if __name__ == "__main__":
    main()
