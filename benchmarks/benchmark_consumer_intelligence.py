"""Empirical benchmark for consumer-aware schema intelligence and migration verification.

Evaluates:
1. Topology grouping & divergence concentration latency across 100 partitions.
2. Consumer impact evaluation scaling across 1 to 20 downstream consumers.
3. Isolated sandbox migration verification throughput (100 to 1,000 synthetic rows).
4. HTML diagnostic report generation latency.
"""

from __future__ import annotations

import json
import math
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List

import pyarrow as pa
import pyarrow.parquet as pq

from parquet_guard.core.auditor import audit_dataset
from parquet_guard.core.consumer import (
    ConsumerContract,
    ConsumerFieldRequirement,
    ConsumerGroupSpecification,
    evaluate_consumer_impact,
)
from parquet_guard.core.grouping import analyze_dataset_topology
from parquet_guard.core.html_reporter import generate_html_report
from parquet_guard.core.inspector import inspect_parquet_schema
from parquet_guard.core.migration_verifier import (
    ExecutableMigrationPlan,
    MigrationActionType,
    MigrationOperation,
    verify_migration_plan,
)
from parquet_guard.core.models import FieldDefinition, SchemaContract


def _calc_stats(samples: List[float]) -> Dict[str, float]:
    n = len(samples)
    if n == 0:
        return {"mean": 0.0, "median": 0.0, "min": 0.0, "max": 0.0, "stdev": 0.0}
    mean_val = sum(samples) / n
    sorted_s = sorted(samples)
    median_val = sorted_s[n // 2] if n % 2 == 1 else (sorted_s[n // 2 - 1] + sorted_s[n // 2]) / 2.0
    var = sum((x - mean_val) ** 2 for x in samples) / (n - 1) if n > 1 else 0.0
    return {
        "mean": round(mean_val, 3),
        "median": round(median_val, 3),
        "min": round(sorted_s[0], 3),
        "max": round(sorted_s[-1], 3),
        "stdev": round(math.sqrt(var), 3),
    }


def main() -> None:
    print("=" * 70)
    print("PARQUET GUARD: CONSUMER-AWARE SCHEMA INTELLIGENCE BENCHMARK")
    print("=" * 70)

    results: Dict[str, Any] = {"topology": {}, "consumer_scaling": {}, "sandbox_verification": {}}

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # 1. Generate 100 partitions with 3 schema groups
        print("\n[Phase 1] Generating 100 partitions (3 schema clusters)...")
        lake_dir = tmp_path / "lake_100"
        lake_dir.mkdir()

        for i in range(1, 101):
            p_dir = lake_dir / f"shard_{i:03d}"
            p_dir.mkdir()
            f_path = p_dir / "data.parquet"

            if i <= 70:
                # Group 1: 70% canonical
                t = pa.Table.from_arrays(
                    [
                        pa.array([i * 10 + j for j in range(500)], type=pa.int64()),
                        pa.array([f"usr_{j}" for j in range(500)], type=pa.string()),
                        pa.array([float(j) * 0.5 for j in range(500)], type=pa.float64()),
                        pa.array([1729000000000 + j for j in range(500)], type=pa.timestamp("ms", tz="UTC")),
                    ],
                    names=["id", "user_id", "score", "timestamp"],
                )
            elif i <= 90:
                # Group 2: 20% added metadata column
                t = pa.Table.from_arrays(
                    [
                        pa.array([i * 10 + j for j in range(500)], type=pa.int64()),
                        pa.array([f"usr_{j}" for j in range(500)], type=pa.string()),
                        pa.array([float(j) * 0.5 for j in range(500)], type=pa.float64()),
                        pa.array([1729000000000 + j for j in range(500)], type=pa.timestamp("ms", tz="UTC")),
                        pa.array(["web" if j % 2 == 0 else "mobile" for j in range(500)], type=pa.string()),
                    ],
                    names=["id", "user_id", "score", "timestamp", "channel"],
                )
            else:
                # Group 3: 10% narrowed score float32
                t = pa.Table.from_arrays(
                    [
                        pa.array([i * 10 + j for j in range(500)], type=pa.int64()),
                        pa.array([f"usr_{j}" for j in range(500)], type=pa.string()),
                        pa.array([float(j) * 0.5 for j in range(500)], type=pa.float32()),
                        pa.array([1729000000000 + j for j in range(500)], type=pa.timestamp("ms", tz="UTC")),
                    ],
                    names=["id", "user_id", "score", "timestamp"],
                )
            pq.write_table(t, f_path)

        # Benchmark audit
        audit_res = audit_dataset(str(lake_dir))
        assert audit_res.total_files_scanned == 100

        # Benchmark topology grouping latency (10 iterations)
        print("[Phase 2] Benchmarking analyze_dataset_topology() on 100 files...")
        topo_times: List[float] = []
        for _ in range(10):
            t0 = time.perf_counter()
            topo_rep = analyze_dataset_topology(audit_res)
            topo_times.append((time.perf_counter() - t0) * 1000.0)

        stats_topo = _calc_stats(topo_times)
        print(f"  • Topology Grouping: {stats_topo['mean']:.2f} ms (median {stats_topo['median']:.2f} ms)")
        print(f"  • Per-Partition Cost: {stats_topo['mean'] / 100.0:.3f} ms/partition")
        print(f"  • Clusters Detected:  {topo_rep.distinct_schemas_count}")
        results["topology"]["100_partitions"] = stats_topo

        # Benchmark Consumer Scaling (1, 5, 10, 20 consumers)
        print("\n[Phase 3] Benchmarking evaluate_consumer_impact() scaling...")
        sample_parquet = lake_dir / "shard_001" / "data.parquet"
        schema_prod = inspect_parquet_schema(sample_parquet)

        for num_c in [1, 5, 10, 20]:
            consumers = [
                ConsumerContract(
                    consumer_id=f"consumer-{k}",
                    consumer_name=f"Consumer Pipeline {k}",
                    compatibility_policy="strict" if k % 2 == 0 else "permissive",
                    fields=[
                        ConsumerFieldRequirement(name="id", permitted_types=["int64"]),
                        ConsumerFieldRequirement(name="user_id", permitted_types=["string"]),
                    ],
                )
                for k in range(num_c)
            ]
            g_spec = ConsumerGroupSpecification(consumers=consumers)

            c_times: List[float] = []
            for _ in range(20):
                t0 = time.perf_counter()
                imp = evaluate_consumer_impact(schema_prod, g_spec)
                c_times.append((time.perf_counter() - t0) * 1000.0)

            stats_c = _calc_stats(c_times)
            print(
                f"  • {num_c:2d} Consumers: {stats_c['mean']:.3f} ms (median {stats_c['median']:.3f} ms, "
                f"{stats_c['mean'] / num_c:.4f} ms/consumer)"
            )
            results["consumer_scaling"][str(num_c)] = stats_c

        # Benchmark Sandbox Migration Verification (100, 500, 1000 rows)
        print("\n[Phase 4] Benchmarking verify_migration_plan() sandbox throughput...")
        m_plan = ExecutableMigrationPlan(
            plan_id="bench-plan",
            title="Widen and dual-write",
            description="Synthetic benchmark migration",
            source_contract=schema_prod,
            target_contract=SchemaContract(
                fields=[
                    FieldDefinition(name="id", type="int64"),
                    FieldDefinition(name="user_id", type="string"),
                    FieldDefinition(name="score", type="float64"),
                    FieldDefinition(name="timestamp", type="timestamp[ms, tz=UTC]"),
                    FieldDefinition(name="id_v2", type="int64"),
                ]
            ),
            operations=[
                MigrationOperation(
                    action=MigrationActionType.DUAL_WRITE,
                    source_column="id",
                    target_column="id_v2",
                    target_type="int64",
                    description="Dual write id to id_v2",
                )
            ],
            affected_consumer_ids=["consumer-0"],
            deployment_order=["Phase 1"],
            rollback_procedure=["Rollback 1"],
            pyarrow_code="",
        )

        for row_cnt in [100, 500, 1000]:
            sand_times: List[float] = []
            for _ in range(10):
                t0 = time.perf_counter()
                _ = verify_migration_plan(m_plan, sample_rows=row_cnt)
                sand_times.append((time.perf_counter() - t0) * 1000.0)

            stats_s = _calc_stats(sand_times)
            print(
                f"  • {row_cnt:4d} Synthetic Rows: {stats_s['mean']:.2f} ms "
                f"(throughput: {(row_cnt / (stats_s['mean'] / 1000.0)):,.0f} rows/sec)"
            )
            results["sandbox_verification"][str(row_cnt)] = stats_s

        # Benchmark HTML Report Generation
        print("\n[Phase 5] Benchmarking generate_html_report()...")
        html_out = tmp_path / "bench_report.html"
        h_times: List[float] = []
        for _ in range(15):
            t0 = time.perf_counter()
            generate_html_report(
                output_path=str(html_out),
                audit_result=audit_res,
                topology_report=topo_rep,
                impact_report=imp,
                dataset_title="Benchmark Dataset",
            )
            h_times.append((time.perf_counter() - t0) * 1000.0)

        stats_h = _calc_stats(h_times)
        print(
            f"  • HTML Report Generation: {stats_h['mean']:.2f} ms (file size: {html_out.stat().st_size:,} bytes)"
        )
        results["html_report"] = stats_h

    # Save benchmark outputs
    out_file = Path(__file__).parent / "consumer_intelligence_benchmarks.json"
    out_file.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"\n✓ Saved empirical benchmark metrics to: {out_file}\n")


if __name__ == "__main__":
    main()
