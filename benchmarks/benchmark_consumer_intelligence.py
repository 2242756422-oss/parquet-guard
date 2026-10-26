"""Empirical benchmark for consumer-aware schema intelligence and migration verification.

Separates and measures:
1. Dataset file discovery (100 partition directories)
2. Parquet footer metadata inspection (`inspect_parquet_schema` across 100 files)
3. Deterministic schema fingerprinting (`compute_schema_fingerprint` across 100 schemas)
4. In-memory topology grouping (`analyze_dataset_topology` on pre-audited result)
5. Full dataset audit + topology grouping (`audit_dataset` + `analyze_dataset_topology`, cold vs warm)
6. Consumer impact evaluation scaling (1, 5, 10, 20 consumers)
7. Isolated sandbox migration verification (100, 500, 1,000 synthetic rows, cold vs warm)
8. Self-contained HTML diagnostic report generation
9. Complete end-to-end pipeline workflow (audit + topology + consumer impact + migration sandbox + HTML report)
"""

from __future__ import annotations

import json
import math
import platform
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List

import pyarrow as pa
import pyarrow.parquet as pq

from parquet_guard.core.auditor import audit_dataset, compute_schema_fingerprint
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
    print("=" * 74)
    print("PARQUET GUARD: CONSUMER-AWARE SCHEMA INTELLIGENCE BENCHMARK")
    print("=" * 74)

    env_info: Dict[str, Any] = {
        "os": f"{platform.system()} {platform.release()}",
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor() or platform.machine(),
        "python_version": sys.version.split()[0],
        "pyarrow_version": pa.__version__,
        "warmup_iterations": 2,
        "measured_repetitions": {
            "discovery_and_inspection": 10,
            "topology_grouping": 10,
            "consumer_scaling": 20,
            "sandbox_verification": 10,
            "html_report": 15,
            "end_to_end": 5,
        },
    }

    results: Dict[str, Any] = {
        "environment": env_info,
        "stages": {},
        "topology": {},
        "consumer_scaling": {},
        "sandbox_verification": {},
        "html_report": {},
    }

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        print("\n[Phase 1] Generating 100 partitions (3 schema clusters)...")
        lake_dir = tmp_path / "lake_100"
        lake_dir.mkdir()

        for i in range(1, 101):
            p_dir = lake_dir / f"shard_{i:03d}"
            p_dir.mkdir()
            f_path = p_dir / "data.parquet"

            if i <= 70:
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

        # Stage 1: Dataset discovery (100 files)
        print("[Phase 2] Benchmarking dataset discovery, footer inspection, fingerprinting, and grouping...")
        disc_times: List[float] = []
        discovered_files: List[Path] = []
        for _ in range(10):
            t0 = time.perf_counter()
            discovered_files = sorted(lake_dir.rglob("*.parquet"))
            disc_times.append((time.perf_counter() - t0) * 1000.0)
        assert len(discovered_files) == 100
        results["stages"]["dataset_discovery_100_files"] = _calc_stats(disc_times)

        # Stage 2: Parquet metadata inspection (100 files)
        insp_times: List[float] = []
        inspected_schemas: List[SchemaContract] = []
        for _ in range(10):
            t0 = time.perf_counter()
            inspected_schemas = [inspect_parquet_schema(fp) for fp in discovered_files]
            insp_times.append((time.perf_counter() - t0) * 1000.0)
        results["stages"]["parquet_metadata_inspection_100_files"] = _calc_stats(insp_times)

        # Stage 3: Schema fingerprinting (100 schemas)
        fp_times: List[float] = []
        for _ in range(10):
            t0 = time.perf_counter()
            _ = [compute_schema_fingerprint(sc) for sc in inspected_schemas]
            fp_times.append((time.perf_counter() - t0) * 1000.0)
        results["stages"]["schema_fingerprinting_100_schemas"] = _calc_stats(fp_times)

        # Stage 4: Full dataset audit (cold + warm) and in-memory topology grouping
        t_cold0 = time.perf_counter()
        audit_res = audit_dataset(str(lake_dir))
        cold_audit_ms = round((time.perf_counter() - t_cold0) * 1000.0, 3)
        assert audit_res.total_files_scanned == 100

        warm_audit_times: List[float] = []
        for _ in range(10):
            t0 = time.perf_counter()
            audit_res = audit_dataset(str(lake_dir))
            warm_audit_times.append((time.perf_counter() - t0) * 1000.0)
        results["stages"]["full_dataset_audit_100_files"] = {
            "cold_first_run_ms": cold_audit_ms,
            **_calc_stats(warm_audit_times),
        }

        topo_times: List[float] = []
        for _ in range(10):
            t0 = time.perf_counter()
            topo_rep = analyze_dataset_topology(audit_res)
            topo_times.append((time.perf_counter() - t0) * 1000.0)

        stats_topo = _calc_stats(topo_times)
        results["stages"]["in_memory_schema_grouping_100_files"] = stats_topo
        results["topology"]["100_partitions"] = {
            **stats_topo,
            "note": "In-memory grouping on pre-audited PartitionAuditResult; see stages.full_dataset_audit_100_files for full I/O + footer scan latency.",
            "full_audit_plus_grouping_mean_ms": round(
                results["stages"]["full_dataset_audit_100_files"]["mean"] + stats_topo["mean"], 3
            ),
        }
        print(
            f"  • Discovery (100 files):         {results['stages']['dataset_discovery_100_files']['mean']:.2f} ms"
        )
        print(
            f"  • Footer Inspection (100 files): {results['stages']['parquet_metadata_inspection_100_files']['mean']:.2f} ms"
        )
        print(
            f"  • Fingerprinting (100 schemas):  {results['stages']['schema_fingerprinting_100_schemas']['mean']:.2f} ms"
        )
        print(f"  • In-Memory Grouping (100):      {stats_topo['mean']:.2f} ms")
        print(
            f"  • Full Audit + Grouping (100):   {results['topology']['100_partitions']['full_audit_plus_grouping_mean_ms']:.2f} ms"
        )

        # Stage 5: Consumer Impact Analysis Scaling (1, 5, 10, 20 consumers)
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

            # Warmup
            for _ in range(2):
                _ = evaluate_consumer_impact(schema_prod, g_spec)

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
        results["stages"]["consumer_impact_analysis"] = results["consumer_scaling"]

        # Stage 6: Sandbox Migration Verification (100, 500, 1000 rows, cold vs warm)
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

        # Measure cold first run on 100 rows before warmup
        t_mig_cold0 = time.perf_counter()
        _ = verify_migration_plan(m_plan, sample_rows=100)
        cold_mig_100_ms = round((time.perf_counter() - t_mig_cold0) * 1000.0, 3)

        # One additional warmup
        _ = verify_migration_plan(m_plan, sample_rows=100)

        for row_cnt in [100, 500, 1000]:
            sand_times: List[float] = []
            for _ in range(10):
                t0 = time.perf_counter()
                mig_res = verify_migration_plan(m_plan, sample_rows=row_cnt)
                sand_times.append((time.perf_counter() - t0) * 1000.0)

            stats_s = _calc_stats(sand_times)
            if row_cnt == 100:
                stats_s["cold_first_run_ms"] = cold_mig_100_ms
            print(
                f"  • {row_cnt:4d} Synthetic Rows (warm): {stats_s['mean']:.2f} ms (median {stats_s['median']:.2f} ms, "
                f"throughput: {(row_cnt / (stats_s['mean'] / 1000.0)):,.0f} rows/sec)"
            )
            results["sandbox_verification"][str(row_cnt)] = stats_s
        results["stages"]["sandbox_migration_verification"] = results["sandbox_verification"]

        # Stage 7: HTML Report Generation
        print("\n[Phase 5] Benchmarking generate_html_report()...")
        html_out = tmp_path / "bench_report.html"
        # Warmup
        generate_html_report(
            output_path=str(html_out),
            audit_result=audit_res,
            topology_report=topo_rep,
            impact_report=imp,
            migration_result=mig_res,
            dataset_title="Benchmark Dataset",
        )
        h_times: List[float] = []
        for _ in range(15):
            t0 = time.perf_counter()
            generate_html_report(
                output_path=str(html_out),
                audit_result=audit_res,
                topology_report=topo_rep,
                impact_report=imp,
                migration_result=mig_res,
                dataset_title="Benchmark Dataset",
            )
            h_times.append((time.perf_counter() - t0) * 1000.0)

        stats_h = _calc_stats(h_times)
        print(
            f"  • HTML Report Generation: {stats_h['mean']:.2f} ms (file size: {html_out.stat().st_size:,} bytes)"
        )
        results["html_report"] = stats_h
        results["stages"]["html_report_generation"] = stats_h

        # Stage 8: Complete End-to-End Workflow (100 files audit + topology + 20 consumers + 100-row migration + HTML report)
        print("\n[Phase 6] Benchmarking complete end-to-end workflow...")
        e2e_times: List[float] = []
        for _ in range(5):
            t0 = time.perf_counter()
            e2e_audit = audit_dataset(str(lake_dir))
            e2e_topo = analyze_dataset_topology(e2e_audit, consumers=g_spec.consumers)
            assert e2e_audit.canonical_schema is not None
            e2e_imp = evaluate_consumer_impact(e2e_audit.canonical_schema, g_spec)
            e2e_mig = verify_migration_plan(m_plan, consumers=g_spec.consumers[:1], sample_rows=100)
            generate_html_report(
                output_path=str(html_out),
                audit_result=e2e_audit,
                topology_report=e2e_topo,
                impact_report=e2e_imp,
                migration_result=e2e_mig,
                dataset_title="End-to-End Benchmark",
            )
            e2e_times.append((time.perf_counter() - t0) * 1000.0)
        stats_e2e = _calc_stats(e2e_times)
        results["stages"]["complete_end_to_end_workflow"] = stats_e2e
        print(
            f"  • Complete End-to-End Workflow: {stats_e2e['mean']:.2f} ms (median {stats_e2e['median']:.2f} ms)"
        )

    out_file = Path(__file__).parent / "consumer_intelligence_benchmarks.json"
    out_file.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"\n✓ Saved empirical benchmark metrics to: {out_file}\n")


if __name__ == "__main__":
    main()
