"""Partitioned Data Lake Schema Topology & Grouping Demonstration.

Demonstrates:
1. Multi-partition Parquet lake with 3 distinct schema clusters (dominant group + 2 drifted groups).
2. Pure metadata inspection and grouping into SchemaGroups without scanning raw rows.
3. Divergence concentration calculation highlighting breaking columns.
4. Standalone HTML dashboard report generation with interactive SVG topology visualization.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from parquet_guard.core.auditor import audit_dataset
from parquet_guard.core.consumer import (
    ConsumerContract,
    ConsumerFieldRequirement,
)
from parquet_guard.core.grouping import analyze_dataset_topology
from parquet_guard.core.html_reporter import generate_html_report


def run_demo() -> None:
    print("======================================================================")
    print("   🌊 PARQUET GUARD: PARTITIONED DATA LAKE TOPOLOGY DEMO")
    print("======================================================================\n")

    with tempfile.TemporaryDirectory() as tmp_dir:
        lake_root = Path(tmp_dir) / "lake"
        lake_root.mkdir()
        report_html = Path(tmp_dir) / "lake_topology_report.html"

        # 1. Create 10 partition files split into 3 schema groups
        # Group 1 (Canonical, 6 files): 4 columns, event_id: int64
        # Group 2 (Drifted A, 3 files): 5 columns, added device_os: string
        # Group 3 (Rogue B, 1 file):    4 columns, event_id: int32 (Narrowed, breaking!)

        print("✓ Generating 10 lake partition files across 3 schema clusters...")

        # Cluster 1: Canonical
        for i in range(1, 7):
            p_dir = lake_root / f"date=2026-10-0{i}"
            p_dir.mkdir()
            tbl = pa.Table.from_arrays(
                [
                    pa.array([1000 + i, 2000 + i], type=pa.int64()),
                    pa.array([f"user_{i}", f"user_{i + 1}"], type=pa.string()),
                    pa.array(["CLICK", "VIEW"], type=pa.string()),
                    pa.array([1729000000000, 1729003600000], type=pa.timestamp("ms", tz="UTC")),
                ],
                names=["event_id", "user_id", "event_type", "event_time"],
            )
            pq.write_table(tbl, p_dir / "part-001.parquet")

        # Cluster 2: Added device_os column
        for i in range(7, 10):
            p_dir = lake_root / f"date=2026-10-0{i}"
            p_dir.mkdir()
            tbl = pa.Table.from_arrays(
                [
                    pa.array([3000 + i, 4000 + i], type=pa.int64()),
                    pa.array([f"user_{i}", f"user_{i + 1}"], type=pa.string()),
                    pa.array(["PURCHASE", "SCROLL"], type=pa.string()),
                    pa.array([1729000000000, 1729003600000], type=pa.timestamp("ms", tz="UTC")),
                    pa.array(["iOS", "Android"], type=pa.string()),
                ],
                names=["event_id", "user_id", "event_type", "event_time", "device_os"],
            )
            pq.write_table(tbl, p_dir / "part-001.parquet")

        # Cluster 3: Rogue narrowed event_id int32
        p_dir = lake_root / "date=2026-10-10"
        p_dir.mkdir()
        tbl_rogue = pa.Table.from_arrays(
            [
                pa.array([5001, 5002], type=pa.int32()),  # Narrowed!
                pa.array(["user_99", "user_100"], type=pa.string()),
                pa.array(["CLICK", "ERROR"], type=pa.string()),
                pa.array([1729000000000, 1729003600000], type=pa.timestamp("ms", tz="UTC")),
            ],
            names=["event_id", "user_id", "event_type", "event_time"],
        )
        pq.write_table(tbl_rogue, p_dir / "part-001.parquet")

        # 2. Define downstream consumers
        c_trino = ConsumerContract(
            consumer_id="trino-lake-queries",
            consumer_name="Trino Federated SQL Engine",
            description="Production lake reporting queries requiring int64 event_id",
            compatibility_policy="strict",
            allow_extra_fields=True,
            fields=[
                ConsumerFieldRequirement(name="event_id", permitted_types=["int64"]),
                ConsumerFieldRequirement(name="user_id", permitted_types=["string"]),
                ConsumerFieldRequirement(name="event_time", permitted_types=["timestamp[ms, tz=UTC]"]),
            ],
        )

        # 3. Audit Dataset Consistency
        audit_result = audit_dataset(str(lake_root))
        print("✓ Audited lake partitions:")
        print(f"  • Files Scanned: {audit_result.total_files_scanned}")
        print(f"  • Total Rows:    {audit_result.total_rows}")
        print(f"  • Has Drift:     {audit_result.has_drift}\n")

        # 4. Analyze Dataset Topology & Schema Grouping
        topology = analyze_dataset_topology(audit_result, consumers=[c_trino])

        print("----------------------------------------------------------------------")
        print("📊 LAKE TOPOLOGY & DIVERGENCE CONCENTRATION REPORT")
        print("----------------------------------------------------------------------")
        print(f"Total Partitions:      {topology.total_files}")
        print(f"Distinct Schemas:      {topology.distinct_schemas_count} clusters detected")
        print(f"Canonical Group ID:    {topology.canonical_group_id}\n")

        print("Schema Clusters Breakdown:")
        for grp in topology.groups:
            is_canon = grp.group_id == topology.canonical_group_id
            badge = "⭐ CANONICAL" if is_canon else "⚡ DIVERGENT"
            print(
                f"  [{badge}] Group '{grp.group_id}': {grp.file_count} files ({grp.percentage:.1f}%) "
                f"- Fingerprint: {grp.schema_fingerprint[:12]}..."
            )
            print(f"     Columns: {[f.name for f in grp.schema_contract.fields]}")
            trino_status = grp.consumer_status.get("trino-lake-queries")
            if trino_status:
                print(f"     Trino Compatibility: {trino_status.value}")
            print()

        print("Divergence Concentration (Hotspots):")
        for div in topology.divergence_concentration:
            breaking_tag = " [BREAKING]" if div.is_breaking_divergence else " [SAFE]"
            print(f"  • Column '{div.field_path}'{breaking_tag}: {div.affected_files_count} files affected")
            for var in div.variants:
                print(f"     - Type: {var.get('type')}, Files: {var.get('file_count')}")

        # 5. Generate Standalone HTML Diagnostic Report
        generate_html_report(
            output_path=str(report_html),
            audit_result=audit_result,
            topology_report=topology,
            dataset_title="Data Lake Partition Topology",
        )
        print("\n✓ Generated standalone HTML report at:", report_html)
        print(f"  Report Size: {report_html.stat().st_size:,} bytes")
        print("\n======================================================================")
        print("   ✅ PARTITIONED LAKE TOPOLOGY DEMO COMPLETED!")
        print("======================================================================\n")


if __name__ == "__main__":
    run_demo()
