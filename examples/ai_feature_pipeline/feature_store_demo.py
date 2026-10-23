"""AI Feature Store Vector Dimension Drift Demonstration.

Demonstrates:
1. Generation of Parquet datasets with fixed-size list vector embeddings.
2. Detection of dimension mismatches (512 vs 768 dimensions) using Arrow container parameter checks.
3. Downstream consumer contract protection for vector database ANN indexers.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from parquet_guard.core.consumer import (
    ConsumerContract,
    ConsumerFieldRequirement,
    ConsumerGroupSpecification,
    evaluate_consumer_impact,
)
from parquet_guard.core.html_reporter import generate_html_report
from parquet_guard.core.inspector import inspect_parquet_schema


def run_demo() -> None:
    print("======================================================================")
    print("   🤖 PARQUET GUARD: AI FEATURE STORE VECTOR DRIFT DEMO")
    print("======================================================================\n")

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        p_v1 = tmp_path / "embeddings_512.parquet"
        p_v2 = tmp_path / "embeddings_768.parquet"
        report_html = tmp_path / "vector_drift_report.html"

        # 1. Generate v1 Parquet file: 512-dimensional vector embeddings
        dim_v1 = 512
        num_rows = 5
        vec_v1 = [[float(i + j) * 0.01 for j in range(dim_v1)] for i in range(num_rows)]
        tbl_v1 = pa.Table.from_arrays(
            [
                pa.array([f"doc_{i}" for i in range(num_rows)], type=pa.string()),
                pa.array(vec_v1, type=pa.list_(pa.float32(), list_size=dim_v1)),
                pa.array(["v1-minilm-512"] * num_rows, type=pa.string()),
            ],
            names=["doc_id", "embedding", "model_version"],
        )
        pq.write_table(tbl_v1, p_v1)

        # 2. Generate v2 Parquet file: 768-dimensional vector embeddings (upgraded backbone)
        dim_v2 = 768
        vec_v2 = [[float(i + j) * 0.01 for j in range(dim_v2)] for i in range(num_rows)]
        tbl_v2 = pa.Table.from_arrays(
            [
                pa.array([f"doc_{i}" for i in range(num_rows)], type=pa.string()),
                pa.array(vec_v2, type=pa.list_(pa.float32(), list_size=dim_v2)),
                pa.array(["v2-bge-base-768"] * num_rows, type=pa.string()),
            ],
            names=["doc_id", "embedding", "model_version"],
        )
        pq.write_table(tbl_v2, p_v2)

        print("✓ Created synthetic feature store partitions:")
        print(f"  • Baseline: {p_v1.name} (fixed_size_list<float32>[512])")
        print(f"  • Upstream Upgrade: {p_v2.name} (fixed_size_list<float32>[768])\n")

        # 3. Define downstream vector DB consumer contract
        c_qdrant = ConsumerContract(
            consumer_id="qdrant-vector-indexer",
            consumer_name="Qdrant ANN Search Indexer",
            description="Vector index allocated for exactly 512-dim Cosine similarity",
            compatibility_policy="strict",
            allow_extra_fields=True,
            metadata={"engine": "Qdrant", "index_dimension": "512"},
            fields=[
                ConsumerFieldRequirement(name="doc_id", permitted_types=["string"]),
                ConsumerFieldRequirement(
                    name="embedding",
                    permitted_types=["fixed_size_list<item: float32>[512]"],
                ),
            ],
        )

        c_recs = ConsumerContract(
            consumer_id="recs-metadata-service",
            consumer_name="Recommendations Metadata Enricher",
            description="Only consumes document identifiers and model tags",
            compatibility_policy="permissive",
            allow_extra_fields=True,
            metadata={"engine": "FastAPI / Redis"},
            fields=[
                ConsumerFieldRequirement(name="doc_id", permitted_types=["string"]),
                ConsumerFieldRequirement(name="model_version", permitted_types=["string"]),
            ],
        )

        group_spec = ConsumerGroupSpecification(
            description="Document Search Embeddings Stream",
            consumers=[c_qdrant, c_recs],
        )

        # 4. Evaluate Consumer Impact on the upgraded 768-dim partition
        schema_v2 = inspect_parquet_schema(p_v2)
        impact_report = evaluate_consumer_impact(schema_v2, group_spec)

        print("----------------------------------------------------------------------")
        print("📊 VECTOR CONSUMER IMPACT REPORT")
        print("----------------------------------------------------------------------")
        print(f"Dataset Status:       {impact_report.overall_status.value}")
        print(f"Total Consumers:      {impact_report.total_consumers}")
        print(f"Incompatible (Crash): {impact_report.incompatible_consumers}")
        print(f"Compatible:           {impact_report.compatible_consumers}\n")

        for ev in impact_report.evaluations:
            badge = "❌ INCOMPATIBLE" if ev.is_blocking else "✅ COMPATIBLE"
            print(f"[{badge}] Consumer: '{ev.consumer_id}' ({ev.consumer_name})")
            if ev.is_blocking:
                for tm in ev.type_mismatches:
                    print(f"   • Vector Mismatch: {tm}")
                for dm in ev.dimension_mismatches:
                    print(f"   • Dimension Violation: {dm}")
                print(f"   ↳ Action: {ev.summary}")
            else:
                print("   • Unaffected: Does not depend on the vector embedding column.")
            print()

        # 5. Generate Standalone HTML Diagnostic Report
        generate_html_report(
            output_path=str(report_html),
            impact_report=impact_report,
            dataset_title="AI Feature Store Embeddings",
        )
        print("✓ Generated standalone HTML report at:", report_html)
        print(f"  Report Size: {report_html.stat().st_size:,} bytes")
        print("\n======================================================================")
        print("   ✅ AI FEATURE STORE VECTOR DRIFT DEMO COMPLETED!")
        print("======================================================================\n")


if __name__ == "__main__":
    run_demo()
