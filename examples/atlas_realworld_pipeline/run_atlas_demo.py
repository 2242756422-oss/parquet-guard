"""Executable end-to-end demonstration of Project Atlas capabilities.

Executes:
1. Multi-partition Parquet schema drift auditing.
2. Downstream consumer contract impact evaluation across 3 real engines.
3. Engine verification probes (PyArrow, DuckDB, Polars) detecting runtime behavior.
4. Safe sandbox migration verification.
5. Standalone HTML diagnostic report generation.
"""

from __future__ import annotations

import json
from pathlib import Path

from parquet_guard.core.auditor import audit_dataset
from parquet_guard.core.consumer import (
    ConsumerContract,
    evaluate_consumer_impact,
)
from parquet_guard.core.grouping import analyze_dataset_topology
from parquet_guard.core.html_reporter import generate_html_diagnostic_report
from parquet_guard.core.inspector import inspect_schema, load_contract
from parquet_guard.core.migration_verifier import (
    ExecutableMigrationPlan,
    MigrationOperation,
    verify_migration_plan,
)
from parquet_guard.verification import (
    EngineProbeOperation,
    format_engine_verification_terminal,
    verify_engine_compatibility,
)


def run_demo() -> None:
    demo_dir = Path(__file__).parent
    dataset_dir = demo_dir / "dataset"
    contracts_dir = demo_dir / "contracts"

    # Step 0: Ensure dataset exists
    import sys

    if str(demo_dir) not in sys.path:
        sys.path.insert(0, str(demo_dir))
    from generate_fixtures import generate_pipeline_dataset

    generate_pipeline_dataset(dataset_dir)

    print("\n" + "=" * 80)
    print(" PROJECT ATLAS — REAL-WORLD SCHEMA RELIABILITY & INTEROPERABILITY DEMO")
    print("=" * 80)

    # -------------------------------------------------------------------------
    # 1. Dataset Partition Audit
    # -------------------------------------------------------------------------
    print("\n[STEP 1] Auditing Partitioned Dataset Schema Consistency (Zero-Data-Scan)...")
    golden_contract = load_contract(contracts_dir / "producer_golden_contract.json")
    audit_res = audit_dataset(
        dataset_dir,
        mode="permissive",
        baseline_contract=golden_contract,
        incremental=True,
        require_content_hash=True,
    )

    print(f" -> Scanned {audit_res.total_files_scanned} partition files.")
    print(f" -> Distinct Schemas Found : {audit_res.distinct_schemas_count}")
    print(f" -> Consistent with Baseline: {audit_res.is_consistent}")
    print(f" -> Compatible (Permissive) : {audit_res.is_compatible}")

    # -------------------------------------------------------------------------
    # 2. Consumer Impact Evaluation
    # -------------------------------------------------------------------------
    print("\n[STEP 2] Evaluating Downstream Consumer Contracts...")
    consumers = [
        ConsumerContract.model_validate(
            json.loads((contracts_dir / "consumer_duckdb_analytics.json").read_text(encoding="utf-8"))
        ),
        ConsumerContract.model_validate(
            json.loads((contracts_dir / "consumer_polars_ml.json").read_text(encoding="utf-8"))
        ),
        ConsumerContract.model_validate(
            json.loads((contracts_dir / "consumer_pyarrow_etl.json").read_text(encoding="utf-8"))
        ),
    ]

    p2_file = list(dataset_dir.glob("**/*202402.parquet"))[0]
    p2_schema = inspect_schema(p2_file)

    impact_report = evaluate_consumer_impact(
        p2_schema, consumers, dataset_identifier="orders-partition-2024-02"
    )

    for eval_item in impact_report.evaluations:
        is_ok = eval_item.status.value == "COMPATIBLE"
        status_icon = "✓ COMPATIBLE" if is_ok else f"✗ {eval_item.status.value}"
        print(f" -> Consumer '{eval_item.consumer_id}' [{eval_item.criticality}]: {status_icon}")
        if not is_ok:
            print(f"    * Summary: {eval_item.summary}")
            if eval_item.type_mismatches:
                print(f"    * Type mismatches: {', '.join(eval_item.type_mismatches)}")

    # -------------------------------------------------------------------------
    # 3. Multi-Engine Runtime Verification
    # -------------------------------------------------------------------------
    print("\n[STEP 3] Probing Real Runtime Query Engines (PyArrow, DuckDB, Polars)...")
    verif_report = verify_engine_compatibility(
        dataset_dir,
        contract=golden_contract,
        engines=["pyarrow", "duckdb", "polars"],
        operations=[EngineProbeOperation.READ, EngineProbeOperation.SCHEMA_UNION],
        row_limit=5,
    )

    print(format_engine_verification_terminal(verif_report))

    # -------------------------------------------------------------------------
    # 4. Safe Migration Verification
    # -------------------------------------------------------------------------
    print("\n[STEP 4] Verifying Schema Migration Plan in Isolated Mode 1 Sandbox...")
    from parquet_guard.core.migration_verifier import MigrationActionType
    from parquet_guard.core.models import FieldDefinition, SchemaContract

    target_contract = SchemaContract(
        name="ecommerce-orders-v2",
        fields=[
            FieldDefinition(name="order_id", type="int64", nullable=False),
            FieldDefinition(name="subtotal", type="decimal128(14, 4)", nullable=False, precision=14, scale=4),
            FieldDefinition(name="tax", type="decimal128(14, 4)", nullable=False, precision=14, scale=4),
            FieldDefinition(
                name="created_at",
                type="timestamp[ms, tz=UTC]",
                nullable=False,
                time_unit="ms",
                timezone="UTC",
            ),
            golden_contract.get_field("customer"),
        ],
    )

    migration_plan = ExecutableMigrationPlan(
        plan_id="plan-widen-subtotal-scale",
        title="Widen Subtotal Scale",
        description="Migrate subtotal from decimal(10,2) to decimal(14,4)",
        source_contract=golden_contract,
        target_contract=target_contract,
        operations=[
            MigrationOperation(
                action=MigrationActionType.SAFE_CAST,
                source_column="subtotal",
                target_type="decimal128(14, 4)",
                description="Widen decimal precision to prevent monetary truncation",
            ),
            MigrationOperation(
                action=MigrationActionType.SAFE_CAST,
                source_column="tax",
                target_type="decimal128(14, 4)",
                description="Widen tax decimal precision",
            ),
        ],
    )

    mig_res = verify_migration_plan(migration_plan, consumers=consumers, sample_rows=20)
    print(f" -> Migration Readiness Verdict : {mig_res.readiness_status.value}")
    print(f" -> Transformation Succeeded    : {mig_res.transformation_succeeded}")
    print(f" -> Schema Matched Desired Target: {mig_res.schema_matched}")
    print(f" -> Safe for Production Deploy  : {mig_res.deployment_ready}")

    # -------------------------------------------------------------------------
    # 5. HTML Report Generation
    # -------------------------------------------------------------------------
    print("\n[STEP 5] Generating Interactive Diagnostic HTML Report...")
    topology = analyze_dataset_topology(audit_res, consumers=consumers)
    report_html = demo_dir / "atlas_diagnostic_report.html"
    generate_html_diagnostic_report(
        output_path=str(report_html),
        audit_result=audit_res,
        impact_report=impact_report,
        topology_report=topology,
        dataset_title="E-Commerce Real-World Partitioned Pipeline",
    )
    print(f" -> Standalone HTML report successfully generated at:\n    {report_html}")
    print("\n" + "=" * 80)
    print(" PROJECT ATLAS PIPELINE DEMO COMPLETED SUCCESSFULLY!")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    run_demo()
