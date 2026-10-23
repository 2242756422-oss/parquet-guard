"""E-Commerce Pipeline Demonstration for Parquet Guard.

Demonstrates:
1. Producer Parquet schema evolution (orders v1 to orders v2).
2. Multi-consumer impact analysis across 3 distinct engine contracts.
3. Isolated synthetic sandbox migration verification.
4. Standalone HTML diagnostic report generation.
"""

from __future__ import annotations

import decimal
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
from parquet_guard.core.migration_verifier import (
    ExecutableMigrationPlan,
    MigrationActionType,
    MigrationOperation,
    verify_migration_plan,
)
from parquet_guard.core.models import FieldDefinition, SchemaContract


def run_demo() -> None:
    print("======================================================================")
    print("   🛒 PARQUET GUARD: E-COMMERCE ORDERS SCHEMA EVOLUTION DEMO")
    print("======================================================================\n")

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        p1 = tmp_path / "orders_v1.parquet"
        p2 = tmp_path / "orders_v2.parquet"
        report_html = tmp_path / "ecommerce_report.html"

        # 1. Generate v1 Parquet file
        tbl_v1 = pa.Table.from_arrays(
            [
                pa.array([1001, 1002, 1003], type=pa.int64()),
                pa.array(["cust_A", "cust_B", "cust_C"], type=pa.string()),
                pa.array(
                    [decimal.Decimal("49.99"), decimal.Decimal("120.50"), decimal.Decimal("15.00")],
                    type=pa.decimal128(10, 2),
                ),
                pa.array(["COMPLETED", "PENDING", "COMPLETED"], type=pa.string()),
                pa.array([1729000000, 1729003600, 1729007200], type=pa.timestamp("s", tz="UTC")),
            ],
            names=["order_id", "customer_id", "amount", "status", "created_at"],
        )
        pq.write_table(tbl_v1, p1)

        # 2. Generate v2 Parquet file with drift:
        # - amount scale changed: decimal128(10, 2) -> decimal128(14, 4)
        # - created_at temporal precision widened: s -> ms
        # - new column: loyalty_discount
        tbl_v2 = pa.Table.from_arrays(
            [
                pa.array([2001, 2002, 2003], type=pa.int64()),
                pa.array(["cust_D", "cust_E", "cust_F"], type=pa.string()),
                pa.array(
                    [decimal.Decimal("49.9900"), decimal.Decimal("120.5000"), decimal.Decimal("15.0000")],
                    type=pa.decimal128(14, 4),
                ),
                pa.array(["COMPLETED", "REFUNDED", "COMPLETED"], type=pa.string()),
                pa.array([1729000000000, 1729003600000, 1729007200000], type=pa.timestamp("ms", tz="UTC")),
                pa.array([5.0, 0.0, 2.5], type=pa.float64()),
            ],
            names=["order_id", "customer_id", "amount", "status", "created_at", "loyalty_discount"],
        )
        pq.write_table(tbl_v2, p2)

        print("✓ Created synthetic production datasets:")
        print(f"  • Baseline: {p1.name} (5 columns, decimal128(10,2))")
        print(f"  • Evolved:  {p2.name} (6 columns, decimal128(14,4) + timestamp[ms] + loyalty_discount)\n")

        # 3. Define 3 downstream consumer contracts
        c_duckdb = ConsumerContract(
            consumer_id="duckdb-financial-reporting",
            consumer_name="DuckDB Financial Accounting ETL",
            description="Strict financial accounting queries requiring exact 2-decimal precision",
            compatibility_policy="strict",
            allow_extra_fields=True,
            metadata={"owner_team": "Finance Data Team", "query_engine": "DuckDB"},
            fields=[
                ConsumerFieldRequirement(name="order_id", permitted_types=["int64"]),
                ConsumerFieldRequirement(name="amount", permitted_types=["decimal128(10, 2)"]),
                ConsumerFieldRequirement(name="status", permitted_types=["string"]),
            ],
        )

        c_polars = ConsumerContract(
            consumer_id="polars-user-segmentation",
            consumer_name="Polars Customer Cohort Analysis",
            description="Permissive marketing user segmentation pipeline",
            compatibility_policy="permissive",
            allow_extra_fields=True,
            metadata={"owner_team": "Marketing Science", "query_engine": "Polars"},
            fields=[
                ConsumerFieldRequirement(name="customer_id", permitted_types=["string"]),
                ConsumerFieldRequirement(name="status", permitted_types=["string"]),
            ],
        )

        c_legacy = ConsumerContract(
            consumer_id="pyarrow-warehouse-export",
            consumer_name="PyArrow Legacy Data Warehouse Dump",
            description="Strict legacy archival job that forbids undeclared columns",
            compatibility_policy="strict",
            allow_extra_fields=False,  # Rejects new unannounced columns!
            metadata={"owner_team": "Core Infrastructure", "query_engine": "PyArrow"},
            fields=[
                ConsumerFieldRequirement(name="order_id", permitted_types=["int64"]),
                ConsumerFieldRequirement(name="customer_id", permitted_types=["string"]),
                ConsumerFieldRequirement(name="amount", permitted_types=["decimal128(10, 2)"]),
                ConsumerFieldRequirement(name="status", permitted_types=["string"]),
                ConsumerFieldRequirement(name="created_at", permitted_types=["timestamp[s, tz=UTC]"]),
            ],
        )

        group_spec = ConsumerGroupSpecification(
            description="Production Checkout Orders Stream",
            consumers=[c_duckdb, c_polars, c_legacy],
        )

        # 4. Evaluate Consumer Impact
        producer_schema = inspect_parquet_schema(p2)
        impact_report = evaluate_consumer_impact(producer_schema, group_spec)

        print("----------------------------------------------------------------------")
        print("📊 CONSUMER IMPACT EVALUATION REPORT")
        print("----------------------------------------------------------------------")
        print(f"Dataset:              {impact_report.dataset_identifier}")
        print(f"Total Consumers:      {impact_report.total_consumers}")
        print(f"Incompatible:         {impact_report.incompatible_consumers} (Blocking!)")
        print(f"Conditionally Comp:   {impact_report.conditionally_compatible_consumers}")
        print(f"Fully Compatible:     {impact_report.compatible_consumers}\n")

        for ev in impact_report.evaluations:
            badge = "❌ INCOMPATIBLE" if ev.is_blocking else "✅ COMPATIBLE"
            print(f"[{badge}] Consumer: '{ev.consumer_id}' ({ev.consumer_name})")
            print(f"   Status: {ev.status.value}")
            if ev.is_blocking:
                for tm in ev.type_mismatches:
                    print(f"   • Type Mismatch: {tm}")
                for ef in ev.unexpected_extra_fields:
                    print(f"   • Unexpected Extra Field: {ef}")
                for mf in ev.missing_required_fields:
                    print(f"   • Missing Required Field: {mf}")
            else:
                print("   • Perfectly compatible with projected schema.")
            print()

        # 5. Synthetic Migration Verification Sandbox
        print("----------------------------------------------------------------------")
        print("🧪 RUNNING SYNTHETIC MIGRATION VERIFICATION SANDBOX")
        print("----------------------------------------------------------------------")

        # Plan to cast amount back to decimal128(10, 2) and created_at to timestamp[s, tz=UTC]
        plan = ExecutableMigrationPlan(
            plan_id="plan-order-compat-v1",
            title="Re-align decimal scale and temporal units",
            description="Re-align decimal scale and temporal units for legacy readers",
            source_contract=producer_schema,
            target_contract=SchemaContract(
                fields=[
                    FieldDefinition(name="order_id", type="int64"),
                    FieldDefinition(name="customer_id", type="string"),
                    FieldDefinition(name="amount", type="decimal128(10, 2)"),
                    FieldDefinition(name="status", type="string"),
                    FieldDefinition(name="created_at", type="timestamp[s, tz=UTC]"),
                ]
            ),
            operations=[
                MigrationOperation(
                    action=MigrationActionType.SAFE_CAST,
                    source_column="amount",
                    target_type="decimal128(10, 2)",
                    description="Downcast decimal scale from 4 to 2 (verifies no precision loss)",
                ),
                MigrationOperation(
                    action=MigrationActionType.SAFE_CAST,
                    source_column="created_at",
                    target_type="timestamp[s, tz=UTC]",
                    description="Coarsen timestamp precision from ms to s",
                ),
                MigrationOperation(
                    action=MigrationActionType.DROP_COLUMN,
                    source_column="loyalty_discount",
                    description="Drop unannounced extra column for strict legacy consumers",
                ),
            ],
            affected_consumer_ids=["duckdb-financial-reporting", "pyarrow-warehouse-export"],
            deployment_order=["Stage shadow reader", "Apply transformation view", "Switch primary writer"],
            rollback_procedure=["Restore un-migrated v2 partitions from object storage backup"],
            pyarrow_code="""import pyarrow as pa
import pyarrow.compute as pc

col_amount = pc.cast(table["amount"], pa.decimal128(10, 2), safe=True)
col_ts = pc.cast(table["created_at"], pa.timestamp("s", tz="UTC"), safe=True)
table = table.set_column(table.schema.get_field_index("amount"), "amount", col_amount)
table = table.set_column(table.schema.get_field_index("created_at"), "created_at", col_ts)
table = table.drop(["loyalty_discount"])
""",
        )

        sandbox_result = verify_migration_plan(
            plan=plan,
            consumers=group_spec.consumers,
            sample_rows=100,
        )

        print(f"Sandbox Verification Succeeded: {sandbox_result.success}")
        print(f"Synthetic Rows Evaluated:       {sandbox_result.rows_tested}")
        print(f"Row Count Preserved:            {sandbox_result.row_count_preserved}")
        print(f"Target Schema Matched:          {sandbox_result.schema_matched}")
        print(f"Value Invariants Passed:        {sandbox_result.value_invariants_passed}")
        print(f"Post-Migration Incompatible:    {len(sandbox_result.incompatible_consumers)}")
        if sandbox_result.errors:
            print(f"Expected Safety Rejection:      {sandbox_result.errors[0]}")
            print("   ↳ 🛡️ Parquet Guard successfully prevented precision loss before production deployment!")
        print()

        # 6. Generate HTML Diagnostic Report
        generate_html_report(
            output_path=str(report_html),
            impact_report=impact_report,
            migration_result=sandbox_result,
            dataset_title="E-Commerce Orders Dataset",
        )
        print("✓ Generated standalone HTML report at:", report_html)
        print(f"  Report Size: {report_html.stat().st_size:,} bytes")
        print("\n======================================================================")
        print("   ✅ E-COMMERCE PIPELINE DEMO COMPLETED SUCCESSFULLY!")
        print("======================================================================\n")


if __name__ == "__main__":
    run_demo()
