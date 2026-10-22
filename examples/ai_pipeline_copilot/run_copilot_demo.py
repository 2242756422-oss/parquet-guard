"""End-to-end runnable demonstration of Parquet Guard AI Copilot in an ML pipeline."""

from __future__ import annotations

import tempfile
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from parquet_guard import (
    compare_schemas,
    diagnose_schema_drift,
    generate_migration_plan,
    inspect_schema,
    load_contract,
    save_contract,
)
from parquet_guard.ai.explainer import format_diagnosis_terminal
from parquet_guard.ai.providers import MockAIProvider


def main() -> None:
    print("=" * 75)
    print("  🛡️ PARQUET GUARD AI — REAL-WORLD PIPELINE EVOLUTION COPILOT DEMO")
    print("=" * 75)

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # 1. Establish production baseline dataset (v1)
        v1_path = tmp_path / "events_v1.parquet"
        contract_path = tmp_path / "events_contract.json"

        # 512-dimensional vector embedding + timestamp[s] + event_id
        vec_type = pa.list_(pa.float32(), 512)
        v1_schema = pa.schema(
            [
                pa.field("event_id", pa.int64(), nullable=False),
                pa.field("timestamp", pa.timestamp("s")),
                pa.field("embedding", vec_type),
            ]
        )
        dummy_vec = [0.1] * 512
        v1_table = pa.Table.from_arrays(
            [
                pa.array([101, 102], type=pa.int64()),
                pa.array(
                    [datetime(2026, 10, 22, 10, 0, 0), datetime(2026, 10, 22, 11, 0, 0)],
                    type=pa.timestamp("s"),
                ),
                pa.array([dummy_vec, dummy_vec], type=vec_type),
            ],
            schema=v1_schema,
        )
        pq.write_table(v1_table, v1_path)
        print(f"\n[Step 1] Baseline Parquet file written: {v1_path.name}")

        # 2. Export golden contract
        contract = inspect_schema(str(v1_path))
        contract.name = "EventsProductionContract"
        save_contract(contract, str(contract_path))
        print(f"[Step 2] Golden contract saved: {contract_path.name} ({len(contract.fields)} fields)")

        # 3. Simulate upstream pipeline changes (v2 with breaking embedding size & timestamp widening)
        v2_path = tmp_path / "events_v2.parquet"
        vec768_type = pa.list_(pa.float32(), 768)
        dummy_vec768 = [0.2] * 768
        v2_schema = pa.schema(
            [
                pa.field("event_id", pa.int64(), nullable=False),
                pa.field("timestamp", pa.timestamp("ns")),  # Temporal precision widened
                pa.field("embedding", vec768_type),  # Vector dimension changed (512 -> 768)
                pa.field("user_id", pa.string()),  # Additive field
            ]
        )
        v2_table = pa.Table.from_arrays(
            [
                pa.array([201, 202], type=pa.int64()),
                pa.array(
                    [datetime(2026, 10, 22, 12, 0, 0), datetime(2026, 10, 22, 13, 0, 0)],
                    type=pa.timestamp("ns"),
                ),
                pa.array([dummy_vec768, dummy_vec768], type=vec768_type),
                pa.array(["usr_alice", "usr_bob"], type=pa.string()),
            ],
            schema=v2_schema,
        )
        pq.write_table(v2_table, v2_path)
        print(f"[Step 3] New upstream batch generated: {v2_path.name} (vector dimension: 768, ts: ns)")

        # 4. Deterministic schema gate
        target_schema = inspect_schema(str(v2_path))
        baseline_schema = load_contract(str(contract_path))
        diff_report = compare_schemas(target_schema, baseline_schema, mode="permissive")

        print("\n[Step 4] Deterministic Validation Outcome:")
        print(f"  - Result State: {diff_report.result_state.value}")
        print(f"  - Breaking Regressions: {diff_report.has_breaking_changes}")
        print(f"  - Total Discrepancies: {len(diff_report.diffs)}")
        for d in diff_report.diffs:
            print(f"    • [{d.severity.value}] {d.field_name}: {d.description}")

        # 5. AI Evidence-Grounded Diagnosis
        print("\n[Step 5] Triggering Parquet Guard AI Diagnosis...")
        provider = MockAIProvider(model_id="mock-copilot")
        diagnosis = diagnose_schema_drift(diff_report, provider=provider)

        print("\n--- AI Diagnosis Output (Terminal Rendering) ---")
        print(format_diagnosis_terminal(diagnosis))

        # 6. AI Migration Planning & Deterministic Verification
        print("[Step 6] Generating and Validating Migration Plan...")
        plan = generate_migration_plan(baseline_schema, target_schema, diff_report=diff_report)
        print(f"  - Plan Title: {plan.title}")
        print(
            f"  - Proposed Contract Valid: {plan.contract_proposal.is_valid_contract if plan.contract_proposal else False}"
        )
        print(
            f"  - Satisfies Target Schema: {plan.contract_proposal.satisfies_target_schema if plan.contract_proposal else False}"
        )

        print("\n--- Auto-Generated PyArrow Migration Script ---")
        print(plan.pyarrow_migration_code)

        print("=" * 75)
        print("  ✅ PIPELINE COPILOT DEMO COMPLETED SUCCESSFULLY!")
        print("=" * 75)


if __name__ == "__main__":
    main()
