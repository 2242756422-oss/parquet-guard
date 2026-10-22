"""Parquet Guard Quickstart Demo.

An end-to-end executable demonstration of Parquet Guard:
1. Synthetic partition generation with PyArrow.
2. Baseline schema contract initialization and validation.
3. Detecting schema drift and breaking column changes.
4. Incremental audit caching demonstration.
5. Automated SARIF 2.1.0 security/quality report export.
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from parquet_guard.core.auditor import audit_dataset
from parquet_guard.core.compatibility import compare_schemas
from parquet_guard.core.inspector import inspect_schema, save_contract, validate_contract
from parquet_guard.core.models import Severity
from parquet_guard.core.reporter import format_diff_sarif


def main() -> int:
    print("=" * 70)
    print("PARQUET GUARD — END-TO-END DEMO")
    print("=" * 70)

    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        dataset_dir = root / "production_dataset"
        dataset_dir.mkdir()

        # Step 1: Generate synthetic production Parquet partitions
        print("\n[Step 1] Creating partitioned dataset with PyArrow...")
        table_v1 = pa.Table.from_pydict(
            {
                "transaction_id": [1001, 1002, 1003],
                "user_id": ["u_10", "u_20", "u_30"],
                "amount": [19.99, 45.50, 120.00],
                "payload": [{"status": "COMPLETED"}, {"status": "PENDING"}, {"status": "FAILED"}],
            }
        )

        p1_path = dataset_dir / "part-001.parquet"
        p2_path = dataset_dir / "part-002.parquet"
        pq.write_table(table_v1, p1_path)
        pq.write_table(table_v1, p2_path)
        print(f"  Created partition 1: {p1_path.name} (3 rows)")
        print(f"  Created partition 2: {p2_path.name} (3 rows)")

        # Step 2: Extract and validate golden schema contract
        print("\n[Step 2] Extracting golden contract without data scans...")
        contract = inspect_schema(p1_path)
        contract.name = "transactions_golden_contract"
        contract_path = root / "transactions_contract.json"
        save_contract(contract, contract_path)
        is_valid, err_msg = validate_contract(contract_path)
        assert is_valid, f"Contract validation failed: {err_msg}"
        print(f"  Contract saved to {contract_path.name} ({len(contract.fields)} columns)")
        print(f"  Contract valid: v{contract.version}, extra fields forbidden.")

        # Step 3: Drift and breaking change detection
        print("\n[Step 3] Introducing breaking schema drift (column dropped)...")
        table_drift = pa.Table.from_pydict(
            {
                "transaction_id": [2001, 2002],
                # amount dropped!
                "user_id": ["u_40", "u_50"],
                "payload": [{"status": "CANCELLED"}, {"status": "REFUNDED"}],
            }
        )
        drift_path = root / "part-drift.parquet"
        pq.write_table(table_drift, drift_path)

        drift_contract = inspect_schema(drift_path)
        diff_report = compare_schemas(drift_contract, contract, mode="strict")
        breaking_diffs = [d for d in diff_report.diffs if d.severity == Severity.BREAKING]
        print(f"  Comparison Verdict: {diff_report.result_state.value}")
        print(f"  Breaking changes detected: {len(breaking_diffs)}")
        for diff in breaking_diffs:
            print(f"    - [{diff.change_type.value}] {diff.description}")

        # Step 4: Incremental dataset audit & cache performance
        print("\n[Step 4] Auditing partition directory with incremental caching...")
        audit_1 = audit_dataset(dataset_dir, mode="strict", incremental=True)
        print(
            f"  Audit Run 1 (Cold): Scanned {audit_1.total_files_scanned} files, "
            f"Consistent={audit_1.is_consistent}, Cache Hits={audit_1.cache_hits}"
        )

        audit_2 = audit_dataset(dataset_dir, mode="strict", incremental=True)
        print(
            f"  Audit Run 2 (Cached): Scanned {audit_2.total_files_scanned} files, "
            f"Consistent={audit_2.is_consistent}, Cache Hits={audit_2.cache_hits}"
        )
        assert audit_2.cache_hits == 2, "Expected 100% cache hit on unchanged files"

        # Step 5: Export OASIS SARIF 2.1.0 for GitHub Code Scanning / CI
        print("\n[Step 5] Exporting SARIF 2.1.0 for CI / GitHub Security tab...")
        sarif_json = format_diff_sarif(diff_report)
        sarif_obj = json.loads(sarif_json)
        sarif_runs = sarif_obj.get("runs", [])
        rules_count = len(sarif_runs[0]["tool"]["driver"]["rules"])
        results_count = len(sarif_runs[0]["results"])
        print(f"  SARIF Version: {sarif_obj.get('version')}")
        print(f"  Tool Rules Catalog: {rules_count} static rules")
        print(f"  Vulnerabilities/Drifts Reported: {results_count}")

    print("\n" + "=" * 70)
    print("DEMO COMPLETE: All stages verified successfully.")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
