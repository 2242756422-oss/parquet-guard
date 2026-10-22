"""Integration test validating the guarded data pipeline example."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from examples.data_pipeline_guard.pipeline import GuardedIngestionPipeline
from parquet_guard import extract_contract, save_contract


def test_guarded_pipeline_lifecycle(tmp_path: Path) -> None:
    # 1. Setup contract
    contract_file = tmp_path / "events.schema.json"
    golden_schema = pa.schema(
        [
            ("user_id", pa.int64(), False),
            ("event_type", pa.string(), False),
            ("timestamp", pa.timestamp("us", tz="UTC"), False),
        ]
    )
    sample_golden_file = tmp_path / "sample_golden.parquet"
    pq.write_table(
        pa.Table.from_arrays(
            [
                pa.array([101], type=pa.int64()),
                pa.array(["login"]),
                pa.array([1700000000000000], type=pa.timestamp("us", tz="UTC")),
            ],
            schema=golden_schema,
        ),
        str(sample_golden_file),
    )
    contract = extract_contract(sample_golden_file, name="user_events")
    save_contract(contract, contract_file)

    storage_dir = tmp_path / "storage"
    quarantine_dir = tmp_path / "quarantine"
    pipeline = GuardedIngestionPipeline(contract_file, storage_dir, quarantine_dir)

    # 2. Batch 1: Valid batch -> Accepted
    batch1_file = tmp_path / "batch1.parquet"
    pq.write_table(
        pa.Table.from_arrays(
            [
                pa.array([102, 103], type=pa.int64()),
                pa.array(["click", "view"]),
                pa.array([1700000001000000, 1700000002000000], type=pa.timestamp("us", tz="UTC")),
            ],
            schema=golden_schema,
        ),
        str(batch1_file),
    )
    ok1, msg1 = pipeline.process_batch(batch1_file, "2026-10-01")
    assert ok1 is True
    assert "ACCEPTED" in msg1
    assert (storage_dir / "partition=2026-10-01" / "data.parquet").is_file()

    # 3. Batch 2: Breaking batch (user_id narrowed to int32) -> Rejected & Quarantined
    breaking_schema = pa.schema(
        [
            ("user_id", pa.int32(), False),  # Breaking type narrowing
            ("event_type", pa.string(), False),
            ("timestamp", pa.timestamp("us", tz="UTC"), False),
        ]
    )
    batch2_file = tmp_path / "batch2_broken.parquet"
    pq.write_table(
        pa.Table.from_arrays(
            [
                pa.array([104], type=pa.int32()),
                pa.array(["purchase"]),
                pa.array([1700000003000000], type=pa.timestamp("us", tz="UTC")),
            ],
            schema=breaking_schema,
        ),
        str(batch2_file),
    )
    ok2, msg2 = pipeline.process_batch(batch2_file, "2026-10-02")
    assert ok2 is False
    assert "REJECTED" in msg2
    assert (quarantine_dir / "2026-10-02_batch2_broken.parquet").is_file()
    assert (quarantine_dir / "2026-10-02_alert.md").is_file()
    # Ensure storage partition 2026-10-02 was NOT created
    assert not (storage_dir / "partition=2026-10-02").exists()

    # 4. Batch 3: Evolved batch (safe nullable column added) -> Accepted
    evolved_schema = pa.schema(
        [
            ("user_id", pa.int64(), False),
            ("event_type", pa.string(), False),
            ("timestamp", pa.timestamp("us", tz="UTC"), False),
            ("device", pa.string(), True),  # Added nullable column
        ]
    )
    batch3_file = tmp_path / "batch3_evolved.parquet"
    pq.write_table(
        pa.Table.from_arrays(
            [
                pa.array([105], type=pa.int64()),
                pa.array(["logout"]),
                pa.array([1700000004000000], type=pa.timestamp("us", tz="UTC")),
                pa.array(["mobile"]),
            ],
            schema=evolved_schema,
        ),
        str(batch3_file),
    )
    ok3, msg3 = pipeline.process_batch(batch3_file, "2026-10-03")
    assert ok3 is True
    assert "ACCEPTED" in msg3
    assert (storage_dir / "partition=2026-10-03" / "data.parquet").is_file()

    # 5. Downstream aggregation operates reliably on clean partitions
    agg = pipeline.run_downstream_aggregation()
    assert agg["status"] == "HEALTHY"
    assert agg["partitions"] == 2  # Only batch 1 and batch 3 were ingested
    assert agg["total_rows"] == 3


if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        test_guarded_pipeline_lifecycle(Path(td))
        print("Integration pipeline test passed successfully!")
