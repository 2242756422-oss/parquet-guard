"""Generate realistic e-commerce partitioned Parquet dataset illustrating schema drift."""

from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def generate_pipeline_dataset(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    # -------------------------------------------------------------------------
    # Partition 1 (2024-01): Baseline contract
    # -------------------------------------------------------------------------
    p1_dir = output_dir / "year=2024" / "month=01"
    p1_dir.mkdir(parents=True, exist_ok=True)
    p1_file = p1_dir / "orders_202401.parquet"

    p1_schema = pa.schema(
        [
            ("order_id", pa.int64(), False),
            ("subtotal", pa.decimal128(10, 2), False),
            ("tax", pa.decimal128(10, 2), False),
            ("created_at", pa.timestamp("ms", tz="UTC"), False),
            (
                "customer",
                pa.struct([pa.field("name", pa.string(), False), pa.field("email", pa.string(), True)]),
                False,
            ),
        ]
    )

    p1_table = pa.Table.from_arrays(
        [
            pa.array([1001, 1002, 1003], type=pa.int64()),
            pa.array([Decimal("49.99"), Decimal("129.50"), Decimal("15.00")], type=pa.decimal128(10, 2)),
            pa.array([Decimal("4.00"), Decimal("10.36"), Decimal("1.20")], type=pa.decimal128(10, 2)),
            pa.array(
                [1704067200000, 1704153600000, 1704240000000],
                type=pa.timestamp("ms", tz="UTC"),
            ),
            pa.array(
                [
                    {"name": "Alice", "email": "alice@example.com"},
                    {"name": "Bob", "email": "bob@example.com"},
                    {"name": "Charlie", "email": None},
                ],
                type=p1_schema.field("customer").type,
            ),
        ],
        schema=p1_schema,
    )
    pq.write_table(p1_table, p1_file)

    # -------------------------------------------------------------------------
    # Partition 2 (2024-02): Safe widening (decimal scale widened, extra struct field)
    # -------------------------------------------------------------------------
    p2_dir = output_dir / "year=2024" / "month=02"
    p2_dir.mkdir(parents=True, exist_ok=True)
    p2_file = p2_dir / "orders_202402.parquet"

    p2_schema = pa.schema(
        [
            ("order_id", pa.int64(), False),
            ("subtotal", pa.decimal128(14, 4), False),  # Widened decimal
            ("tax", pa.decimal128(14, 4), False),
            ("created_at", pa.timestamp("ms", tz="UTC"), False),
            (
                "customer",
                pa.struct(
                    [
                        pa.field("name", pa.string(), False),
                        pa.field("email", pa.string(), True),
                        pa.field("tier", pa.string(), True),  # Additive struct child
                    ]
                ),
                False,
            ),
        ]
    )

    p2_table = pa.Table.from_arrays(
        [
            pa.array([1004, 1005], type=pa.int64()),
            pa.array([Decimal("89.1250"), Decimal("245.9999")], type=pa.decimal128(14, 4)),
            pa.array([Decimal("7.1300"), Decimal("19.6800")], type=pa.decimal128(14, 4)),
            pa.array(
                [1706745600000, 1706832000000],
                type=pa.timestamp("ms", tz="UTC"),
            ),
            pa.array(
                [
                    {"name": "Diana", "email": "diana@example.com", "tier": "gold"},
                    {"name": "Evan", "email": "evan@example.com", "tier": "silver"},
                ],
                type=p2_schema.field("customer").type,
            ),
        ],
        schema=p2_schema,
    )
    pq.write_table(p2_table, p2_file)


if __name__ == "__main__":
    out = Path(__file__).parent / "dataset"
    generate_pipeline_dataset(out)
    print(f"Generated realistic dataset partitions at: {out}")
