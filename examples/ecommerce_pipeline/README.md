# E-Commerce Orders Pipeline: Consumer-Aware Impact & Migration Verification

This end-to-end example demonstrates how **Parquet Guard** prevents catastrophic downstream query breakages during schema evolution in high-throughput data pipelines.

---

## 1. The Scenario

An upstream checkout microservice generates transactional orders in Apache Parquet format. Over time, three independent consumer pipelines read these files:

1. **`duckdb-financial-reporting`**: Strict financial ETL calculating daily revenues, requiring exact decimal scale `(10, 2)`.
2. **`polars-user-segmentation`**: Permissive customer analytics cluster tracking user statuses; unaffected by financial column alterations.
3. **`pyarrow-warehouse-export`**: Highly sensitive legacy ingestion pipeline with strict schema enforcement (`allow_extra_fields = False`).

### The Upstream Schema Evolution

The upstream engineering team introduces:
- **`amount` scale change**: `decimal128(10, 2)` -> `decimal128(14, 4)` (alters physical decimal representation).
- **`created_at` temporal widening**: `timestamp[s, tz=UTC]` -> `timestamp[ms, tz=UTC]`.
- **New column**: `loyalty_discount: float64`.

---

## 2. Running the Demonstration

Execute the demonstration script:

```bash
python examples/ecommerce_pipeline/pipeline_demo.py
```

Or run via the Parquet Guard CLI:

```bash
# Analyze impact across all consumer contracts
parquet-guard impact orders_v2.parquet -c consumers.json

# Run synthetic migration verification sandbox
parquet-guard verify-migration orders_v2.parquet --plan migration_plan.json -c consumers.json

# Generate professional HTML diagnostic report
parquet-guard report orders_v2.parquet -c consumers.json -o ecommerce_diagnostic_report.html
```

---

## 3. Key Findings & Protections

| Consumer | Verdict | Breaking Discrepancy |
| :--- | :---: | :--- |
| **`duckdb-financial-reporting`** | ❌ **BROKEN** | `amount` decimal scale mismatch `(10, 2)` vs `(14, 4)`. |
| **`polars-user-segmentation`** | ✅ **UNAFFECTED** | Does not project `amount`; operates normally. |
| **`pyarrow-warehouse-export`** | ❌ **BROKEN** | Extra field `loyalty_discount` rejected by strict consumer policy. |

The verified sandbox execution isolates and tests PyArrow migration casts on synthetic data without touching production partition files.
