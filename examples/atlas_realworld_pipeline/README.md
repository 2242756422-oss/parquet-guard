# Project Atlas — Real-World Pipeline Schema Reliability Walkthrough

This example demonstrates an end-to-end production data pipeline where an upstream team evolves an e-commerce order schema, and **Parquet Guard** proactively prevents downstream pipeline outages across **PyArrow**, **DuckDB**, and **Polars**.

---

## 1. The Scenario

An upstream producer produces monthly partitioned Parquet files:
- `year=2024/month=01`: Orders with `subtotal: decimal128(10, 2)` and customer struct `{name, email}`.
- `year=2024/month=02`: Producer widens monetary precision to `subtotal: decimal128(14, 4)` and adds `{tier}` to customer struct.

Downstream consumers have distinct requirements:
1. **DuckDB Analytics Warehouse**: Permissive SQL queries accepting decimal scale up to 4 (`decimal128(14, 4)`).
2. **Polars Fraud ML Model**: Strictly expects exact `decimal128(10, 2)` for feature tensor extraction.
3. **PyArrow Nightly ETL**: Legacy financial reporting pipeline.

---

## 2. Running the Demo

Run the end-to-end demo script:

```bash
python examples/atlas_realworld_pipeline/run_atlas_demo.py
```

Or run via the CLI:

```bash
# 1. Audit partition consistency
parquet-guard audit examples/atlas_realworld_pipeline/dataset -c examples/atlas_realworld_pipeline/contracts/producer_golden_contract.json

# 2. Evaluate downstream consumer impact
parquet-guard impact examples/atlas_realworld_pipeline/dataset/year=2024/month=02/orders_202402.parquet \
  -c examples/atlas_realworld_pipeline/contracts/consumer_polars_ml.json \
  -c examples/atlas_realworld_pipeline/contracts/consumer_duckdb_analytics.json

# 3. Probe real runtime engines (PyArrow, DuckDB, Polars)
parquet-guard verify-engine examples/atlas_realworld_pipeline/dataset \
  -c examples/atlas_realworld_pipeline/contracts/producer_golden_contract.json
```

---

## 3. What Project Atlas Validates

1. **Zero-Data-Scan Partition Auditing**: Reads file footers only, detecting that 2024-02 drifted from 2024-01 without scanning record payloads.
2. **Consumer-Aware Impact**: Identifies that DuckDB is compatible, while the Polars ML model will break due to unexpected decimal scale widening (scale 4 vs 2).
3. **Multi-Engine Runtime Verification**: Probes actual query engines on disk to determine whether multi-file `SCHEMA_UNION` succeeds or fails across engines.
4. **Sandbox Migration Assurance**: Tests candidate transformations safely in an isolated sandbox before production rollout.
5. **Interactive Diagnostic Report**: Generates a self-contained HTML report with full topological and compatibility diagnostics.
