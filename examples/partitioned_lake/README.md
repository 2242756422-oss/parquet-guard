# Partitioned Data Lake: Topology Grouping & Divergence Concentration

This example demonstrates how **Parquet Guard** analyzes large, multi-partition Parquet datasets to discover schema clusters, quantify divergence concentration, and evaluate downstream consumer compatibility across an entire lake.

---

## 1. The Scenario

In modern analytics lakes (S3, GCS, ADLS, MinIO), pipelines write daily or hourly partitions. Over months, subtle upstream changes cause files to diverge into multiple distinct schema variants.

Query engines like **DuckDB**, **Trino**, or **Snowflake** fail unexpectedly when a scan hits a rogue partition with an incompatible type.

### The Lake Topology

In this scenario, a lake directory contains 10 partition files split into 3 schema clusters:
- **Canonical Cluster (60% of files)**: Standard schema with 4 columns (`event_id: int64`).
- **Cluster B (30% of files)**: Extended schema with an added column (`device_os: string`).
- **Rogue Cluster C (10% of files)**: Broken partition where `event_id` was written as `int32` instead of `int64`.

---

## 2. Running the Demonstration

```bash
python examples/partitioned_lake/lake_topology_demo.py
```

Or using the Parquet Guard CLI:

```bash
parquet-guard audit ./lake_partitions/
parquet-guard report ./lake_partitions/ -o lake_topology_report.html
```

---

## 3. Results & Protections

Parquet Guard's topology analyzer:
1. **Groups files by fingerprint** without reading any row data (pure metadata inspection).
2. **Identifies the canonical group** (highest file count).
3. **Calculates divergence concentration**: identifies rogue files causing downstream query failure.
4. **Generates an SVG topology graph and HTML dashboard**.
