# AI Feature Store: Vector Dimension Drift Guard

This example demonstrates how **Parquet Guard** detects and blocks vector dimension mismatches in AI feature stores and vector database ingestion pipelines.

---

## 1. The Scenario

Modern production ML systems store continuous vector embeddings in Apache Parquet files using Arrow's `fixed_size_list` data type.

A downstream vector search indexing engine (**Qdrant / Milvus / FAISS / pgvector**) consumes these embeddings:
- Fixed index dimension: **512 float32 elements**.
- Engine expects exact length layout to allocate fixed SIMD register buffers.

### The Upstream Drift

An upstream ML platform updates its embedding backbone from a 512-dimension model to a 768-dimension transformer (`bge-base` or `text-embedding-3-small`).
While the column name (`embedding`) and item type (`float32`) remain identical, the list size changes from `[512]` to `[768]`.

Without contract validation, zero-copy batch readers pass the wrong buffer strides directly into C++ ANN indexing libraries, resulting in **SIGSEGV crashes** or index corruption.

---

## 2. Running the Demonstration

```bash
python examples/ai_feature_pipeline/feature_store_demo.py
```

Or using the Parquet Guard CLI:

```bash
parquet-guard impact embeddings_v2.parquet -c vector_consumer.json
```

---

## 3. Results & Protections

Parquet Guard immediately identifies:
- **`dimension_mismatches`**: `embedding` length altered from `512` to `768`.
- **Severity**: `BREAKING` (`INCOMPATIBLE`).
- Blocks deployment and provides remediation advice (dual-write to `embedding_768` column or create a parallel vector collection).
