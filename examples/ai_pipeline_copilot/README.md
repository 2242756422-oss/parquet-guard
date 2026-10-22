# 🤖 Parquet Guard AI — Real-World Pipeline Evolution Copilot

This runnable demonstration illustrates how `parquet-guard` and `parquet-guard AI` protect modern data and ML pipelines from silent schema drift, diagnose operational risk, and plan safe contract migrations.

## Scenario: Vector Feature Store Evolution

1. **V1 Pipeline**: An ML feature pipeline stores embedding vectors (`fixed_size_list<float32>[512]`), timestamps (`timestamp[s]`), and event IDs (`int64`).
2. **Contract Enforced**: A golden contract (`contracts/events.schema.json`) is committed into Git.
3. **Upstream Schema Drift**:
   - A new model release unexpectedly produces **768-dimensional embeddings** instead of 512.
   - Timestamps are upgraded to nanoseconds (`timestamp[ns]`), introducing signed int64 overflow risks.
   - An extra `user_id` field is added.
4. **Deterministic Gate**: `parquet-guard check` detects the breaking changes and blocks the CI pull request.
5. **AI Drift Diagnosis**: `parquet-guard explain` receives the normalized structural findings and delivers an evidence-grounded diagnosis with zero raw data leaks.
6. **AI Migration Planner**: `parquet-guard propose-migration` generates an updated contract proposal and validated PyArrow conversion code.
7. **Deterministic Verification**: The proposed migration contract is deterministically verified against the target schema, proving full compatibility before deployment.

## Architectural Boundaries

- **Offline Simulation vs Live Models**: This demo runs with `MockAIProvider` (`mock-copilot`), demonstrating deterministic fallback and structural reasoning without external network calls or API keys.
- **Contract Check vs Physical Migration**: Step 6 deterministically validates the proposed schema contract in memory and synthesizes a runnable PyArrow conversion script. Physical migration of production files is performed by executing the generated script.
- **Privacy & Metadata Boundary**: Zero data rows are scanned or read. Only structural schema ASTs, field names, and error codes are normalized into diagnosis prompts.

## Running the Demo

```bash
python examples/ai_pipeline_copilot/run_copilot_demo.py
```
