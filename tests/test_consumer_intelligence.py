"""Unit and integration tests for consumer-aware data contract intelligence."""

from __future__ import annotations

from pathlib import Path

import pytest

from parquet_guard.core.consumer import (
    ConsumerCompatibilityStatus,
    ConsumerContract,
    ConsumerFieldRequirement,
    ConsumerGroupSpecification,
    evaluate_consumer_impact,
    evaluate_single_consumer,
)
from parquet_guard.core.models import FieldDefinition, SchemaContract


def test_consumer_contract_validation() -> None:
    """Test ConsumerContract model validation and helper methods."""
    c = ConsumerContract(
        consumer_id="duckdb-analytics",
        consumer_name="DuckDB Analytics Consumer",
        fields=[
            ConsumerFieldRequirement(
                name="user_id",
                required=True,
                permitted_types=["int64", "int32"],
            ),
            ConsumerFieldRequirement(
                name="amount",
                required=True,
                permitted_types=["decimal128(10, 2)"],
                scale=2,
            ),
            ConsumerFieldRequirement(
                name="opt_tag",
                required=False,
                permitted_types=["string"],
            ),
        ],
        allow_extra_fields=True,
    )
    assert c.consumer_id == "duckdb-analytics"
    assert c.required_field_names() == ["user_id", "amount"]
    assert c.get_field_requirement("user_id") is not None
    assert c.get_field_requirement("unknown") is None

    # Invalid version should raise ValueError
    with pytest.raises(ValueError, match="Unsupported consumer contract version"):
        ConsumerContract(
            version="2.0",
            consumer_id="test",
            consumer_name="Test",
        )


def test_mandatory_acceptance_three_consumers_distinct_outcomes(tmp_path: Path) -> None:
    """Section 6.5 Mandatory Acceptance Test:

    One synthetic dataset with at least three downstream consumers whose
    compatibility outcomes differ for the same upstream schema change.
    The system must produce distinct, explainable classifications for each consumer.
    """
    # Upstream Producer evolved schema:
    # - order_id: string (previously was int64)
    # - amount: decimal128(12, 4) (previously was decimal128(10, 2) with scale 2)
    # - order_time: timestamp[us, tz=UTC] (previously was timestamp[ms, tz=UTC])
    # - extra_meta: string (newly added column)
    producer_schema = SchemaContract(
        fields=[
            FieldDefinition(name="order_id", type="string"),
            FieldDefinition(name="amount", type="decimal128(12, 4)", precision=12, scale=4),
            FieldDefinition(
                name="order_time",
                type="timestamp[us, tz=UTC]",
                time_unit="us",
                timezone="UTC",
            ),
            FieldDefinition(name="extra_meta", type="string"),
        ]
    )

    # Consumer 1: DuckDB Analytics (Strict: requires order_id as int64, cannot parse string directly in joins)
    duckdb_consumer = ConsumerContract(
        consumer_id="duckdb-analytics",
        consumer_name="DuckDB Analytical Engine",
        fields=[
            ConsumerFieldRequirement(
                name="order_id",
                required=True,
                permitted_types=["int64", "int32"],
                critical=True,
            ),
            ConsumerFieldRequirement(
                name="amount",
                required=True,
                permitted_types=["decimal128(12, 4)", "decimal128(10, 2)"],
                scale=4,
            ),
        ],
        allow_extra_fields=True,
    )

    # Consumer 2: Polars ETL Pipeline (Requires amount scale=2 for accountingLedger invariants)
    polars_consumer = ConsumerContract(
        consumer_id="polars-etl",
        consumer_name="Polars Accounting Pipeline",
        fields=[
            ConsumerFieldRequirement(
                name="order_id",
                required=True,
                permitted_types=["string"],
            ),
            ConsumerFieldRequirement(
                name="amount",
                required=True,
                permitted_types=["decimal128(10, 2)"],
                scale=2,  # Strict scale requirement
                critical=True,
            ),
        ],
        allow_extra_fields=True,
    )

    # Consumer 3: PyArrow Event Streamer (Accepts string order_id, decimal(12,4), timestamp[us])
    pyarrow_consumer = ConsumerContract(
        consumer_id="pyarrow-streamer",
        consumer_name="PyArrow Event Streamer",
        fields=[
            ConsumerFieldRequirement(
                name="order_id",
                required=True,
                permitted_types=["string"],
            ),
            ConsumerFieldRequirement(
                name="amount",
                required=True,
                permitted_types=["decimal128(12, 4)"],
            ),
            ConsumerFieldRequirement(
                name="order_time",
                required=True,
                time_unit="us",
                timezone="UTC",
            ),
        ],
        allow_extra_fields=True,
    )

    # Consumer 4: ML Ingestion Service (Requires timestamp[ms]; us is conditionally compatible / overflow risk)
    ml_consumer = ConsumerContract(
        consumer_id="ml-feature-service",
        consumer_name="ML Feature Ingestion Service",
        fields=[
            ConsumerFieldRequirement(
                name="order_id",
                required=True,
                permitted_types=["string"],
            ),
            ConsumerFieldRequirement(
                name="order_time",
                required=True,
                time_unit="ms",  # Finer unit in producer triggers conditional compatibility
                critical=True,
            ),
        ],
        allow_extra_fields=True,
        compatibility_policy="permissive",
    )

    consumers = [duckdb_consumer, polars_consumer, pyarrow_consumer, ml_consumer]
    report = evaluate_consumer_impact(producer_schema, consumers, dataset_identifier="orders_lake")

    # Verify DISTINCT classifications
    res_duckdb = next(e for e in report.evaluations if e.consumer_id == "duckdb-analytics")
    res_polars = next(e for e in report.evaluations if e.consumer_id == "polars-etl")
    res_pyarrow = next(e for e in report.evaluations if e.consumer_id == "pyarrow-streamer")
    res_ml = next(e for e in report.evaluations if e.consumer_id == "ml-feature-service")

    # DuckDB is INCOMPATIBLE due to order_id type mismatch
    assert res_duckdb.status == ConsumerCompatibilityStatus.INCOMPATIBLE
    assert "order_id" in res_duckdb.type_mismatches

    # Polars is INCOMPATIBLE due to decimal scale mismatch
    assert res_polars.status == ConsumerCompatibilityStatus.INCOMPATIBLE
    assert "amount" in res_polars.type_mismatches or any(
        "scale" in str(fe.violation_reason) for fe in res_polars.field_evaluations
    )

    # PyArrow is fully COMPATIBLE
    assert res_pyarrow.status == ConsumerCompatibilityStatus.COMPATIBLE
    assert len(res_pyarrow.missing_required_fields) == 0
    assert len(res_pyarrow.type_mismatches) == 0

    # ML is CONDITIONALLY_COMPATIBLE due to timestamp precision widening
    assert res_ml.status == ConsumerCompatibilityStatus.CONDITIONALLY_COMPATIBLE

    # Overall verdict reflects blocking incompatibilities
    assert report.overall_status == ConsumerCompatibilityStatus.INCOMPATIBLE
    assert report.incompatible_consumers == 2
    assert report.compatible_consumers == 1
    assert report.conditionally_compatible_consumers == 1

    # Verify Impact Graph topology
    assert len(report.impact_graph.nodes) == 6  # producer + dataset + 4 consumers
    assert len(report.impact_graph.edges) == 5  # producer->dataset + dataset->4 consumers


def test_embedding_vector_fixed_size_list_dimension_constraint() -> None:
    """Verify fixed-size list dimension constraint checking for ML embeddings."""
    schema_768 = SchemaContract(
        fields=[
            FieldDefinition(
                name="embedding",
                type="fixed_size_list<item: float32>[768]",
                list_size=768,
            )
        ]
    )

    # Consumer expecting 512 dimensions
    legacy_vector_consumer = ConsumerContract(
        consumer_id="milvus-index-512",
        consumer_name="Legacy Vector Search Index",
        fields=[
            ConsumerFieldRequirement(
                name="embedding",
                required=True,
                list_size=512,
            )
        ],
    )

    # Consumer expecting 768 dimensions
    transformer_consumer = ConsumerContract(
        consumer_id="transformer-v2",
        consumer_name="Transformer Embedding Consumer",
        fields=[
            ConsumerFieldRequirement(
                name="embedding",
                required=True,
                list_size=768,
            )
        ],
    )

    ev_legacy = evaluate_single_consumer(schema_768, legacy_vector_consumer)
    assert ev_legacy.status == ConsumerCompatibilityStatus.INCOMPATIBLE
    assert "embedding" in ev_legacy.dimension_mismatches

    ev_transformer = evaluate_single_consumer(schema_768, transformer_consumer)
    assert ev_transformer.status == ConsumerCompatibilityStatus.COMPATIBLE


def test_nullability_and_extra_fields_enforcement() -> None:
    """Verify strict nullability and extra fields policy enforcement."""
    producer_schema = SchemaContract(
        fields=[
            FieldDefinition(name="id", type="int64", nullable=True),
            FieldDefinition(name="unrequested_column", type="string"),
        ]
    )

    # Consumer requiring non-null id and forbidding extra fields
    strict_consumer = ConsumerContract(
        consumer_id="c-strict",
        consumer_name="Strict Consumer",
        fields=[
            ConsumerFieldRequirement(
                name="id",
                required=True,
                allow_null=False,
            )
        ],
        allow_extra_fields=False,
        strict_nullability=True,
    )

    ev = evaluate_single_consumer(producer_schema, strict_consumer)
    assert ev.status == ConsumerCompatibilityStatus.INCOMPATIBLE
    assert "id" in ev.nullability_violations
    assert "unrequested_column" in ev.unexpected_extra_fields


def test_consumer_group_specification_roundtrip() -> None:
    """Verify ConsumerGroupSpecification JSON serialization and parsing."""
    group = ConsumerGroupSpecification(
        version="1.0",
        description="Analytics Consumer Group",
        consumers=[
            ConsumerContract(
                consumer_id="c1",
                consumer_name="Consumer 1",
                fields=[ConsumerFieldRequirement(name="col1")],
            ),
            ConsumerContract(
                consumer_id="c2",
                consumer_name="Consumer 2",
                fields=[ConsumerFieldRequirement(name="col2")],
            ),
        ],
    )
    json_str = group.model_dump_json(indent=2)
    parsed = ConsumerGroupSpecification.model_validate_json(json_str)
    assert len(parsed.consumers) == 2
    assert parsed.get_consumer("c1") is not None
    assert parsed.get_consumer("nonexistent") is None
