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


def test_p0_short_circuit_exact_permitted_type_with_contradictory_list_size() -> None:
    """Regression 1: Exact permitted type with contradictory list_size must be INCOMPATIBLE."""
    schema = SchemaContract(
        fields=[
            FieldDefinition(
                name="embedding",
                type="fixed_size_list<item: float32>[512]",
                list_size=512,
            )
        ]
    )
    consumer = ConsumerContract(
        consumer_id="vec-consumer",
        consumer_name="Vector Consumer",
        fields=[
            ConsumerFieldRequirement(
                name="embedding",
                required=True,
                permitted_types=[
                    "fixed_size_list<item: float32>[512]",
                    "fixed_size_list<item: float32>[768]",
                ],
                list_size=768,
            )
        ],
    )
    ev = evaluate_single_consumer(schema, consumer)
    assert ev.status == ConsumerCompatibilityStatus.INCOMPATIBLE
    assert "embedding" in ev.dimension_mismatches
    assert "768" in (ev.field_evaluations[0].violation_reason or "")


def test_p0_short_circuit_exact_permitted_type_with_contradictory_decimal_scale() -> None:
    """Regression 2: Exact permitted type with contradictory decimal scale must be INCOMPATIBLE."""
    schema = SchemaContract(
        fields=[
            FieldDefinition(
                name="price",
                type="decimal128(12, 4)",
                precision=12,
                scale=4,
            )
        ]
    )
    consumer = ConsumerContract(
        consumer_id="ledger-consumer",
        consumer_name="Ledger Consumer",
        fields=[
            ConsumerFieldRequirement(
                name="price",
                required=True,
                permitted_types=["decimal128(12, 4)", "decimal128(12, 2)"],
                scale=2,
            )
        ],
    )
    ev = evaluate_single_consumer(schema, consumer)
    assert ev.status == ConsumerCompatibilityStatus.INCOMPATIBLE
    assert "price" in ev.type_mismatches
    assert "scale" in (ev.field_evaluations[0].violation_reason or "").lower()


def test_p0_short_circuit_exact_permitted_type_with_contradictory_timestamp_unit() -> None:
    """Regression 3: Exact permitted type with contradictory timestamp unit/tz must not short-circuit."""
    schema = SchemaContract(
        fields=[
            FieldDefinition(
                name="event_time",
                type="timestamp[ns, tz=UTC]",
                time_unit="ns",
                timezone="UTC",
            )
        ]
    )
    # Strict policy consumer requiring 'ms' even though 'timestamp[ns, tz=UTC]' was listed in permitted_types
    consumer_unit = ConsumerContract(
        consumer_id="ts-consumer-strict",
        consumer_name="Strict Timestamp Consumer",
        compatibility_policy="strict",
        fields=[
            ConsumerFieldRequirement(
                name="event_time",
                required=True,
                permitted_types=["timestamp[ns, tz=UTC]", "timestamp[ms, tz=UTC]"],
                time_unit="ms",
                timezone="UTC",
            )
        ],
    )
    ev_unit = evaluate_single_consumer(schema, consumer_unit)
    assert ev_unit.status == ConsumerCompatibilityStatus.INCOMPATIBLE
    assert "unit" in (ev_unit.field_evaluations[0].violation_reason or "").lower()

    # Contradictory timezone even in permissive mode
    consumer_tz = ConsumerContract(
        consumer_id="ts-consumer-tz",
        consumer_name="TZ Consumer",
        compatibility_policy="permissive",
        fields=[
            ConsumerFieldRequirement(
                name="event_time",
                required=True,
                permitted_types=["timestamp[ns, tz=UTC]"],
                time_unit="ns",
                timezone="America/New_York",
            )
        ],
    )
    ev_tz = evaluate_single_consumer(schema, consumer_tz)
    assert ev_tz.status == ConsumerCompatibilityStatus.INCOMPATIBLE
    assert "timezone" in (ev_tz.field_evaluations[0].violation_reason or "").lower()


def test_p0_exact_type_with_valid_additional_constraints() -> None:
    """Regression 4: Exact permitted type with valid additional constraints is COMPATIBLE."""
    schema = SchemaContract(
        fields=[
            FieldDefinition(name="price", type="decimal128(10, 2)", precision=10, scale=2),
            FieldDefinition(
                name="embedding",
                type="fixed_size_list<item: float32>[256]",
                list_size=256,
            ),
            FieldDefinition(
                name="ts",
                type="timestamp[ms, tz=UTC]",
                time_unit="ms",
                timezone="UTC",
            ),
        ]
    )
    consumer = ConsumerContract(
        consumer_id="valid-all",
        consumer_name="Valid Multi-Constraint Consumer",
        fields=[
            ConsumerFieldRequirement(
                name="price",
                permitted_types=["decimal128(10, 2)"],
                precision=10,
                scale=2,
                allow_null=True,
            ),
            ConsumerFieldRequirement(
                name="embedding",
                permitted_types=["fixed_size_list<item: float32>[256]"],
                list_size=256,
            ),
            ConsumerFieldRequirement(
                name="ts",
                permitted_types=["timestamp[ms, tz=UTC]"],
                time_unit="ms",
                timezone="UTC",
            ),
        ],
    )
    ev = evaluate_single_consumer(schema, consumer)
    assert ev.status == ConsumerCompatibilityStatus.COMPATIBLE
    assert all(fe.status == ConsumerCompatibilityStatus.COMPATIBLE for fe in ev.field_evaluations)


def test_p0_several_independently_conditional_constraints() -> None:
    """Regression 5: Several independently conditional constraints are all evaluated and composed."""
    schema = SchemaContract(
        fields=[
            FieldDefinition(
                name="amount",
                type="decimal128(18, 2)",
                nullable=True,
                precision=18,
                scale=2,
            )
        ]
    )
    consumer = ConsumerContract(
        consumer_id="cond-consumer",
        consumer_name="Conditional Multi-Constraint Consumer",
        compatibility_policy="permissive",
        strict_nullability=False,
        fields=[
            ConsumerFieldRequirement(
                name="amount",
                required=True,
                # Producer decimal128(18, 2) widens safely to consumer's permitted decimal256(18, 2)
                permitted_types=["decimal256(18, 2)"],
                # Producer precision 18 > consumer precision bound 12 (conditional warning)
                precision=12,
                scale=2,
                # Producer is nullable while consumer requests allow_null=False with strict_nullability=False
                allow_null=False,
            )
        ],
    )
    ev = evaluate_single_consumer(schema, consumer)
    assert ev.status == ConsumerCompatibilityStatus.CONDITIONALLY_COMPATIBLE
    reason = ev.field_evaluations[0].violation_reason or ""
    assert "safe widening" in reason.lower()
    assert "precision" in reason.lower()
    assert "null" in reason.lower()


def test_p0_unsupported_or_unparseable_type_representations() -> None:
    """Regression 6: Unsupported or unparseable type representations must not pass as compatible."""
    schema_bad_prod = SchemaContract(
        fields=[FieldDefinition(name="mystery", type="unsupported_ weird_type<123>")]
    )
    consumer_any = ConsumerContract(
        consumer_id="c-any",
        consumer_name="Any Type Consumer",
        fields=[ConsumerFieldRequirement(name="mystery", required=True)],
    )
    ev1 = evaluate_single_consumer(schema_bad_prod, consumer_any)
    # Insufficient knowledge is UNCERTAIN (never COMPATIBLE) and blocks a critical requirement.
    assert ev1.status == ConsumerCompatibilityStatus.UNCERTAIN
    assert ev1.is_blocking is True
    assert "unsupported" in (ev1.field_evaluations[0].violation_reason or "").lower()

    # Even if consumer explicitly put the same unparseable type in permitted_types
    consumer_matching_bad = ConsumerContract(
        consumer_id="c-bad",
        consumer_name="Matching Bad Type Consumer",
        fields=[
            ConsumerFieldRequirement(
                name="mystery",
                required=True,
                permitted_types=["unsupported_ weird_type<123>"],
            )
        ],
    )
    ev2 = evaluate_single_consumer(schema_bad_prod, consumer_matching_bad)
    # A string-identical but unrecognized type still cannot be verified.
    assert ev2.status == ConsumerCompatibilityStatus.UNCERTAIN
    assert ev2.is_blocking is True

    # Valid producer type, but consumer specifies an unparseable permitted_type
    schema_good = SchemaContract(fields=[FieldDefinition(name="id", type="int64")])
    consumer_bad_req = ConsumerContract(
        consumer_id="c-bad-req",
        consumer_name="Bad Permitted Type Consumer",
        fields=[
            ConsumerFieldRequirement(
                name="id",
                required=True,
                permitted_types=["bogus_int_999"],
            )
        ],
    )
    ev3 = evaluate_single_consumer(schema_good, consumer_bad_req)
    assert ev3.status == ConsumerCompatibilityStatus.INCOMPATIBLE


def test_p0_producer_wider_than_consumer_is_incompatible() -> None:
    """Producer-side widening beyond consumer's permitted type (narrowing into consumer) is INCOMPATIBLE."""
    # Producer emits int64, consumer only permits int32 -> reading int64 into int32 risks overflow!
    schema = SchemaContract(fields=[FieldDefinition(name="user_id", type="int64")])
    consumer = ConsumerContract(
        consumer_id="int32-reader",
        consumer_name="32-bit Reader",
        compatibility_policy="permissive",
        fields=[
            ConsumerFieldRequirement(
                name="user_id",
                required=True,
                permitted_types=["int32"],
            )
        ],
    )
    ev = evaluate_single_consumer(schema, consumer)
    assert ev.status == ConsumerCompatibilityStatus.INCOMPATIBLE
    assert "user_id" in ev.type_mismatches


def test_p0_nested_field_paths_traversal_and_edge_cases() -> None:
    """Verify nested path resolution across Struct, List, Map, escaped literal dots, and failure modes."""
    schema = SchemaContract(
        fields=[
            # 1. Nested struct -> struct -> primitive
            FieldDefinition(
                name="payload",
                type="struct<event_time: timestamp[ms, tz=UTC], meta: struct<source: string>>",
                children=[
                    FieldDefinition(
                        name="event_time",
                        type="timestamp[ms, tz=UTC]",
                        time_unit="ms",
                        timezone="UTC",
                    ),
                    FieldDefinition(
                        name="meta",
                        type="struct<source: string>",
                        children=[FieldDefinition(name="source", type="string")],
                    ),
                ],
            ),
            # 2. List of structs
            FieldDefinition(
                name="items",
                type="list<item: struct<sku: string, qty: int32>>",
                children=[
                    FieldDefinition(
                        name="item",
                        type="struct<sku: string, qty: int32>",
                        children=[
                            FieldDefinition(name="sku", type="string"),
                            FieldDefinition(name="qty", type="int32"),
                        ],
                    )
                ],
            ),
            # 3. Map key/value
            FieldDefinition(
                name="tags",
                type="map<string, int64>",
                children=[
                    FieldDefinition(name="key", type="string"),
                    FieldDefinition(name="value", type="int64"),
                ],
            ),
            # 4. Literal field name containing a dot
            FieldDefinition(name="metric.v1", type="float64"),
            # 5. Struct with duplicate child names (malformed / ambiguous)
            FieldDefinition(
                name="dup_struct",
                type="struct<x: int32, x: int64>",
                children=[
                    FieldDefinition(name="x", type="int32"),
                    FieldDefinition(name="x", type="int64"),
                ],
            ),
            # 6. Scalar leaf
            FieldDefinition(name="scalar_id", type="int64"),
        ]
    )

    # Valid nested consumer
    valid_nested_consumer = ConsumerContract(
        consumer_id="nested-ok",
        consumer_name="Nested Reader",
        fields=[
            ConsumerFieldRequirement(
                name="payload.event_time",
                permitted_types=["timestamp[ms, tz=UTC]"],
                time_unit="ms",
                timezone="UTC",
            ),
            ConsumerFieldRequirement(
                name="payload.meta.source",
                permitted_types=["string"],
            ),
            ConsumerFieldRequirement(
                name="items.item.sku",
                permitted_types=["string"],
            ),
            ConsumerFieldRequirement(
                name="tags.key",
                permitted_types=["string"],
            ),
            ConsumerFieldRequirement(
                name="tags.value",
                permitted_types=["int64"],
            ),
            ConsumerFieldRequirement(
                name=r"metric\.v1",
                permitted_types=["float64"],
            ),
        ],
    )
    ev_ok = evaluate_single_consumer(schema, valid_nested_consumer)
    assert ev_ok.status == ConsumerCompatibilityStatus.COMPATIBLE
    assert len(ev_ok.missing_required_fields) == 0

    # Edge cases: duplicate child name, missing intermediate node, traversal through scalar leaf
    edge_consumer = ConsumerContract(
        consumer_id="nested-edge",
        consumer_name="Edge Case Reader",
        fields=[
            ConsumerFieldRequirement(name="dup_struct.x", permitted_types=["int32"]),
            ConsumerFieldRequirement(name="payload.missing_node.leaf", permitted_types=["string"]),
            ConsumerFieldRequirement(name="scalar_id.sub_field", permitted_types=["int64"]),
        ],
    )
    ev_edge = evaluate_single_consumer(schema, edge_consumer)
    assert ev_edge.status == ConsumerCompatibilityStatus.INCOMPATIBLE
    reasons_by_name = {fe.field_name: fe.violation_reason or "" for fe in ev_edge.field_evaluations}
    assert "duplicate" in reasons_by_name["dup_struct.x"].lower()
    assert "missing" in reasons_by_name["payload.missing_node.leaf"].lower()
    assert (
        "non-nested" in reasons_by_name["scalar_id.sub_field"].lower()
        or "scalar" in reasons_by_name["scalar_id.sub_field"].lower()
        or "cannot traverse" in reasons_by_name["scalar_id.sub_field"].lower()
    )

