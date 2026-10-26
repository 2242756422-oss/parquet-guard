"""Unit and integration tests for dataset topology, grouping, and HTML report generation."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from parquet_guard.core.auditor import audit_dataset
from parquet_guard.core.consumer import (
    ConsumerCompatibilityStatus,
    ConsumerContract,
    ConsumerFieldRequirement,
    evaluate_consumer_impact,
)
from parquet_guard.core.grouping import analyze_dataset_topology
from parquet_guard.core.html_reporter import generate_html_diagnostic_report


def _create_partition_file(path: Path, schema: pa.Schema, rows: int = 10) -> None:
    data = {}
    for f in schema:
        if pa.types.is_integer(f.type):
            data[f.name] = pa.array(list(range(rows)), type=f.type)
        elif pa.types.is_string(f.type):
            data[f.name] = pa.array([f"val_{i}" for i in range(rows)], type=f.type)
        elif pa.types.is_decimal(f.type):
            import decimal

            data[f.name] = pa.array([decimal.Decimal("10.50")] * rows, type=f.type)
        else:
            data[f.name] = pa.array([None] * rows, type=f.type)
    pq.write_table(pa.table(data, schema=schema), str(path))


def test_schema_grouping_and_divergence_concentration(tmp_path: Path) -> None:
    """Verify grouping files into canonical and divergent groups with divergence concentration."""
    dataset_dir = tmp_path / "lake"
    dataset_dir.mkdir()

    # Schema 1: Majority Canonical (4 files)
    s1 = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
        ]
    )
    for i in range(4):
        p = dataset_dir / f"canon_{i}.parquet"
        _create_partition_file(p, s1)

    # Schema 2: Divergent Group (1 file, id is int32)
    s2 = pa.schema(
        [
            pa.field("id", pa.int32()),
            pa.field("name", pa.string()),
        ]
    )
    _create_partition_file(dataset_dir / "divergent_1.parquet", s2)

    # Schema 3: Divergent Group (1 file, added 'tag' string)
    s3 = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("tag", pa.string()),
        ]
    )
    _create_partition_file(dataset_dir / "divergent_2.parquet", s3)

    # Audit dataset
    audit_res = audit_dataset(str(dataset_dir))
    assert audit_res.total_files_scanned == 6
    assert audit_res.is_consistent is False

    # Consumer expecting strictly int64 id
    consumer = ConsumerContract(
        consumer_id="strict-int64",
        consumer_name="Strict Int64 Consumer",
        fields=[ConsumerFieldRequirement(name="id", permitted_types=["int64"])],
        compatibility_policy="strict",
    )

    topology = analyze_dataset_topology(audit_res, consumers=[consumer])
    assert topology.total_files == 6
    assert topology.distinct_schemas_count == 3
    assert len(topology.groups) == 3

    # Majority group should be canonical group G01 with 4 files (66.67%)
    g1 = topology.groups[0]
    assert g1.file_count == 4
    assert g1.percentage == pytest.approx(66.67, abs=0.1)
    assert g1.consumer_status["strict-int64"] == ConsumerCompatibilityStatus.COMPATIBLE

    # G02 has id as int32, which is incompatible with strict consumer
    g_int32 = next(g for g in topology.groups if g.schema_contract.get_field("id").type == "int32")
    assert g_int32.consumer_status["strict-int64"] == ConsumerCompatibilityStatus.INCOMPATIBLE

    # Divergence concentration: 'id' and 'tag' diverge across files
    div_fields = {d.field_path for d in topology.divergence_concentration}
    assert "id" in div_fields
    assert "tag" in div_fields


def test_html_diagnostic_report_generation(tmp_path: Path) -> None:
    """Verify generating standalone HTML diagnostic report."""
    dataset_dir = tmp_path / "orders_data"
    dataset_dir.mkdir()

    s = pa.schema([pa.field("order_id", pa.int64()), pa.field("customer", pa.string())])
    _create_partition_file(dataset_dir / "f1.parquet", s)

    audit_res = audit_dataset(str(dataset_dir))
    topology = analyze_dataset_topology(audit_res)

    consumer = ConsumerContract(
        consumer_id="c1",
        consumer_name="Consumer 1",
        fields=[ConsumerFieldRequirement(name="order_id")],
    )
    assert audit_res.canonical_schema is not None
    impact = evaluate_consumer_impact(
        audit_res.canonical_schema, [consumer], dataset_identifier="orders_data"
    )

    out_html = tmp_path / "report.html"
    generate_html_diagnostic_report(
        output_path=str(out_html),
        audit_result=audit_res,
        impact_report=impact,
        topology_report=topology,
        dataset_title="Orders Lakehouse",
    )

    assert out_html.exists()
    content = out_html.read_text(encoding="utf-8")
    assert "<!DOCTYPE html>" in content
    assert "Parquet Guard AI" in content
    assert "Orders Lakehouse" in content
    assert "<svg" in content
    assert "Producer-Dataset-Consumer Impact Graph" in content
    assert "Downstream Consumer Compatibility Matrix" in content
