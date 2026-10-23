"""Core subsystems of parquet-guard."""

from parquet_guard.core.consumer import (
    ConsumerCompatibilityStatus,
    ConsumerContract,
    ConsumerFieldEvaluation,
    ConsumerFieldRequirement,
    ConsumerGroupSpecification,
    ConsumerImpactEvaluation,
    ConsumerImpactGraph,
    ConsumerImpactReport,
    evaluate_consumer_impact,
    evaluate_single_consumer,
)
from parquet_guard.core.models import (
    ChangeType,
    DiffReport,
    ErrorCode,
    FieldDefinition,
    FieldDiff,
    PartitionAuditResult,
    PartitionFileReport,
    ResultState,
    SchemaContract,
    Severity,
)

__all__ = [
    "ChangeType",
    "ConsumerCompatibilityStatus",
    "ConsumerContract",
    "ConsumerFieldEvaluation",
    "ConsumerFieldRequirement",
    "ConsumerGroupSpecification",
    "ConsumerImpactEvaluation",
    "ConsumerImpactGraph",
    "ConsumerImpactReport",
    "DiffReport",
    "ErrorCode",
    "FieldDefinition",
    "FieldDiff",
    "PartitionAuditResult",
    "PartitionFileReport",
    "ResultState",
    "SchemaContract",
    "Severity",
    "evaluate_consumer_impact",
    "evaluate_single_consumer",
]
