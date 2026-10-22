"""parquet-guard: High-performance, zero-data-scan Parquet schema contract & drift detection guard.

Designed for modern data pipelines (DuckDB, Polars, PyArrow, Spark),
MLOps feature stores, and CI/CD pull request validation.
"""

from __future__ import annotations

# ruff: noqa: I001

from parquet_guard.core.auditor import audit_dataset, compute_schema_fingerprint
from parquet_guard.core.cache import (
    CachedFileInfo,
    DatasetAuditCache,
    load_audit_cache,
    save_audit_cache,
)
from parquet_guard.core.compatibility import analyze_compatibility, compare_schemas
from parquet_guard.core.inspector import (
    ContractValidationError,
    ParquetReadError,
    arrow_field_to_definition,
    extract_contract,
    extract_file_metadata,
    inspect_schema,
    load_contract,
    save_contract,
    validate_contract,
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
from parquet_guard.core.reporter import (
    format_audit_json,
    format_audit_markdown,
    format_audit_sarif,
    format_audit_terminal,
    format_diff_json,
    format_diff_markdown,
    format_diff_sarif,
    format_diff_terminal,
)
from parquet_guard.ai import (  # noqa: E402, I001
    diagnose_schema_drift,
    generate_migration_plan,
    propose_contract_evolution,
    run_evaluation_suite,
)

__version__ = "0.2.0"
__all__ = [
    "__version__",
    "inspect_schema",
    "extract_contract",
    "extract_file_metadata",
    "load_contract",
    "save_contract",
    "validate_contract",
    "compare_schemas",
    "analyze_compatibility",
    "audit_dataset",
    "compute_schema_fingerprint",
    "diagnose_schema_drift",
    "propose_contract_evolution",
    "generate_migration_plan",
    "run_evaluation_suite",
    "arrow_field_to_definition",
    "FieldDefinition",
    "SchemaContract",
    "DiffReport",
    "FieldDiff",
    "ChangeType",
    "Severity",
    "ResultState",
    "ErrorCode",
    "PartitionAuditResult",
    "PartitionFileReport",
    "CachedFileInfo",
    "DatasetAuditCache",
    "load_audit_cache",
    "save_audit_cache",
    "ParquetReadError",
    "ContractValidationError",
    "format_diff_terminal",
    "format_diff_markdown",
    "format_diff_json",
    "format_diff_sarif",
    "format_audit_terminal",
    "format_audit_markdown",
    "format_audit_json",
    "format_audit_sarif",
]
