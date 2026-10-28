"""Runtime query engine verification layer for PyArrow, DuckDB, and Polars."""

from parquet_guard.verification.adapters import (
    BaseEngineAdapter,
    quote_sql_identifier,
    sanitize_error_message,
    validate_row_limit,
)
from parquet_guard.verification.duckdb_engine import DuckDBEngineAdapter
from parquet_guard.verification.models import (
    DisagreementClassification,
    EngineProbeOperation,
    EngineProbeResult,
    EngineVerificationReport,
    MetadataVerdict,
    RuleVsRuntimeDisagreement,
    RuntimeVerdict,
    ValidationScope,
)
from parquet_guard.verification.polars_engine import PolarsEngineAdapter
from parquet_guard.verification.pyarrow_engine import PyArrowEngineAdapter
from parquet_guard.verification.runner import (
    classify_disagreement,
    format_engine_verification_json,
    format_engine_verification_markdown,
    format_engine_verification_terminal,
    get_engine_adapter,
    list_available_engines,
    register_engine_adapter,
    verify_engine_compatibility,
)

__all__ = [
    "BaseEngineAdapter",
    "quote_sql_identifier",
    "sanitize_error_message",
    "validate_row_limit",
    "PyArrowEngineAdapter",
    "DuckDBEngineAdapter",
    "PolarsEngineAdapter",
    "RuntimeVerdict",
    "MetadataVerdict",
    "ValidationScope",
    "EngineProbeOperation",
    "DisagreementClassification",
    "EngineProbeResult",
    "RuleVsRuntimeDisagreement",
    "EngineVerificationReport",
    "register_engine_adapter",
    "get_engine_adapter",
    "list_available_engines",
    "classify_disagreement",
    "verify_engine_compatibility",
    "format_engine_verification_terminal",
    "format_engine_verification_json",
    "format_engine_verification_markdown",
]
