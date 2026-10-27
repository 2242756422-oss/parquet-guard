"""Base interface and error normalizer for real query engine verification adapters."""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple

from parquet_guard.verification.models import (
    EngineProbeOperation,
    EngineProbeResult,
    RuntimeVerdict,
)


def quote_sql_identifier(identifier: str) -> str:
    """Safely escape and double-quote a SQL identifier preventing SQL injection.

    Replaces internal double quotes with escaped double quotes ("").
    """
    if not isinstance(identifier, str):
        raise TypeError("SQL identifier must be a string")
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def quote_column_path(path: str) -> str:
    """Safely quote a column name or dot-separated nested path for SQL engines.

    e.g. 'user.email' -> '"user"."email"'
    e.g. 'col_name' -> '"col_name"'
    """
    if not isinstance(path, str):
        raise TypeError("Column path must be a string")
    parts = path.split(".")
    return ".".join(quote_sql_identifier(p) for p in parts)


def sanitize_error_message(msg: str) -> str:
    """Sanitize runtime error message removing local absolute paths and system credentials."""
    # Redact common path patterns
    msg = re.sub(r"/(?:Users|home|tmp|private|var)/[a-zA-Z0-9_\-./]+", "<path>", msg)
    # Redact potential keys or tokens
    msg = re.sub(r"(?:key|token|secret|password)=[^\s&,;]+", "<redacted>", msg, flags=re.IGNORECASE)
    # Truncate overly long error traces
    if len(msg) > 600:
        msg = msg[:600] + "... [truncated]"
    return msg


class BaseEngineAdapter(ABC):
    """Abstract base class for all query and execution engine adapters."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the engine (e.g., 'pyarrow', 'duckdb', 'polars')."""
        ...

    @abstractmethod
    def is_available(self) -> bool:
        """Return True if the underlying engine library is installed and importable."""
        ...

    @abstractmethod
    def version(self) -> Optional[str]:
        """Return engine library version string or None if unavailable."""
        ...

    @abstractmethod
    def probe_read(
        self,
        file_paths: List[str],
        expected_columns: Optional[List[str]] = None,
        row_limit: int = 10,
    ) -> EngineProbeResult:
        """Probe if the engine can read file(s) and project expected columns."""
        ...

    @abstractmethod
    def probe_cast(
        self,
        file_path: str,
        target_field_types: Dict[str, str],
        row_limit: int = 10,
    ) -> EngineProbeResult:
        """Probe if the engine can cast existing columns to specified target Arrow types."""
        ...

    @abstractmethod
    def probe_schema_union(
        self,
        file_paths: List[str],
        row_limit: int = 10,
    ) -> EngineProbeResult:
        """Probe if the engine can unify multiple partition files with drifted schemas."""
        ...

    def normalize_error(self, exc: Exception) -> Tuple[str, str]:
        """Normalize an engine exception into (error_category, sanitized_message)."""
        raw_msg = str(exc)
        sanitized = sanitize_error_message(raw_msg)
        err_lower = raw_msg.lower()

        if "not found" in err_lower or "no such file" in err_lower:
            category = "FILE_NOT_FOUND"
        elif "schema" in err_lower or "type mismatch" in err_lower or "cannot cast" in err_lower:
            category = "SCHEMA_INCOMPATIBILITY"
        elif "conversion" in err_lower or "overflow" in err_lower or "out of range" in err_lower:
            category = "NUMERIC_OVERFLOW_OR_CONVERSION"
        elif "null" in err_lower or "non-null" in err_lower:
            category = "NULLABILITY_VIOLATION"
        elif "corrupt" in err_lower or "magic" in err_lower or "bad parquet" in err_lower:
            category = "PARQUET_CORRUPTED"
        else:
            category = "ENGINE_RUNTIME_ERROR"

        return category, sanitized

    def make_unsupported_result(self, operation: EngineProbeOperation) -> EngineProbeResult:
        """Return standard UNSUPPORTED probe result when engine library is not installed."""
        return EngineProbeResult(
            engine_name=self.name,
            engine_version=None,
            operation=operation,
            runtime_verdict=RuntimeVerdict.UNSUPPORTED,
            duration_ms=0.0,
            error_category="ENGINE_NOT_INSTALLED",
            error_message=f"Engine '{self.name}' is not installed in the current environment.",
            tested_invariants=[],
            details={"available": False},
        )
