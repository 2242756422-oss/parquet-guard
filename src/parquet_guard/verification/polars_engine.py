"""Polars runtime verification engine adapter."""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from parquet_guard.verification.adapters import BaseEngineAdapter, validate_row_limit
from parquet_guard.verification.models import (
    EngineProbeOperation,
    EngineProbeResult,
    RuntimeVerdict,
)

try:
    import polars as pl

    _POLARS_AVAILABLE = True
except ImportError:
    pl = None  # type: ignore[assignment]
    _POLARS_AVAILABLE = False


class PolarsEngineAdapter(BaseEngineAdapter):
    """Adapter for verifying Polars query compatibility, schema alignment, and type casting."""

    @property
    def name(self) -> str:
        return "polars"

    def is_available(self) -> bool:
        return _POLARS_AVAILABLE

    def version(self) -> Optional[str]:
        if not _POLARS_AVAILABLE:
            return None
        return str(pl.__version__)

    def probe_read(
        self,
        file_paths: List[str],
        expected_columns: Optional[List[str]] = None,
        row_limit: int = 10,
    ) -> EngineProbeResult:
        if not self.is_available():
            return self.make_unsupported_result(EngineProbeOperation.READ)

        if not file_paths:
            return EngineProbeResult(
                engine_name=self.name,
                engine_version=self.version(),
                operation=EngineProbeOperation.READ,
                runtime_verdict=RuntimeVerdict.ERROR,
                error_category="EMPTY_INPUT",
                error_message="No file paths provided to Polars probe_read.",
            )

        row_limit = validate_row_limit(row_limit)
        start_time = time.perf_counter()
        invariants = ["polars_scan_parquet", "column_projection"]
        try:
            for p in file_paths:
                lf = pl.scan_parquet(p)
                if expected_columns:
                    select_exprs = []
                    for c in expected_columns:
                        if "." in c:
                            parts = c.split(".")
                            expr = pl.col(parts[0])
                            for sub in parts[1:]:
                                expr = expr.struct.field(sub)
                            select_exprs.append(expr)
                        else:
                            select_exprs.append(pl.col(c))
                    lf = lf.select(select_exprs)
                if row_limit > 0:
                    lf = lf.limit(row_limit)
                _ = lf.collect()

            dur_ms = (time.perf_counter() - start_time) * 1000.0
            return EngineProbeResult(
                engine_name=self.name,
                engine_version=self.version(),
                operation=EngineProbeOperation.READ,
                runtime_verdict=RuntimeVerdict.PASS,
                duration_ms=dur_ms,
                tested_invariants=invariants,
                details={"files_tested": len(file_paths), "columns": expected_columns},
            )
        except Exception as exc:
            dur_ms = (time.perf_counter() - start_time) * 1000.0
            cat, msg = self.normalize_error(exc)
            return EngineProbeResult(
                engine_name=self.name,
                engine_version=self.version(),
                operation=EngineProbeOperation.READ,
                runtime_verdict=RuntimeVerdict.FAIL,
                duration_ms=dur_ms,
                error_category=cat,
                error_message=msg,
                tested_invariants=invariants,
                details={"files_tested": len(file_paths)},
            )

    def probe_cast(
        self,
        file_path: str,
        target_field_types: Dict[str, str],
        row_limit: int = 10,
    ) -> EngineProbeResult:
        if not self.is_available():
            return self.make_unsupported_result(EngineProbeOperation.CAST)

        row_limit = validate_row_limit(row_limit)
        start_time = time.perf_counter()
        invariants = ["polars_strict_cast"]
        try:
            # Bounded streaming read via lazy frame scanning
            lf = pl.scan_parquet(file_path)
            if row_limit > 0:
                lf = lf.limit(row_limit)
            df = lf.collect()

            cast_exprs = []
            for col, target_type in target_field_types.items():
                if col in df.columns:
                    pl_type = self._map_to_polars_type(target_type)
                    if pl_type is None:
                        return EngineProbeResult(
                            engine_name=self.name,
                            engine_version=self.version(),
                            operation=EngineProbeOperation.CAST,
                            runtime_verdict=RuntimeVerdict.UNSUPPORTED,
                            error_category="UNSUPPORTED_TYPE_MAPPING",
                            error_message=f"Target type '{target_type}' for column '{col}' cannot be mapped to Polars.",
                            tested_invariants=invariants,
                        )
                    cast_exprs.append(pl.col(col).cast(pl_type, strict=True))

            if cast_exprs:
                df = df.with_columns(cast_exprs)

            dur_ms = (time.perf_counter() - start_time) * 1000.0
            return EngineProbeResult(
                engine_name=self.name,
                engine_version=self.version(),
                operation=EngineProbeOperation.CAST,
                runtime_verdict=RuntimeVerdict.PASS,
                duration_ms=dur_ms,
                tested_invariants=invariants,
                details={"casted_columns_count": len(cast_exprs)},
            )
        except Exception as exc:
            dur_ms = (time.perf_counter() - start_time) * 1000.0
            cat, msg = self.normalize_error(exc)
            return EngineProbeResult(
                engine_name=self.name,
                engine_version=self.version(),
                operation=EngineProbeOperation.CAST,
                runtime_verdict=RuntimeVerdict.FAIL,
                duration_ms=dur_ms,
                error_category=cat,
                error_message=msg,
                tested_invariants=invariants,
            )

    def probe_schema_union(
        self,
        file_paths: List[str],
        row_limit: int = 10,
    ) -> EngineProbeResult:
        if not self.is_available():
            return self.make_unsupported_result(EngineProbeOperation.SCHEMA_UNION)

        if len(file_paths) < 2:
            return EngineProbeResult(
                engine_name=self.name,
                engine_version=self.version(),
                operation=EngineProbeOperation.SCHEMA_UNION,
                runtime_verdict=RuntimeVerdict.INCONCLUSIVE,
                duration_ms=0.0,
                error_category="INSUFFICIENT_PARTITIONS",
                error_message="Multi-partition schema union requires at least 2 files; single file skipped.",
                tested_invariants=[],
                details={"notice": "Single file provided; schema union inconclusive."},
            )

        row_limit = validate_row_limit(row_limit)
        start_time = time.perf_counter()
        invariants = ["polars_multi_file_union", "partition_schema_alignment"]
        try:
            # Polars supports multi-file scan_parquet directly
            lf = pl.scan_parquet(file_paths)
            if row_limit > 0:
                lf = lf.limit(row_limit)
            df = lf.collect()

            dur_ms = (time.perf_counter() - start_time) * 1000.0
            return EngineProbeResult(
                engine_name=self.name,
                engine_version=self.version(),
                operation=EngineProbeOperation.SCHEMA_UNION,
                runtime_verdict=RuntimeVerdict.PASS,
                duration_ms=dur_ms,
                tested_invariants=invariants,
                details={"files_unified": len(file_paths), "rows_sampled": len(df)},
            )
        except Exception as exc:
            dur_ms = (time.perf_counter() - start_time) * 1000.0
            cat, msg = self.normalize_error(exc)
            return EngineProbeResult(
                engine_name=self.name,
                engine_version=self.version(),
                operation=EngineProbeOperation.SCHEMA_UNION,
                runtime_verdict=RuntimeVerdict.FAIL,
                duration_ms=dur_ms,
                error_category=cat,
                error_message=msg,
                tested_invariants=invariants,
                details={"files_count": len(file_paths)},
            )

    @staticmethod
    def _map_to_polars_type(arrow_type: str) -> Optional[Any]:
        """Map standard Arrow type identifiers to Polars data types."""
        t = arrow_type.strip().lower()
        mapping = {
            "int8": pl.Int8,
            "int16": pl.Int16,
            "int32": pl.Int32,
            "int64": pl.Int64,
            "uint8": pl.UInt8,
            "uint16": pl.UInt16,
            "uint32": pl.UInt32,
            "uint64": pl.UInt64,
            "float": pl.Float32,
            "float32": pl.Float32,
            "double": pl.Float64,
            "float64": pl.Float64,
            "string": pl.String,
            "large_string": pl.String,
            "utf8": pl.String,
            "large_utf8": pl.String,
            "bool": pl.Boolean,
            "boolean": pl.Boolean,
            "date": pl.Date,
            "date32": pl.Date,
        }
        if t.startswith("decimal"):
            import re

            m = re.search(r"decimal(?:128|256)?\((\d+),\s*(-?\d+)\)", t)
            if m:
                return pl.Decimal(int(m.group(1)), int(m.group(2)))
            return pl.Decimal(38, 9)

        return mapping.get(t, None)
