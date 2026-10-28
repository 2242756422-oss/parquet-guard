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

        if not target_field_types:
            return EngineProbeResult(
                engine_name=self.name,
                engine_version=self.version(),
                operation=EngineProbeOperation.CAST,
                runtime_verdict=RuntimeVerdict.INCONCLUSIVE,
                duration_ms=0.0,
                error_category="EMPTY_TARGET_TYPES",
                error_message="No target field types provided for CAST probe.",
                tested_invariants=[],
                details={
                    "requested_cast_fields": [],
                    "attempted_cast_fields": [],
                    "successful_cast_fields": [],
                    "failed_or_skipped_cast_fields": [],
                },
            )

        requested_fields = list(target_field_types.keys())
        try:
            # Bounded streaming read via lazy frame scanning
            lf = pl.scan_parquet(file_path)
            if row_limit > 0:
                lf = lf.limit(row_limit)
            df = lf.collect()

            # Fail closed on missing source columns
            missing_fields = [c for c in requested_fields if c not in df.columns]
            if missing_fields:
                dur_ms = (time.perf_counter() - start_time) * 1000.0
                return EngineProbeResult(
                    engine_name=self.name,
                    engine_version=self.version(),
                    operation=EngineProbeOperation.CAST,
                    runtime_verdict=RuntimeVerdict.FAIL,
                    duration_ms=dur_ms,
                    error_category="MISSING_TARGET_COLUMNS",
                    error_message=f"Requested target column(s) absent from source Parquet file: {missing_fields}",
                    tested_invariants=invariants,
                    details={
                        "requested_cast_fields": requested_fields,
                        "attempted_cast_fields": [c for c in requested_fields if c in df.columns],
                        "successful_cast_fields": [],
                        "failed_or_skipped_cast_fields": missing_fields,
                    },
                )

            is_empty = len(df) == 0
            if is_empty:
                invariants = ["polars_strict_cast", "empty_dataset_schema_cast"]

            cast_exprs = []
            attempted_fields: List[str] = []
            for col, target_type in target_field_types.items():
                pl_type = self._map_to_polars_type(target_type)
                if pl_type is None:
                    dur_ms = (time.perf_counter() - start_time) * 1000.0
                    return EngineProbeResult(
                        engine_name=self.name,
                        engine_version=self.version(),
                        operation=EngineProbeOperation.CAST,
                        runtime_verdict=RuntimeVerdict.UNSUPPORTED,
                        duration_ms=dur_ms,
                        error_category="UNSUPPORTED_TYPE_MAPPING",
                        error_message=f"Target type '{target_type}' for column '{col}' cannot be mapped to Polars.",
                        tested_invariants=invariants,
                        details={
                            "requested_cast_fields": requested_fields,
                            "attempted_cast_fields": attempted_fields + [col],
                            "successful_cast_fields": attempted_fields,
                            "failed_or_skipped_cast_fields": [col],
                        },
                    )
                cast_exprs.append(pl.col(col).cast(pl_type, strict=True))
                attempted_fields.append(col)

            df = df.with_columns(cast_exprs)

            dur_ms = (time.perf_counter() - start_time) * 1000.0
            details_dict: Dict[str, Any] = {
                "requested_cast_fields": requested_fields,
                "attempted_cast_fields": requested_fields,
                "successful_cast_fields": requested_fields,
                "failed_or_skipped_cast_fields": [],
                "casted_columns_count": len(cast_exprs),
                "rows_sampled": len(df),
            }
            if is_empty:
                details_dict["empty_dataset"] = True
                details_dict["notice"] = (
                    "Empty dataset: validated schema-level castability without data values."
                )

            return EngineProbeResult(
                engine_name=self.name,
                engine_version=self.version(),
                operation=EngineProbeOperation.CAST,
                runtime_verdict=RuntimeVerdict.PASS,
                duration_ms=dur_ms,
                tested_invariants=invariants,
                details=details_dict,
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
                details={
                    "requested_cast_fields": requested_fields,
                    "attempted_cast_fields": requested_fields,
                    "successful_cast_fields": [],
                    "failed_or_skipped_cast_fields": requested_fields,
                },
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
                details={
                    "files_requested": len(file_paths),
                    "files_metadata_inspected": len(file_paths),
                    "files_row_probed": 0,
                    "files_skipped": len(file_paths),
                    "coverage_complete": False,
                    "coverage_limitations": ["Single file provided; schema union inconclusive."],
                },
            )

        row_limit = validate_row_limit(row_limit)
        start_time = time.perf_counter()
        invariants = ["polars_multi_file_union", "partition_schema_alignment"]
        files_row_probed = 0
        total_rows = 0
        try:
            # 1. Global multi-file scan
            lf = pl.scan_parquet(file_paths)
            if row_limit > 0:
                lf = lf.limit(row_limit)
            df = lf.collect()
            total_rows += len(df)

            # 2. Per-partition validation under unified scan
            per_file_lim = max(1, min(row_limit, 5))
            try:
                lf_parts = pl.scan_parquet(file_paths, include_file_paths="__file_path__")
                has_file_col = True
            except Exception:
                has_file_col = False

            for p in file_paths:
                if has_file_col:
                    p_df = lf_parts.filter(pl.col("__file_path__") == p).limit(per_file_lim).collect()
                else:
                    p_df = pl.scan_parquet(p).limit(per_file_lim).collect()
                files_row_probed += 1
                total_rows += len(p_df)

            dur_ms = (time.perf_counter() - start_time) * 1000.0
            coverage_complete = files_row_probed == len(file_paths)
            coverage_limitations = (
                []
                if coverage_complete
                else [f"Sampled row probing covered {files_row_probed}/{len(file_paths)} partition files."]
            )
            return EngineProbeResult(
                engine_name=self.name,
                engine_version=self.version(),
                operation=EngineProbeOperation.SCHEMA_UNION,
                runtime_verdict=RuntimeVerdict.PASS if coverage_complete else RuntimeVerdict.INCONCLUSIVE,
                duration_ms=dur_ms,
                tested_invariants=invariants,
                details={
                    "files_requested": len(file_paths),
                    "files_metadata_inspected": len(file_paths),
                    "files_row_probed": files_row_probed,
                    "files_skipped": len(file_paths) - files_row_probed,
                    "coverage_complete": coverage_complete,
                    "coverage_limitations": coverage_limitations,
                    "files_unified": len(file_paths),
                    "rows_sampled": total_rows,
                },
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
                details={
                    "files_requested": len(file_paths),
                    "files_metadata_inspected": len(file_paths),
                    "files_row_probed": files_row_probed,
                    "files_skipped": len(file_paths) - files_row_probed,
                    "coverage_complete": False,
                    "coverage_limitations": [f"Failure during union probing: {msg}"],
                    "files_count": len(file_paths),
                },
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
