"""PyArrow runtime verification engine adapter."""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from parquet_guard.verification.adapters import BaseEngineAdapter, validate_row_limit
from parquet_guard.verification.models import (
    EngineProbeOperation,
    EngineProbeResult,
    RuntimeVerdict,
)


class PyArrowEngineAdapter(BaseEngineAdapter):
    """Adapter for testing compatibility against the PyArrow C++ dataset and compute engine."""

    @property
    def name(self) -> str:
        return "pyarrow"

    def is_available(self) -> bool:
        return True

    def version(self) -> Optional[str]:
        return str(pa.__version__)

    def probe_read(
        self,
        file_paths: List[str],
        expected_columns: Optional[List[str]] = None,
        row_limit: int = 10,
    ) -> EngineProbeResult:
        if not file_paths:
            return EngineProbeResult(
                engine_name=self.name,
                engine_version=self.version(),
                operation=EngineProbeOperation.READ,
                runtime_verdict=RuntimeVerdict.ERROR,
                error_category="EMPTY_INPUT",
                error_message="No file paths provided to PyArrow probe_read.",
            )

        row_limit = validate_row_limit(row_limit)
        start_time = time.perf_counter()
        invariants = ["file_readable", "columns_projectable"]
        try:
            for p in file_paths:
                pf = pq.ParquetFile(p)
                if expected_columns:
                    for col in expected_columns:
                        parts = col.split(".")
                        curr = pf.schema_arrow
                        found = True
                        for part in parts:
                            if hasattr(curr, "get_field_index"):
                                idx = curr.get_field_index(part)
                                if idx == -1:
                                    found = False
                                    break
                                curr = curr.field(idx).type
                            else:
                                found = False
                                break
                        if not found:
                            raise KeyError(f"Expected column '{col}' missing from PyArrow table")
                # Bounded streaming read: do not load whole table into memory
                batches = []
                total_read = 0
                batch_size = max(1, row_limit) if row_limit > 0 else 10
                for batch in pf.iter_batches(batch_size=batch_size, columns=expected_columns):
                    batches.append(batch)
                    total_read += len(batch)
                    if row_limit > 0 and total_read >= row_limit:
                        break
                tbl = pa.Table.from_batches(batches) if batches else pf.schema_arrow.empty_table()
                if row_limit > 0 and len(tbl) > row_limit:
                    tbl = tbl.slice(0, row_limit)

            dur_ms = (time.perf_counter() - start_time) * 1000.0
            return EngineProbeResult(
                engine_name=self.name,
                engine_version=self.version(),
                operation=EngineProbeOperation.READ,
                runtime_verdict=RuntimeVerdict.PASS,
                duration_ms=dur_ms,
                tested_invariants=invariants,
                details={"files_tested": len(file_paths), "columns_tested": expected_columns},
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
        row_limit = validate_row_limit(row_limit)
        start_time = time.perf_counter()
        invariants = ["safe_cast_supported", "no_precision_loss"]

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
            pf = pq.ParquetFile(file_path)
            existing_cols = pf.schema_arrow.names

            # Fail closed on missing source columns (all or subset)
            missing_fields = [c for c in requested_fields if c not in existing_cols]
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
                        "attempted_cast_fields": [c for c in requested_fields if c in existing_cols],
                        "successful_cast_fields": [],
                        "failed_or_skipped_cast_fields": missing_fields,
                    },
                )

            batches = []
            total_read = 0
            batch_size = max(1, row_limit) if row_limit > 0 else 10
            for batch in pf.iter_batches(batch_size=batch_size, columns=requested_fields):
                batches.append(batch)
                total_read += len(batch)
                if row_limit > 0 and total_read >= row_limit:
                    break
            tbl = pa.Table.from_batches(batches) if batches else pf.schema_arrow.empty_table()
            if row_limit > 0 and len(tbl) > row_limit:
                tbl = tbl.slice(0, row_limit)

            is_empty = len(tbl) == 0
            if is_empty:
                invariants = ["safe_cast_supported", "empty_dataset_schema_cast"]

            casted_cols = {}
            successful_fields: List[str] = []
            for col_name, target_type_str in target_field_types.items():
                arrow_type = None
                try:
                    if hasattr(pa, "type_for_alias"):
                        arrow_type = pa.type_for_alias(target_type_str)
                    if arrow_type is None:
                        arrow_type = pa.schema([(col_name, target_type_str)]).field(0).type
                except Exception:
                    arrow_type = None

                if arrow_type is None:
                    dur_ms = (time.perf_counter() - start_time) * 1000.0
                    return EngineProbeResult(
                        engine_name=self.name,
                        engine_version=self.version(),
                        operation=EngineProbeOperation.CAST,
                        runtime_verdict=RuntimeVerdict.UNSUPPORTED,
                        duration_ms=dur_ms,
                        error_category="UNSUPPORTED_TYPE_MAPPING",
                        error_message=f"Target type '{target_type_str}' for column '{col_name}' cannot be mapped to PyArrow.",
                        tested_invariants=invariants,
                        details={
                            "requested_cast_fields": requested_fields,
                            "attempted_cast_fields": successful_fields + [col_name],
                            "successful_cast_fields": successful_fields,
                            "failed_or_skipped_cast_fields": [col_name],
                        },
                    )

                try:
                    col = tbl[col_name]
                    casted_col = col.cast(arrow_type, safe=True)
                    casted_cols[col_name] = str(casted_col.type)
                    successful_fields.append(col_name)
                except Exception as cast_exc:
                    dur_ms = (time.perf_counter() - start_time) * 1000.0
                    cat, msg = self.normalize_error(cast_exc)
                    return EngineProbeResult(
                        engine_name=self.name,
                        engine_version=self.version(),
                        operation=EngineProbeOperation.CAST,
                        runtime_verdict=RuntimeVerdict.FAIL,
                        duration_ms=dur_ms,
                        error_category=cat,
                        error_message=f"Failed casting column '{col_name}' to '{target_type_str}': {msg}",
                        tested_invariants=invariants,
                        details={
                            "requested_cast_fields": requested_fields,
                            "attempted_cast_fields": successful_fields + [col_name],
                            "successful_cast_fields": successful_fields,
                            "failed_or_skipped_cast_fields": [col_name],
                        },
                    )

            dur_ms = (time.perf_counter() - start_time) * 1000.0
            details_dict: Dict[str, Any] = {
                "requested_cast_fields": requested_fields,
                "attempted_cast_fields": requested_fields,
                "successful_cast_fields": successful_fields,
                "failed_or_skipped_cast_fields": [],
                "casted_columns": casted_cols,
                "rows_sampled": len(tbl),
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
                    "attempted_cast_fields": [],
                    "successful_cast_fields": [],
                    "failed_or_skipped_cast_fields": requested_fields,
                },
            )

    def probe_schema_union(
        self,
        file_paths: List[str],
        row_limit: int = 10,
    ) -> EngineProbeResult:
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
        invariants = ["dataset_schema_unification", "multi_partition_read"]
        files_row_probed = 0
        total_rows_sampled = 0
        try:
            # Test pyarrow dataset unification
            schemas = [pq.read_schema(p) for p in file_paths]
            unified_schema = pa.unify_schemas(schemas)
            _ = ds.dataset(file_paths, format="parquet", schema=unified_schema)

            # Sample bounded rows from each file's scanner to ensure multi-file data access
            per_file_limit = max(1, min(row_limit, 5))
            for p in file_paths:
                sub_ds = ds.dataset(p, format="parquet", schema=unified_schema)
                head_tbl = sub_ds.head(per_file_limit)
                files_row_probed += 1
                total_rows_sampled += len(head_tbl)

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
                    "unified_field_count": len(unified_schema.names),
                    "rows_sampled": total_rows_sampled,
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
