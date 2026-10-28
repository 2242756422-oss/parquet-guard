"""PyArrow runtime verification engine adapter."""

from __future__ import annotations

import time
from typing import Dict, List, Optional

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
        try:
            pf = pq.ParquetFile(file_path)
            target_cols = [c for c in target_field_types if c in pf.schema_arrow.names]
            batches = []
            total_read = 0
            batch_size = max(1, row_limit) if row_limit > 0 else 10
            for batch in pf.iter_batches(batch_size=batch_size, columns=target_cols or None):
                batches.append(batch)
                total_read += len(batch)
                if row_limit > 0 and total_read >= row_limit:
                    break
            tbl = pa.Table.from_batches(batches) if batches else pf.schema_arrow.empty_table()
            if row_limit > 0 and len(tbl) > row_limit:
                tbl = tbl.slice(0, row_limit)

            casted_cols = {}
            for col_name, target_type_str in target_field_types.items():
                if col_name not in tbl.column_names:
                    continue
                arrow_type = None
                try:
                    if hasattr(pa, "type_for_alias"):
                        arrow_type = pa.type_for_alias(target_type_str)
                    if arrow_type is None:
                        arrow_type = pa.schema([(col_name, target_type_str)]).field(0).type
                except Exception:
                    arrow_type = None

                if arrow_type is None:
                    return EngineProbeResult(
                        engine_name=self.name,
                        engine_version=self.version(),
                        operation=EngineProbeOperation.CAST,
                        runtime_verdict=RuntimeVerdict.UNSUPPORTED,
                        error_category="UNSUPPORTED_TYPE_MAPPING",
                        error_message=f"Target type '{target_type_str}' for column '{col_name}' cannot be mapped to PyArrow.",
                        tested_invariants=invariants,
                    )

                col = tbl[col_name]
                casted_col = col.cast(arrow_type, safe=True)
                casted_cols[col_name] = str(casted_col.type)

            dur_ms = (time.perf_counter() - start_time) * 1000.0
            return EngineProbeResult(
                engine_name=self.name,
                engine_version=self.version(),
                operation=EngineProbeOperation.CAST,
                runtime_verdict=RuntimeVerdict.PASS,
                duration_ms=dur_ms,
                tested_invariants=invariants,
                details={"casted_columns": casted_cols},
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
                details={"target_field_types": target_field_types},
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
                details={"notice": "Single file provided; schema union inconclusive."},
            )

        row_limit = validate_row_limit(row_limit)
        start_time = time.perf_counter()
        invariants = ["dataset_schema_unification", "multi_partition_read"]
        try:
            # Test pyarrow dataset unification
            schemas = [pq.read_schema(p) for p in file_paths]
            unified_schema = pa.unify_schemas(schemas)
            _ = ds.dataset(file_paths, format="parquet", schema=unified_schema)
            # Sample bounded rows from each file's scanner to ensure multi-file data access
            for p in file_paths:
                sub_ds = ds.dataset(p, format="parquet", schema=unified_schema)
                _ = sub_ds.head(max(1, min(row_limit, 5)))

            dur_ms = (time.perf_counter() - start_time) * 1000.0
            return EngineProbeResult(
                engine_name=self.name,
                engine_version=self.version(),
                operation=EngineProbeOperation.SCHEMA_UNION,
                runtime_verdict=RuntimeVerdict.PASS,
                duration_ms=dur_ms,
                tested_invariants=invariants,
                details={
                    "files_unified": len(file_paths),
                    "unified_field_count": len(unified_schema.names),
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
                details={"files_count": len(file_paths)},
            )
