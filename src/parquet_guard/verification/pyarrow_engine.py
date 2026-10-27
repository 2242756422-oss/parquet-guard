"""PyArrow runtime verification engine adapter."""

from __future__ import annotations

import time
from typing import Dict, List, Optional

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from parquet_guard.verification.adapters import BaseEngineAdapter
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

        start_time = time.perf_counter()
        invariants = ["file_readable", "columns_projectable"]
        try:
            for p in file_paths:
                tbl = pq.read_table(p, columns=expected_columns)
                if expected_columns:
                    for col in expected_columns:
                        if col not in tbl.column_names:
                            raise KeyError(f"Expected column '{col}' missing from PyArrow table")
                if row_limit > 0:
                    _ = tbl.slice(0, row_limit)

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
        start_time = time.perf_counter()
        invariants = ["safe_cast_supported", "no_precision_loss"]
        try:
            tbl = pq.read_table(file_path)
            if row_limit > 0:
                tbl = tbl.slice(0, row_limit)

            casted_cols = {}
            for col_name, target_type_str in target_field_types.items():
                if col_name not in tbl.column_names:
                    continue
                arrow_type = pa.type_for_alias(target_type_str) if hasattr(pa, "type_for_alias") else None
                # Fallback parser for standard string types
                if arrow_type is None:
                    # Let PyArrow schema parser interpret or use schema parsing
                    arrow_type = pa.schema([(col_name, target_type_str)]).field(0).type

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
                runtime_verdict=RuntimeVerdict.PASS,
                duration_ms=0.0,
                tested_invariants=["schema_union"],
                details={"notice": "Trivially unified single file."},
            )

        start_time = time.perf_counter()
        invariants = ["dataset_schema_unification", "multi_partition_read"]
        try:
            # Test pyarrow dataset unification
            schemas = [pq.read_schema(p) for p in file_paths]
            unified_schema = pa.unify_schemas(schemas)
            dataset = ds.dataset(file_paths, format="parquet", schema=unified_schema)
            if row_limit > 0:
                _ = dataset.head(row_limit)

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
