"""DuckDB runtime verification engine adapter."""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Tuple

from parquet_guard.verification.adapters import (
    BaseEngineAdapter,
    quote_column_path,
    quote_sql_identifier,
    validate_row_limit,
)
from parquet_guard.verification.models import (
    EngineProbeOperation,
    EngineProbeResult,
    RuntimeVerdict,
)

try:
    import duckdb

    _DUCKDB_AVAILABLE = True
except ImportError:
    duckdb = None  # type: ignore[assignment]
    _DUCKDB_AVAILABLE = False


class DuckDBEngineAdapter(BaseEngineAdapter):
    """Adapter for verifying DuckDB query compatibility, partition union, and type casting."""

    @property
    def name(self) -> str:
        return "duckdb"

    def is_available(self) -> bool:
        return _DUCKDB_AVAILABLE

    def version(self) -> Optional[str]:
        if not _DUCKDB_AVAILABLE:
            return None
        return str(duckdb.__version__)

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
                error_message="No file paths provided to DuckDB probe_read.",
            )

        row_limit = validate_row_limit(row_limit)
        start_time = time.perf_counter()
        invariants = ["duckdb_read_parquet", "column_projection"]
        executed_sql = None
        con = None
        try:
            con = duckdb.connect(":memory:")
            cols_clause = "*"
            if expected_columns:
                cols_clause = ", ".join(quote_column_path(c) for c in expected_columns)

            for p in file_paths:
                # Use parameterized query for file path
                executed_sql = f"SELECT {cols_clause} FROM read_parquet(?) LIMIT {int(row_limit)}"
                rel = con.execute(executed_sql, [p])
                _ = rel.fetchall()

            dur_ms = (time.perf_counter() - start_time) * 1000.0
            return EngineProbeResult(
                engine_name=self.name,
                engine_version=self.version(),
                operation=EngineProbeOperation.READ,
                runtime_verdict=RuntimeVerdict.PASS,
                duration_ms=dur_ms,
                executed_expression=executed_sql,
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
                executed_expression=executed_sql,
                tested_invariants=invariants,
                details={"files_tested": len(file_paths)},
            )
        finally:
            if con is not None:
                con.close()

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
        invariants = ["duckdb_cast_conversion"]
        executed_sql = None
        con = None

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
            con = duckdb.connect(":memory:")
            # Inspect existing columns
            desc = con.execute("DESCRIBE SELECT * FROM read_parquet(?) LIMIT 1", [file_path]).fetchall()
            existing_cols = {row[0] for row in desc}

            # Fail closed on missing source columns
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

            cast_exprs = []
            null_check_exprs = []
            mapped_cols: List[Tuple[str, str]] = []
            for col, target_type in target_field_types.items():
                ddb_type = self._map_to_duckdb_type(target_type)
                if ddb_type is None:
                    dur_ms = (time.perf_counter() - start_time) * 1000.0
                    return EngineProbeResult(
                        engine_name=self.name,
                        engine_version=self.version(),
                        operation=EngineProbeOperation.CAST,
                        runtime_verdict=RuntimeVerdict.UNSUPPORTED,
                        duration_ms=dur_ms,
                        error_category="UNSUPPORTED_TYPE_MAPPING",
                        error_message=f"Target type '{target_type}' for column '{col}' cannot be mapped to DuckDB.",
                        tested_invariants=invariants,
                        details={
                            "requested_cast_fields": requested_fields,
                            "attempted_cast_fields": [c for c, _ in mapped_cols] + [col],
                            "successful_cast_fields": [c for c, _ in mapped_cols],
                            "failed_or_skipped_cast_fields": [col],
                        },
                    )
                q_col = quote_sql_identifier(col)
                cast_exprs.append(f"CAST({q_col} AS {ddb_type}) AS {q_col}")
                null_check_exprs.append(
                    f"COUNT({q_col}) AS {quote_sql_identifier(col + '__orig_nn')}, "
                    f"COUNT(TRY_CAST({q_col} AS {ddb_type})) AS {quote_sql_identifier(col + '__cast_nn')}"
                )
                mapped_cols.append((col, ddb_type))

            # 1. Null-injection check: ensure TRY_CAST does not silently turn valid values to NULL
            null_check_sql = f"SELECT {', '.join(null_check_exprs)} FROM (SELECT * FROM read_parquet(?) LIMIT {int(row_limit)}) sub"
            row_check = con.execute(null_check_sql, [file_path]).fetchone()
            if row_check:
                for i, (col, ddb_type) in enumerate(mapped_cols):
                    orig_nn = row_check[i * 2]
                    cast_nn = row_check[i * 2 + 1]
                    if orig_nn is not None and cast_nn is not None and cast_nn < orig_nn:
                        dur_ms = (time.perf_counter() - start_time) * 1000.0
                        return EngineProbeResult(
                            engine_name=self.name,
                            engine_version=self.version(),
                            operation=EngineProbeOperation.CAST,
                            runtime_verdict=RuntimeVerdict.FAIL,
                            duration_ms=dur_ms,
                            error_category="NULL_INJECTION",
                            error_message=(
                                f"Cast on column '{col}' to '{ddb_type}' resulted in null injection: "
                                f"{orig_nn - cast_nn} non-null values failed conversion."
                            ),
                            executed_expression=null_check_sql,
                            tested_invariants=invariants,
                            details={
                                "requested_cast_fields": requested_fields,
                                "attempted_cast_fields": requested_fields,
                                "successful_cast_fields": [c for c, _ in mapped_cols[:i]],
                                "failed_or_skipped_cast_fields": [col],
                            },
                        )

            # 2. Strict CAST evaluation
            select_clause = ", ".join(cast_exprs)
            executed_sql = f"SELECT {select_clause} FROM read_parquet(?) LIMIT {int(row_limit)}"
            res = con.execute(executed_sql, [file_path]).fetchall()

            dur_ms = (time.perf_counter() - start_time) * 1000.0
            is_empty = len(res) == 0
            if is_empty:
                invariants = ["duckdb_cast_conversion", "empty_dataset_schema_cast"]

            details_dict: Dict[str, Any] = {
                "requested_cast_fields": requested_fields,
                "attempted_cast_fields": requested_fields,
                "successful_cast_fields": requested_fields,
                "failed_or_skipped_cast_fields": [],
                "casted_row_count": len(res),
                "rows_sampled": len(res),
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
                executed_expression=executed_sql,
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
                executed_expression=executed_sql,
                tested_invariants=invariants,
                details={
                    "requested_cast_fields": requested_fields,
                    "attempted_cast_fields": requested_fields,
                    "successful_cast_fields": [],
                    "failed_or_skipped_cast_fields": requested_fields,
                },
            )
        finally:
            if con is not None:
                con.close()

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
        invariants = ["duckdb_union_by_name", "partition_schema_reconciliation"]
        executed_sql = None
        con = None
        files_row_probed = 0
        total_rows = 0
        try:
            con = duckdb.connect(":memory:")
            # 1. DuckDB multi-file read_parquet with union_by_name=True
            executed_sql = f"SELECT * FROM read_parquet(?, union_by_name=true) LIMIT {int(row_limit)}"
            res = con.execute(executed_sql, [file_paths]).fetchall()
            total_rows += len(res)

            # 2. Per-partition validation under union schema to ensure every partition can be row-probed
            per_file_lim = max(1, min(row_limit, 5))
            part_sql = f"SELECT * FROM read_parquet(?, union_by_name=true, filename=true) WHERE filename = ? LIMIT {per_file_lim}"
            for p in file_paths:
                part_rows = con.execute(part_sql, [file_paths, p]).fetchall()
                files_row_probed += 1
                total_rows += len(part_rows)

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
                executed_expression=executed_sql,
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
                executed_expression=executed_sql,
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
        finally:
            if con is not None:
                con.close()

    @staticmethod
    def _map_to_duckdb_type(arrow_type: str) -> Optional[str]:
        """Map standard Arrow type identifiers to DuckDB SQL types."""
        t = arrow_type.strip().lower()
        if t in ("int8", "int16", "int32", "int64"):
            return t.upper()
        if t in ("uint8", "uint16", "uint32", "uint64"):
            return t.upper()
        if t in ("float", "float32"):
            return "FLOAT"
        if t in ("double", "float64"):
            return "DOUBLE"
        if t in ("string", "large_string", "utf8", "large_utf8"):
            return "VARCHAR"
        if t in ("bool", "boolean"):
            return "BOOLEAN"
        if t.startswith("decimal"):
            import re

            m = re.search(r"decimal(?:128|256)?\((\d+),\s*(-?\d+)\)", t)
            if m:
                return f"DECIMAL({m.group(1)}, {m.group(2)})"
            return "DECIMAL"
        if t.startswith("timestamp"):
            return "TIMESTAMP"
        if t.startswith("date"):
            return "DATE"
        return None
