"""DuckDB runtime verification engine adapter."""

from __future__ import annotations

import time
from typing import Dict, List, Optional

from parquet_guard.verification.adapters import (
    BaseEngineAdapter,
    quote_column_path,
    quote_sql_identifier,
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

        start_time = time.perf_counter()
        invariants = ["duckdb_cast_conversion"]
        executed_sql = None
        con = None
        try:
            con = duckdb.connect(":memory:")
            # Inspect existing columns
            desc = con.execute("DESCRIBE SELECT * FROM read_parquet(?) LIMIT 1", [file_path]).fetchall()
            existing_cols = {row[0] for row in desc}

            cast_exprs = []
            for col, target_type in target_field_types.items():
                if col in existing_cols:
                    # Map standard Arrow type strings to DuckDB types where needed
                    ddb_type = self._map_to_duckdb_type(target_type)
                    cast_exprs.append(
                        f"TRY_CAST({quote_sql_identifier(col)} AS {ddb_type}) AS {quote_sql_identifier(col)}"
                    )

            if not cast_exprs:
                select_clause = "*"
            else:
                select_clause = ", ".join(cast_exprs)

            executed_sql = f"SELECT {select_clause} FROM read_parquet(?) LIMIT {int(row_limit)}"
            res = con.execute(executed_sql, [file_path]).fetchall()

            dur_ms = (time.perf_counter() - start_time) * 1000.0
            return EngineProbeResult(
                engine_name=self.name,
                engine_version=self.version(),
                operation=EngineProbeOperation.CAST,
                runtime_verdict=RuntimeVerdict.PASS,
                duration_ms=dur_ms,
                executed_expression=executed_sql,
                tested_invariants=invariants,
                details={"casted_row_count": len(res)},
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
                runtime_verdict=RuntimeVerdict.PASS,
                duration_ms=0.0,
                tested_invariants=["schema_union"],
                details={"notice": "Trivially unified single file."},
            )

        start_time = time.perf_counter()
        invariants = ["duckdb_union_by_name", "partition_schema_reconciliation"]
        executed_sql = None
        con = None
        try:
            con = duckdb.connect(":memory:")
            # DuckDB read_parquet with union_by_name=True
            executed_sql = f"SELECT * FROM read_parquet(?, union_by_name=true) LIMIT {int(row_limit)}"
            res = con.execute(executed_sql, [file_paths]).fetchall()

            dur_ms = (time.perf_counter() - start_time) * 1000.0
            return EngineProbeResult(
                engine_name=self.name,
                engine_version=self.version(),
                operation=EngineProbeOperation.SCHEMA_UNION,
                runtime_verdict=RuntimeVerdict.PASS,
                duration_ms=dur_ms,
                executed_expression=executed_sql,
                tested_invariants=invariants,
                details={"files_unified": len(file_paths), "rows_sampled": len(res)},
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
                details={"files_count": len(file_paths)},
            )
        finally:
            if con is not None:
                con.close()

    @staticmethod
    def _map_to_duckdb_type(arrow_type: str) -> str:
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
        return "VARCHAR"
