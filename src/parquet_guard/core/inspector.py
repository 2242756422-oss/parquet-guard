"""Zero-data-scan Parquet metadata inspector and contract extractor."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pyarrow as pa
import pyarrow.parquet as pq

from parquet_guard.core.models import FieldDefinition, SchemaContract


class ParquetReadError(Exception):
    """Raised when a Parquet file cannot be read or metadata footer is corrupted."""

    pass


class ContractValidationError(ParquetReadError):
    """Raised when a schema contract fails structural or schema validation."""

    pass


def _decode_metadata_bytes(data: Union[bytes, str]) -> str:
    """Decode raw metadata bytes using surrogateescape to ensure 100% reversible, bijective mapping."""
    if isinstance(data, str):
        return data
    return data.decode("utf-8", errors="surrogateescape")


def arrow_field_to_definition(field: pa.Field) -> FieldDefinition:
    """Convert a PyArrow Field into a FieldDefinition, recursively preserving nested children and parameters."""
    meta: Dict[str, str] = {}
    if field.metadata:
        for k, v in field.metadata.items():
            k_str = _decode_metadata_bytes(k)
            v_str = _decode_metadata_bytes(v)
            meta[k_str] = v_str

    pa_type = field.type
    children: List[FieldDefinition] = []
    list_size: Optional[int] = None
    ordered: Optional[bool] = None
    keys_sorted: Optional[bool] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    time_unit: Optional[str] = None
    timezone: Optional[str] = None
    index_type: Optional[str] = None
    value_type: Optional[str] = None

    # Inspect nested and parameterized types
    if pa.types.is_struct(pa_type):
        for i in range(pa_type.num_fields):
            children.append(arrow_field_to_definition(pa_type.field(i)))
    elif pa.types.is_fixed_size_list(pa_type):
        list_size = pa_type.list_size
        if hasattr(pa_type, "value_field") and pa_type.value_field is not None:
            children.append(arrow_field_to_definition(pa_type.value_field))
        elif hasattr(pa_type, "value_type") and pa_type.value_type is not None:
            children.append(FieldDefinition(name="item", type=str(pa_type.value_type), nullable=True))
    elif pa.types.is_list(pa_type) or pa.types.is_large_list(pa_type):
        if hasattr(pa_type, "value_field") and pa_type.value_field is not None:
            children.append(arrow_field_to_definition(pa_type.value_field))
        elif hasattr(pa_type, "value_type") and pa_type.value_type is not None:
            children.append(FieldDefinition(name="item", type=str(pa_type.value_type), nullable=True))
    elif pa.types.is_map(pa_type):
        keys_sorted = bool(pa_type.keys_sorted)
        if hasattr(pa_type, "key_field") and pa_type.key_field is not None:
            children.append(arrow_field_to_definition(pa_type.key_field))
        if hasattr(pa_type, "item_field") and pa_type.item_field is not None:
            children.append(arrow_field_to_definition(pa_type.item_field))
    elif pa.types.is_dictionary(pa_type):
        ordered = bool(pa_type.ordered)
        index_type = str(pa_type.index_type)
        value_type = str(pa_type.value_type)
        children.append(FieldDefinition(name="index", type=index_type, nullable=False))
        children.append(FieldDefinition(name="value", type=value_type, nullable=True))
    elif pa.types.is_decimal(pa_type):
        precision = int(pa_type.precision)
        scale = int(pa_type.scale)
    elif pa.types.is_timestamp(pa_type):
        time_unit = str(pa_type.unit)
        if pa_type.tz is not None:
            timezone = str(pa_type.tz)
    elif pa.types.is_duration(pa_type):
        time_unit = str(pa_type.unit)

    return FieldDefinition(
        name=field.name,
        type=str(field.type),
        nullable=field.nullable,
        metadata=meta,
        children=children,
        list_size=list_size,
        ordered=ordered,
        keys_sorted=keys_sorted,
        precision=precision,
        scale=scale,
        time_unit=time_unit,
        timezone=timezone,
        index_type=index_type,
        value_type=value_type,
    )


def inspect_schema(file_path: Union[str, Path]) -> SchemaContract:
    """Inspect and extract canonical schema from a Parquet file footer without loading row data.

    Args:
        file_path: Path to the Parquet file.

    Returns:
        SchemaContract representing all columns, types, nullabilities, and metadata.

    Raises:
        ParquetReadError: If the file is not found, unreadable, or invalid Parquet.
    """
    path = Path(file_path)
    if not path.is_file():
        raise ParquetReadError(f"Target file does not exist or is not a file: {file_path}")

    try:
        parquet_file = pq.ParquetFile(str(path))
        schema = parquet_file.schema_arrow
        fields = [arrow_field_to_definition(field) for field in schema]

        top_meta: Dict[str, str] = {}
        if schema.metadata:
            for k, v in schema.metadata.items():
                k_str = _decode_metadata_bytes(k)
                v_str = _decode_metadata_bytes(v)
                top_meta[k_str] = v_str

        return SchemaContract(
            version="1.0",
            name=path.stem,
            fields=fields,
            metadata=top_meta,
        )

    except Exception as exc:
        if isinstance(exc, ParquetReadError):
            raise
        raise ParquetReadError(f"Failed to inspect Parquet metadata for {file_path}: {exc}") from exc


def extract_file_metadata(file_path: Union[str, Path]) -> Dict[str, Any]:
    """Extract physical layout and performance statistics from Parquet metadata footer.

    Args:
        file_path: Path to the Parquet file.

    Returns:
        Dictionary containing row counts, row groups, compression codecs, and sizes.
    """
    path = Path(file_path)
    if not path.is_file():
        raise ParquetReadError(f"File not found: {file_path}")

    try:
        pf = pq.ParquetFile(str(path))
        meta = pf.metadata

        codecs = set()
        for rg_idx in range(meta.num_row_groups):
            rg = meta.row_group(rg_idx)
            for c_idx in range(rg.num_columns):
                codecs.add(rg.column(c_idx).compression)

        return {
            "file_name": path.name,
            "file_size_bytes": path.stat().st_size,
            "num_rows": meta.num_rows,
            "num_columns": meta.num_columns,
            "num_row_groups": meta.num_row_groups,
            "created_by": meta.created_by,
            "format_version": meta.format_version,
            "compression_codecs": sorted(list(codecs)),
            "serialized_size_bytes": meta.serialized_size,
        }
    except Exception as exc:
        raise ParquetReadError(f"Failed to extract metadata from {file_path}: {exc}") from exc


def extract_contract(file_path: Union[str, Path], name: Optional[str] = None) -> SchemaContract:
    """Extract a golden SchemaContract instance from a target Parquet file."""
    contract = inspect_schema(file_path)
    if name:
        contract.name = name
    return contract


def save_contract(contract: SchemaContract, output_path: Union[str, Path]) -> None:
    """Save SchemaContract to JSON file."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(contract.model_dump(exclude_none=True), f, indent=2, ensure_ascii=True)


def load_contract(contract_path: Union[str, Path]) -> SchemaContract:
    """Load and strictly validate SchemaContract from JSON file."""
    path = Path(contract_path)
    if not path.is_file():
        raise ParquetReadError(f"Contract file does not exist: {contract_path}")

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as exc:
        raise ContractValidationError(f"Invalid JSON syntax in contract file {contract_path}: {exc}") from exc
    except Exception as exc:
        raise ParquetReadError(f"Failed to read contract at {contract_path}: {exc}") from exc

    try:
        contract = SchemaContract.model_validate(data)
    except Exception as exc:
        raise ContractValidationError(
            f"Contract schema validation failed for {contract_path}: {exc}"
        ) from exc

    if contract.has_duplicate_fields():
        raise ContractValidationError(
            f"Contract contains duplicate column path(s): {contract.duplicate_field_names()}"
        )
    return contract


def validate_contract(contract_path: Union[str, Path]) -> Tuple[bool, Optional[str]]:
    """Validate a contract JSON file against SchemaContract specifications."""
    try:
        load_contract(contract_path)
        return True, None
    except Exception as exc:
        return False, str(exc)
