"""Semantic compatibility algebra engine for Parquet/Arrow schemas."""

from __future__ import annotations

import re
from typing import Dict, List, Optional, Set, Tuple

from parquet_guard.core.models import (
    ChangeType,
    DiffReport,
    ErrorCode,
    FieldDefinition,
    FieldDiff,
    ResultState,
    SchemaContract,
    Severity,
)

# Known safe unidirectional type widening transitions in Arrow / Parquet
# old_type -> set of compatible new_types
SAFE_WIDENINGS: Dict[str, Set[str]] = {
    # Signed integers
    "int8": {"int16", "int32", "int64", "float32", "float64", "double"},
    "int16": {"int32", "int64", "float32", "float64", "double"},
    "int32": {"int64", "float64", "double"},
    # Unsigned integers
    "uint8": {"uint16", "uint32", "uint64", "int16", "int32", "int64"},
    "uint16": {"uint32", "uint64", "int32", "int64"},
    "uint32": {"uint64", "int64"},
    # Floating point
    "float": {"double", "float64"},
    "float32": {"float64", "double"},
    "halffloat": {"float", "float32", "double", "float64"},
    # Strings and binaries
    "string": {"large_string"},
    "utf8": {"large_utf8", "large_string", "string"},
    "binary": {"large_binary"},
    # Dates & Timestamps
    "date32": {"date64"},
    # Time
    "time32[s]": {"time32[ms]", "time64[us]", "time64[ns]"},
    "time32[ms]": {"time64[us]", "time64[ns]"},
    "time64[us]": {"time64[ns]"},
}


def _normalize_type_str(type_str: str) -> str:
    """Normalize Arrow type string for canonical comparison."""
    t = type_str.strip().lower()
    t = t.replace("double", "float64")
    t = t.replace("utf8", "string")
    return t


def _parse_timestamp_params(type_str: str) -> Optional[Tuple[str, Optional[str]]]:
    """Parse timestamp parameters (unit, timezone) from type string."""
    m = re.match(r"^timestamp\[(s|ms|us|ns)(?:,\s*tz=([^\]]+))?\]$", type_str.strip(), re.IGNORECASE)
    if m:
        unit = m.group(1).lower()
        tz = m.group(2) if m.group(2) else None
        return unit, tz
    return None


def _parse_decimal_params(type_str: str) -> Optional[Tuple[str, int, int]]:
    """Parse decimal parameters (decimal128/256, precision, scale)."""
    m = re.match(r"^(decimal128|decimal256)\((\d+),\s*(-?\d+)\)$", type_str.strip(), re.IGNORECASE)
    if m:
        dec_type = m.group(1).lower()
        precision = int(m.group(2))
        scale = int(m.group(3))
        return dec_type, precision, scale
    return None


def _parse_list_inner(type_str: str) -> Optional[str]:
    """Extract inner element type from list<item: T>, large_list<T>, or fixed_size_list<T>[N]."""
    m = re.match(
        r"^(?:large_list|list|fixed_size_list)<(?:item:\s*)?(.+)>(?:\[\d+\])?$",
        type_str.strip(),
        re.IGNORECASE,
    )
    if m:
        return m.group(1).strip()
    return None


def _parse_map_params(type_str: str) -> Optional[Tuple[str, str]]:
    """Extract key and value types from map<K, V> or map<K, V, keys_sorted>."""
    t = type_str.strip()
    if not t.lower().startswith("map<") or not t.endswith(">"):
        return None

    inner = t[4:-1].strip()
    # Strip optional trailing , keys_sorted flag
    m_sorted = re.search(r",\s*keys_sorted$", inner, re.IGNORECASE)
    if m_sorted:
        inner = inner[: m_sorted.start()].strip()

    # Split on first comma not enclosed in nested brackets
    depth = 0
    comma_pos = -1
    for i, ch in enumerate(inner):
        if ch in "<([":
            depth += 1
        elif ch in ">)]":
            depth -= 1
        elif ch == "," and depth == 0:
            comma_pos = i
            break

    if comma_pos == -1:
        return None

    return inner[:comma_pos].strip(), inner[comma_pos + 1 :].strip()


def is_safe_widening(old_type: str, new_type: str) -> bool:
    """Return True if transitioning from old_type to new_type is safely lossless."""
    norm_old = _normalize_type_str(old_type)
    norm_new = _normalize_type_str(new_type)

    if norm_old == norm_new:
        return True

    # 1. Direct dictionary lookups
    allowed = SAFE_WIDENINGS.get(norm_old, set())
    if norm_new in allowed or any(_normalize_type_str(a) == norm_new for a in allowed):
        return True

    # 2. Timestamp compatibility:
    # safe precision widening: s -> ms -> us -> ns (preserving without truncation)
    ts_old = _parse_timestamp_params(old_type)
    ts_new = _parse_timestamp_params(new_type)
    if ts_old and ts_new:
        units = ["s", "ms", "us", "ns"]
        u_old, tz_old = ts_old
        u_new, tz_new = ts_new
        if tz_old == tz_new and u_old in units and u_new in units:
            if units.index(u_new) >= units.index(u_old):
                return True
        return False

    # 3. Decimal compatibility
    dec_old = _parse_decimal_params(old_type)
    dec_new = _parse_decimal_params(new_type)
    if dec_old and dec_new:
        t_old, p_old, s_old = dec_old
        t_new, p_new, s_new = dec_new
        # In physical Parquet storage, scale changes alter the numeric interpretation of stored values.
        # Scale must be strictly identical, while precision may be safely widened.
        if s_old == s_new and p_new >= p_old:
            if t_old == t_new or (t_old == "decimal128" and t_new == "decimal256"):
                return True
        return False

    # 4. List element compatibility (recursive)
    list_old = _parse_list_inner(old_type)
    list_new = _parse_list_inner(new_type)
    if list_old and list_new:
        return is_safe_widening(list_old, list_new)

    # 5. Map compatibility (key must match exactly, value may widen)
    map_old = _parse_map_params(old_type)
    map_new = _parse_map_params(new_type)
    if map_old and map_new:
        k_old, v_old = map_old
        k_new, v_new = map_new
        if _normalize_type_str(k_old) == _normalize_type_str(k_new):
            return is_safe_widening(v_old, v_new)
        return False

    return False


CONTAINER_KINDS = {"struct", "list", "large_list", "fixed_size_list", "map", "dictionary"}


def get_container_kind(type_str: str) -> Optional[str]:
    """Identify container constructor kind if type_str is a complex Arrow container."""
    norm = type_str.strip().lower()
    if norm.startswith("struct"):
        return "struct"
    if norm.startswith("large_list"):
        return "large_list"
    if norm.startswith("fixed_size_list"):
        return "fixed_size_list"
    if norm.startswith("list"):
        return "list"
    if norm.startswith("map"):
        return "map"
    if norm.startswith("dictionary"):
        return "dictionary"
    return None


def analyze_compatibility(
    old_field: FieldDefinition,
    new_field: FieldDefinition,
    mode: str = "permissive",
    check_metadata: bool = True,
    check_field_order: bool = False,
    field_path: str = "",
) -> List[FieldDiff]:
    """Analyze discrepancies between two field definitions and return identified diffs.

    Args:
        old_field: Baseline field specification.
        new_field: Target field specification.
        mode: 'permissive' (permits safe widening) or 'strict' (forbids any type/nullability change).
        check_metadata: Whether to check field metadata changes.
        check_field_order: Whether to enforce/report child field order in nested containers.
        field_path: Hierarchical field path for nested reporting (e.g. 'user.address.zip').
    """
    diffs: List[FieldDiff] = []
    curr_path = field_path if field_path else old_field.name

    old_container = get_container_kind(old_field.type)
    new_container = get_container_kind(new_field.type)

    # 1. Type & Container constructor check
    if old_container is not None or new_container is not None:
        if old_container is None or new_container is None:
            # Transition between container and scalar type
            diffs.append(
                FieldDiff(
                    field_name=curr_path,
                    change_type=ChangeType.TYPE_MISMATCH,
                    severity=Severity.BREAKING,
                    old_value=old_field.type,
                    new_value=new_field.type,
                    description=(
                        f"Field '{curr_path}' incompatible transition between container "
                        f"'{old_container or old_field.type}' and scalar '{new_container or new_field.type}'."
                    ),
                    recommendation="Cannot convert between structured container and primitive scalar type.",
                )
            )
        else:
            # Both are containers. Check container constructor compatibility
            if old_container == "struct" and new_container == "struct":
                # Both are structs. Container constructor matches.
                # Inner field changes are evaluated structurally in the children section.
                pass
            elif old_container in ("list", "large_list", "fixed_size_list") and new_container in (
                "list",
                "large_list",
                "fixed_size_list",
            ):
                if old_container == "fixed_size_list" and new_container == "fixed_size_list":
                    len_old = old_field.list_size
                    len_new = new_field.list_size
                    if (len_old is not None and len_new is not None and len_old != len_new) or (
                        _normalize_type_str(old_field.type) != _normalize_type_str(new_field.type)
                    ):
                        diffs.append(
                            FieldDiff(
                                field_name=curr_path,
                                field_path=curr_path,
                                change_type=ChangeType.CONTAINER_PARAM_CHANGED,
                                severity=Severity.BREAKING,
                                error_code=ErrorCode.CONTAINER_PARAM_MISMATCH,
                                old_value=str(len_old) if len_old is not None else old_field.type,
                                new_value=str(len_new) if len_new is not None else new_field.type,
                                description=(
                                    f"Field '{curr_path}' fixed-size list length constraint altered: "
                                    f"baseline={len_old or old_field.type} vs target={len_new or new_field.type}."
                                ),
                                recommendation="Keep identical fixed-size list length to avoid memory layout violations.",
                            )
                        )
                elif old_container == new_container:
                    # Same list/large_list container constructor. Inner item checked in children section.
                    pass
                elif old_container == "list" and new_container == "large_list":
                    # Safe widening of list offset width (32-bit to 64-bit)
                    sev = Severity.BREAKING if mode == "strict" else Severity.SAFE
                    diffs.append(
                        FieldDiff(
                            field_name=curr_path,
                            field_path=curr_path,
                            change_type=ChangeType.TYPE_WIDENED,
                            severity=sev,
                            error_code=ErrorCode.TYPE_WIDENED,
                            old_value=old_field.type,
                            new_value=new_field.type,
                            description=f"Field '{curr_path}' list container offset safely widened from 32-bit ('list') to 64-bit ('large_list').",
                            recommendation="Ensure readers support large_list offset representation.",
                        )
                    )
                elif old_container == "large_list" and new_container == "list":
                    # Narrowing of list offset width (64-bit to 32-bit) -> BREAKING
                    diffs.append(
                        FieldDiff(
                            field_name=curr_path,
                            field_path=curr_path,
                            change_type=ChangeType.TYPE_NARROWED,
                            severity=Severity.BREAKING,
                            error_code=ErrorCode.TYPE_NARROWED,
                            old_value=old_field.type,
                            new_value=new_field.type,
                            description=f"Field '{curr_path}' list container offset narrowed from 64-bit ('large_list') to 32-bit ('list').",
                            recommendation="Avoid narrowing list offset representations to prevent 2GB partition buffer overflow.",
                        )
                    )
                elif old_container.startswith("fixed_size_list") and new_container in ("list", "large_list"):
                    # Widening from fixed-size to variable-size list
                    sev = Severity.BREAKING if mode == "strict" else Severity.SAFE
                    diffs.append(
                        FieldDiff(
                            field_name=curr_path,
                            field_path=curr_path,
                            change_type=ChangeType.TYPE_WIDENED,
                            severity=sev,
                            error_code=ErrorCode.TYPE_WIDENED,
                            old_value=old_field.type,
                            new_value=new_field.type,
                            description=f"Field '{curr_path}' widened from fixed-size list to variable-size '{new_container}'.",
                            recommendation="Update downstream readers if fixed-length layout is expected.",
                        )
                    )
                elif old_container in ("list", "large_list") and new_container.startswith("fixed_size_list"):
                    # Narrowing from variable-size list to fixed-size list -> BREAKING
                    diffs.append(
                        FieldDiff(
                            field_name=curr_path,
                            field_path=curr_path,
                            change_type=ChangeType.TYPE_NARROWED,
                            severity=Severity.BREAKING,
                            error_code=ErrorCode.TYPE_NARROWED,
                            old_value=old_field.type,
                            new_value=new_field.type,
                            description=f"Field '{curr_path}' narrowed from variable-size list to fixed-size list constraint.",
                            recommendation="Ensure all variable-length partitions satisfy fixed length requirement.",
                        )
                    )
            elif old_container == "map" and new_container == "map":
                # Check keys_sorted attribute
                if (
                    old_field.keys_sorted is not None
                    and new_field.keys_sorted is not None
                    and old_field.keys_sorted != new_field.keys_sorted
                ):
                    sev = Severity.BREAKING if mode == "strict" else Severity.WARNING
                    diffs.append(
                        FieldDiff(
                            field_name=curr_path,
                            field_path=curr_path,
                            change_type=ChangeType.KEYS_SORTED_CHANGED,
                            severity=sev,
                            error_code=ErrorCode.MAP_SORTED_MISMATCH,
                            old_value=f"keys_sorted={old_field.keys_sorted}",
                            new_value=f"keys_sorted={new_field.keys_sorted}",
                            description=(
                                f"Field '{curr_path}' Map keys_sorted attribute altered: "
                                f"baseline={old_field.keys_sorted} vs target={new_field.keys_sorted}."
                            ),
                            recommendation="Align Map keys_sorted property to ensure consistent sorting semantics.",
                        )
                    )
            elif old_container == "dictionary" and new_container == "dictionary":
                # Check ordered attribute
                if (
                    old_field.ordered is not None
                    and new_field.ordered is not None
                    and old_field.ordered != new_field.ordered
                ):
                    sev = Severity.BREAKING if mode == "strict" else Severity.WARNING
                    diffs.append(
                        FieldDiff(
                            field_name=curr_path,
                            field_path=curr_path,
                            change_type=ChangeType.ORDERED_FLAG_CHANGED,
                            severity=sev,
                            error_code=ErrorCode.DICTIONARY_ORDERED_MISMATCH,
                            old_value=f"ordered={old_field.ordered}",
                            new_value=f"ordered={new_field.ordered}",
                            description=(
                                f"Field '{curr_path}' Dictionary ordered attribute altered: "
                                f"baseline={old_field.ordered} vs target={new_field.ordered}."
                            ),
                            recommendation="Align Dictionary ordered flag to preserve categorical ordering expectations.",
                        )
                    )
            else:
                # Incompatible container constructors (e.g. struct vs map, struct vs list)
                diffs.append(
                    FieldDiff(
                        field_name=curr_path,
                        field_path=curr_path,
                        change_type=ChangeType.TYPE_MISMATCH,
                        severity=Severity.BREAKING,
                        error_code=ErrorCode.CONTAINER_MISMATCH,
                        old_value=old_field.type,
                        new_value=new_field.type,
                        description=f"Field '{curr_path}' incompatible container type transition from '{old_container}' to '{new_container}'.",
                        recommendation="Cannot convert between disparate container types.",
                    )
                )
    else:
        # Both are scalar/primitive types
        # Check decimal scale and precision explicitly
        if old_field.scale is not None and new_field.scale is not None and old_field.scale != new_field.scale:
            diffs.append(
                FieldDiff(
                    field_name=curr_path,
                    field_path=curr_path,
                    change_type=ChangeType.TYPE_MISMATCH,
                    severity=Severity.BREAKING,
                    error_code=ErrorCode.DECIMAL_SCALE_MISMATCH,
                    old_value=f"scale={old_field.scale}",
                    new_value=f"scale={new_field.scale}",
                    description=f"Field '{curr_path}' decimal scale changed from {old_field.scale} to {new_field.scale}.",
                    recommendation="Keep identical decimal scale to avoid shifting decimal digits in Parquet physical storage.",
                )
            )
        elif (
            old_field.time_unit is not None
            and new_field.time_unit is not None
            and old_field.timezone != new_field.timezone
        ):
            diffs.append(
                FieldDiff(
                    field_name=curr_path,
                    field_path=curr_path,
                    change_type=ChangeType.TYPE_MISMATCH,
                    severity=Severity.BREAKING,
                    error_code=ErrorCode.TIMESTAMP_TIMEZONE_MISMATCH,
                    old_value=f"tz={old_field.timezone}",
                    new_value=f"tz={new_field.timezone}",
                    description=f"Field '{curr_path}' timestamp timezone mismatch: '{old_field.timezone}' vs '{new_field.timezone}'.",
                    recommendation="Ensure timestamp timezone aligns with contract.",
                )
            )
        elif _normalize_type_str(old_field.type) != _normalize_type_str(new_field.type):
            if is_safe_widening(old_field.type, new_field.type):
                sev = Severity.BREAKING if mode == "strict" else Severity.SAFE
                diffs.append(
                    FieldDiff(
                        field_name=curr_path,
                        field_path=curr_path,
                        change_type=ChangeType.TYPE_WIDENED,
                        severity=sev,
                        error_code=ErrorCode.TYPE_WIDENED,
                        old_value=old_field.type,
                        new_value=new_field.type,
                        description=(
                            f"Field '{curr_path}' type changed from '{old_field.type}' to '{new_field.type}' "
                            + (
                                "(type widening is strictly forbidden in strict mode)."
                                if mode == "strict"
                                else "(safely widened)."
                            )
                        ),
                        recommendation=(
                            f"Keep field type as '{old_field.type}' to satisfy strict schema contract."
                            if mode == "strict"
                            else "No action required. Downstream readers supporting promotion will read seamlessly."
                        ),
                    )
                )
            else:
                diffs.append(
                    FieldDiff(
                        field_name=curr_path,
                        field_path=curr_path,
                        change_type=ChangeType.TYPE_MISMATCH,
                        severity=Severity.BREAKING,
                        error_code=ErrorCode.TYPE_MISMATCH,
                        old_value=old_field.type,
                        new_value=new_field.type,
                        description=f"Field '{curr_path}' incompatible type conversion from '{old_field.type}' to '{new_field.type}'.",
                        recommendation=f"Revert type to '{old_field.type}' or introduce a new column name to avoid breaking downstream readers.",
                    )
                )

    # 2. Nullability check
    if old_field.nullable != new_field.nullable:
        if not old_field.nullable and new_field.nullable:
            diffs.append(
                FieldDiff(
                    field_name=curr_path,
                    change_type=ChangeType.NULLABILITY_CHANGED,
                    severity=Severity.BREAKING,
                    old_value="nullable=False",
                    new_value="nullable=True",
                    description=f"Field '{curr_path}' relaxed from NOT NULL to NULLABLE.",
                    recommendation="Downstream consumers expecting strictly non-null values may crash upon receiving nulls. Ensure consumers handle nulls before relaxing constraint.",
                )
            )
        elif old_field.nullable and not new_field.nullable:
            sev = Severity.BREAKING if mode == "strict" else Severity.SAFE
            diffs.append(
                FieldDiff(
                    field_name=curr_path,
                    change_type=ChangeType.NULLABILITY_CHANGED,
                    severity=sev,
                    old_value="nullable=True",
                    new_value="nullable=False",
                    description=(
                        f"Field '{curr_path}' tightened from NULLABLE to NOT NULL"
                        + (" (forbidden in strict mode)." if mode == "strict" else ".")
                    ),
                    recommendation="Ensure upstream writers always populate this field to avoid ingestion rejections.",
                )
            )

    # 3. Field metadata check
    if check_metadata and old_field.metadata != new_field.metadata:
        diff_keys = set(old_field.metadata.keys()) ^ set(new_field.metadata.keys())
        changed_keys = {
            k
            for k in old_field.metadata.keys() & new_field.metadata.keys()
            if old_field.metadata[k] != new_field.metadata[k]
        }
        all_meta_diffs = diff_keys | changed_keys
        if all_meta_diffs:
            sev = Severity.BREAKING if mode == "strict" else Severity.WARNING
            diffs.append(
                FieldDiff(
                    field_name=curr_path,
                    change_type=ChangeType.METADATA_CHANGED,
                    severity=sev,
                    old_value=str(old_field.metadata),
                    new_value=str(new_field.metadata),
                    description=f"Field '{curr_path}' metadata keys altered: {sorted(list(all_meta_diffs))}.",
                    recommendation="Sync field metadata if custom serialization attributes depend on it.",
                )
            )

    # 4. Nested Children check (for Struct, List, Map, etc.)
    if old_field.children or new_field.children:
        should_check_child_order = (mode == "strict") or check_field_order
        old_child_map = {c.name: c for c in old_field.children}
        new_child_map = {c.name: c for c in new_field.children}

        # Check child field ordering in strict mode or when check_field_order=True
        if should_check_child_order:
            old_child_names = [c.name for c in old_field.children if c.name in new_child_map]
            new_child_names = [c.name for c in new_field.children if c.name in old_child_map]
            if old_child_names != new_child_names:
                sev = Severity.BREAKING if mode == "strict" else Severity.WARNING
                diffs.append(
                    FieldDiff(
                        field_name=f"{curr_path}.<order>",
                        change_type=ChangeType.FIELD_ORDER_CHANGED,
                        severity=sev,
                        old_value=", ".join(old_child_names),
                        new_value=", ".join(new_child_names),
                        description=(
                            f"Nested child field ordering in '{curr_path}' altered: "
                            f"baseline={old_child_names} vs target={new_child_names}."
                        ),
                        recommendation="Preserve identical child field ordering in nested structures.",
                    )
                )

        # Check for removed child fields
        for old_c in old_field.children:
            child_path = f"{curr_path}.{old_c.name}"
            if old_c.name not in new_child_map:
                diffs.append(
                    FieldDiff(
                        field_name=child_path,
                        change_type=ChangeType.FIELD_REMOVED,
                        severity=Severity.BREAKING,
                        old_value=f"{old_c.name}: {old_c.type}",
                        new_value=None,
                        description=f"Nested child field '{child_path}' missing in target.",
                        recommendation=f"Retain child field '{old_c.name}' inside '{curr_path}'.",
                    )
                )
            else:
                new_c = new_child_map[old_c.name]
                child_diffs = analyze_compatibility(
                    old_c,
                    new_c,
                    mode=mode,
                    check_metadata=check_metadata,
                    check_field_order=check_field_order,
                    field_path=child_path,
                )
                diffs.extend(child_diffs)

        # Check for newly added child fields
        for new_c in new_field.children:
            child_path = f"{curr_path}.{new_c.name}"
            if new_c.name not in old_child_map:
                if mode == "strict":
                    diffs.append(
                        FieldDiff(
                            field_name=child_path,
                            change_type=ChangeType.FIELD_ADDED,
                            severity=Severity.BREAKING,
                            old_value=None,
                            new_value=f"{new_c.name}: {new_c.type}",
                            description=f"Unexpected nested child field '{child_path}' added in strict mode.",
                            recommendation=f"Remove added child field '{new_c.name}' from '{curr_path}'.",
                        )
                    )
                else:
                    sev = Severity.SAFE if new_c.nullable else Severity.WARNING
                    desc = (
                        f"Nested nullable child field '{child_path}' ({new_c.type}) added."
                        if new_c.nullable
                        else f"New NOT NULL nested child field '{child_path}' ({new_c.type}) added without default."
                    )
                    diffs.append(
                        FieldDiff(
                            field_name=child_path,
                            change_type=ChangeType.FIELD_ADDED,
                            severity=sev,
                            old_value=None,
                            new_value=f"{new_c.name}: {new_c.type}",
                            description=desc,
                            recommendation="Update contract if this is an intended additive evolution.",
                        )
                    )

    return diffs


def compare_schemas(
    target: SchemaContract,
    baseline: SchemaContract,
    mode: str = "permissive",
    check_metadata: bool = True,
    check_field_order: bool = False,
) -> DiffReport:
    """Compare a target schema against a baseline contract.

    Args:
        target: Target schema being evaluated (e.g. from newly generated Parquet).
        baseline: Expected contract schema.
        mode: 'permissive' (allows safe widening and additive fields) or 'strict' (disallows any discrepancy).
        check_metadata: Whether to check top-level and field-level metadata changes.
        check_field_order: Whether to enforce field order matching (automatically True if mode == 'strict').

    Returns:
        DiffReport with categorized diffs and compatibility verdict.
    """
    diffs: List[FieldDiff] = []
    target_field_map = {f.name: f for f in target.fields}
    baseline_field_map = {f.name: f for f in baseline.fields}

    # Automatically enforce field order in strict mode
    should_check_order = check_field_order or (mode == "strict")

    # 0. Check for duplicate column names in baseline or target
    if baseline.has_duplicate_fields():
        dups = baseline.duplicate_field_names()
        diffs.append(
            FieldDiff(
                field_name=", ".join(dups),
                change_type=ChangeType.TYPE_MISMATCH,
                severity=Severity.BREAKING,
                description=f"Baseline contract contains duplicate column name(s): {dups}.",
                recommendation="Ensure all column names in baseline contract are distinct.",
            )
        )
    if target.has_duplicate_fields():
        dups = target.duplicate_field_names()
        diffs.append(
            FieldDiff(
                field_name=", ".join(dups),
                change_type=ChangeType.TYPE_MISMATCH,
                severity=Severity.BREAKING,
                description=f"Target schema contains duplicate column name(s): {dups}.",
                recommendation="Ensure all column names in target Parquet file are distinct.",
            )
        )

    # 1. Field ordering check
    if should_check_order:
        base_names = [f.name for f in baseline.fields if f.name in target_field_map]
        target_names = [f.name for f in target.fields if f.name in baseline_field_map]
        if base_names != target_names:
            sev = Severity.BREAKING if mode == "strict" else Severity.WARNING
            diffs.append(
                FieldDiff(
                    field_name="<schema>",
                    change_type=ChangeType.FIELD_ORDER_CHANGED,
                    severity=sev,
                    old_value=", ".join(base_names),
                    new_value=", ".join(target_names),
                    description="Field ordering differs from baseline (Strict mode enforces identical column order).",
                    recommendation="Preserve identical column ordering in your Parquet writer.",
                )
            )

    # 2. Top-level Schema Metadata check
    if check_metadata and baseline.metadata != target.metadata:
        diff_keys = set(baseline.metadata.keys()) ^ set(target.metadata.keys())
        changed_keys = {
            k
            for k in baseline.metadata.keys() & target.metadata.keys()
            if baseline.metadata[k] != target.metadata[k]
        }
        all_meta_diffs = diff_keys | changed_keys
        if all_meta_diffs:
            sev = Severity.BREAKING if mode == "strict" else Severity.WARNING
            diffs.append(
                FieldDiff(
                    field_name="<metadata>",
                    change_type=ChangeType.METADATA_CHANGED,
                    severity=sev,
                    old_value=str(baseline.metadata),
                    new_value=str(target.metadata),
                    description=f"Schema top-level metadata changed for keys: {sorted(list(all_meta_diffs))}.",
                    recommendation="Align schema metadata dictionary keys and values.",
                )
            )

    # 3. Check for removed or modified baseline fields
    for base_field in baseline.fields:
        if base_field.name not in target_field_map:
            diffs.append(
                FieldDiff(
                    field_name=base_field.name,
                    change_type=ChangeType.FIELD_REMOVED,
                    severity=Severity.BREAKING,
                    old_value=f"{base_field.name}: {base_field.type}",
                    new_value=None,
                    description=f"Baseline field '{base_field.name}' is missing in target schema.",
                    recommendation=f"Restore column '{base_field.name}' to maintain compatibility with existing consumers.",
                )
            )
        else:
            tgt_field = target_field_map[base_field.name]
            field_diffs = analyze_compatibility(
                base_field,
                tgt_field,
                mode=mode,
                check_metadata=check_metadata,
                check_field_order=should_check_order,
                field_path=base_field.name,
            )
            diffs.extend(field_diffs)

    # 4. Check for newly introduced fields in target
    for tgt_field in target.fields:
        if tgt_field.name not in baseline_field_map:
            if mode == "strict":
                diffs.append(
                    FieldDiff(
                        field_name=tgt_field.name,
                        change_type=ChangeType.FIELD_ADDED,
                        severity=Severity.BREAKING,
                        old_value=None,
                        new_value=f"{tgt_field.name}: {tgt_field.type}",
                        description=f"Unexpected field '{tgt_field.name}' added to schema in strict mode.",
                        recommendation="Remove extraneous field or update contract definition to include it.",
                    )
                )
            else:
                # Permissive mode: adding nullable field is SAFE; adding NOT NULL field is WARNING
                severity = Severity.SAFE if tgt_field.nullable else Severity.WARNING
                desc = (
                    f"New nullable field '{tgt_field.name}' ({tgt_field.type}) added."
                    if tgt_field.nullable
                    else f"New NOT NULL field '{tgt_field.name}' ({tgt_field.type}) added without default."
                )
                diffs.append(
                    FieldDiff(
                        field_name=tgt_field.name,
                        change_type=ChangeType.FIELD_ADDED,
                        severity=severity,
                        old_value=None,
                        new_value=f"{tgt_field.name}: {tgt_field.type}",
                        description=desc,
                        recommendation="Update baseline contract if this is an intentional additive evolution.",
                    )
                )

    report = DiffReport(
        target_name=target.name or "target",
        baseline_name=baseline.name or "baseline",
        mode=mode,
        diffs=diffs,
    )
    if report.has_breaking_changes:
        report.result_state = ResultState.INCOMPATIBLE
    elif report.has_warnings or any(d.severity == Severity.SAFE for d in diffs):
        report.result_state = ResultState.DRIFTED
    else:
        report.result_state = ResultState.VALID

    return report
