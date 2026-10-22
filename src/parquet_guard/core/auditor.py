"""Partition directory and multi-file Parquet dataset schema consistency auditor."""

from __future__ import annotations

import glob
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from parquet_guard.core.cache import (
    CachedFileInfo,
    DatasetAuditCache,
    load_audit_cache,
    save_audit_cache,
)
from parquet_guard.core.compatibility import compare_schemas
from parquet_guard.core.inspector import extract_file_metadata, inspect_schema
from parquet_guard.core.models import (
    FieldDefinition,
    PartitionAuditResult,
    PartitionFileReport,
    ResultState,
    SchemaContract,
    Severity,
)


def _serialize_field(
    f: FieldDefinition, include_metadata: bool = True, include_field_order: bool = True
) -> Dict[str, Any]:
    f_entry: Dict[str, Any] = {
        "name": f.name,
        "type": f.type.strip(),
        "nullable": f.nullable,
    }
    if f.list_size is not None:
        f_entry["list_size"] = f.list_size
    if f.ordered is not None:
        f_entry["ordered"] = f.ordered
    if f.keys_sorted is not None:
        f_entry["keys_sorted"] = f.keys_sorted
    if f.precision is not None:
        f_entry["precision"] = f.precision
    if f.scale is not None:
        f_entry["scale"] = f.scale
    if f.time_unit is not None:
        f_entry["time_unit"] = f.time_unit
    if f.timezone is not None:
        f_entry["timezone"] = f.timezone

    if include_metadata and f.metadata:
        f_entry["metadata"] = sorted(list(f.metadata.items()))
    if f.children:
        child_list = f.children if include_field_order else sorted(f.children, key=lambda c: c.name)
        f_entry["children"] = [
            _serialize_field(c, include_metadata=include_metadata, include_field_order=include_field_order)
            for c in child_list
        ]
    return f_entry


def compute_schema_fingerprint(
    contract: SchemaContract,
    include_metadata: bool = True,
    include_field_order: bool = True,
) -> str:
    """Compute a deterministic, canonical SHA-256 fingerprint for a SchemaContract.

    Guarantees:
    - 256-bit cryptographic collision resistance (full 64-hex SHA-256).
    - Preserves field order when include_field_order=True.
    - Preserves recursive nested children structure, order, nullability, and metadata.
    - Preserves typed container parameters (list_size, ordered, keys_sorted).
    - Deterministic sorted key-value serialization for metadata.
    """
    field_list = contract.fields if include_field_order else sorted(contract.fields, key=lambda f: f.name)
    fields_data = [
        _serialize_field(f, include_metadata=include_metadata, include_field_order=include_field_order)
        for f in field_list
    ]

    payload: Dict[str, Any] = {
        "fields": fields_data,
    }
    if include_metadata and contract.metadata:
        payload["metadata"] = sorted(list(contract.metadata.items()))

    canonical_json = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


# Backwards compatibility alias
_compute_schema_fingerprint = compute_schema_fingerprint


def audit_dataset(
    target_path_or_glob: Union[str, Path],
    mode: str = "strict",
    recursive: bool = True,
    check_metadata: bool = True,
    check_field_order: bool = True,
    baseline_contract: Optional[SchemaContract] = None,
    incremental: bool = False,
    cache_path: Optional[Union[str, Path]] = None,
    force_full: bool = False,
) -> PartitionAuditResult:
    """Scan and audit a collection of Parquet files or partitioned directory tree.

    Args:
        target_path_or_glob: Path to directory, single file, or glob pattern.
        mode: 'strict' (enforces exact identity in order, metadata, and types) or
              'permissive' (permits backward-compatible widening and additive fields).
        recursive: Whether to search subdirectories recursively when directory path is provided.
        check_metadata: Whether to include metadata in consistency checks.
        check_field_order: Whether to enforce identical column order across files.
        baseline_contract: Optional explicit contract to validate all partitions against.
        incremental: Whether to enable local-filesystem caching for unchanged partition files.
        cache_path: Optional custom path for the audit cache manifest JSON file.
        force_full: When True, ignores existing cache and forces full metadata re-audit.

    Returns:
        PartitionAuditResult containing global consistency status and per-file inspection details.
    """
    path_str = str(target_path_or_glob)
    files: List[Path] = []

    target_path = Path(path_str)
    if target_path.is_file():
        files = [target_path]
    elif target_path.is_dir():
        pattern = "**/*.parquet" if recursive else "*.parquet"
        files = sorted(list(target_path.glob(pattern)))
    else:
        # Glob pattern provided
        matched = glob.glob(path_str, recursive=recursive)
        files = sorted([Path(p) for p in matched if Path(p).is_file() and p.endswith(".parquet")])

    if not files:
        return PartitionAuditResult(
            root_path=path_str,
            total_files_scanned=0,
            total_rows=0,
            total_bytes=0,
            distinct_schemas_count=0,
            is_consistent=False,
            is_compatible=False,
            result_state=ResultState.INSPECTION_ERROR,
            divergent_files_count=0,
            files=[],
            divergence_details=[f"No Parquet files found matching specified path or pattern: '{path_str}'."],
        )

    file_reports: List[PartitionFileReport] = []
    schemas_by_fingerprint: Dict[str, SchemaContract] = {}
    divergence_details: List[str] = []
    divergent_count = 0
    total_rows = 0
    total_bytes = 0
    cache_hits = 0
    cache_misses = 0

    should_check_order = check_field_order or (mode == "strict")

    # Incremental cache resolution
    active_cache_path: Optional[Path] = None
    loaded_cache: Optional[DatasetAuditCache] = None
    is_cache_valid = False

    if incremental:
        if cache_path:
            active_cache_path = Path(cache_path)
        elif target_path.is_dir():
            active_cache_path = target_path / ".parquet_guard_cache.json"
        elif files:
            active_cache_path = files[0].parent / ".parquet_guard_cache.json"
        else:
            active_cache_path = Path(".parquet_guard_cache.json")

        if not force_full and active_cache_path:
            loaded_cache = load_audit_cache(active_cache_path)
            if loaded_cache and loaded_cache.is_cache_valid():
                is_cache_valid = True

    new_cache_entries: Dict[str, CachedFileInfo] = {}

    for file_path in files:
        file_key = str(file_path.resolve())
        stat = file_path.stat() if file_path.exists() else None

        # Check incremental cache hit (supports absolute paths and portable relative paths)
        cached_candidate: Optional[CachedFileInfo] = None
        if is_cache_valid and loaded_cache and stat:
            if file_key in loaded_cache.files:
                cached_candidate = loaded_cache.files[file_key]
            else:
                try:
                    rel_str = str(file_path)
                    if rel_str in loaded_cache.files:
                        cached_candidate = loaded_cache.files[rel_str]
                    elif target_path.is_dir() and file_path.resolve().is_relative_to(target_path.resolve()):
                        rel_target = str(file_path.resolve().relative_to(target_path.resolve()))
                        if rel_target in loaded_cache.files:
                            cached_candidate = loaded_cache.files[rel_target]
                except Exception:
                    pass

        if cached_candidate is not None and stat:
            cached = cached_candidate
            if cached.file_size_bytes == stat.st_size and cached.mtime_ns == stat.st_mtime_ns:
                # Cache HIT!
                cache_hits += 1
                total_rows += cached.row_count
                total_bytes += cached.file_size_bytes
                new_cache_entries[file_key] = cached

                if cached.is_corrupted:
                    err_msg = cached.error_message or "Cached record indicates corrupted file."
                    rep = PartitionFileReport(
                        file_path=str(file_path),
                        file_size_bytes=cached.file_size_bytes,
                        row_count=cached.row_count,
                        row_groups=cached.row_groups,
                        column_count=cached.column_count,
                        schema_fingerprint="CORRUPTED",
                        is_divergent=True,
                        divergence_reason=err_msg,
                    )
                    file_reports.append(rep)
                    divergence_details.append(f"File '{file_path}': {err_msg}")
                    divergent_count += 1
                    continue

                if cached.has_duplicate_columns:
                    err_msg = cached.error_message or "Cached record indicates duplicate columns."
                    rep = PartitionFileReport(
                        file_path=str(file_path),
                        file_size_bytes=cached.file_size_bytes,
                        row_count=cached.row_count,
                        row_groups=cached.row_groups,
                        column_count=cached.column_count,
                        schema_fingerprint="DUPLICATE_COLUMNS",
                        is_divergent=True,
                        divergence_reason=err_msg,
                    )
                    file_reports.append(rep)
                    divergence_details.append(f"File '{file_path}': {err_msg}")
                    divergent_count += 1
                    continue

                # Ensure schema contract is loaded into schemas_by_fingerprint from cache
                if cached.schema_fingerprint not in schemas_by_fingerprint:
                    if loaded_cache is not None and cached.schema_fingerprint in loaded_cache.schemas:
                        try:
                            schemas_by_fingerprint[cached.schema_fingerprint] = SchemaContract.model_validate(
                                loaded_cache.schemas[cached.schema_fingerprint]
                            )
                        except Exception:
                            try:
                                contract = inspect_schema(file_path)
                                schemas_by_fingerprint[cached.schema_fingerprint] = contract
                            except Exception:
                                pass
                    else:
                        try:
                            contract = inspect_schema(file_path)
                            schemas_by_fingerprint[cached.schema_fingerprint] = contract
                        except Exception:
                            pass

                file_reports.append(
                    PartitionFileReport(
                        file_path=str(file_path),
                        file_size_bytes=cached.file_size_bytes,
                        row_count=cached.row_count,
                        row_groups=cached.row_groups,
                        column_count=cached.column_count,
                        schema_fingerprint=cached.schema_fingerprint,
                        is_divergent=False,
                    )
                )
                continue

        # Cache MISS
        cache_misses += 1
        try:
            meta = extract_file_metadata(file_path)
            contract = inspect_schema(file_path)
            total_rows += meta["num_rows"]
            total_bytes += meta["file_size_bytes"]

            # Check for duplicate column names in file
            if contract.has_duplicate_fields():
                dups = contract.duplicate_field_names()
                reason = f"Schema contains duplicate column name(s): {dups}."
                rep = PartitionFileReport(
                    file_path=str(file_path),
                    file_size_bytes=meta["file_size_bytes"],
                    row_count=meta["num_rows"],
                    row_groups=meta["num_row_groups"],
                    column_count=meta["num_columns"],
                    schema_fingerprint="DUPLICATE_COLUMNS",
                    is_divergent=True,
                    divergence_reason=reason,
                )
                file_reports.append(rep)
                divergence_details.append(f"File '{file_path}': {reason}")
                divergent_count += 1
                if stat:
                    new_cache_entries[file_key] = CachedFileInfo(
                        file_size_bytes=meta["file_size_bytes"],
                        mtime_ns=stat.st_mtime_ns,
                        row_count=meta["num_rows"],
                        row_groups=meta["num_row_groups"],
                        column_count=meta["num_columns"],
                        schema_fingerprint="DUPLICATE_COLUMNS",
                        is_corrupted=False,
                        has_duplicate_columns=True,
                        error_message=reason,
                    )
                continue

            fp = compute_schema_fingerprint(
                contract,
                include_metadata=check_metadata,
                include_field_order=should_check_order,
            )

            if fp not in schemas_by_fingerprint:
                schemas_by_fingerprint[fp] = contract

            rep = PartitionFileReport(
                file_path=str(file_path),
                file_size_bytes=meta["file_size_bytes"],
                row_count=meta["num_rows"],
                row_groups=meta["num_row_groups"],
                column_count=meta["num_columns"],
                schema_fingerprint=fp,
                is_divergent=False,
            )
            file_reports.append(rep)
            if stat:
                new_cache_entries[file_key] = CachedFileInfo(
                    file_size_bytes=meta["file_size_bytes"],
                    mtime_ns=stat.st_mtime_ns,
                    row_count=meta["num_rows"],
                    row_groups=meta["num_row_groups"],
                    column_count=meta["num_columns"],
                    schema_fingerprint=fp,
                    is_corrupted=False,
                    has_duplicate_columns=False,
                )

        except Exception as exc:
            err_reason = f"Failed to read Parquet metadata: {exc}"
            rep = PartitionFileReport(
                file_path=str(file_path),
                file_size_bytes=file_path.stat().st_size if file_path.exists() else 0,
                row_count=0,
                row_groups=0,
                column_count=0,
                schema_fingerprint="CORRUPTED",
                is_divergent=True,
                divergence_reason=err_reason,
            )
            file_reports.append(rep)
            divergence_details.append(f"File '{file_path}': {err_reason}")
            divergent_count += 1
            if stat:
                new_cache_entries[file_key] = CachedFileInfo(
                    file_size_bytes=stat.st_size,
                    mtime_ns=stat.st_mtime_ns,
                    row_count=0,
                    row_groups=0,
                    column_count=0,
                    schema_fingerprint="CORRUPTED",
                    is_corrupted=True,
                    has_duplicate_columns=False,
                    error_message=err_reason,
                )

    def _persist_cache() -> None:
        if incremental and active_cache_path:
            schemas_data = {
                fp: contract.model_dump(exclude_none=True) for fp, contract in schemas_by_fingerprint.items()
            }
            save_audit_cache(
                DatasetAuditCache(
                    dataset_root=path_str,
                    updated_at="",
                    files=new_cache_entries,
                    schemas=schemas_data,
                ),
                active_cache_path,
            )

    valid_schemas: Dict[str, SchemaContract] = {}
    fingerprint_counts: Dict[str, int] = {}
    for r in file_reports:
        if r.schema_fingerprint not in ("CORRUPTED", "DUPLICATE_COLUMNS"):
            valid_schemas[r.schema_fingerprint] = schemas_by_fingerprint[r.schema_fingerprint]
            fingerprint_counts[r.schema_fingerprint] = fingerprint_counts.get(r.schema_fingerprint, 0) + 1

    distinct_count = len(valid_schemas)
    canonical_schema: Optional[SchemaContract] = None
    drifts: List[str] = []
    warnings: List[str] = []
    warnings_count = 0
    breaking_count = 0

    # Case A: Explicit baseline contract provided
    if baseline_contract is not None:
        canonical_schema = baseline_contract
        if baseline_contract.has_duplicate_fields():
            dups = baseline_contract.duplicate_field_names()
            msg = f"Baseline contract contains duplicate column path(s): {dups}."
            divergence_details.append(msg)
            drifts.append(msg)
            for r in file_reports:
                if not r.is_divergent:
                    r.is_divergent = True
                    r.divergence_reason = f"Baseline contract has duplicate columns: {dups}."
                    divergent_count += 1
                    breaking_count += 1
        else:
            for r in file_reports:
                if not r.is_divergent:
                    target_schema = schemas_by_fingerprint[r.schema_fingerprint]
                    diff = compare_schemas(
                        target=target_schema,
                        baseline=baseline_contract,
                        mode=mode,
                        check_metadata=check_metadata,
                        check_field_order=should_check_order,
                    )
                    # 1. Breaking diffs
                    breaking = [d for d in diff.diffs if d.severity == Severity.BREAKING]
                    if breaking:
                        r.is_divergent = True
                        divergent_count += 1
                        breaking_count += len(breaking)
                        r.divergence_reason = "; ".join(
                            [
                                f"[{d.severity.value}] ({d.change_type.value}) {d.description}"
                                for d in breaking
                            ]
                        )
                        divergence_details.append(f"File '{r.file_path}': {r.divergence_reason}")
                        for d in breaking:
                            drifts.append(
                                f"File '{r.file_path}': [{d.severity.value}] ({d.change_type.value}) {d.description}"
                            )

                    # 2. Warning diffs
                    warning_list = [d for d in diff.diffs if d.severity == Severity.WARNING]
                    if warning_list:
                        r.warnings = [
                            f"[{d.severity.value}] ({d.change_type.value}) {d.description}"
                            for d in warning_list
                        ]
                        warnings_count += len(warning_list)
                        for w in r.warnings:
                            divergence_details.append(f"File '{r.file_path}': {w}")
                            warnings.append(f"File '{r.file_path}': {w}")
                            drifts.append(f"File '{r.file_path}': {w}")

                    # 3. Safe diffs
                    safe_list = [d for d in diff.diffs if d.severity == Severity.SAFE]
                    if safe_list:
                        for s in safe_list:
                            drifts.append(
                                f"File '{r.file_path}': [{s.severity.value}] ({s.change_type.value}) {s.description}"
                            )

        is_comp = divergent_count == 0
        has_drift_val = len(drifts) > 0 or not is_comp
        is_consist = is_comp if mode == "permissive" else (is_comp and not has_drift_val)

        if not is_comp:
            res_state = ResultState.INCOMPATIBLE
        elif has_drift_val or warnings_count > 0:
            res_state = ResultState.DRIFTED
        else:
            res_state = ResultState.VALID

        _persist_cache()
        return PartitionAuditResult(
            root_path=path_str,
            total_files_scanned=len(files),
            total_rows=total_rows,
            total_bytes=total_bytes,
            distinct_schemas_count=distinct_count,
            is_consistent=is_consist,
            is_compatible=is_comp,
            has_drift=has_drift_val,
            result_state=res_state,
            warnings_count=warnings_count,
            breaking_count=breaking_count,
            canonical_schema=canonical_schema,
            divergent_files_count=divergent_count,
            files=file_reports,
            divergence_details=divergence_details,
            drifts=drifts,
            warnings=warnings,
            cache_hits=cache_hits,
            cache_misses=cache_misses,
            incremental=incremental,
        )

    # Case B: No explicit baseline contract (Ungrounded audit)
    # If all valid files share the exact same fingerprint
    if distinct_count <= 1:
        canonical_schema = next(iter(valid_schemas.values())) if valid_schemas else None
        is_comp = divergent_count == 0
        has_drift_val = not is_comp or len(drifts) > 0
        res_state = ResultState.INCOMPATIBLE if not is_comp else ResultState.VALID

        _persist_cache()
        return PartitionAuditResult(
            root_path=path_str,
            total_files_scanned=len(files),
            total_rows=total_rows,
            total_bytes=total_bytes,
            distinct_schemas_count=distinct_count,
            is_consistent=is_comp,
            is_compatible=is_comp,
            has_drift=has_drift_val,
            result_state=res_state,
            warnings_count=warnings_count,
            breaking_count=breaking_count,
            canonical_schema=canonical_schema,
            divergent_files_count=divergent_count,
            files=file_reports,
            divergence_details=divergence_details,
            drifts=drifts,
            warnings=warnings,
            cache_hits=cache_hits,
            cache_misses=cache_misses,
            incremental=incremental,
        )

    # If multiple distinct schemas are present:
    if mode == "strict":
        # Strict mode requires all files to have identical schema fingerprint
        canonical_fp = min(fingerprint_counts.keys(), key=lambda fp: (-fingerprint_counts[fp], fp))
        canonical_schema = valid_schemas[canonical_fp]

        for r in file_reports:
            if not r.is_divergent and r.schema_fingerprint != canonical_fp:
                target_schema = valid_schemas[r.schema_fingerprint]
                diff = compare_schemas(
                    target_schema,
                    canonical_schema,
                    mode="strict",
                    check_metadata=check_metadata,
                    check_field_order=should_check_order,
                )
                r.is_divergent = True
                divergent_count += 1
                breaking_count += len(diff.diffs)
                issues = [f"[{d.severity.value}] ({d.change_type.value}) {d.description}" for d in diff.diffs]
                r.divergence_reason = "; ".join(issues)
                divergence_details.append(f"File '{r.file_path}': {r.divergence_reason}")
                for d in diff.diffs:
                    drifts.append(
                        f"File '{r.file_path}': [{d.severity.value}] ({d.change_type.value}) {d.description}"
                    )

        _persist_cache()
        return PartitionAuditResult(
            root_path=path_str,
            total_files_scanned=len(files),
            total_rows=total_rows,
            total_bytes=total_bytes,
            distinct_schemas_count=distinct_count,
            is_consistent=False,
            is_compatible=False,
            has_drift=True,
            result_state=ResultState.INCOMPATIBLE,
            warnings_count=warnings_count,
            breaking_count=breaking_count,
            canonical_schema=canonical_schema,
            divergent_files_count=divergent_count,
            files=file_reports,
            divergence_details=divergence_details,
            drifts=drifts,
            warnings=warnings,
            cache_hits=cache_hits,
            cache_misses=cache_misses,
            incremental=incremental,
        )

    # In permissive mode without explicit baseline:
    candidate_baselines: List[Tuple[str, SchemaContract]] = []
    for c_fp, c_schema in valid_schemas.items():
        is_candidate_valid = True
        for t_fp, t_schema in valid_schemas.items():
            if t_fp == c_fp:
                continue
            diff = compare_schemas(
                target=t_schema,
                baseline=c_schema,
                mode="permissive",
                check_metadata=check_metadata,
                check_field_order=should_check_order,
            )
            if diff.has_breaking_changes:
                is_candidate_valid = False
                break
        if is_candidate_valid:
            candidate_baselines.append((c_fp, c_schema))

    if candidate_baselines:
        # A valid baseline exists! Pick minimal baseline (fewest fields, tie-break by fp)
        canonical_fp, canonical_schema = min(
            candidate_baselines, key=lambda item: (len(item[1].fields), item[0])
        )
        for r in file_reports:
            if not r.is_divergent:
                target_schema = valid_schemas[r.schema_fingerprint]
                diff = compare_schemas(
                    target=target_schema,
                    baseline=canonical_schema,
                    mode="permissive",
                    check_metadata=check_metadata,
                    check_field_order=should_check_order,
                )
                r.is_divergent = False

                # Collect warning diffs
                warning_list = [d for d in diff.diffs if d.severity == Severity.WARNING]
                if warning_list:
                    r.warnings = [
                        f"[{d.severity.value}] ({d.change_type.value}) {d.description}" for d in warning_list
                    ]
                    warnings_count += len(warning_list)
                    for w in r.warnings:
                        divergence_details.append(f"File '{r.file_path}': {w}")
                        warnings.append(f"File '{r.file_path}': {w}")
                        drifts.append(f"File '{r.file_path}': {w}")

                # Collect safe diffs
                safe_list = [d for d in diff.diffs if d.severity == Severity.SAFE]
                if safe_list:
                    for s in safe_list:
                        drifts.append(
                            f"File '{r.file_path}': [{s.severity.value}] ({s.change_type.value}) {s.description}"
                        )

        is_comp = divergent_count == 0
        has_drift_val = len(drifts) > 0 or not is_comp
        res_state = ResultState.DRIFTED if (has_drift_val or warnings_count > 0) else ResultState.VALID

        _persist_cache()
        return PartitionAuditResult(
            root_path=path_str,
            total_files_scanned=len(files),
            total_rows=total_rows,
            total_bytes=total_bytes,
            distinct_schemas_count=distinct_count,
            is_consistent=is_comp,
            is_compatible=is_comp,
            has_drift=has_drift_val,
            result_state=res_state,
            warnings_count=warnings_count,
            breaking_count=breaking_count,
            canonical_schema=canonical_schema,
            divergent_files_count=divergent_count,
            files=file_reports,
            divergence_details=divergence_details,
            drifts=drifts,
            warnings=warnings,
            cache_hits=cache_hits,
            cache_misses=cache_misses,
            incremental=incremental,
        )

    # If no schema in the dataset can serve as a baseline for all files:
    incompatible_pairs: List[str] = []
    fps = sorted(list(valid_schemas.keys()))
    for i in range(len(fps)):
        for j in range(i + 1, len(fps)):
            s1 = valid_schemas[fps[i]]
            s2 = valid_schemas[fps[j]]
            d12 = compare_schemas(
                s2, s1, mode="permissive", check_metadata=check_metadata, check_field_order=should_check_order
            )
            d21 = compare_schemas(
                s1, s2, mode="permissive", check_metadata=check_metadata, check_field_order=should_check_order
            )
            if d12.has_breaking_changes and d21.has_breaking_changes:
                incompatible_pairs.append(f"'{s1.name or fps[i][:8]}' <-> '{s2.name or fps[j][:8]}'")

    diag = (
        "Dataset Incompatibility: No single baseline schema in the dataset satisfies all partitions under permissive evolution. "
        f"Mutually incompatible partition schema groups: {incompatible_pairs if incompatible_pairs else 'disparate column sets'}. "
        "Recommendation: Specify an explicit golden contract (--contract / -c) or harmonize partition schemas."
    )
    divergence_details.append(diag)
    drifts.append(diag)

    for r in file_reports:
        if r.schema_fingerprint not in ("CORRUPTED", "DUPLICATE_COLUMNS"):
            r.is_divergent = True
            r.divergence_reason = (
                f"Schema group ({r.schema_fingerprint[:8]}) is mutually incompatible with other partitions. "
                "No universal baseline exists; specify explicit contract (--contract)."
            )
            divergent_count += 1
            breaking_count += 1
            divergence_details.append(f"File '{r.file_path}': {r.divergence_reason}")

    return PartitionAuditResult(
        root_path=path_str,
        total_files_scanned=len(files),
        total_rows=total_rows,
        total_bytes=total_bytes,
        distinct_schemas_count=distinct_count,
        is_consistent=False,
        is_compatible=False,
        has_drift=True,
        result_state=ResultState.INDETERMINATE,
        warnings_count=warnings_count,
        breaking_count=breaking_count,
        canonical_schema=None,
        divergent_files_count=divergent_count,
        files=file_reports,
        divergence_details=divergence_details,
        drifts=drifts,
        warnings=warnings,
        cache_hits=cache_hits,
        cache_misses=cache_misses,
        incremental=incremental,
    )
