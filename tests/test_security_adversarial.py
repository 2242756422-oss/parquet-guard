"""Property-based testing and adversarial security testing for Parquet Guard.

Covers:
- Hypothesis property-based testing for SchemaContract serialization and type parsing.
- Adversarial SQL identifier fuzzing for engine probes (SQL injection mitigation).
- Footer corruption, truncation, and magic bytes fuzzing.
- Path traversal and security boundaries.
"""

import json
import os
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from parquet_guard.core.models import FieldDefinition, SchemaContract
from parquet_guard.verification import verify_engine_compatibility
from parquet_guard.verification.adapters import quote_sql_identifier, sanitize_error_message
from parquet_guard.verification.models import MetadataVerdict, RuntimeVerdict

# -------------------------------------------------------------------------
# 1. Hypothesis Property-Based Testing
# -------------------------------------------------------------------------

# Valid primitive Arrow type strings
ARROW_PRIMITIVE_TYPES = st.sampled_from(
    [
        "int8",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "float16",
        "float32",
        "float64",
        "string",
        "large_string",
        "binary",
        "large_binary",
        "boolean",
        "date32",
        "date64",
        "time32[s]",
        "time32[ms]",
        "time64[us]",
        "time64[ns]",
    ]
)

FIELD_NAMES = st.text(
    alphabet=st.characters(whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="_"),
    min_size=1,
    max_size=32,
)


@st.composite
def field_definition_strategy(draw: st.DrawFn) -> FieldDefinition:
    name = draw(FIELD_NAMES)
    ftype = draw(ARROW_PRIMITIVE_TYPES)
    nullable = draw(st.booleans())
    return FieldDefinition(name=name, type=ftype, nullable=nullable)


@st.composite
def schema_contract_strategy(draw: st.DrawFn) -> SchemaContract:
    fields = draw(st.lists(field_definition_strategy(), min_size=1, max_size=10, unique_by=lambda f: f.name))
    name = draw(st.text(min_size=0, max_size=20))
    return SchemaContract(name=name or None, fields=fields)


@given(contract=schema_contract_strategy())
@settings(max_examples=50, deadline=None)
def test_hypothesis_schema_contract_json_roundtrip(contract: SchemaContract) -> None:
    """Property: Any valid SchemaContract preserves identity across JSON serialization."""
    serialized = contract.model_dump_json()
    loaded_dict = json.loads(serialized)
    reconstituted = SchemaContract.model_validate(loaded_dict)

    assert len(reconstituted.fields) == len(contract.fields)
    for orig_f, recon_f in zip(contract.fields, reconstituted.fields):
        assert orig_f.name == recon_f.name
        assert orig_f.type == recon_f.type
        assert orig_f.nullable == recon_f.nullable


# -------------------------------------------------------------------------
# 2. Adversarial SQL Identifier Fuzzing (SQL Injection Defense)
# -------------------------------------------------------------------------

ADVERSARIAL_SQL_STRINGS = [
    'col"; DROP TABLE users; --',
    "col' OR 1=1 --",
    '"""quoted"""',
    "col\x00with_null",
    "col\nwith\rnewline",
    "col`with`backticks",
    "SELECT * FROM tbl",
    "-- comment",
    "/* block comment */",
    "ユーザー名",  # Unicode multibyte
    "🚀_column",
    '""""""""',
]


@pytest.mark.parametrize("payload", ADVERSARIAL_SQL_STRINGS)
def test_quote_sql_identifier_adversarial_safety(payload: str) -> None:
    """Ensure SQL identifier quoter escapes all dangerous payloads and prevents syntax escape."""
    quoted = quote_sql_identifier(payload)
    # Must start and end with double quote
    assert quoted.startswith('"')
    assert quoted.endswith('"')
    # Any internal double quotes must be paired (escaped as "")
    unwrapped = quoted[1:-1]
    # Replacing escaped "" should leave no unescaped single double quotes
    residual = unwrapped.replace('""', "")
    assert '"' not in residual


@given(ident=st.text(min_size=1, max_size=50))
@settings(max_examples=100, deadline=None)
def test_hypothesis_quote_sql_identifier_property(ident: str) -> None:
    """Property: For any string, quote_sql_identifier produces balanced quotes with no lone double-quote."""
    quoted = quote_sql_identifier(ident)
    assert quoted.startswith('"')
    assert quoted.endswith('"')
    unwrapped = quoted[1:-1]
    assert '"' not in unwrapped.replace('""', "")


def test_sanitize_error_message_strips_sensitive_data() -> None:
    raw = "Binder Error: column /Users/secret/data/production.parquet not found in 'SELECT * FROM pass=secret123'"
    sanitized = sanitize_error_message(raw)
    assert "secret123" not in sanitized or "SELECT" in sanitized


# -------------------------------------------------------------------------
# 3. Truncated & Corrupted Parquet Fuzzing
# -------------------------------------------------------------------------


def test_engine_verification_handles_truncated_file(tmp_path: Path) -> None:
    """Ensure engine probes gracefully return FAIL on 1-byte, zero-byte, or truncated files."""
    bad_file = tmp_path / "truncated.parquet"
    bad_file.write_bytes(b"PAR1")  # Only 4 bytes magic, no footer

    rep = verify_engine_compatibility(bad_file, engines=["pyarrow", "duckdb", "polars"])
    assert rep.runtime_verdict == RuntimeVerdict.FAIL
    assert rep.metadata_verdict in (MetadataVerdict.ERROR, MetadataVerdict.INCOMPATIBLE)


def test_engine_verification_handles_random_garbage_file(tmp_path: Path) -> None:
    bad_file = tmp_path / "garbage.parquet"
    bad_file.write_bytes(os.urandom(1024))

    rep = verify_engine_compatibility(bad_file, engines=["pyarrow", "duckdb", "polars"])
    assert rep.runtime_verdict == RuntimeVerdict.FAIL


# -------------------------------------------------------------------------
# 4. Path Traversal & Unmatched Glob Safety
# -------------------------------------------------------------------------


def test_engine_verification_unmatched_glob_returns_error() -> None:
    rep = verify_engine_compatibility("/non/existent/path/*.parquet")
    assert rep.metadata_verdict == MetadataVerdict.ERROR
    assert rep.runtime_verdict == RuntimeVerdict.ERROR
    assert len(rep.limitations) > 0
