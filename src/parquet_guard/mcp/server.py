"""Model Context Protocol (MCP) Server for parquet-guard.

Implements JSON-RPC 2.0 over stdio transport compliant with MCP Specification (2024-11-05).
Compatible with Python 3.9+ without external heavyweight SDK dependencies.

Exposed Tools:
- `inspect_schema`: Extract structural schema, metadata, and row groups without scanning payload.
- `validate_contract`: Check Parquet file against a golden SchemaContract.
- `diff_schemas`: Compare target Parquet file against baseline Parquet file.
- `audit_dataset`: Audit schema consistency and grouping across partitioned directory.
- `analyze_consumer_impact`: Evaluate dataset schema against downstream consumer contracts.
- `verify_migration`: Execute candidate migration plan in an isolated synthetic sandbox.
- `generate_report`: Generate self-contained standalone HTML diagnostic report.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from parquet_guard.core.auditor import audit_dataset_consistency
from parquet_guard.core.compatibility import compare_schemas
from parquet_guard.core.consumer import (
    ConsumerContract,
    ConsumerGroupSpecification,
    evaluate_consumer_impact,
)
from parquet_guard.core.grouping import analyze_dataset_topology
from parquet_guard.core.html_reporter import generate_html_diagnostic_report
from parquet_guard.core.inspector import inspect_parquet_schema
from parquet_guard.core.migration_verifier import (
    ExecutableMigrationPlan,
    verify_migration_plan,
)
from parquet_guard.core.models import SchemaContract

SERVER_NAME = "parquet-guard-mcp"
SERVER_VERSION = "0.4.0a1"
PROTOCOL_VERSION = "2024-11-05"
SUPPORTED_PROTOCOL_VERSIONS = {"2024-11-05", "2025-03-26"}
MAX_JSON_PAYLOAD_BYTES = 10 * 1024 * 1024  # 10 MB bound for JSON contracts/plans
MAX_MCP_SAMPLE_ROWS = 10_000


class MCPSecurityError(Exception):
    """Raised when a requested path or operation violates security policy."""


def _resolve_safe_path(requested_path: Any, allowed_dirs: Optional[List[str]] = None) -> Path:
    """Resolve and validate path against directory boundaries, NUL bytes, and symlink escapes."""
    if not isinstance(requested_path, str) or not requested_path.strip() or "\x00" in requested_path:
        raise MCPSecurityError("Invalid or empty file path provided.")
    p = Path(requested_path).expanduser().resolve()
    if allowed_dirs:
        allowed_resolved = [Path(d).expanduser().resolve() for d in allowed_dirs]
        if not any(p == allowed or allowed in p.parents for allowed in allowed_resolved):
            raise MCPSecurityError(
                f"Path '{requested_path}' is outside permitted directory boundaries: {allowed_dirs}"
            )
    return p


def _read_bounded_json(path: Path, max_bytes: int = MAX_JSON_PAYLOAD_BYTES) -> Dict[str, Any]:
    """Read and parse a JSON file after verifying size bounds."""
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"JSON file not found: '{path}'")
    size = path.stat().st_size
    if size > max_bytes:
        raise MCPSecurityError(
            f"File '{path.name}' size ({size} bytes) exceeds maximum allowed JSON payload ({max_bytes} bytes)."
        )
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object in '{path.name}', got {type(data).__name__}.")
    return data


TOOLS_DEFINITIONS: List[Dict[str, Any]] = [
    {
        "name": "inspect_schema",
        "description": "Inspect structural schema, column types, and metadata of a Parquet file without reading data rows.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "file_path": {"type": "string", "description": "Path to the Parquet file to inspect"}
            },
            "required": ["file_path"],
        },
    },
    {
        "name": "validate_contract",
        "description": "Validate a Parquet file's schema against a golden SchemaContract specification.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "file_path": {"type": "string", "description": "Path to the target Parquet file"},
                "contract_path": {"type": "string", "description": "Path to golden SchemaContract JSON file"},
                "mode": {
                    "type": "string",
                    "enum": ["strict", "permissive"],
                    "default": "permissive",
                    "description": "Validation mode ('strict' forbids widening, 'permissive' allows safe widening)",
                },
            },
            "required": ["file_path", "contract_path"],
        },
    },
    {
        "name": "diff_schemas",
        "description": "Compare and diff schemas between a target Parquet file and a baseline Parquet file.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "target_path": {"type": "string", "description": "Path to target Parquet file"},
                "baseline_path": {"type": "string", "description": "Path to baseline Parquet file"},
                "mode": {"type": "string", "enum": ["strict", "permissive"], "default": "permissive"},
            },
            "required": ["target_path", "baseline_path"],
        },
    },
    {
        "name": "audit_dataset",
        "description": "Audit schema consistency, schema grouping, and drift concentration across all Parquet files in a directory.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dataset_dir": {"type": "string", "description": "Directory path containing Parquet files"},
                "recursive": {
                    "type": "boolean",
                    "default": True,
                    "description": "Scan subdirectories recursively",
                },
            },
            "required": ["dataset_dir"],
        },
    },
    {
        "name": "analyze_consumer_impact",
        "description": "Evaluate an upstream Parquet schema against declared downstream consumer contracts (e.g. DuckDB, Polars, PyArrow ETL).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "file_path": {"type": "string", "description": "Path to representative Parquet file"},
                "consumers_path": {
                    "type": "string",
                    "description": "Path to ConsumerContract or ConsumerGroupSpecification JSON file",
                },
            },
            "required": ["file_path", "consumers_path"],
        },
    },
    {
        "name": "verify_migration",
        "description": "Execute a candidate migration plan against representative synthetic data in an isolated sandbox to verify row count, schema, value invariants, and consumer compatibility.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "migration_plan_path": {
                    "type": "string",
                    "description": "Path to ExecutableMigrationPlan JSON",
                },
                "consumers_path": {
                    "type": "string",
                    "description": "Optional path to ConsumerContract or ConsumerGroupSpecification JSON",
                },
                "sample_rows": {
                    "type": "integer",
                    "default": 50,
                    "minimum": 1,
                    "maximum": MAX_MCP_SAMPLE_ROWS,
                    "description": f"Number of synthetic rows to test (1..{MAX_MCP_SAMPLE_ROWS})",
                },
            },
            "required": ["migration_plan_path"],
        },
    },
    {
        "name": "generate_report",
        "description": "Generate a self-contained, standalone HTML diagnostic report for a dataset and consumer impact results.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "output_html": {"type": "string", "description": "Destination path for generated .html file"},
                "dataset_dir": {"type": "string", "description": "Directory path of audited dataset"},
                "consumers_path": {"type": "string", "description": "Optional consumer contract JSON path"},
                "overwrite": {
                    "type": "boolean",
                    "default": False,
                    "description": "Allow overwriting an existing HTML file (default False for safety)",
                },
            },
            "required": ["output_html", "dataset_dir"],
        },
    },
    {
        "name": "verify_engine_compatibility",
        "description": "Probe and verify runtime query engine compatibility (PyArrow, DuckDB, Polars) on Parquet files, detecting rule vs runtime disagreements.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "file_path": {"type": "string", "description": "Path to Parquet file or directory"},
                "contract_path": {"type": "string", "description": "Optional path to SchemaContract JSON"},
                "row_limit": {
                    "type": "integer",
                    "default": 10,
                    "description": "Sample row read limit for runtime probes",
                },
            },
            "required": ["file_path"],
        },
    },
]

_TOOL_SCHEMA_MAP: Dict[str, Dict[str, Any]] = {t["name"]: t["inputSchema"] for t in TOOLS_DEFINITIONS}


class MCPServer:
    """JSON-RPC 2.0 stdio server handling MCP protocol interactions."""

    def __init__(
        self,
        allowed_dirs: Optional[List[str]] = None,
        allowed_write_dirs: Optional[List[str]] = None,
    ) -> None:
        self.allowed_dirs = allowed_dirs or [os.getcwd()]
        self.allowed_write_dirs = allowed_write_dirs or self.allowed_dirs

    def _validate_tool_arguments(self, name: str, arguments: Dict[str, Any]) -> Optional[str]:
        """Validate required parameters and basic JSON schema types for tool arguments."""
        schema = _TOOL_SCHEMA_MAP.get(name)
        if not schema:
            return None
        required = schema.get("required", [])
        for req_key in required:
            if req_key not in arguments or arguments[req_key] is None:
                return f"Missing required argument '{req_key}' for tool '{name}'."
        props = schema.get("properties", {})
        for k, val in arguments.items():
            if k not in props:
                return f"Unknown argument '{k}' for tool '{name}'."
            expected_type = props[k].get("type")
            if expected_type == "string" and not isinstance(val, str):
                return f"Argument '{k}' must be a string."
            if expected_type == "boolean" and not isinstance(val, bool):
                return f"Argument '{k}' must be a boolean."
            if expected_type == "integer" and (isinstance(val, bool) or not isinstance(val, int)):
                return f"Argument '{k}' must be an integer."
            if "enum" in props[k] and val not in props[k]["enum"]:
                return f"Argument '{k}' must be one of {props[k]['enum']}."
        return None

    def handle_tool_call(self, name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Dispatch and execute an MCP tool call."""
        try:
            if name not in _TOOL_SCHEMA_MAP:
                return {
                    "isError": True,
                    "content": [{"type": "text", "text": f"Unknown tool: '{name}'"}],
                }

            arg_err = self._validate_tool_arguments(name, arguments)
            if arg_err:
                return {
                    "isError": True,
                    "content": [{"type": "text", "text": f"Invalid tool arguments: {arg_err}"}],
                }

            if name == "inspect_schema":
                p = _resolve_safe_path(arguments["file_path"], self.allowed_dirs)
                contract = inspect_parquet_schema(str(p))
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": contract.model_dump_json(indent=2),
                        }
                    ]
                }

            elif name == "validate_contract":
                f_path = _resolve_safe_path(arguments["file_path"], self.allowed_dirs)
                c_path = _resolve_safe_path(arguments["contract_path"], self.allowed_dirs)
                mode = arguments.get("mode", "permissive")

                contract_data = _read_bounded_json(c_path)
                contract = SchemaContract.model_validate(contract_data)
                actual_schema = inspect_parquet_schema(str(f_path))
                report = compare_schemas(actual_schema, contract, mode=mode)
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": report.model_dump_json(indent=2),
                        }
                    ]
                }

            elif name == "diff_schemas":
                t_path = _resolve_safe_path(arguments["target_path"], self.allowed_dirs)
                b_path = _resolve_safe_path(arguments["baseline_path"], self.allowed_dirs)
                mode = arguments.get("mode", "permissive")

                t_schema = inspect_parquet_schema(str(t_path))
                b_schema = inspect_parquet_schema(str(b_path))
                report = compare_schemas(t_schema, b_schema, mode=mode)
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": report.model_dump_json(indent=2),
                        }
                    ]
                }

            elif name == "audit_dataset":
                d_path = _resolve_safe_path(arguments["dataset_dir"], self.allowed_dirs)
                recursive = arguments.get("recursive", True)
                audit_res = audit_dataset_consistency(str(d_path), recursive=recursive)
                topology = analyze_dataset_topology(audit_res)
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": topology.model_dump_json(indent=2),
                        }
                    ]
                }

            elif name == "analyze_consumer_impact":
                f_path = _resolve_safe_path(arguments["file_path"], self.allowed_dirs)
                c_path = _resolve_safe_path(arguments["consumers_path"], self.allowed_dirs)

                schema = inspect_parquet_schema(str(f_path))
                c_data = _read_bounded_json(c_path)

                if "consumers" in c_data:
                    group = ConsumerGroupSpecification.model_validate(c_data)
                    consumers = group.consumers
                else:
                    consumers = [ConsumerContract.model_validate(c_data)]

                impact = evaluate_consumer_impact(schema, consumers, dataset_identifier=f_path.name)
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": impact.model_dump_json(indent=2),
                        }
                    ]
                }

            elif name == "verify_migration":
                m_path = _resolve_safe_path(arguments["migration_plan_path"], self.allowed_dirs)
                sample_rows = int(arguments.get("sample_rows", 50))
                if sample_rows < 1 or sample_rows > MAX_MCP_SAMPLE_ROWS:
                    raise MCPSecurityError(
                        f"sample_rows ({sample_rows}) is outside allowed bounds [1, {MAX_MCP_SAMPLE_ROWS}]."
                    )
                m_data = _read_bounded_json(m_path)
                plan = ExecutableMigrationPlan.model_validate(m_data)

                consumers_list: Optional[List[ConsumerContract]] = None
                consumers_path_str = arguments.get("consumers_path")
                if consumers_path_str:
                    c_path = _resolve_safe_path(consumers_path_str, self.allowed_dirs)
                    c_data = _read_bounded_json(c_path)
                    if "consumers" in c_data:
                        consumers_list = ConsumerGroupSpecification.model_validate(c_data).consumers
                    else:
                        consumers_list = [ConsumerContract.model_validate(c_data)]

                verif_res = verify_migration_plan(plan, consumers=consumers_list, sample_rows=sample_rows)
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": verif_res.model_dump_json(indent=2),
                        }
                    ]
                }

            elif name == "generate_report":
                out_path = _resolve_safe_path(arguments["output_html"], self.allowed_write_dirs)
                if out_path.suffix.lower() not in (".html", ".htm"):
                    raise MCPSecurityError(
                        f"Output report path '{out_path.name}' must end with '.html' or '.htm'."
                    )
                overwrite = bool(arguments.get("overwrite", False))
                if out_path.exists() and not overwrite:
                    raise MCPSecurityError(
                        f"Destination file '{out_path}' already exists and overwrite=False."
                    )

                d_path = _resolve_safe_path(arguments["dataset_dir"], self.allowed_dirs)
                consumers_path_str = arguments.get("consumers_path")

                consumers = None
                if consumers_path_str:
                    c_path = _resolve_safe_path(consumers_path_str, self.allowed_dirs)
                    c_data = _read_bounded_json(c_path)
                    if "consumers" in c_data:
                        consumers = ConsumerGroupSpecification.model_validate(c_data).consumers
                    else:
                        consumers = [ConsumerContract.model_validate(c_data)]

                audit_res = audit_dataset_consistency(str(d_path))
                topology = analyze_dataset_topology(audit_res, consumers=consumers)

                impact_rep = None
                if consumers and audit_res.canonical_schema:
                    impact_rep = evaluate_consumer_impact(
                        audit_res.canonical_schema, consumers, dataset_identifier=d_path.name
                    )

                generate_html_diagnostic_report(
                    output_path=str(out_path),
                    audit_result=audit_res,
                    impact_report=impact_rep,
                    topology_report=topology,
                    dataset_title=d_path.name,
                )
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": f"Successfully generated HTML report at: {out_path}",
                        }
                    ]
                }

            elif name == "verify_engine_compatibility":
                from parquet_guard.verification import verify_engine_compatibility

                f_path = _resolve_safe_path(arguments["file_path"], self.allowed_dirs)
                c_path_str = arguments.get("contract_path")
                contract_obj = None
                if c_path_str:
                    c_path = _resolve_safe_path(c_path_str, self.allowed_dirs)
                    c_data = _read_bounded_json(c_path)
                    contract_obj = SchemaContract.model_validate(c_data)

                row_lim = int(arguments.get("row_limit", 10))
                verif_report = verify_engine_compatibility(
                    f_path,
                    contract=contract_obj,
                    row_limit=row_lim,
                )
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": verif_report.model_dump_json(indent=2),
                        }
                    ]
                }

            return {
                "isError": True,
                "content": [{"type": "text", "text": f"Unknown tool: '{name}'"}],
            }

        except Exception as exc:
            return {
                "isError": True,
                "content": [{"type": "text", "text": f"Error executing tool '{name}': {exc}"}],
            }

    def process_message(self, message: Any) -> Optional[Dict[str, Any]]:
        """Process a single JSON-RPC 2.0 request and return response."""
        if not isinstance(message, dict):
            return {
                "jsonrpc": "2.0",
                "id": None,
                "error": {"code": -32600, "message": "Invalid Request: expected JSON object."},
            }

        req_id = message.get("id")
        is_notification = "id" not in message
        jsonrpc_ver = message.get("jsonrpc")
        method = message.get("method")
        params = message.get("params", {})

        if jsonrpc_ver != "2.0" or not isinstance(method, str) or not method:
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {
                    "code": -32600,
                    "message": "Invalid Request: require jsonrpc='2.0' and non-empty string 'method'.",
                },
            }

        if params is not None and not isinstance(params, dict):
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32602, "message": "Invalid params: 'params' must be an object."},
            }
        params = params or {}

        if method == "initialize":
            requested_proto = params.get("protocolVersion", PROTOCOL_VERSION)
            negotiated_proto = (
                requested_proto if requested_proto in SUPPORTED_PROTOCOL_VERSIONS else PROTOCOL_VERSION
            )
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "protocolVersion": negotiated_proto,
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
                },
            }

        elif method in ("notifications/initialized", "notifications/cancelled"):
            return None

        elif method == "ping":
            return {"jsonrpc": "2.0", "id": req_id, "result": {}}

        elif method == "tools/list":
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"tools": TOOLS_DEFINITIONS},
            }

        elif method == "tools/call":
            tool_name = params.get("name")
            tool_args = params.get("arguments", {})
            if not isinstance(tool_name, str) or not tool_name:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {
                        "code": -32602,
                        "message": "Invalid params: 'name' must be a non-empty string.",
                    },
                }
            if not isinstance(tool_args, dict):
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {
                        "code": -32602,
                        "message": "Invalid params: 'arguments' must be an object.",
                    },
                }
            res = self.handle_tool_call(tool_name, tool_args)
            return {"jsonrpc": "2.0", "id": req_id, "result": res}

        else:
            if is_notification and method.startswith("notifications/"):
                return None
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {
                    "code": -32601,
                    "message": f"Method '{method}' not found",
                },
            }

    def run_stdio(self) -> None:
        """Run the MCP server reading line-delimited JSON-RPC from sys.stdin."""
        sys.stderr.write(f"Starting {SERVER_NAME} v{SERVER_VERSION} (stdio mode)...\n")
        sys.stderr.flush()

        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError as exc:
                err_resp = {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {
                        "code": -32700,
                        "message": f"Parse error: {exc}",
                    },
                }
                sys.stdout.write(json.dumps(err_resp) + "\n")
                sys.stdout.flush()
                continue

            try:
                resp = self.process_message(msg)
                if resp is not None:
                    sys.stdout.write(json.dumps(resp) + "\n")
                    sys.stdout.flush()
            except Exception as exc:
                req_id = msg.get("id") if isinstance(msg, dict) else None
                err_resp = {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {
                        "code": -32603,
                        "message": f"Internal error: {exc}",
                    },
                }
                sys.stdout.write(json.dumps(err_resp) + "\n")
                sys.stdout.flush()
