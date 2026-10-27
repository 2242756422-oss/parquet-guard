"""Unit and protocol integration tests for Model Context Protocol (MCP) server."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from parquet_guard.mcp.server import (
    PROTOCOL_VERSION,
    SERVER_NAME,
    SERVER_VERSION,
    MCPServer,
)


def _create_sample_parquet(path: Path) -> None:
    table = pa.table(
        {
            "user_id": pa.array([1, 2, 3], type=pa.int64()),
            "email": pa.array(["a@ex.com", "b@ex.com", "c@ex.com"], type=pa.string()),
        }
    )
    pq.write_table(table, str(path))


def test_mcp_handshake_and_ping() -> None:
    """Test MCP protocol handshake and ping messages."""
    server = MCPServer()

    # 1. Initialize
    init_msg = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": {"name": "test-client", "version": "1.0"},
        },
    }
    resp = server.process_message(init_msg)
    assert resp is not None
    assert resp["jsonrpc"] == "2.0"
    assert resp["id"] == 1
    assert resp["result"]["serverInfo"]["name"] == SERVER_NAME
    assert resp["result"]["serverInfo"]["version"] == SERVER_VERSION
    assert "tools" in resp["result"]["capabilities"]

    # 2. Initialized notification
    notif_msg = {"jsonrpc": "2.0", "method": "notifications/initialized"}
    assert server.process_message(notif_msg) is None

    # 3. Ping
    ping_msg = {"jsonrpc": "2.0", "id": 2, "method": "ping"}
    ping_resp = server.process_message(ping_msg)
    assert ping_resp is not None
    assert ping_resp["id"] == 2
    assert ping_resp["result"] == {}


def test_mcp_tools_list() -> None:
    """Verify tool definitions returned by tools/list."""
    server = MCPServer()
    list_msg = {"jsonrpc": "2.0", "id": 3, "method": "tools/list"}
    resp = server.process_message(list_msg)
    assert resp is not None
    tools = resp["result"]["tools"]
    tool_names = {t["name"] for t in tools}
    assert {
        "inspect_schema",
        "validate_contract",
        "diff_schemas",
        "audit_dataset",
        "analyze_consumer_impact",
        "verify_migration",
        "generate_report",
    }.issubset(tool_names)


def test_mcp_tools_call_inspect_and_diff(tmp_path: Path) -> None:
    """Test executing inspect_schema and diff_schemas through MCP protocol."""
    server = MCPServer(allowed_dirs=[str(tmp_path)])

    p1 = tmp_path / "t1.parquet"
    p2 = tmp_path / "t2.parquet"
    _create_sample_parquet(p1)
    _create_sample_parquet(p2)

    # 1. Call inspect_schema
    call_inspect = {
        "jsonrpc": "2.0",
        "id": 10,
        "method": "tools/call",
        "params": {
            "name": "inspect_schema",
            "arguments": {"file_path": str(p1)},
        },
    }
    resp = server.process_message(call_inspect)
    assert resp is not None
    assert "result" in resp
    content = resp["result"]["content"][0]["text"]
    assert "user_id" in content
    assert "int64" in content

    # 2. Call diff_schemas
    call_diff = {
        "jsonrpc": "2.0",
        "id": 11,
        "method": "tools/call",
        "params": {
            "name": "diff_schemas",
            "arguments": {"target_path": str(p1), "baseline_path": str(p2)},
        },
    }
    diff_resp = server.process_message(call_diff)
    assert diff_resp is not None
    assert "VALID" in diff_resp["result"]["content"][0]["text"]


def test_mcp_path_traversal_sandboxing(tmp_path: Path) -> None:
    """Verify MCP server rejects paths outside allowed directory boundary."""
    allowed = tmp_path / "safe"
    allowed.mkdir()
    restricted = tmp_path / "restricted"
    restricted.mkdir()
    secret_file = restricted / "secret.parquet"
    _create_sample_parquet(secret_file)

    server = MCPServer(allowed_dirs=[str(allowed)])

    # Attempting to read outside allowed boundary
    msg = {
        "jsonrpc": "2.0",
        "id": 99,
        "method": "tools/call",
        "params": {
            "name": "inspect_schema",
            "arguments": {"file_path": str(secret_file)},
        },
    }
    resp = server.process_message(msg)
    assert resp is not None
    assert resp["result"].get("isError") is True
    assert "outside permitted directory boundaries" in resp["result"]["content"][0]["text"]


def test_mcp_unknown_tool_and_method() -> None:
    """Verify error handling for unknown tools and methods."""
    server = MCPServer()

    # Unknown tool
    msg_tool = {
        "jsonrpc": "2.0",
        "id": 100,
        "method": "tools/call",
        "params": {"name": "unsupported_tool", "arguments": {}},
    }
    resp_tool = server.process_message(msg_tool)
    assert resp_tool is not None
    assert resp_tool["result"]["isError"] is True
    assert "Unknown tool" in resp_tool["result"]["content"][0]["text"]

    # Unknown JSON-RPC method
    msg_method = {"jsonrpc": "2.0", "id": 101, "method": "invalid/method"}
    resp_method = server.process_message(msg_method)
    assert resp_method is not None
    assert "error" in resp_method
    assert resp_method["error"]["code"] == -32601


def test_mcp_report_overwrite_and_resource_bounds(tmp_path: Path) -> None:
    """Verify generate_report refuses to overwrite existing files by default and verify_migration bounds sample_rows."""
    import json

    server = MCPServer(allowed_dirs=[str(tmp_path)])
    p1 = tmp_path / "data.parquet"
    _create_sample_parquet(p1)

    report_html = tmp_path / "diag.html"
    report_html.write_text("existing content", encoding="utf-8")

    # 1. Calling generate_report when file exists and overwrite=False (default) must fail
    msg_no_overwrite = {
        "jsonrpc": "2.0",
        "id": 201,
        "method": "tools/call",
        "params": {
            "name": "generate_report",
            "arguments": {
                "output_html": str(report_html),
                "dataset_dir": str(tmp_path),
            },
        },
    }
    resp1 = server.process_message(msg_no_overwrite)
    assert resp1 is not None
    assert resp1["result"]["isError"] is True
    assert "already exists" in resp1["result"]["content"][0]["text"]
    assert report_html.read_text(encoding="utf-8") == "existing content"

    # 2. Calling generate_report with non-HTML extension must fail
    msg_bad_ext = {
        "jsonrpc": "2.0",
        "id": 202,
        "method": "tools/call",
        "params": {
            "name": "generate_report",
            "arguments": {
                "output_html": str(tmp_path / "script.py"),
                "dataset_dir": str(tmp_path),
            },
        },
    }
    resp2 = server.process_message(msg_bad_ext)
    assert resp2 is not None
    assert resp2["result"]["isError"] is True
    assert "must end with '.html'" in resp2["result"]["content"][0]["text"]

    # 3. Calling generate_report with overwrite=True succeeds
    msg_overwrite = {
        "jsonrpc": "2.0",
        "id": 203,
        "method": "tools/call",
        "params": {
            "name": "generate_report",
            "arguments": {
                "output_html": str(report_html),
                "dataset_dir": str(tmp_path),
                "overwrite": True,
            },
        },
    }
    resp3 = server.process_message(msg_overwrite)
    assert resp3 is not None
    assert resp3["result"].get("isError") is not True
    assert "<!DOCTYPE html>" in report_html.read_text(encoding="utf-8")

    # 4. verify_migration bounds sample_rows
    plan_file = tmp_path / "plan.json"
    plan_file.write_text(
        json.dumps(
            {
                "plan_id": "M1",
                "title": "t",
                "description": "d",
                "source_contract": {"fields": [{"name": "user_id", "type": "int64"}]},
                "target_contract": {"fields": [{"name": "user_id", "type": "int64"}]},
                "operations": [],
            }
        ),
        encoding="utf-8",
    )
    msg_excessive_rows = {
        "jsonrpc": "2.0",
        "id": 204,
        "method": "tools/call",
        "params": {
            "name": "verify_migration",
            "arguments": {
                "migration_plan_path": str(plan_file),
                "sample_rows": 999999,
            },
        },
    }
    resp4 = server.process_message(msg_excessive_rows)
    assert resp4 is not None
    assert resp4["result"]["isError"] is True
    assert "outside allowed bounds" in resp4["result"]["content"][0]["text"]


def test_mcp_real_subprocess_stdio_roundtrip(tmp_path: Path) -> None:
    """Launch `parquet-guard serve-mcp` as a real OS subprocess and verify stdio JSON-RPC framing & error codes."""
    import json
    import subprocess
    import sys

    sample_file = tmp_path / "events.parquet"
    _create_sample_parquet(sample_file)

    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "parquet_guard.cli",
            "serve-mcp",
            "--allowed-dir",
            str(tmp_path),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert proc.stdin is not None
    assert proc.stdout is not None

    from typing import Any

    def send_and_recv(payload_str: str) -> dict[str, Any]:
        assert proc.stdin is not None and proc.stdout is not None
        proc.stdin.write(payload_str + "\n")
        proc.stdin.flush()
        line = proc.stdout.readline()
        assert line, "Expected JSON-RPC response line from subprocess stdout"
        parsed: dict[str, Any] = json.loads(line)
        return parsed

    try:
        # 1. Initialize handshake
        r_init = send_and_recv(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {"protocolVersion": PROTOCOL_VERSION},
                }
            )
        )
        assert r_init["id"] == 1
        assert r_init["result"]["serverInfo"]["name"] == SERVER_NAME

        # 2. Send notifications/initialized (no response expected) + ping
        proc.stdin.write(json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}) + "\n")
        proc.stdin.flush()
        r_ping = send_and_recv(json.dumps({"jsonrpc": "2.0", "id": 2, "method": "ping"}))
        assert r_ping["id"] == 2
        assert r_ping["result"] == {}

        # 3. tools/list
        r_list = send_and_recv(json.dumps({"jsonrpc": "2.0", "id": 3, "method": "tools/list"}))
        assert r_list["id"] == 3
        assert len(r_list["result"]["tools"]) == 8

        # 4. tools/call inspect_schema
        r_call = send_and_recv(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 4,
                    "method": "tools/call",
                    "params": {
                        "name": "inspect_schema",
                        "arguments": {"file_path": str(sample_file)},
                    },
                }
            )
        )
        assert r_call["id"] == 4
        assert r_call["result"].get("isError") is not True
        assert "user_id" in r_call["result"]["content"][0]["text"]

        # 5. Malformed JSON line -> -32700 Parse error
        r_parse_err = send_and_recv("{not valid json")
        assert r_parse_err["error"]["code"] == -32700

        # 6. Invalid JSON-RPC structure -> -32600 Invalid Request
        r_inv_req = send_and_recv(json.dumps({"jsonrpc": "1.0", "id": 6, "method": "ping"}))
        assert r_inv_req["error"]["code"] == -32600

        # 7. Invalid params on tools/call -> -32602 Invalid params
        r_inv_params = send_and_recv(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 7,
                    "method": "tools/call",
                    "params": {"name": "", "arguments": "not-a-dict"},
                }
            )
        )
        assert r_inv_params["error"]["code"] == -32602

        # 8. Missing required argument inside tool arguments -> tool error
        r_missing_arg = send_and_recv(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 8,
                    "method": "tools/call",
                    "params": {"name": "inspect_schema", "arguments": {}},
                }
            )
        )
        assert r_missing_arg["result"]["isError"] is True
        assert "Missing required argument" in r_missing_arg["result"]["content"][0]["text"]

    finally:
        proc.stdin.close()
        proc.wait(timeout=5)
    assert proc.returncode == 0


def test_mcp_verify_engine_compatibility(tmp_path: Path) -> None:
    """Test verify_engine_compatibility tool execution over MCP server."""
    import json

    pq_file = tmp_path / "test.parquet"
    _create_sample_parquet(pq_file)

    server = MCPServer(allowed_dirs=[str(tmp_path)])
    call_msg = {
        "jsonrpc": "2.0",
        "id": 10,
        "method": "tools/call",
        "params": {
            "name": "verify_engine_compatibility",
            "arguments": {
                "file_path": str(pq_file),
                "row_limit": 5,
            },
        },
    }
    resp = server.process_message(call_msg)
    assert resp is not None
    assert resp.get("error") is None
    result = resp.get("result", {})
    assert result.get("isError") is not True
    content_text = result["content"][0]["text"]
    data = json.loads(content_text)
    assert data["runtime_verdict"] == "PASS"
