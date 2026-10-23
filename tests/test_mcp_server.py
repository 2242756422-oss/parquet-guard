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
