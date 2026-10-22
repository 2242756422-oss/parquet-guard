"""Integration tests for GitHub Composite Action behavior.

Verifies:
1. Safe argument handling with zero shell and zero Python code injection risk.
2. Adversarial path handling (spaces, quotes, semicolons, Python delimiters, Unicode).
3. Verdict extraction to $GITHUB_OUTPUT across VALID, INCOMPATIBLE, DRIFTED, and INSPECTION_ERROR states.
4. Deterministic non-zero exit codes on schema breaking regressions and fail-on-warnings.
5. SARIF report generation even when the audit fails.
6. Validation of every action input (mode, format, fail-on-warnings, install) with predictable exit codes.
7. Action metadata and YAML structure validity.
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ACTION_YML_PATH = Path(__file__).parent.parent / ".github" / "actions" / "parquet-guard" / "action.yml"


def _extract_action_script() -> str:
    """Extract the embedded bash script from action.yml without external yaml dependency."""
    content = ACTION_YML_PATH.read_text(encoding="utf-8")
    run_marker = "run: |\n"
    idx = content.find(run_marker)
    if idx == -1:
        raise ValueError("run: | marker not found in action.yml")
    lines = content[idx + len(run_marker) :].splitlines()
    script_lines: list[str] = []
    for line in lines:
        if line.startswith("        ") or line.strip() == "":
            script_lines.append(line[8:] if line.startswith("        ") else "")
        else:
            break
    return "\n".join(script_lines)


def run_action_script(
    env_vars: dict[str, str],
    cwd: Path,
) -> subprocess.CompletedProcess[str]:
    """Execute the composite action's bash script with specified environment variables."""
    script = _extract_action_script()
    full_env = os.environ.copy()
    full_env.update(env_vars)
    full_env["ACTION_PATH"] = str(ACTION_YML_PATH.parent)

    return subprocess.run(
        ["bash", "-c", script],
        cwd=str(cwd),
        env=full_env,
        capture_output=True,
        text=True,
    )


def test_action_script_valid_dataset(tmp_path: Path):
    """Test action on a consistent Parquet dataset."""
    p1 = tmp_path / "part-1.parquet"
    p2 = tmp_path / "part-2.parquet"
    table = pa.Table.from_pydict({"id": [1, 2], "val": ["a", "b"]})
    pq.write_table(table, p1)
    pq.write_table(table, p2)

    github_output = tmp_path / "github_output.txt"
    sarif_file = tmp_path / "audit.sarif"

    env = {
        "INPUT_PATH": str(tmp_path),
        "INPUT_MODE": "strict",
        "INPUT_FORMAT": "text",
        "INPUT_SARIF_FILE": str(sarif_file),
        "INPUT_FAIL_ON_WARNINGS": "false",
        "INPUT_INSTALL": "false",
        "INPUT_CONTRACT": "",
        "GITHUB_OUTPUT": str(github_output),
    }

    proc = run_action_script(env, tmp_path)
    assert proc.returncode == 0, f"STDOUT: {proc.stdout}\nSTDERR: {proc.stderr}"

    output_lines = github_output.read_text(encoding="utf-8")
    assert "verdict=VALID" in output_lines

    assert sarif_file.is_file()
    sarif_data = json.loads(sarif_file.read_text(encoding="utf-8"))
    assert sarif_data["version"] == "2.1.0"
    assert len(sarif_data["runs"][0]["results"]) == 0


def test_action_script_incompatible_dataset_generates_sarif_and_fails(tmp_path: Path):
    """Ensure action writes SARIF report and fails CI when schemas diverge."""
    p1 = tmp_path / "part-1.parquet"
    p2 = tmp_path / "part-2.parquet"
    t1 = pa.Table.from_pydict({"id": [1, 2], "val": ["a", "b"]})
    t2 = pa.Table.from_pydict({"id": [1, 2], "val": [100, 200]})  # string vs int64
    pq.write_table(t1, p1)
    pq.write_table(t2, p2)

    github_output = tmp_path / "github_output.txt"
    sarif_file = tmp_path / "audit.sarif"

    env = {
        "INPUT_PATH": str(tmp_path),
        "INPUT_MODE": "strict",
        "INPUT_FORMAT": "text",
        "INPUT_SARIF_FILE": str(sarif_file),
        "INPUT_FAIL_ON_WARNINGS": "false",
        "INPUT_INSTALL": "false",
        "INPUT_CONTRACT": "",
        "GITHUB_OUTPUT": str(github_output),
    }

    proc = run_action_script(env, tmp_path)
    # Must exit with non-zero (1) to fail CI
    assert proc.returncode == 1

    output_lines = github_output.read_text(encoding="utf-8")
    assert "verdict=INCOMPATIBLE" in output_lines

    # SARIF file MUST still be produced and non-empty
    assert sarif_file.is_file()
    sarif_data = json.loads(sarif_file.read_text(encoding="utf-8"))
    assert sarif_data["version"] == "2.1.0"
    assert len(sarif_data["runs"][0]["results"]) > 0


def test_action_script_python_code_injection_mitigation(tmp_path: Path):
    """Adversarial test: prove user-controlled sarif-file path cannot inject Python code or create sentinels."""
    p1 = tmp_path / "part-1.parquet"
    table = pa.Table.from_pydict({"x": [1, 2]})
    pq.write_table(table, p1)

    sentinel_file = tmp_path / "sentinel_should_not_exist.txt"
    # Malicious path containing Python string escape, semicolon, import, and OS commands
    malicious_sarif = (
        tmp_path
        / "test'); import os, pathlib; pathlib.Path('sentinel_should_not_exist.txt').write_text('pwned'); ('"
    )

    github_output = tmp_path / "github_output.txt"

    env = {
        "INPUT_PATH": str(tmp_path),
        "INPUT_MODE": "strict",
        "INPUT_FORMAT": "text",
        "INPUT_SARIF_FILE": str(malicious_sarif),
        "INPUT_FAIL_ON_WARNINGS": "false",
        "INPUT_INSTALL": "false",
        "INPUT_CONTRACT": "",
        "GITHUB_OUTPUT": str(github_output),
    }

    proc = run_action_script(env, tmp_path)
    assert proc.returncode == 0, f"STDOUT: {proc.stdout}\nSTDERR: {proc.stderr}"

    # CRITICAL SECURITY ASSERTION: The sentinel file MUST NOT exist
    assert not sentinel_file.exists(), "Code injection vulnerability detected! Sentinel file was created."
    # The literal file with weird punctuation was created safely by Path
    assert malicious_sarif.exists()


def test_action_script_paths_with_spaces_and_unicode(tmp_path: Path):
    """Test safe handling of paths with spaces, single quotes, and Unicode characters."""
    dataset_dir = tmp_path / "数据 分区 '2026'"
    dataset_dir.mkdir(parents=True)
    p1 = dataset_dir / "文件 01.parquet"
    table = pa.Table.from_pydict({"col": ["alpha", "beta"]})
    pq.write_table(table, p1)

    sarif_file = tmp_path / "输出 目录 'dir'" / "报告.sarif"
    github_output = tmp_path / "github_output.txt"

    env = {
        "INPUT_PATH": str(dataset_dir),
        "INPUT_MODE": "strict",
        "INPUT_FORMAT": "text",
        "INPUT_SARIF_FILE": str(sarif_file),
        "INPUT_FAIL_ON_WARNINGS": "false",
        "INPUT_INSTALL": "false",
        "INPUT_CONTRACT": "",
        "GITHUB_OUTPUT": str(github_output),
    }

    proc = run_action_script(env, tmp_path)
    assert proc.returncode == 0, f"STDOUT: {proc.stdout}\nSTDERR: {proc.stderr}"
    assert sarif_file.is_file()
    sarif_data = json.loads(sarif_file.read_text(encoding="utf-8"))
    assert sarif_data["version"] == "2.1.0"


def test_action_script_drifted_verdict_and_fail_on_warnings(tmp_path: Path):
    """Test DRIFTED verdict under permissive mode with fail-on-warnings true and false."""
    # Create baseline with field 'a', second partition adds nullable 'b' (permissive drift)
    p1 = tmp_path / "part-1.parquet"
    p2 = tmp_path / "part-2.parquet"
    t1 = pa.Table.from_pydict({"a": [1, 2]})
    t2 = pa.Table.from_pydict({"a": [3, 4], "b": ["x", "y"]})
    pq.write_table(t1, p1)
    pq.write_table(t2, p2)

    github_output = tmp_path / "github_output.txt"

    # Case 1: fail-on-warnings=false -> exit code 0
    env_pass = {
        "INPUT_PATH": str(tmp_path),
        "INPUT_MODE": "permissive",
        "INPUT_FORMAT": "json",
        "INPUT_SARIF_FILE": "",
        "INPUT_FAIL_ON_WARNINGS": "false",
        "INPUT_INSTALL": "false",
        "INPUT_CONTRACT": "",
        "GITHUB_OUTPUT": str(github_output),
    }
    proc1 = run_action_script(env_pass, tmp_path)
    assert proc1.returncode == 0
    assert "verdict=DRIFTED" in github_output.read_text(encoding="utf-8")

    # Case 2: fail-on-warnings=true -> exit code 1
    env_fail = {
        "INPUT_PATH": str(tmp_path),
        "INPUT_MODE": "permissive",
        "INPUT_FORMAT": "json",
        "INPUT_SARIF_FILE": "",
        "INPUT_FAIL_ON_WARNINGS": "true",
        "INPUT_INSTALL": "false",
        "INPUT_CONTRACT": "",
        "GITHUB_OUTPUT": str(github_output),
    }
    proc2 = run_action_script(env_fail, tmp_path)
    assert proc2.returncode == 1
    assert "verdict=DRIFTED" in github_output.read_text(encoding="utf-8")


def test_action_script_inspection_error_missing_files(tmp_path: Path):
    """Test exit code 2 and INSPECTION_ERROR verdict on missing target path."""
    missing_dir = tmp_path / "nonexistent_parquet_dir"
    github_output = tmp_path / "github_output.txt"

    env = {
        "INPUT_PATH": str(missing_dir),
        "INPUT_MODE": "strict",
        "INPUT_FORMAT": "text",
        "INPUT_SARIF_FILE": "",
        "INPUT_FAIL_ON_WARNINGS": "false",
        "INPUT_INSTALL": "false",
        "INPUT_CONTRACT": "",
        "GITHUB_OUTPUT": str(github_output),
    }

    proc = run_action_script(env, tmp_path)
    assert proc.returncode == 2
    assert "verdict=INSPECTION_ERROR" in github_output.read_text(encoding="utf-8")


def test_action_script_input_validation_errors(tmp_path: Path):
    """Test validation of mode, format, fail-on-warnings, and install options."""
    base_env = {
        "INPUT_PATH": str(tmp_path),
        "INPUT_MODE": "strict",
        "INPUT_FORMAT": "text",
        "INPUT_SARIF_FILE": "",
        "INPUT_FAIL_ON_WARNINGS": "false",
        "INPUT_INSTALL": "auto",
        "INPUT_CONTRACT": "",
        "GITHUB_OUTPUT": "",
    }

    # 1. Invalid mode
    e1 = dict(base_env, INPUT_MODE="unsafe_mode")
    assert run_action_script(e1, tmp_path).returncode == 2

    # 2. Invalid format
    e2 = dict(base_env, INPUT_FORMAT="xml")
    assert run_action_script(e2, tmp_path).returncode == 2

    # 3. Invalid fail-on-warnings
    e3 = dict(base_env, INPUT_FAIL_ON_WARNINGS="maybe")
    assert run_action_script(e3, tmp_path).returncode == 2

    # 4. Invalid install
    e4 = dict(base_env, INPUT_INSTALL="pip_always")
    assert run_action_script(e4, tmp_path).returncode == 2


def test_action_script_install_false_when_cli_missing(tmp_path: Path):
    """Test that install=false exits with code 2 when parquet-guard is missing from PATH."""
    env = {
        "INPUT_PATH": str(tmp_path),
        "INPUT_MODE": "strict",
        "INPUT_FORMAT": "text",
        "INPUT_SARIF_FILE": "",
        "INPUT_FAIL_ON_WARNINGS": "false",
        "INPUT_INSTALL": "false",
        "INPUT_CONTRACT": "",
        "GITHUB_OUTPUT": "",
        "PATH": "/usr/bin:/bin",  # Restrict PATH to exclude parquet-guard
    }
    proc = run_action_script(env, tmp_path)
    assert proc.returncode == 2
    assert "parquet-guard executable not found and install=false" in (proc.stdout + proc.stderr)


def test_action_script_output_formats(tmp_path: Path):
    """Verify that text, markdown, json, and sarif presentation modes work properly."""
    p1 = tmp_path / "part-1.parquet"
    table = pa.Table.from_pydict({"x": [10, 20]})
    pq.write_table(table, p1)

    for fmt in ["text", "markdown", "json", "sarif"]:
        env = {
            "INPUT_PATH": str(tmp_path),
            "INPUT_MODE": "strict",
            "INPUT_FORMAT": fmt,
            "INPUT_SARIF_FILE": "",
            "INPUT_FAIL_ON_WARNINGS": "false",
            "INPUT_INSTALL": "false",
            "INPUT_CONTRACT": "",
            "GITHUB_OUTPUT": "",
        }
        proc = run_action_script(env, tmp_path)
        assert proc.returncode == 0
        if fmt == "json":
            json_start = proc.stdout.find("{")
            json_end = proc.stdout.rfind("}") + 1
            data = json.loads(proc.stdout[json_start:json_end])
            assert data["result_state"] == "VALID"
        elif fmt == "sarif":
            assert '"version": "2.1.0"' in proc.stdout


def test_action_metadata_structure():
    """Verify that action.yml conforms to GitHub Composite Action specifications."""
    content = ACTION_YML_PATH.read_text(encoding="utf-8")
    assert 'name: "Parquet Guard Schema Audit"' in content
    assert 'using: "composite"' in content
    for expected_input in [
        "path:",
        "contract:",
        "mode:",
        "format:",
        "sarif-file:",
        "fail-on-warnings:",
        "install:",
    ]:
        assert expected_input in content
    for expected_output in ["verdict:", "sarif-file:"]:
        assert expected_output in content
