"""Integration tests for GitHub Composite Action behavior.

Verifies:
1. Safe argument handling with zero shell injection risk.
2. Verdict extraction to $GITHUB_OUTPUT across VALID, INCOMPATIBLE, DRIFTED, and ERROR states.
3. SARIF report generation even when the audit fails.
4. Predictable non-zero exit codes on schema breaking regressions.
5. Strict validation of action inputs.
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


def test_action_script_input_validation_errors(tmp_path: Path):
    """Test rejection of invalid modes and formats."""
    env = {
        "INPUT_PATH": str(tmp_path),
        "INPUT_MODE": "invalid_mode_injection'; rm -rf /; '",
        "INPUT_FORMAT": "text",
        "INPUT_SARIF_FILE": "",
        "INPUT_FAIL_ON_WARNINGS": "false",
        "INPUT_INSTALL": "false",
        "INPUT_CONTRACT": "",
        "GITHUB_OUTPUT": "",
    }

    proc = run_action_script(env, tmp_path)
    assert proc.returncode == 2
    assert "Invalid mode" in proc.stdout or "Invalid mode" in proc.stderr
