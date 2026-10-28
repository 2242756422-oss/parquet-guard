"""Tests for parquet-guard verify-engine CLI command."""

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from click.testing import CliRunner

from parquet_guard.cli import main
from parquet_guard.core.inspector import inspect_schema


def test_cli_verify_engine_single_file_text_and_json(tmp_path: Path) -> None:
    f = tmp_path / "data.parquet"
    table = pa.Table.from_arrays([pa.array([1, 2, 3])], names=["id"])
    pq.write_table(table, f)

    runner = CliRunner()
    # 1. Text format
    res = runner.invoke(main, ["verify-engine", str(f), "--engine", "pyarrow", "--allow-row-probe"])
    assert res.exit_code == 0
    assert "ENGINE RUNTIME VERIFICATION REPORT" in res.output
    assert "pyarrow" in res.output

    # 2. JSON format
    res_json = runner.invoke(
        main, ["verify-engine", str(f), "--engine", "pyarrow", "--allow-row-probe", "--format", "json"]
    )
    assert res_json.exit_code == 0
    data = json.loads(res_json.output)
    assert data["runtime_verdict"] == "PASS"
    assert len(data["engine_results"]) > 0


def test_cli_verify_engine_with_contract_and_disagreement(tmp_path: Path) -> None:
    f1 = tmp_path / "part1.parquet"
    table1 = pa.Table.from_arrays([pa.array([100], type=pa.int64())], names=["val"])
    pq.write_table(table1, f1)

    contract_path = tmp_path / "contract.json"
    # Create contract expecting int64
    contract = inspect_schema(f1)
    contract_path.write_text(contract.model_dump_json(), encoding="utf-8")

    runner = CliRunner()
    res = runner.invoke(
        main,
        [
            "verify-engine",
            str(f1),
            "-c",
            str(contract_path),
            "--engine",
            "pyarrow",
            "--fail-on-disagreement",
            "--allow-row-probe",
        ],
    )
    assert res.exit_code == 0


def test_cli_verify_engine_multi_file_union(tmp_path: Path) -> None:
    dir_path = tmp_path / "dataset"
    dir_path.mkdir()
    f1 = dir_path / "f1.parquet"
    f2 = dir_path / "f2.parquet"
    t1 = pa.Table.from_arrays([pa.array([1, 2])], names=["x"])
    t2 = pa.Table.from_arrays([pa.array([3, 4])], names=["x"])
    pq.write_table(t1, f1)
    pq.write_table(t2, f2)

    runner = CliRunner()
    res = runner.invoke(
        main,
        ["verify-engine", str(dir_path), "--engine", "pyarrow", "--scope", "union", "--allow-row-probe"],
    )
    assert res.exit_code == 0
    assert "pyarrow" in res.output
