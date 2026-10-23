from __future__ import annotations

import os
import pathlib
import re
import subprocess
import sys
import textwrap

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
RELEASE_WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "release.yml"
CI_WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "ci.yml"
PUBLISHING_DOC_PATH = REPO_ROOT / "docs" / "PUBLISHING.md"


def _extract_validate_python_script() -> str:
    content = RELEASE_WORKFLOW_PATH.read_text(encoding="utf-8")
    match = re.search(r"python - <<'PY'\n(.*?)\n\s*PY\b", content, re.DOTALL)
    assert match is not None, "Could not locate embedded Python script in validate-release"
    return textwrap.dedent(match.group(1))


def _extract_run_blocks(yaml_text: str) -> list[str]:
    """Extract all single-line and multi-line `run:` blocks from a GitHub workflow YAML file."""
    lines = yaml_text.splitlines()
    blocks: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        m = re.match(r"^(\s*(?:-\s+)?)run:\s*(.*)$", line)
        if not m:
            i += 1
            continue
        prefix_indent = len(line) - len(line.lstrip())
        remainder = m.group(2).strip()
        if remainder in ("|", "|-", ">", ">-"):
            block_lines: list[str] = []
            i += 1
            while i < len(lines):
                sub = lines[i]
                if sub.strip() == "":
                    block_lines.append(sub)
                    i += 1
                    continue
                sub_indent = len(sub) - len(sub.lstrip())
                if sub_indent <= prefix_indent:
                    break
                block_lines.append(sub)
                i += 1
            blocks.append("\n".join(block_lines))
        else:
            blocks.append(remainder)
            i += 1
    return blocks


def _extract_job_section(yaml_text: str, job_name: str) -> str:
    lines = yaml_text.splitlines()
    in_job = False
    job_lines: list[str] = []
    for line in lines:
        if not in_job:
            if re.match(rf"^  {re.escape(job_name)}:\s*$", line):
                in_job = True
                job_lines.append(line)
        else:
            if re.match(r"^  [a-zA-Z0-9_-]+:\s*$", line) or re.match(r"^[a-zA-Z0-9_-]+:", line):
                break
            job_lines.append(line)
    assert job_lines, f"Job {job_name!r} not found in release.yml"
    return "\n".join(job_lines)


def _init_git_repo_with_origin_main(
    tmp_path: pathlib.Path,
    pyproject_version: str = "0.3.0a1",
    init_version: str = "0.3.0a1",
    on_unmerged_branch: bool = False,
) -> pathlib.Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "src" / "parquet_guard").mkdir(parents=True)
    (repo / "pyproject.toml").write_text(
        f'[project]\nname = "parquet-guard"\nversion = "{pyproject_version}"\n',
        encoding="utf-8",
    )
    (repo / "src" / "parquet_guard" / "__init__.py").write_text(
        f'__version__ = "{init_version}"\n',
        encoding="utf-8",
    )

    def git(*args: str) -> None:
        subprocess.run(
            ["git", *args],
            cwd=repo,
            check=True,
            capture_output=True,
            text=True,
        )

    git("init", "-b", "main")
    git("config", "user.email", "test@example.com")
    git("config", "user.name", "Test User")
    git("add", ".")
    git("commit", "-m", "Initial main commit")
    git("update-ref", "refs/remotes/origin/main", "HEAD")

    if on_unmerged_branch:
        git("checkout", "-b", "feat/unapproved")
        (repo / "README.md").write_text("unmerged work\n", encoding="utf-8")
        git("add", "README.md")
        git("commit", "-m", "Unmerged commit")

    return repo


def _run_validation_script(
    repo: pathlib.Path,
    env_overrides: dict[str, str],
) -> tuple[subprocess.CompletedProcess[str], dict[str, str]]:
    script = _extract_validate_python_script()
    output_file = repo / "github_output.txt"
    env = os.environ.copy()
    env.update(env_overrides)
    env["GITHUB_OUTPUT"] = str(output_file)

    proc = subprocess.run(
        [sys.executable, "-c", script],
        cwd=repo,
        env=env,
        capture_output=True,
        text=True,
    )
    outputs: dict[str, str] = {}
    if output_file.exists():
        for line in output_file.read_text(encoding="utf-8").splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                outputs[k] = v
    return proc, outputs


def test_release_workflow_triggers_and_ci_isolation() -> None:
    release_text = RELEASE_WORKFLOW_PATH.read_text(encoding="utf-8")
    assert "pull_request:" not in release_text
    assert "- 'v[0-9]*.[0-9]*.[0-9]*'" in release_text
    assert "- 'v*'" not in release_text

    ci_text = CI_WORKFLOW_PATH.read_text(encoding="utf-8")
    assert "pypa/gh-action-pypi-publish" not in ci_text
    assert "id-token: write" not in ci_text


def test_release_workflow_has_no_expression_interpolation_in_run_blocks() -> None:
    release_text = RELEASE_WORKFLOW_PATH.read_text(encoding="utf-8")
    run_blocks = _extract_run_blocks(release_text)
    assert len(run_blocks) >= 5
    for block in run_blocks:
        assert "${{" not in block, f"Found GitHub expression interpolation inside run: block:\n{block}"


def test_release_workflow_permissions_and_job_dependencies() -> None:
    release_text = RELEASE_WORKFLOW_PATH.read_text(encoding="utf-8")
    assert "permissions:\n  contents: read" in release_text

    build_job = _extract_job_section(release_text, "build-and-verify")
    testpypi_job = _extract_job_section(release_text, "publish-testpypi")
    pypi_job = _extract_job_section(release_text, "publish-pypi")
    gh_release_job = _extract_job_section(release_text, "create-github-release")

    assert "id-token: write" not in build_job
    assert "contents: read" in build_job

    assert "id-token: write" in testpypi_job
    assert "contents: read" in testpypi_job

    assert "id-token: write" in pypi_job
    assert "contents: read" in pypi_job
    assert "github.ref == 'refs/heads/main'" in pypi_job

    assert "id-token: write" not in gh_release_job
    assert "contents: write" in gh_release_job
    assert "- build-and-verify" in gh_release_job
    assert "- publish-pypi" in gh_release_job
    assert "needs.build-and-verify.outputs.is_prerelease == 'true'" in gh_release_job


@pytest.mark.parametrize(
    ("version_str", "expected_prerelease"),
    [
        ("0.3.0a1", "true"),
        ("0.3.0b2", "true"),
        ("0.3.0rc1", "true"),
        ("0.3.0", "false"),
        ("1.0.0", "false"),
        ("2.10.4", "false"),
    ],
)
def test_validate_release_pep440_prerelease_detection(
    tmp_path: pathlib.Path,
    version_str: str,
    expected_prerelease: str,
) -> None:
    repo = _init_git_repo_with_origin_main(
        tmp_path,
        pyproject_version=version_str,
        init_version=version_str,
    )
    proc, outputs = _run_validation_script(
        repo,
        {
            "EVENT_NAME": "push",
            "REF_NAME": f"v{version_str}",
            "REF_TYPE": "tag",
            "FULL_REF": f"refs/tags/v{version_str}",
            "PUBLISH_TARGET": "",
        },
    )
    assert proc.returncode == 0, proc.stderr
    assert outputs["version"] == version_str
    assert outputs["is_prerelease"] == expected_prerelease


def test_validate_release_rejects_tag_and_metadata_mismatch(tmp_path: pathlib.Path) -> None:
    repo = _init_git_repo_with_origin_main(
        tmp_path,
        pyproject_version="0.3.0a1",
        init_version="0.3.0a1",
    )
    proc, _ = _run_validation_script(
        repo,
        {
            "EVENT_NAME": "push",
            "REF_NAME": "v0.3.0",
            "REF_TYPE": "tag",
            "FULL_REF": "refs/tags/v0.3.0",
            "PUBLISH_TARGET": "",
        },
    )
    assert proc.returncode != 0
    assert "does not match package version" in proc.stderr


def test_validate_release_rejects_pyproject_and_init_mismatch(tmp_path: pathlib.Path) -> None:
    repo = _init_git_repo_with_origin_main(
        tmp_path,
        pyproject_version="0.3.0a1",
        init_version="0.2.0",
    )
    proc, _ = _run_validation_script(
        repo,
        {
            "EVENT_NAME": "push",
            "REF_NAME": "v0.3.0a1",
            "REF_TYPE": "tag",
            "FULL_REF": "refs/tags/v0.3.0a1",
            "PUBLISH_TARGET": "",
        },
    )
    assert proc.returncode != 0
    assert "Version mismatch between pyproject.toml" in proc.stderr


def test_validate_release_rejects_unapproved_commit_lineage(tmp_path: pathlib.Path) -> None:
    repo = _init_git_repo_with_origin_main(
        tmp_path,
        pyproject_version="0.3.0a1",
        init_version="0.3.0a1",
        on_unmerged_branch=True,
    )
    proc, _ = _run_validation_script(
        repo,
        {
            "EVENT_NAME": "push",
            "REF_NAME": "v0.3.0a1",
            "REF_TYPE": "tag",
            "FULL_REF": "refs/tags/v0.3.0a1",
            "PUBLISH_TARGET": "",
        },
    )
    assert proc.returncode != 0
    assert "not an ancestor of origin/main" in proc.stderr


def test_validate_release_restricts_manual_pypi_dispatch_to_main(tmp_path: pathlib.Path) -> None:
    repo = _init_git_repo_with_origin_main(
        tmp_path,
        pyproject_version="0.3.0a1",
        init_version="0.3.0a1",
    )
    proc_feature, _ = _run_validation_script(
        repo,
        {
            "EVENT_NAME": "workflow_dispatch",
            "REF_NAME": "release/v0.3.0a1",
            "REF_TYPE": "branch",
            "FULL_REF": "refs/heads/release/v0.3.0a1",
            "PUBLISH_TARGET": "pypi",
        },
    )
    assert proc_feature.returncode != 0
    assert "restricted to refs/heads/main" in proc_feature.stderr

    proc_main, outputs = _run_validation_script(
        repo,
        {
            "EVENT_NAME": "workflow_dispatch",
            "REF_NAME": "main",
            "REF_TYPE": "branch",
            "FULL_REF": "refs/heads/main",
            "PUBLISH_TARGET": "pypi",
        },
    )
    assert proc_main.returncode == 0, proc_main.stderr
    assert outputs["version"] == "0.3.0a1"


def test_publishing_docs_cover_signed_tags_and_pending_publishers() -> None:
    doc = PUBLISHING_DOC_PATH.read_text(encoding="utf-8")
    assert "Pending Trusted Publisher" in doc
    assert "404" in doc
    assert "git tag -s" in doc
    assert "git tag -a" in doc
    assert "without** a cryptographic GPG/SSH signature" in doc
