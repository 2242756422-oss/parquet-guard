# 🔐 Secure Publishing Guide with PyPI Trusted Publishing (OIDC)

This document describes the secure distribution architecture for `parquet-guard` using GitHub Actions and PyPI Trusted Publishing via OpenID Connect (OIDC), eliminating the need for long-lived API tokens or credentials stored in repository secrets.

---

## 1. Security Architecture & Release Safeguards

1. **Short-Lived OIDC Tokens**:
   Authentication between GitHub Actions and PyPI uses cryptographic OpenID Connect identity tokens issued by GitHub's token authority. No static API tokens (`pypi-...`) or passwords are stored in GitHub repository secrets.

2. **Least-Privilege Permissions & Separate Publishing Jobs**:
   - Top-level workflow permissions are restricted to `contents: read`.
   - `publish-testpypi` and `publish-pypi` are strictly separated jobs with `id-token: write` and `contents: read` scoped **only** to those two publishing jobs.
   - `build-and-verify` runs with `contents: read` only, and `create-github-release` runs with `contents: write` only (without `id-token: write`).
   - Routine CI runs (`.github/workflows/ci.yml`), branch pushes, and pull requests **never** trigger `.github/workflows/release.yml` and never receive `id-token: write` authority.

3. **Zero Shell Expression Interpolation**:
   All dynamic GitHub context values (`github.event_name`, `github.ref_name`, `github.ref_type`, `github.ref`, `inputs.publish_target`, and step outputs) are passed exclusively through job/step `env:` variables rather than interpolated with `${{ ... }}` inside `run:` shell scripts.

4. **Strict Version Agreement & PEP 440 Validation**:
   Before building or uploading any artifact, the `validate-release` step verifies that:
   - `project.version` in `pyproject.toml` and `__version__` in `src/parquet_guard/__init__.py` match identically.
   - The version string is a canonical PEP 440 release or prerelease (`X.Y.Z`, `X.Y.ZaN`, `X.Y.ZbN`, or `X.Y.ZrcN`) parsed via `packaging.version.Version`.
   - On tag pushes, the Git tag (`v<version>`, e.g., `v0.3.0a1`) matches `f"v{pyproject_version}"` exactly. Any mismatch aborts the workflow before building or uploading artifacts.
   - Prerelease status (`is_prerelease`) is computed using PEP 440 semantics (`Version(ver).is_prerelease or Version(ver).is_devrelease`), ensuring stable tags such as `v0.3.0` or `v1.0.0` are not misclassified because of the leading `v` prefix.

5. **Approved Release Lineage Enforcement**:
   - Tag-triggered releases must match the tag pattern `v[0-9]*.[0-9]*.[0-9]*` and pass `git merge-base --is-ancestor HEAD refs/remotes/origin/main`, ensuring arbitrary tags on unmerged branches cannot trigger a release.
   - Manual `workflow_dispatch` runs targeting production `pypi` are restricted both in the job `if:` condition (`github.ref == 'refs/heads/main'`) and in `validate-release` ancestry checks so unreviewed feature branches cannot be manually published to production PyPI.

6. **GitHub Release Gated on PyPI Publication**:
   The `create-github-release` job declares `needs: [build-and-verify, publish-pypi]`. If building, metadata verification, or PyPI publication fails, the GitHub Release is not created.

7. **Protected GitHub Environments**:
   Publishing jobs target dedicated GitHub Environments (`pypi` and `testpypi`), allowing repository maintainers to enforce required reviewers and branch/tag deployment rules before OIDC tokens are minted.

---

## 2. Configuring PyPI & TestPyPI Trusted Publishing

To link GitHub repository `2242756422-oss/parquet-guard` to PyPI and TestPyPI:

### Step 1: Configure a Pending Trusted Publisher (First Release) or Project Publisher

- **Before the first release (when `parquet-guard` does not yet exist on PyPI or TestPyPI)**:
  Configure a **Pending Trusted Publisher**:
  1. Log into your maintainer account at [pypi.org](https://pypi.org/) (and [test.pypi.org](https://test.pypi.org/) for TestPyPI).
  2. Navigate to **Account settings** > **Publishing** (or `https://pypi.org/manage/account/publishing/`).
  3. Under **Add a new pending publisher**, select **GitHub** and register the workflow parameters below. When the first OIDC publish job runs, PyPI automatically creates the project and converts the pending publisher into the project's active Trusted Publisher.

- **For subsequent releases (once the project already exists on PyPI/TestPyPI)**:
  Navigate to the project's **Manage** > **Publishing** page (`https://pypi.org/manage/project/parquet-guard/settings/publishing/`) to view or manage active Trusted Publishers.

> **Important Note on PyPI Name Availability & HTTP 404 Responses**
> Receiving an HTTP `404 Not Found` from `https://pypi.org/project/<name>/` or `https://pypi.org/pypi/<name>/json` does **not** guarantee that a package name is available or claimable on PyPI:
> - **PEP 503 / PEP 508 Normalization**: PyPI normalizes runs of `-`, `_`, and `.` to a single dash (`-`) case-insensitively (`parquet-guard`, `parquet_guard`, and `parquet.guard` collide in the same namespace).
> - **Typo-Squatting & Similarity Blocks**: PyPI rejects names that are too visually or phonetically similar to existing popular packages.
> - **Reserved, Prohibited, or Deleted Names**: Previously deleted, quarantined, or administrator-reserved package names return HTTP 404 on public endpoints but remain blocked from registration.
> Always confirm name eligibility directly by registering the **Pending Trusted Publisher** in the PyPI account settings UI before tagging a release.

### Step 2: Trusted Publisher Parameters

| Parameter | Production PyPI (`pypi.org`) | TestPyPI (`test.pypi.org`) |
| :--- | :--- | :--- |
| **PyPI Project Name** | `parquet-guard` | `parquet-guard` |
| **Owner** | `2242756422-oss` | `2242756422-oss` |
| **Repository name** | `parquet-guard` | `parquet-guard` |
| **Workflow name** | `release.yml` | `release.yml` |
| **Environment name** | `pypi` | `testpypi` |

### Step 3: Configure GitHub Environment Protections
1. On GitHub, navigate to `https://github.com/2242756422-oss/parquet-guard/settings/environments`.
2. Create both the `pypi` and `testpypi` environments.
3. For the `pypi` environment:
   - Enable **Required reviewers** so a designated maintainer must approve production publication.
   - Restrict **Deployment branches and tags** to `main` and release tags matching `v[0-9]*.[0-9]*.[0-9]*`.

---

## 3. Releasing a New Version

When maintainers are ready to publish a release from an approved commit on `main` (e.g., `v0.4.0a1`):

### Option A: Tag-Triggered Release (Standard)
1. Ensure the release PR has been merged into `main` and all CI checks pass on `main`.
2. Switch to local `main`, pull `origin/main`, and tag the release commit.
   - **Recommended — Cryptographically Signed Tag (`git tag -s`)**:
     Creates a GPG- or SSH-signed tag whose cryptographic signature can be verified independently:
     ```bash
     git checkout main
     git pull --ff-only origin main
     git tag -s v0.4.0a1 -m "Release v0.4.0a1 (Consumer-Aware Schema Intelligence Alpha)"
     git tag -v v0.4.0a1
     git push origin v0.4.0a1
     ```
   - **Alternative — Unsigned Annotated Tag (`git tag -a`)**:
     Creates a Git tag object with tagger metadata and message, but **without** a cryptographic GPG/SSH signature:
     ```bash
     git checkout main
     git pull --ff-only origin main
     git tag -a v0.4.0a1 -m "Release v0.4.0a1 (Consumer-Aware Schema Intelligence Alpha)"
     git push origin v0.4.0a1
     ```
3. Pushing the `v0.4.0a1` tag triggers `.github/workflows/release.yml`:
   - `build-and-verify` checks tag/version agreement, PEP 440 format, and `origin/main` ancestry, builds the wheel and sdist, runs `twine check --strict`, and smoke-tests installation in a clean virtual environment.
   - `publish-pypi` publishes the verified distributions to PyPI via OIDC Trusted Publishing.
   - `create-github-release` runs **only after `publish-pypi` succeeds**, creating the GitHub Release and setting `prerelease: true` or `false` automatically from the PEP 440 version.

### Option B: Manual Dispatch (`workflow_dispatch`)
1. Go to **Actions** > **Release & Publish** > **Run workflow**.
2. Select target:
   - `testpypi`: Allowed from `main` or `release/*` branches to verify end-to-end packaging on TestPyPI prior to tagging.
   - `pypi`: Allowed **only** when triggered from `refs/heads/main` on a commit belonging to `origin/main`.
3. Click **Run workflow**.

---

## 4. Verification Checklist Before Tagging

- [ ] `pyproject.toml` version matches target release (e.g., `0.4.0a1`).
- [ ] `src/parquet_guard/__init__.py` `__version__` matches `pyproject.toml` exactly.
- [ ] Target release version is a valid canonical PEP 440 version (`X.Y.Z`, `X.Y.ZaN`, `X.Y.ZbN`, or `X.Y.ZrcN`).
- [ ] `CHANGELOG.md` contains a dated release entry with all notable changes.
- [ ] Automated tests pass 100% on all Python versions (3.9–3.13) across Linux, macOS, and Windows.
- [ ] `python -m build && twine check --strict dist/*` succeeds without warnings.
- [ ] Installation into a fresh virtual environment executes `parquet-guard --version` and matches the expected version.
- [ ] Pending or active Trusted Publisher is configured on PyPI (`pypi` environment) and TestPyPI (`testpypi` environment).
