# 🔐 Secure Publishing Guide with PyPI Trusted Publishing (OIDC)

This document describes the secure distribution architecture for `parquet-guard` using GitHub Actions and PyPI Trusted Publishing via OpenID Connect (OIDC), eliminating the need for long-lived API tokens or credentials stored in repository secrets.

---

## 1. Security Architecture

1. **Short-Lived OIDC Tokens**:
   Authentication between GitHub Actions and PyPI uses cryptographic OpenID Connect identity tokens issued by GitHub's token authority. No static API tokens (`pypi-...`) or passwords are stored in GitHub repository secrets.

2. **Least Privilege Principle**:
   - `id-token: write` permission is strictly scoped **only** to the publishing jobs (`publish-pypi` and `publish-testpypi`).
   - Normal CI runs (`.github/workflows/ci.yml`) and pull requests **never** receive `id-token: write` authority.

3. **Separation of Build & Publish**:
   The `build-and-verify` job independently builds the distribution artifacts, validates metadata strictly with `twine check --strict`, and verifies clean installation in an isolated virtual environment before any publishing job can execute.

4. **Protected Environments**:
   Publishing targets use GitHub Environments (`pypi` and `testpypi`), allowing repository maintainers to require required reviewers or branch protections before artifacts are dispatched to PyPI.

---

## 2. Maintaining PyPI Trusted Publisher Configuration

To link GitHub repository `2242756422-oss/parquet-guard` to PyPI:

### Step 1: Create or Claim the Project on PyPI
1. Log into your maintainer account at [pypi.org](https://pypi.org/).
2. Navigate to **Account settings** > **Publishing** > **Add a publisher**.
3. Select **GitHub**.

### Step 2: Configure Publisher Parameters
- **PyPI Project Name**: `parquet-guard`
- **Owner**: `2242756422-oss`
- **Repository name**: `parquet-guard`
- **Workflow name**: `release.yml`
- **Environment name**: `pypi`

*(Repeat the same configuration on [test.pypi.org](https://test.pypi.org/) if test publishing is desired, using environment name `testpypi`.)*

### Step 3: Configure GitHub Environment Protections
1. On GitHub, navigate to `https://github.com/2242756422-oss/parquet-guard/settings/environments`.
2. Ensure the `pypi` environment is created.
3. (Optional but recommended) Enable **Required reviewers** for the `pypi` environment to require explicit maintainer confirmation before publication.

---

## 3. Releasing a New Version

When maintainers are ready to publish a release (e.g., `v0.3.0a1`):

### Option A: Tag-Triggered Release (Standard)
1. Verify all tests, linters, and documentation pass on `main`.
2. Create and push a signed Git tag:
   ```bash
   git tag -a v0.3.0a1 -m "Release v0.3.0a1 (Alpha Prerelease with Schema Copilot)"
   git push origin v0.3.0a1
   ```
3. GitHub Actions `.github/workflows/release.yml` triggers automatically:
   - Builds artifacts and runs Twine metadata checks.
   - Publishes to PyPI via OIDC.
   - Creates a GitHub Release with attached wheel and sdist files.

### Option B: Manual Dispatch (Dry Run / TestPyPI)
1. Go to **Actions** > **Release & Publish** > **Run workflow**.
2. Select target: `testpypi` or `pypi`.
3. Click **Run workflow**.

---

## 4. Verification Checklist Before Tagging

- [ ] `pyproject.toml` version matches target release (e.g. `0.3.0a1`).
- [ ] `src/parquet_guard/__init__.py` `__version__` matches `pyproject.toml`.
- [ ] `CHANGELOG.md` contains dated release entry with all notable changes.
- [ ] Automated tests pass 100% on all Python versions (3.9–3.13) across Linux, macOS, and Windows.
- [ ] `python -m build && twine check --strict dist/*` succeeds without warnings.
- [ ] Installation into a fresh virtual environment executes `parquet-guard --version` correctly.
