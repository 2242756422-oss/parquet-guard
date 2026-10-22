# Contributing to parquet-guard

Thank you for your interest in improving `parquet-guard`! We welcome contributions from data engineers, open-source maintainers, and developers of all backgrounds.

---

## Code of Conduct

All contributors and participants are expected to adhere to our [Code of Conduct](CODE_OF_CONDUCT.md).

---

## Development Setup

1. **Fork and Clone**:
   ```bash
   git clone https://github.com/<your-username>/parquet-guard.git
   cd parquet-guard
   ```

2. **Environment Setup**:
   Ensure you have Python 3.9+ and `uv` or `pip` installed:
   ```bash
   make install
   ```

3. **Running Quality Checks**:
   ```bash
   # Run tests
   make test

   # Run linter and formatting checks
   make lint
   make format

   # Run type analysis
   make typecheck

   # Run all checks
   make check-all
   ```

---

## Pull Request Guidelines

1. **Branch Naming**: Use descriptive branch names like `feat/support-map-types` or `fix/timestamp-precision-diff`.
2. **Commit Messages**: Follow [Conventional Commits](https://www.conventionalcommits.org/) format (e.g., `feat: ...`, `fix: ...`, `docs: ...`, `test: ...`).
3. **Tests Required**: Every bug fix and feature addition must include automated tests in `tests/`.
4. **Documentation**: If your change modifies CLI flags, configuration, or API behavior, please update `README.md` and `docs/PRODUCT_SPEC.md` accordingly.
5. **No Breaking Changes Without Notice**: Backward compatibility is our core mission. Any change to public interfaces must be carefully discussed.

---

## Community & Discussions

Feel free to open an issue or GitHub Discussion if you have questions, architectural proposals, or feedback!
