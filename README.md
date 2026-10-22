# Parquet Guard

**Parquet schema inspection, contract validation, and partitioned-dataset drift reporting.**

> **Project status: v0.2.0 Alpha / work in progress.** This is an early public preview, not a production-readiness claim. The project is seeking correctness testing and feedback, especially on nested Arrow types and incremental cache behavior. See [Known limitations](docs/KNOWN_LIMITATIONS.md).

Parquet Guard is a Python-based command-line tool and library built on [PyArrow](https://arrow.apache.org/docs/python/). It is intended to help maintainers detect structural schema differences between Parquet files and data contracts before a data pipeline consumes them.

## What it currently includes

- Inspect a Parquet file's metadata and Arrow schema without loading table rows.
- Export a baseline contract to JSON and validate a file against it.
- Compare schemas in `strict` or `permissive` mode.
- Audit a directory or group of Parquet files, reporting per-file schema differences.
- Output human-readable, JSON, Markdown, or SARIF-formatted results for supported commands.
- Experimental incremental auditing. **Do not rely on cache hits for high-assurance validation until the documented issues are addressed.**

Not all Arrow type combinations or deployment environments have been independently verified. Metadata-first inspection still performs file I/O and is not a guarantee of zero cost or speed.

## Install from this repository

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
parquet-guard --version
```

On Windows, activate the virtual environment using the relevant shell's activation command.

The package has **not** been verified as publicly available on PyPI. Avoid `pip install parquet-guard` until publication is confirmed.

## Quick start

To generate a test Parquet file in the current folder:

```bash
python -c "import pyarrow as pa, pyarrow.parquet as pq; pq.write_table(pa.table({'id': pa.array([1, 2], type=pa.int32())}), 'sample.parquet')"
parquet-guard inspect sample.parquet
parquet-guard init-contract sample.parquet --output sample.contract.json
parquet-guard validate-contract sample.contract.json
parquet-guard check sample.parquet --contract sample.contract.json --mode strict
```

Other commands:

```bash
parquet-guard diff sample.parquet sample.parquet --mode strict
parquet-guard audit sample.parquet --mode strict
```

Run `parquet-guard --help` for available commands. See `examples/data_pipeline_guard/` for a larger project example.

## Contributing and verification

```bash
python -m pytest tests/ -v
python -m ruff check .
python -m ruff format --check .
```

GitHub Actions runs tests, linting, and formatting checks on pushes and pull requests. See the [Actions tab](https://github.com/2242756422-oss/parquet-guard/actions) for the latest results.


Bugs, reproduction cases, and feedback on the documented limitations are welcome through GitHub Issues. Please follow [CONTRIBUTING.md](CONTRIBUTING.md).

## Benchmarks and project scope

Scripts are under `benchmarks/`. Results are workload- and environment-dependent. The scripts should be rerun for fair comparisons; this README intentionally does not claim a specific speedup or superiority over other tools.

See [CHANGELOG.md](CHANGELOG.md) for release development history and [ROADMAP.md](ROADMAP.md) for planned work.

## License

Apache License 2.0. See [LICENSE](LICENSE).
