# Known limitations and verification status (v0.3.0a1 Alpha)

This document identifies risks to be checked before a stable release. It is not a claim that they were reproduced by the package creator in every environment.

- **Dictionary contracts:** parameters represented separately from nested field structures may not be consistently included in structural equality and schema fingerprinting.
- **Incremental cache:** exceptional conditions after cache hits may not always produce clean inspection-error results. Use full audits for high-assurance workflows pending additional validation.
- **Contract type validation:** unsupported Arrow type identifiers may not always be rejected early.
- **Hosted CI:** cross-platform GitHub Actions success is not established by merely including workflow files.
- **Performance:** no independently verified performance advantage is asserted in this public Alpha readme.
- **Package distribution:** the source is provided for evaluation. PyPI availability is not asserted.
- **Temporal unit precision vs. dynamic range:** a finer time unit increases precision but reduces the representable dynamic range of the underlying signed 64-bit integer. For example, `timestamp[ns]` can only represent timestamps between 1677-09-21 and 2262-04-11, and `duration[ns]` is limited to approximately ±292 years. Because `parquet-guard` performs zero-data-scan metadata inspection, it cannot inspect row values. In permissive mode, temporal precision widening is classified as a warning (`TEMPORAL_OVERFLOW_RISK`) to alert users to potential `ArrowInvalid` casting failures in downstream readers. Use strict mode if identical temporal resolution must be enforced.

See `tests/` and submit reproducible bug reports through GitHub Issues. Before declaring a stable release, fix or explicitly constrain the affected capabilities and run a real platform test matrix.
