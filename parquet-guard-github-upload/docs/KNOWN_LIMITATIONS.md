# Known limitations and verification status (v0.2.0 Alpha)

This document identifies risks to be checked before a stable release. It is not a claim that they were reproduced by the package creator in every environment.

- **Dictionary contracts:** parameters represented separately from nested field structures may not be consistently included in structural equality and schema fingerprinting.
- **Incremental cache:** exceptional conditions after cache hits may not always produce clean inspection-error results. Use full audits for high-assurance workflows pending additional validation.
- **Contract type validation:** unsupported Arrow type identifiers may not always be rejected early.
- **Hosted CI:** cross-platform GitHub Actions success is not established by merely including workflow files.
- **Performance:** no independently verified performance advantage is asserted in this public Alpha readme.
- **Package distribution:** the source is provided for evaluation. PyPI availability is not asserted.

See `tests/` and submit reproducible bug reports through GitHub Issues. Before declaring a stable release, fix or explicitly constrain the affected capabilities and run a real platform test matrix.
