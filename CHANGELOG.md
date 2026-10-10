# Changelog

This file records user-visible changes to `django-native-postgres`.

## 0.1.1 - 2026-10-10

### Changed

- Made the required Django fork installation command prominent and clarified
  that installing `django-native-postgres` alone cannot resolve the fork from
  PyPI.
- Separated published-package installation from source-development
  requirements in the README.

## 0.1.0 - 2026-10-10

Initial experimental release.

### Added

- Native async execution for Django ORM reads, writes, iteration, aggregates,
  related managers, prefetching, and model operations.
- A Rust-owned Tokio runtime and bounded PostgreSQL connection pool.
- Async transactions, nested savepoints, transaction options, rollback state,
  and `on_commit()` callbacks.
- Cancellation for active queries and queries waiting for a pool connection.
- PostgreSQL error mapping with structured database diagnostics.
- Runtime and pool recovery after POSIX `fork()`.
- Linux, macOS, and Windows wheel builds using the Python 3.12 stable ABI.

### Known limitations

- The backend requires the project's Django fork and cannot be used with an
  upstream Django release.
- Native query parameters and decoded results support only the types listed in
  the supported-types documentation.
- Query results are buffered in Rust before Django receives the first chunk.
- PostgreSQL connections do not yet support TLS.
- The full Django PostgreSQL compatibility suite and supported platform matrix
  have not been completed.
