# Documentation

`django-native-postgres` is an experimental Django PostgreSQL backend that
executes async ORM database I/O through Rust, Tokio, and `tokio-postgres`.
Django remains responsible for the ORM and SQL compilation; this project owns
the native async execution path, pool, and PostgreSQL connections.

## Start here

- [Configuration](configuration.md) lists Django settings and native pool
  options.
- [Async ORM usage](async-orm.md) describes the currently tested ORM surface,
  cancellation, and application deadlines.
- [Transactions and savepoints](transactions.md) covers async atomic blocks and
  transaction options.
- [Database errors and diagnostics](database-errors.md) documents Django
  exception mapping, native PostgreSQL diagnostics, and retry boundaries.
- [Development](development.md) explains how to build and test the project.
- [Django fork requirement](django-fork.md) explains why upstream Django
  cannot currently use the native execution path.
- [Compatibility](compatibility.md) lists what is tested and what is not yet
  supported.
- [Supported PostgreSQL values](supported-types.md) lists the current parameter
  and result types.

## Design

- [Architecture](architecture.md) describes the complete Django-to-PostgreSQL
  execution flow and component responsibilities.
- [Connection pool design](connection-pool-design.md) describes Rust ownership,
  bounded concurrency, transaction affinity, cancellation, and process safety.
- [Architecture decision records](decisions/README.md) defines how major design
  decisions are recorded.

## Planned guides

The following guides will be added as the documentation work continues:

- installation from a published package;
- operational behavior and security;
- contributing and testing against the Django fork; and
- packaging and release procedures.

## Release status

The project can be evaluated from source, but it is not ready to be presented
as a production backend. The first package release should be explicitly marked
as experimental or pre-alpha and must document the required Django fork,
limited value support, buffered results, and lack of TLS.
