# Compatibility and limitations

This page describes the implementation and integration tests in the current
development branch. It is not yet a stable support policy.

## Runtime versions

| Component | Current project requirement |
| --- | --- |
| Python | 3.12 or newer |
| Rust | 1.85 or newer |
| Django | Project fork based on Django development code |
| PostgreSQL | Integration tests default to PostgreSQL 18 |

Python classifiers currently include Python 3.12, 3.13, and 3.14. A release
must not claim an operating-system or PostgreSQL version matrix until CI builds
and tests that matrix.

## Tested native async ORM behavior

The project suite covers the native path for:

- queryset existence checks and async iteration;
- `values()`, `values_list()`, named values, and flat values;
- `aget()`, `afirst()`, `alast()`, `aearliest()`, and `alatest()`;
- `acontains()` and `ain_bulk()`;
- counts, aggregates, and explanations;
- creates, saves, refreshes, updates, deletes, and bulk writes;
- `aget_or_create()` and `aupdate_or_create()`;
- cascade, protected, and restricted deletion behavior;
- related-manager and many-to-many async operations;
- prefetching, nested prefetches, generic relations, and content types;
- raw query iteration, named parameters, translations, and prefetching;
- `select_for_update()` transaction validation;
- async `transaction.atomic()`, savepoints, rollback state, durable blocks,
  decorators, and `on_commit()` callbacks; and
- isolation level, read-only, and deferrable transaction options.

This coverage proves those project scenarios. It does not mean every Django
ORM combination, expression, custom field, router, or third-party package is
already compatible.

The native infrastructure suite also covers bounded pool concurrency,
connection reuse and replacement, cancellation, process forks, transaction
connection pinning, savepoints, and database error mapping. See
[Connection pool design](connection-pool-design.md) and
[Database errors](database-errors.md) for the exact behavior.

## Synchronous Django behavior

The backend subclasses Django's PostgreSQL backend. Synchronous operations,
migrations, schema editing, and other synchronous backend behavior continue to
use Django's normal Psycopg path. The native path is for the fork's async
backend contract.

## Known limitations

### Django fork

Upstream Django does not yet contain the required native async backend
boundary. See [Django fork requirement](django-fork.md).

### PostgreSQL values

Only text and integer families are currently usable through the native value
boundary. See [Supported PostgreSQL values](supported-types.md).

### Result buffering

Django requests async iterator results in chunks, but the native executor
currently collects and decodes the complete PostgreSQL result before Python's
cursor serves those chunks. Large-result memory use is therefore proportional
to the complete result size.

### TLS

Native connections currently use `tokio_postgres::NoTls`. Do not use the
native connection path where encrypted or certificate-verified PostgreSQL
connections are required.

### Session configuration

The native pool does not yet reproduce every PostgreSQL session setting that
Django and Psycopg may configure, including a complete policy for time zones,
roles, search paths, and custom connection options.

### Test breadth

The project has its own integration suite, but it has not yet passed the full
relevant Django ORM and PostgreSQL backend test suites. Custom fields and
third-party adapter compatibility remain open work. `select_related()` uses a
joined query and can use the native execution path, but it still needs a
dedicated integration test before it is included in the tested list above.

## Release interpretation

An initial package release should be described as experimental or pre-alpha.
It is suitable for evaluation, compatibility testing, and continued backend
development, not as a drop-in production replacement for Django's PostgreSQL
backend.
