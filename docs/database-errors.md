# Database errors and diagnostics

The native layer preserves structured PostgreSQL diagnostics, and the Python
backend translates failures into Django's database exception hierarchy.

## PostgreSQL server errors

For a PostgreSQL server error translated from the native path, the Django
exception keeps the native exception as its `__cause__`:

```python
from django.db import IntegrityError


try:
    await Book.objects.acreate(name=None)
except IntegrityError as error:
    native_error = error.__cause__
    print(native_error.sqlstate)
    print(native_error.constraint_name)
```

The native cause exposes:

- `sqlstate`;
- `severity`;
- `message_primary`;
- `detail`;
- `hint`;
- `schema_name`;
- `table_name`;
- `column_name`;
- `datatype_name`; and
- `constraint_name`.

Optional diagnostic fields are `None` when PostgreSQL does not provide them.

Do not assume every Django database exception has this native cause. Errors
raised by Django validation, synchronous Psycopg execution, or a compatibility
fallback can have a different cause.

## Django exception mapping

SQLSTATE classes map to the corresponding Django DB-API exception family.
Important examples include:

| SQLSTATE class or code | Django exception |
| --- | --- |
| `22` data exception | `DataError` |
| `23` integrity constraint violation | `IntegrityError` |
| `24`, `25`, `XX` internal/state errors | `InternalError` |
| `0A` unsupported feature | `NotSupportedError` |
| `08`, `28`, `40`, `53`-`58` operational errors | `OperationalError` |
| `21`, `26`, `34`, `3D`, `3F`, `42`, `44` programming errors | `ProgrammingError` |
| Unclassified server error | `DatabaseError` |

Serialization failure `40001` and deadlock detection `40P01` therefore become
`OperationalError`. Applications can inspect the native cause's exact
`sqlstate` instead of parsing error text.

If an application retries either error, it must retry the complete logical
operation from a fresh transaction boundary. Retrying only the failed statement
inside the same failed transaction is not safe. Backoff, retry limits, and
idempotency remain application responsibilities.

## Native failures

Failures without a PostgreSQL SQLSTATE are categorized separately:

- connection, pool acquisition, cancellation, and uncertain query failures
  become `OperationalError`;
- runtime channel failures, closed native handles, and transaction handles
  inherited by another process become `InterfaceError`;
- native decode failures become `DataError`;
- placeholder errors become `ProgrammingError`; and
- unsupported PostgreSQL result types become `NotSupportedError`.

Invalid configuration is validated separately and does not necessarily map to
`InterfaceError`.

## Transactions

The same translation applies to begin, query, savepoint, commit, and rollback
operations. Deferred constraint violations raised during commit still become
`IntegrityError` and retain their SQLSTATE and constraint diagnostics.

## Sensitive information

Errors include operation context and PostgreSQL diagnostics but do not include
the complete database URL or password. Logging application SQL and parameter
values is a separate policy and is not added implicitly by the native layer.
