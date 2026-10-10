# django-native-postgres

Native asynchronous PostgreSQL execution for Django, powered by Rust, Tokio,
and `tokio-postgres`.

> [!WARNING]
> This project is experimental and it currently requires a
> development fork of Django, supports only a small set
> of PostgreSQL value types, buffers complete query results in memory, and
> connects without TLS.

## What this project does

`django-native-postgres` keeps Django's ORM, models, query construction, SQL
compiler, relations, and model creation. Compiled SQL is handed to this
backend, which performs the database I/O asynchronously without moving that
I/O into a `sync_to_async()` worker thread.

```text
Django async ORM
      |
      v
Django SQL compiler
      |
      v
django-native-postgres (Python)
      |
      v
PyO3 -> Tokio -> Rust-owned PostgreSQL pool
      |
      v
PostgreSQL
```

This is a PostgreSQL backend for Django. It is not a replacement ORM.

## Current capabilities

The project integration suite currently covers:

- async queryset reads, iteration, values, aggregates, and raw queries;
- async creates, updates, deletes, bulk operations, and model methods;
- related managers, prefetching, generic relations, and content types;
- native async transactions, nested savepoints, rollback state, and
  `on_commit()` callbacks;
- transaction isolation, read-only, and deferrable options;
- Rust-owned connection pooling with bounded concurrency and acquisition
  timeouts;
- cancellation of active and pool-waiting queries;
- runtime and pool recreation after POSIX `fork()`, with inherited transaction
  handles rejected.

See [Compatibility](docs/compatibility.md) for the exact tested surface and
current limitations.

## Current limitations

Native async execution works for the ORM operations covered by the integration
suite, but the project still has the following release limitations:

- The native async ORM contract exists only in the project's Django fork.
- Query parameters currently support only strings, integers, and `None`.
- Result decoding currently supports text and integer PostgreSQL types.
- Async iteration is exposed in chunks, but Rust currently buffers the entire
  result before the first chunk reaches Django.
- Native PostgreSQL connections currently use `NoTls`.
- The project has not yet completed Django's PostgreSQL compatibility suite or
  a supported platform and PostgreSQL version matrix.

Unsupported native PostgreSQL result types fail explicitly. The Django fork
still preserves compatibility fallbacks for backends and custom ORM paths that
do not use the native async contract.

## Development quick start

The current version must be used with the exact tested Django fork. Install the
fork and this package in the same command so the dependency resolver uses the
required Django distribution:

```console
python -m pip install \
    "Django @ git+https://github.com/p-r-a-v-i-n/django.git@312860d0e58e0d54fef10bae7ecf63f537723d17" \
    django-native-postgres
```

The package metadata requires the fork's exact generated Django version. A
plain installation without the fork therefore fails instead of silently using
an incompatible upstream Django release. See
[Django fork requirement](docs/django-fork.md) for details.

Requirements:

- Python 3.12 or newer;
- Rust 1.98 or newer;
- `uv`;
- `just`; and
- Docker or another Docker-compatible container runtime for PostgreSQL
  integration tests.

Create the environment and build the native extension:

```console
just setup
```

Configure the backend through Django's normal `DATABASES` setting:

```python
DATABASES = {
    "default": {
        "ENGINE": "django_native_postgres",
        "NAME": "application",
        "USER": "application",
        "PASSWORD": "secret",
        "HOST": "127.0.0.1",
        "PORT": "5432",
        "OPTIONS": {
            "native_pool": {
                "max_size": 16,
                "wait_timeout_ms": 30_000,
            },
        },
    }
}
```

Supported Django async ORM calls then use the native backend path:

```python
from your_app.models import Book


async def find_book(book_id):
    return await Book.objects.aget(pk=book_id)
```

The complete configuration surface is described in
[Configuration](docs/configuration.md).

Run formatting, linting, Rust checks, and tests that do not require a live
PostgreSQL server:

```console
just check
```

Run the PostgreSQL integration suite:

```console
just test-postgres
```

Run one PostgreSQL test:

```console
just test-postgres tests/test_postgres_execution.py::test_name
```

The test database configuration and other development commands are described
in [Development](docs/development.md).

## Documentation

Start with the [documentation index](docs/index.md). Important references are:

- [Architecture](docs/architecture.md)
- [Django fork requirement](docs/django-fork.md)
- [Connection pool design](docs/connection-pool-design.md)
- [Supported PostgreSQL values](docs/supported-types.md)
- [Compatibility and limitations](docs/compatibility.md)

The documentation is written in Markdown so it can be published with MkDocs
later without conversion.

## License

Licensed under the BSD 3-Clause License. See [LICENSE](LICENSE).
