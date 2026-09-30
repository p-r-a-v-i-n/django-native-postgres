# django-native-postgres

Native asynchronous PostgreSQL execution for Django, powered by Rust.

> [!WARNING]
> This project is in early development and is not yet usable as a Django
> database `ENGINE`.

## Purpose

`django-native-postgres` keeps Django's ORM, models, SQL compiler, migrations,
and field behavior in Python. It aims to execute Django's compiled PostgreSQL
queries through a native asynchronous runtime, with connection management and
pooling handled in Rust. It is not a replacement ORM.

## Development

Requirements:

- Python 3.12 or newer
- Rust 1.85 or newer
- `uv`
- `just`
- Docker or another Docker-compatible container runtime for integration tests

Create the environment and build the native extension:

```console
just setup
```

Run the checks:

```console
just check
```

### PostgreSQL integration tests

The native execution tests use a dedicated PostgreSQL container bound only to
the local machine. Start it and run the integration tests with:

```console
just test-postgres
```

The defaults can be overridden through environment variables or an untracked
`.env` file:

```console
DNP_POSTGRES_IMAGE=postgres:17-alpine
DNP_POSTGRES_CONTAINER=django-native-postgres-test
DNP_POSTGRES_PORT=55432
DNP_POSTGRES_USER=django_native
DNP_POSTGRES_PASSWORD=django_native
DNP_POSTGRES_DB=django_native_postgres_tests
```

Set `CONTAINER_RUNTIME` to another Docker-compatible executable when needed.
An explicit `DJANGO_NATIVE_POSTGRES_TEST_DATABASE_URL` overrides the URL built
from the values above. Without that variable, direct `pytest` runs skip the
database test; `just test-postgres` configures it automatically.

Stop the test database without deleting its data:

```console
just db-down
```

## License

Licensed under the BSD 3-Clause License. See `LICENSE`.
