# Development

This guide describes the local development workflow for
`django-native-postgres`.

## Requirements

- Python 3.12 or newer
- Rust 1.85 or newer
- `uv`
- `just`
- Docker or another Docker-compatible container runtime, only for the
  PostgreSQL integration tests

The project uses Maturin to build the PyO3 extension into its `uv` virtual
environment.

## Set up the project

```console
git clone https://github.com/p-r-a-v-i-n/django-native-postgres.git
cd django-native-postgres
just setup
```

`just setup` synchronizes development dependencies and installs the native
extension as an editable package.

The current `uv` configuration installs the required Django fork. Running only
`uv lock` changes the lock file; run `uv sync --no-install-project` or
`just setup` when the locked Django revision changes.

## Run checks

```console
just format
just check
just build
```

`just check` verifies Python formatting and linting, Rust formatting and strict
Clippy warnings, and runs the Python test suite. It does not run Rust unit
tests; use `cargo test` for those.

PostgreSQL tests skip during a normal pytest run when
`DJANGO_NATIVE_POSTGRES_TEST_DATABASE_URL` is not configured. If that variable
is already present in the environment, `just check` can run those tests against
that database without starting the project container.

Rust unit tests can also be run directly:

```console
cargo test
```

`just build` runs `maturin develop`, which installs an editable development
build into the project virtual environment. It does not produce a
distributable release wheel.

## PostgreSQL integration tests

Run the complete PostgreSQL suite with:

```console
just test-postgres
```

The recipe starts a dedicated PostgreSQL container when necessary and passes
its connection URL to pytest.

Run one test or a selected group by passing normal pytest targets:

```console
just test-postgres tests/test_postgres_execution.py::test_name
just test-postgres tests/test_django_postgres.py
```

Stop the container without deleting its persistent data:

```console
just db-down
```

## Test database configuration

Defaults can be overridden in the environment or an untracked `.env` file:

```text
DNP_POSTGRES_IMAGE=postgres:18-alpine
DNP_POSTGRES_CONTAINER=django-native-postgres-test
DNP_POSTGRES_PORT=55432
DNP_POSTGRES_USER=django_native
DNP_POSTGRES_PASSWORD=django_native
DNP_POSTGRES_DB=django_native_postgres_tests
```

Set `CONTAINER_RUNTIME` to a Docker-compatible executable when Docker is not
the desired runtime. `DJANGO_NATIVE_POSTGRES_TEST_DATABASE_URL` can override
the complete test URL.

Direct pytest runs skip PostgreSQL tests when that URL is not configured. When
it is configured, the caller is responsible for making sure the referenced
database is available and safe to use for tests.

## Before committing

Run these commands after changing Python and Rust code:

```console
just format
just check
just build
just test-postgres
cargo test
```

Documentation-only changes do not require rebuilding the extension, but the
Markdown links and examples should still be reviewed from the repository root.
