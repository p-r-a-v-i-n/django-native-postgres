set dotenv-load

container_runtime := env_var_or_default("CONTAINER_RUNTIME", "docker")
postgres_image := env_var_or_default("DNP_POSTGRES_IMAGE", "postgres:18-alpine")
postgres_container := env_var_or_default("DNP_POSTGRES_CONTAINER", "django-native-postgres-test")
postgres_port := env_var_or_default("DNP_POSTGRES_PORT", "55432")
postgres_user := env_var_or_default("DNP_POSTGRES_USER", "django_native")
postgres_password := env_var_or_default("DNP_POSTGRES_PASSWORD", "django_native")
postgres_database := env_var_or_default("DNP_POSTGRES_DB", "django_native_postgres_tests")
default_database_url := "postgresql://" + postgres_user + ":" + postgres_password + "@127.0.0.1:" + postgres_port + "/" + postgres_database
test_database_url := env_var_or_default("DJANGO_NATIVE_POSTGRES_TEST_DATABASE_URL", default_database_url)

export CONTAINER_RUNTIME := container_runtime
export DNP_POSTGRES_IMAGE := postgres_image
export DNP_POSTGRES_CONTAINER := postgres_container
export DNP_POSTGRES_PORT := postgres_port
export DNP_POSTGRES_USER := postgres_user
export DNP_POSTGRES_PASSWORD := postgres_password
export DNP_POSTGRES_DB := postgres_database
export DNP_TEST_DATABASE_URL := test_database_url

default:
    @just --list

setup:
    uv sync --no-install-project
    uv run --no-sync maturin develop

build:
    uv run --no-sync maturin develop

test:
    uv run --no-sync pytest

lint:
    uv run --no-sync ruff check .
    cargo clippy --all-targets -- -D warnings

format:
    uv run --no-sync ruff check . --fix
    uv run --no-sync ruff format .
    cargo fmt

format-check:
    uv run --no-sync ruff format --check .
    cargo fmt --check

check: format-check lint test

db-up:
    #!/usr/bin/env bash
    set -euo pipefail

    if "$CONTAINER_RUNTIME" container inspect "$DNP_POSTGRES_CONTAINER" >/dev/null 2>&1; then
        "$CONTAINER_RUNTIME" start "$DNP_POSTGRES_CONTAINER" >/dev/null
    else
        "$CONTAINER_RUNTIME" run --detach \
            --name "$DNP_POSTGRES_CONTAINER" \
            --label django-native-postgres.role=test-database \
            --env POSTGRES_USER="$DNP_POSTGRES_USER" \
            --env POSTGRES_PASSWORD="$DNP_POSTGRES_PASSWORD" \
            --env POSTGRES_DB="$DNP_POSTGRES_DB" \
            --publish "127.0.0.1:$DNP_POSTGRES_PORT:5432" \
            --health-cmd "pg_isready -U $DNP_POSTGRES_USER -d $DNP_POSTGRES_DB" \
            --health-interval 1s \
            --health-timeout 5s \
            --health-retries 30 \
            "$DNP_POSTGRES_IMAGE" >/dev/null
    fi

    attempt=0
    until "$CONTAINER_RUNTIME" exec "$DNP_POSTGRES_CONTAINER" \
        pg_isready --username "$DNP_POSTGRES_USER" --dbname "$DNP_POSTGRES_DB" >/dev/null 2>&1; do
        attempt=$((attempt + 1))
        if (( attempt >= 30 )); then
            "$CONTAINER_RUNTIME" logs "$DNP_POSTGRES_CONTAINER"
            exit 1
        fi
        sleep 1
    done

    echo "PostgreSQL is ready on 127.0.0.1:$DNP_POSTGRES_PORT"

db-down:
    #!/usr/bin/env bash
    set -euo pipefail

    if "$CONTAINER_RUNTIME" container inspect "$DNP_POSTGRES_CONTAINER" >/dev/null 2>&1; then
        "$CONTAINER_RUNTIME" stop "$DNP_POSTGRES_CONTAINER" >/dev/null
        echo "Stopped $DNP_POSTGRES_CONTAINER"
    fi

test-postgres: db-up
    @DJANGO_NATIVE_POSTGRES_TEST_DATABASE_URL="$DNP_TEST_DATABASE_URL" uv run --no-sync pytest -q tests/test_postgres_execution.py tests/test_django_postgres.py
