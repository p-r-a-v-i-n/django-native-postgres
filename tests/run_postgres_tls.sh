#!/usr/bin/env bash
set -euo pipefail

container_runtime="${CONTAINER_RUNTIME:-docker}"
postgres_image="${DNP_POSTGRES_IMAGE:-postgres:18-alpine}"
postgres_container="${DNP_POSTGRES_TLS_CONTAINER:-django-native-postgres-tls-test}"
postgres_port="${DNP_POSTGRES_TLS_PORT:-55433}"
postgres_user="${DNP_POSTGRES_USER:-django_native}"
postgres_password="${DNP_POSTGRES_PASSWORD:-django_native}"
postgres_database="${DNP_POSTGRES_DB:-django_native_postgres_tests}"
certificate_directory="$(mktemp -d)"

cleanup() {
    "$container_runtime" rm --force "$postgres_container" >/dev/null 2>&1 || true
    rm -rf -- "$certificate_directory"
}
trap cleanup EXIT

"$container_runtime" rm --force "$postgres_container" >/dev/null 2>&1 || true

openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 2 \
    -subj "/CN=django-native-postgres test CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -keyout "$certificate_directory/ca.key" \
    -out "$certificate_directory/ca.crt" >/dev/null 2>&1

openssl req -newkey rsa:2048 -nodes -sha256 \
    -subj "/CN=localhost" \
    -keyout "$certificate_directory/server.key" \
    -out "$certificate_directory/server.csr" >/dev/null 2>&1

printf '%s\n' \
    "subjectAltName=DNS:localhost" \
    "extendedKeyUsage=serverAuth" \
    >"$certificate_directory/server.ext"

openssl x509 -req -sha256 -days 2 \
    -in "$certificate_directory/server.csr" \
    -CA "$certificate_directory/ca.crt" \
    -CAkey "$certificate_directory/ca.key" \
    -CAcreateserial \
    -extfile "$certificate_directory/server.ext" \
    -out "$certificate_directory/server.crt" >/dev/null 2>&1

openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 2 \
    -subj "/CN=untrusted test CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -keyout "$certificate_directory/untrusted-ca.key" \
    -out "$certificate_directory/untrusted-ca.crt" >/dev/null 2>&1

"$container_runtime" run --detach \
    --name "$postgres_container" \
    --label django-native-postgres.role=tls-test-database \
    --env POSTGRES_USER="$postgres_user" \
    --env POSTGRES_PASSWORD="$postgres_password" \
    --env POSTGRES_DB="$postgres_database" \
    --publish "127.0.0.1:$postgres_port:5432" \
    --volume "$certificate_directory:/certificates:ro" \
    --entrypoint sh \
    "$postgres_image" \
    -c '
        set -eu
        mkdir -p /var/lib/postgresql/tls
        cp /certificates/server.crt /var/lib/postgresql/tls/server.crt
        cp /certificates/server.key /var/lib/postgresql/tls/server.key
        chown postgres:postgres /var/lib/postgresql/tls/server.crt /var/lib/postgresql/tls/server.key
        chmod 600 /var/lib/postgresql/tls/server.key
        exec docker-entrypoint.sh postgres \
            -c ssl=on \
            -c ssl_cert_file=/var/lib/postgresql/tls/server.crt \
            -c ssl_key_file=/var/lib/postgresql/tls/server.key
    ' >/dev/null

attempt=0
until "$container_runtime" exec "$postgres_container" \
    pg_isready --username "$postgres_user" --dbname "$postgres_database" >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if ((attempt >= 30)); then
        "$container_runtime" logs "$postgres_container"
        exit 1
    fi
    sleep 1
done

database_url="postgresql://$postgres_user:$postgres_password@localhost:$postgres_port/$postgres_database"

DNP_TLS_DATABASE_URL="$database_url" \
DNP_TLS_ROOT_CERT="$certificate_directory/ca.crt" \
DNP_TLS_UNTRUSTED_ROOT_CERT="$certificate_directory/untrusted-ca.crt" \
    uv run --no-sync pytest -q tests/test_postgres_tls.py
