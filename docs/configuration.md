# Configuration

The backend uses Django's normal `DATABASES` setting and adds a small native
pool configuration section.

## Database engine

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
            "sslmode": "require",
            "sslrootcert": "/etc/ssl/postgres-ca.pem",
            "native_pool": {
                "max_size": 16,
                "wait_timeout_ms": 30_000,
            },
        },
    },
}
```

The package must be importable and the application must use the compatible
Django fork described in [Django fork requirement](django-fork.md).

## Native pool options

### `max_size`

Maximum number of PostgreSQL connections owned by this database wrapper's
native pool.

- Default: `16`
- Required type: positive integer
- Boolean values are rejected even though `bool` is an `int` subclass in
  Python.

Every process owns its own pool, and separate Django database wrappers own
separate pool handles. The total possible connection count must therefore be
calculated across worker processes and database aliases.

### `wait_timeout_ms`

Maximum time an operation waits to borrow a connection from an exhausted
native pool.

- Default: `30_000`
- Required type: positive integer

This is not a PostgreSQL statement timeout. It limits only pool acquisition.

## TLS options

### `sslmode`

Controls whether the native PostgreSQL connection uses TLS.

- `disable` uses a plaintext connection.
- `prefer` requests TLS and permits a plaintext server. This is the default.
- `require` requires TLS and rejects a plaintext server.

TLS connections verify the server certificate and hostname. This makes the
current `require` behavior stricter than libpq's encryption-only `require`
mode. Other libpq modes, including `allow`, `verify-ca`, and `verify-full`, are
rejected until their behavior is implemented explicitly.

### `sslrootcert`

Path to a PEM certificate or certificate bundle used to verify the PostgreSQL
server. When omitted, the native connection uses the operating system's root
certificates. A configured file replaces, rather than extends, that native
root store.

`sslrootcert` is ignored when `sslmode` is `disable`. Missing, empty,
malformed, and unusable certificate files fail explicitly when the native
pool first connects.

Client certificates through `sslcert` and `sslkey` are not yet supported.

## Connection settings currently forwarded

The native executor currently builds its connection configuration from:

- `NAME`;
- `USER`;
- `PASSWORD`;
- `HOST`;
- `PORT`;
- `OPTIONS["sslmode"]`;
- `OPTIONS["sslrootcert"]`; and
- Django's resolved database time zone.

If `NAME` is empty, it defaults to `postgres`.

The synchronous Django PostgreSQL backend may understand additional Psycopg
and `OPTIONS` settings, but they are not automatically applied to the native
Tokio connections. Client certificates, roles, service files, search paths,
application names, and arbitrary PostgreSQL connection options need explicit
native support before they can be documented as compatible.

## Session initialization

Every newly created physical connection is initialized with
`DatabaseWrapper.timezone_name`. Django resolves this from `USE_TZ`, the
project `TIME_ZONE`, and the database `TIME_ZONE` setting.

Initialization runs once when PostgreSQL opens the connection, not every time
the pool lends that connection to a query. Reusing an initialized connection
therefore adds no initialization query.

The pool does not currently reset arbitrary session changes before reuse.
Applications must not rely on standalone queries preserving `SET ROLE`,
`search_path`, temporary tables, listeners, session advisory locks, or other
session-local state. Transaction-specific settings should use `SET LOCAL`
inside `transaction.atomic()` so PostgreSQL removes them when the transaction
ends.

## Standard Django settings not yet applied to native connections

The native pool does not currently use these standard Django connection
settings:

- `CONN_MAX_AGE`;
- `CONN_HEALTH_CHECKS`;
- `AUTOCOMMIT=False`;
- `ATOMIC_REQUESTS`;
- most PostgreSQL `OPTIONS`; or
- session initialization other than the database time zone.

These settings may still affect the inherited synchronous Psycopg path. They
do not configure the Tokio and `deadpool-postgres` connections. In particular,
native operations run in PostgreSQL autocommit mode unless they are inside the
supported async `transaction.atomic()` path or a direct native transaction.
Applications must not use `AUTOCOMMIT=False` as if it controlled native
execution.

## Multiple databases

Each Django `DatabaseWrapper` lazily creates and caches its own
`NativeExecutor`. A database alias therefore has an independent native pool.
Database router and multiple-alias behavior still requires broader
compatibility testing before it becomes a supported release claim.

## Pool shutdown

`NativeExecutor.close()` closes one handle and is terminal for that handle.
The backend does not yet connect this method to `DatabaseWrapper.close()`, so
the native pool currently remains alive until it is explicitly closed or the
process exits.

`_native.close_pools()` closes the complete process-local registry. It is a
low-level operation used primarily by the test suite, not the normal
application lifecycle API.

See [Connection pool design](connection-pool-design.md) for connection reuse,
transactions, cancellation, closure, and process-fork behavior.
