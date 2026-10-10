# Architecture

`django-native-postgres` keeps Django's ORM and replaces the database I/O path
for opted-in async operations. Rust owns the asynchronous runtime, PostgreSQL
pool, connections, and transaction connection affinity.

## Responsibility boundary

```text
Django QuerySet and model APIs
              |
              v
Django query objects and SQL compiler
              |
              | SQL + parameters
              v
Django async backend contract
              |
              v
DatabaseWrapper and NativeAsyncCursor
              |
              v
NativeExecutor and NativeTransaction
              |
              | PyO3 awaitable
              v
process-local Tokio runtime service
              |
              v
deadpool-postgres + tokio-postgres
              |
              v
PostgreSQL
```

Django remains responsible for:

- queryset and model APIs;
- query construction;
- SQL compilation;
- ORM result conversion;
- relation and prefetch behavior;
- model construction and state; and
- migrations and synchronous backend behavior.

The project is responsible for:

- async database execution;
- Django placeholder rewriting;
- native parameter binding and result decoding;
- bounded PostgreSQL connection pooling;
- transaction and savepoint execution;
- task cancellation and connection recovery;
- process-local runtime and pool ownership; and
- translation of native and PostgreSQL failures into Django exceptions.

## Django integration

Upstream Django's async ORM methods generally wrap synchronous ORM methods in
`sync_to_async()`. The project fork adds an async compiler/backend seam. The
compiler still performs synchronous CPU work to create SQL; only database I/O
crosses the native async boundary.

In the native path, SQL compilation and most result conversion and model
construction run synchronously on the Python event-loop thread. Prefetching
also performs synchronous in-memory work while connecting related objects.
These operations do not make the PostgreSQL I/O synchronous, but unusually
expensive ORM CPU work can delay the event loop.

The backend advertises that it supports async execution. The fork then calls
the backend's async cursor contract instead of running its synchronous cursor
in a thread. Existing backends which do not opt in retain their current path.

See [Django fork requirement](django-fork.md) for the dependency and release
constraints created by this contract.

## Python backend layer

`DatabaseWrapper` subclasses Django's PostgreSQL wrapper. It lazily creates one
`NativeExecutor` for its database wrapper and exposes the async execution and
transaction methods used by the Django fork.

`NativeAsyncCursor` provides Django with:

- async execution;
- row count and column description metadata;
- `afetchone()`;
- `afetchmany()`; and
- `afetchall()`.

The cursor currently receives the entire decoded result from Rust. Its fetch
methods control how rows are presented to Django, not how many rows Rust keeps
in memory. True bounded streaming is future architecture work.

## PyO3 boundary

Python receives opaque `PoolHandle` and `TransactionHandle` objects. It never
receives or manipulates a `tokio-postgres` connection.

Crossing the boundary once per operation keeps connection ownership in Rust
and avoids one Python/Rust call per result value. Future streaming should cross
once per bounded row batch.

The native boundary exposes three result shapes. `execute()` returns rows,
`execute_result()` adds the affected-row count, and `execute_with_metadata()`
also adds column names. `NativeAsyncCursor` uses the metadata form. SQLSTATE
and selected PostgreSQL diagnostics are attached to native database exceptions
so the Python layer can map them into Django's exception hierarchy without
parsing error text.

## Runtime ownership

Each operating-system process has one lazily initialized Tokio runtime service.
The service runs a current-thread Tokio runtime on a dedicated background
operating-system thread. Python sends commands to it through asynchronous
channels, and the service spawns each command as a Tokio task. The service also
owns the registry of Rust pools.

After `fork()`, the child process detects that its process ID has changed and
creates a new runtime and empty pool registry. It never attempts to reuse the
parent's Tokio runtime or PostgreSQL sockets.

## Pool ownership

Each `NativeExecutor` owns an opaque pool handle. The handle ID, rather than a
database URL or Django alias, identifies the Rust pool. Two executors using the
same connection settings therefore have independent pool limits and lifecycle.

The pool is created lazily on first execution. Autocommit queries borrow any
available connection and return it after execution. Pool acquisition has a
separate configurable wait timeout.

See [Connection pool design](connection-pool-design.md) for lifecycle and
failure behavior.

## Transactions and task ownership

A native transaction borrows one connection for its complete lifetime. Rust
returns an opaque transaction handle, and every query using that handle is
sent to the actor which owns the pinned connection.

Python stores the active `NativeTransaction` in a `ContextVar`. This isolates
unrelated asyncio tasks while allowing the executor to route queries from the
owning task through the active transaction. A child task cannot use an
inherited transaction context because transaction ownership is validated
against the original asyncio task.

Nested atomic blocks use PostgreSQL savepoints on the same connection. Blocks
without a savepoint propagate rollback-only state to the root transaction.

## Cancellation

Cancelling or dropping an in-flight PyO3 awaitable drops its Rust cancellation
guard, which signals the corresponding command. If the operation is waiting
for a pool connection, its acquisition future is dropped before SQL execution
begins. An active query is cancelled with PostgreSQL's cancellation protocol.

Rust drains the original query response before deciding whether the connection
can be reused. An autocommit connection whose state is uncertain is detached
from the pool. When a query inside a `NativeTransaction` is cancelled, the
Python transaction wrapper marks the transaction rollback-only. Exiting the
transaction context then rolls it back.

## Error boundary

PostgreSQL server errors preserve:

- SQLSTATE;
- severity and primary message;
- detail and hint;
- schema, table, column, and datatype names; and
- constraint name.

Python maps SQLSTATE classes to Django's `DataError`, `IntegrityError`,
`InternalError`, `NotSupportedError`, `OperationalError`, `ProgrammingError`,
or base `DatabaseError`. Native pool and connection failures map to
`OperationalError`. Runtime channel failures, closed native handles, and
transaction handles inherited by another process map to `InterfaceError`.

The native exception remains available as the Django exception's `__cause__`.
Error messages do not include the database URL or password.

See [Database errors and diagnostics](database-errors.md) for the complete
mapping, diagnostic fields, and transaction retry guidance.

## Synchronous behavior

This backend still subclasses Django's PostgreSQL backend and depends on
Psycopg for synchronous behavior. Migrations, schema editing, and synchronous
ORM operations are not moved onto the Tokio runtime by this project.

## Work still in progress

The main areas still being developed are:

1. support for more PostgreSQL parameter and result types;
2. bounded result streaming and backpressure;
3. TLS and additional PostgreSQL connection options;
4. session initialization and reset behavior;
5. broader Django compatibility testing; and
6. reproducible installation of the Django fork dependency.

These areas can be added incrementally without changing the core ownership
model. Unsupported native values already fail explicitly, and native connection
options are supported only where the documentation says so. The Django fork's
documented compatibility fallbacks for other backends and custom ORM paths are
separate from these native capability limits.
