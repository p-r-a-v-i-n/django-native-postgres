# Connection pool design

Creating a PostgreSQL connection for every query is too expensive for a busy
application. The backend uses `deadpool-postgres` to reuse connections and set
a hard upper bound on concurrent database connections for each pool handle.

Rust owns the pools and physical PostgreSQL connections. Python holds only
opaque handles.

## Ownership

```text
Django DatabaseWrapper
        |
        v
NativeExecutor
        |
        v
opaque Rust PoolHandle
        |
        v
deadpool-postgres pool
        |
        v
tokio-postgres connections
```

Each `NativeExecutor` creates a distinct handle. Two handles using the same
database URL still create independent pools. This keeps configuration and
lifecycle explicit and prevents the runtime from using a database URL, which
may contain credentials, as a global identity.

Creating a handle does not immediately connect to PostgreSQL. The process-local
runtime creates the real pool lazily when the first query or transaction needs
a connection.

## Configuration

Pool configuration is read from `OPTIONS["native_pool"]`. See
[Configuration](configuration.md) for the complete `DATABASES` example and
validation rules.

`max_size` is the maximum number of physical connections owned by this handle.
`wait_timeout_ms` limits how long an operation waits to borrow a connection
when the pool is exhausted. Both must be positive integers.

`max_size` is not a whole-application limit. Separate database wrappers,
aliases, and worker processes own separate pools. The possible total is the
sum of every pool's maximum across every running process; for equally sized
pools, this is roughly `max_size * handles per process * worker processes`.

The pool wait timeout does not limit PostgreSQL statement execution time.

## Autocommit query lifecycle

```text
execute request
      |
      v
resolve or lazily create pool
      |
      v
wait for one connection
      |
      v
execute and decode query
      |
      v
return healthy connection to pool
```

The pool provides backpressure. When every connection is busy, another query
waits instead of opening connections without a bound. If the configured wait
timeout expires, Django receives `OperationalError`.

## Transaction connection affinity

A transaction must use one physical connection from begin through commit or
rollback. Beginning a native transaction borrows and pins one pooled
connection. Rust starts a transaction actor and returns an opaque
`TransactionHandle` to Python.

```text
begin transaction
      |
      v
borrow and pin connection
      |
      +--> query
      +--> savepoint
      +--> query
      |
      v
commit or rollback
      |
      v
return connection to pool
```

Nested Django atomic blocks use savepoints on that connection. Dropping a live
transaction handle causes rollback, preventing an abandoned transaction from
being returned to the pool.

An active transaction can finish after its pool handle is closed because it
already owns its connection. Once returned, the closed pool drops that
connection.

## Query errors and connection reuse

A normal PostgreSQL statement error does not necessarily damage the connection.
When PostgreSQL returns a normal `ERROR` and the client remains open, an
autocommit connection can return to the pool.

Connections are detached when they are closed or when cancellation or another
failure leaves their protocol state uncertain. A later request can create a
replacement. Returning an uncertain connection is never preferred over losing
one pooled connection.

Inside a transaction, a PostgreSQL error leaves the transaction in a failed
state until rollback or rollback to a savepoint. Python tracks rollback-only
state so later transaction operations cannot silently continue on an invalid
transaction.

## Cancellation

Cancellation is handled differently depending on where a query is waiting:

- A request waiting for a pool connection is removed and never executes later.
- An active query is cancelled through PostgreSQL's cancellation protocol.
- Rust drains the original response before reusing the connection.
- Cancelling a transaction query marks the Python transaction rollback-only;
  leaving its atomic context then rolls it back.

This prevents a cancelled Python request from leaving invisible work running
and holding a pooled connection.

## Pool closure

`NativeExecutor.close()` closes only its own pool handle. Closing is terminal
for that handle:

- idle connections are closed;
- an already active query or transaction may finish;
- returned connections are dropped after closure;
- new work through that handle fails with `InterfaceError`; and
- closing the handle repeatedly is safe.

`close_pools()` is a process-wide test and cleanup operation. It closes the
current runtime registry. A handle that was not individually closed may lazily
create its pool again after this global reset.

The backend does not yet call `NativeExecutor.close()` from
`DatabaseWrapper.close()`. Normal Django wrapper shutdown therefore does not
currently close the native pool; explicit lifecycle integration remains open
work.

## Process safety

The runtime service records its operating-system process ID. After a worker
forks, the child detects the new ID and creates a new Tokio runtime and empty
pool registry. PostgreSQL sockets and Tokio state from the parent are never
reused in the child.

A transaction handle inherited across a fork is rejected because its pinned
connection belongs to the parent process.

## Covered behavior

The integration suite verifies:

- sequential connection reuse;
- bounded concurrent connections;
- separate pools for separate handles;
- successful waiting and pool acquisition timeout;
- replacement of broken connections;
- reuse after safe query errors;
- independent pool closure;
- closure before first use and repeated closure;
- closure during active queries and transactions;
- transaction connection pinning and release;
- cancellation while waiting and while executing; and
- runtime and pool recreation after `fork()`.

## Remaining pool work

Work still in progress includes:

- TLS;
- complete PostgreSQL connection options;
- session initialization and reset rules;
- idle connection and maximum-lifetime policies;
- integration with Django wrapper and process shutdown;
- metrics and tracing hooks; and
- restart, failover, and longer-running concurrency tests.
