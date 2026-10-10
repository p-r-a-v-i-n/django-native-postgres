# Transactions and savepoints

The native transaction path pins one pooled PostgreSQL connection until commit
or rollback. Python stores transaction routing state in a `ContextVar`, while
Rust owns the physical connection.

## Async atomic blocks

The Django fork supports `transaction.atomic()` as an async context manager:

```python
from django.db import transaction


async with transaction.atomic():
    await Book.objects.acreate(name="Django")
```

An exception leaving the block causes rollback:

```python
try:
    async with transaction.atomic():
        await Book.objects.acreate(name="temporary")
        raise ValueError("roll back")
except ValueError:
    pass
```

Async function decoration is also covered by the integration suite:

```python
@transaction.atomic
async def create_book():
    return await Book.objects.acreate(name="Django")
```

## Nested atomic blocks

Nested blocks create PostgreSQL savepoints on the same pinned connection. A
failed nested block rolls back to its savepoint without discarding successful
outer work when the error is handled correctly.

`savepoint=False` skips savepoint creation. If such a nested block fails, the
root transaction becomes rollback-only.

## Rollback state

PostgreSQL errors inside a transaction leave it unusable until rollback or
rollback to a savepoint. The executor records rollback-only state even when
application code catches the original query error.

Django's rollback-state APIs are connected to the native transaction state.
Attempting to continue a transaction marked for rollback raises an error
instead of sending more work to a failed PostgreSQL transaction.

## Transaction options

Options are accepted only by the outermost transaction:

```python
async with transaction.atomic(
    isolation_level="serializable",
    read_only=True,
    deferrable=True,
):
    ...
```

Supported isolation-level strings are:

- `read_uncommitted`;
- `read_committed`;
- `repeatable_read`; and
- `serializable`.

PostgreSQL treats read-uncommitted behavior according to its own transaction
semantics. Invalid isolation values and options on nested blocks are rejected
before they silently alter another transaction.

PostgreSQL accepts `deferrable` as a transaction characteristic, but it is
practically useful for a read-only transaction at the `serializable` isolation
level. Other combinations should not be assumed to provide the same waiting
and snapshot behavior.

## Commit callbacks

`transaction.on_commit()` callbacks are held until the root native transaction
commits. Callbacks registered in rolled-back savepoints are discarded. Robust
callbacks log their own failures and allow later callbacks to run.

The callbacks themselves are synchronous, matching Django's existing
`on_commit()` interface. If a callback returns a coroutine, it is not awaited
automatically.

## Async task ownership

Independent asyncio tasks have independent transaction context. A transaction
cannot be used from a child task which inherited its parent's context because
the executor validates the owning task.

Pass data across task boundaries rather than sharing an active transaction.

## Cancellation

Cancelling a query inside an atomic block marks the transaction rollback-only.
Exiting the transaction context rolls it back. Rust drains the cancelled query
response before deciding whether its connection can safely return to the pool.

## Current limitations

- `ATOMIC_REQUESTS` has not been established as supported.
- Commit and rollback do not yet have independently configurable deadlines.
- Connection-loss behavior where commit outcome is unknown needs explicit
  compatibility and operational documentation.
- Broader testing against Django's complete transaction suite remains future
  work.
