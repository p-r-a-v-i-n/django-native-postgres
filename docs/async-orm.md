# Using Django's async ORM

**Django's existing async ORM API is supported through the native backend path
for the operations covered by the integration suite.** Applications do not
need a project-specific QuerySet or model API.

Django still constructs queries, compiles SQL, applies field converters, and
creates model instances. This backend executes the compiled SQL and parameters
through Rust and PostgreSQL without moving the database I/O to a
`sync_to_async()` worker.

The exact operations covered by the integration suite are listed in
[Compatibility](compatibility.md).

Only the parameter and result types documented in
[Supported PostgreSQL values](supported-types.md) can currently cross the
native boundary.

## Loading relations

`select_related()` adds joins to the main query. The resulting joined query is
executed once through the native backend:

```python
book = await Book.objects.select_related("author").aget(pk=1)
author = book.author
```

Access to the selected related object does not issue another query.

`prefetch_related()` performs additional queries when the main queryset is
evaluated. Those related queries also use the native async backend:

```python
articles = [
    article
    async for article in Article.objects.order_by("title").prefetch_related(
        "tags"
    )
]

for article in articles:
    tags = [tag async for tag in article.tags.all()]
```

The integration suite covers foreign-key and many-to-many related managers,
nested prefetching, custom `Prefetch` querysets, generic relations, generic
foreign keys, and content types. Compatibility with every custom manager,
descriptor, database router, and third-party field is not yet established.

## Raw queries

Raw querysets support async iteration:

```python
books = [
    book
    async for book in Book.objects.raw(
        "SELECT id, title FROM library_book WHERE id = %s",
        [1],
    )
]
```

Django still requires the model's primary key in a raw query result. Positional
and named parameters, translated column names, empty results, and prefetching
are covered by the integration suite.

## Async `transaction.atomic()`

**Async `transaction.atomic()` is supported by the Django fork and this
backend.** It can be used as an async context manager:

```python
from django.db import transaction


async with transaction.atomic():
    book = await Book.objects.acreate(title="Django")
    await AuditEntry.objects.acreate(book=book, action="created")
```

Every query inside the block uses the same pinned native PostgreSQL connection
until commit or rollback. Async function decoration, nested blocks, savepoints,
rollback state, transaction options, task ownership, and commit callbacks are
documented in [Transactions and savepoints](transactions.md).

## Cancellation and application deadlines

Cancelling an asyncio task propagates cancellation to an in-flight native
operation. If the operation is waiting for the pool, it is removed before its
SQL executes. If PostgreSQL is already running the query, Rust sends a
cancellation request and drains the original response before deciding whether
the connection can be reused.

Applications can apply a deadline with `asyncio.timeout()`:

```python
import asyncio


try:
    async with asyncio.timeout(2):
        await Book.objects.acount()
except TimeoutError:
    ...
```

When the deadline expires, `asyncio.timeout()` cancels the current task and
raises `TimeoutError` outside the context manager. Cancellation can race with
normal query completion, so cleanup still waits for the original PostgreSQL
response when required. Like every asyncio deadline, it can be delivered only
when the event loop regains control; it does not preempt synchronous query
compilation or model conversion.

An application deadline covers the complete awaited operation and is different
from the native pool acquisition timeout. The backend does not currently set a
PostgreSQL statement timeout by default. Application deadlines, pool wait
timeouts, and PostgreSQL statement timeouts are separate controls.

The internal lifecycle and connection-reuse rules are documented in
[Architecture](architecture.md) and
[Connection pool design](connection-pool-design.md).

## Async iteration and memory

`aiterator(chunk_size=...)` asks Django's async cursor for bounded batches:

```python
async for book in Book.objects.aiterator(chunk_size=500):
    await process(book)
```

The current Rust executor still buffers and decodes the complete result before
the cursor returns its first batch. `chunk_size` controls Django's consumption
and prefetch behavior, not native memory use.

Do not treat the current implementation as safe for arbitrarily large query
results. Native result streaming and backpressure remain required work.

## Execution boundaries

Synchronous queryset operations still use Django's normal PostgreSQL backend
and Psycopg; they are not moved onto Tokio. Query construction, SQL compilation,
field conversion, and model construction remain synchronous CPU work. In the
native path, that CPU work runs on the Python event-loop thread while the
PostgreSQL I/O is asynchronous.

Custom QuerySet, manager, compiler, or model-method overrides may use the
Django fork's compatibility fallback instead of the native path. Treat an
extension as natively async only after testing its database execution path.
