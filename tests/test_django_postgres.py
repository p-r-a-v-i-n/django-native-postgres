import asyncio
import os
from unittest import mock

import pytest
from django.db import connections
from django_native_postgres import _native
from psycopg.conninfo import make_conninfo

from tests.integration_app.models import Book

pytestmark = [
    pytest.mark.django_db,
    pytest.mark.skipif(
        "DJANGO_NATIVE_POSTGRES_TEST_DATABASE_URL" not in os.environ,
        reason="DJANGO_NATIVE_POSTGRES_TEST_DATABASE_URL is not configured",
    ),
]


@pytest.fixture(scope="module", autouse=True)
def close_native_pools_after_tests():
    yield
    asyncio.run(_native.close_pools())


@pytest.fixture
def compiled_book_query(transactional_db):
    Book.objects.create(name="Django")

    queryset = Book.objects.filter(name="Django").values_list(
        "name",
        flat=True,
    )
    sql, params = queryset.query.get_compiler(using="default").as_sql()

    settings = connections["default"].settings_dict
    database_url = make_conninfo(
        dbname=settings["NAME"],
        user=settings["USER"],
        password=settings["PASSWORD"],
        host=settings["HOST"],
        port=settings["PORT"],
    )

    return database_url, sql, params


@pytest.fixture
def compiled_book_exists_query(transactional_db):
    Book.objects.create(name="Django")

    query = Book.objects.filter(name="Django").query.exists()
    return query.get_compiler(using="default").as_sql()


@pytest.fixture
def django_book(transactional_db):
    return Book.objects.create(name="Django")


@pytest.fixture
def django_books(transactional_db):
    return Book.objects.bulk_create(
        [
            Book(name="Async"),
            Book(name="Django"),
            Book(name="PostgreSQL"),
        ]
    )


def test_django_can_create_and_query_model_with_sync_backend():
    Book.objects.create(name="Django")

    assert Book.objects.filter(name="Django").exists()


@pytest.mark.asyncio
async def test_django_compiled_query_executes_through_rust(
    compiled_book_query,
):
    database_url, sql, params = compiled_book_query
    pool = _native.create_pool(database_url=database_url)

    rows = await _native.execute(
        pool=pool,
        sql=sql,
        params=params,
    )

    assert rows == [["Django"]]


@pytest.mark.asyncio
async def test_django_connection_executes_compiled_query_through_native_executor(
    compiled_book_query,
):
    _, sql, params = compiled_book_query
    rows = await connections["default"].aexecute(sql, params)
    assert rows == [["Django"]]


@pytest.mark.asyncio
async def test_django_connection_uses_active_native_transaction(transactional_db):
    connection = connections["default"]
    executor = connection.get_async_executor()

    async with executor.transaction():
        await connection.aexecute(
            'INSERT INTO "integration_app_book" ("name") VALUES (%s)',
            ("committed",),
        )

    with pytest.raises(ValueError, match="roll back this transaction"):
        async with executor.transaction():
            await connection.aexecute(
                'INSERT INTO "integration_app_book" ("name") VALUES (%s)',
                ("rolled back",),
            )
            raise ValueError("roll back this transaction")

    rows = await connection.aexecute(
        'SELECT "name" FROM "integration_app_book" ORDER BY "name"'
    )

    assert rows == [["committed"]]


@pytest.mark.asyncio
async def test_django_exists_query_executes_through_native_executor(
    compiled_book_exists_query,
):
    sql, params = compiled_book_exists_query

    rows = await connections["default"].aexecute(sql, params)

    assert rows == [[1]]


@pytest.mark.asyncio
@pytest.mark.parametrize("expected", [True, False])
async def test_django_aexists_executes_through_native_backend(
    django_book,
    expected,
):
    connection = connections["default"]
    executor = connection.get_async_executor()
    name = django_book.name if expected else "missing"

    with mock.patch.object(
        executor,
        "execute",
        wraps=executor.execute,
    ) as execute:
        result = await Book.objects.filter(name=name).aexists()

    assert result is expected
    execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_django_async_iteration_executes_through_native_backend(django_books):
    executor = connections["default"].get_async_executor()

    with mock.patch.object(
        executor,
        "execute",
        wraps=executor.execute,
    ) as execute:
        books = [book async for book in Book.objects.order_by("name")]

    assert [book.name for book in books] == ["Async", "Django", "PostgreSQL"]
    execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_django_async_iteration_supports_values_iterables(django_books):
    executor = connections["default"].get_async_executor()

    with mock.patch.object(
        executor,
        "execute",
        wraps=executor.execute,
    ) as execute:
        values = [value async for value in Book.objects.order_by("name").values("name")]
        tuples = [
            value async for value in Book.objects.order_by("name").values_list("name")
        ]
        flat_values = [
            value
            async for value in Book.objects.order_by("name").values_list(
                "name", flat=True
            )
        ]
        named_values = [
            value
            async for value in Book.objects.order_by("name").values_list(
                "name", named=True
            )
        ]

    names = ["Async", "Django", "PostgreSQL"]
    assert values == [{"name": name} for name in names]
    assert tuples == [(name,) for name in names]
    assert flat_values == names
    assert [value.name for value in named_values] == names
    assert execute.await_count == 4


@pytest.mark.asyncio
async def test_django_aiterator_fetches_all_rows_in_chunks(django_books):
    books = [
        book async for book in Book.objects.order_by("name").aiterator(chunk_size=2)
    ]

    assert [book.name for book in books] == ["Async", "Django", "PostgreSQL"]


@pytest.mark.asyncio
async def test_django_aiterator_can_be_closed_early(django_books):
    iterator = (
        Book.objects.order_by("name")
        .values_list("name", flat=True)
        .aiterator(chunk_size=1)
    )

    assert await anext(iterator) == "Async"
    await iterator.aclose()

    names = [
        name
        async for name in Book.objects.order_by("name").values_list("name", flat=True)
    ]
    assert names == ["Async", "Django", "PostgreSQL"]
