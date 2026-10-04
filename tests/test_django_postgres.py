import asyncio
import os
from unittest import mock

import pytest
from django.db import connections
from django.db.models import Count
from django.db.models.deletion import ProtectedError, RestrictedError
from django.db.models.signals import post_delete, pre_delete
from django_native_postgres import _native
from psycopg.conninfo import make_conninfo

from tests.integration_app.models import (
    Book,
    CallableSetChild,
    CascadeChild,
    CascadeGrandchild,
    DeletionParent,
    NullableChild,
    ProtectedChild,
    ProtectedGrandchild,
    RestrictedChild,
)

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


@pytest.fixture
def deletion_graph(transactional_db):
    parent = DeletionParent.objects.create(name="parent")
    cascade_child = CascadeChild.objects.create(parent=parent)
    CascadeGrandchild.objects.create(parent=cascade_child)
    nullable_child = NullableChild.objects.create(parent=parent)
    callable_set_child = CallableSetChild.objects.create(parent=parent)
    return parent, cascade_child, nullable_child, callable_set_child


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
        "execute_result",
        wraps=executor.execute_result,
    ) as execute_result:
        result = await Book.objects.filter(name=name).aexists()

    assert result is expected
    execute_result.assert_awaited_once()


@pytest.mark.asyncio
async def test_django_async_iteration_executes_through_native_backend(django_books):
    executor = connections["default"].get_async_executor()

    with mock.patch.object(
        executor,
        "execute_result",
        wraps=executor.execute_result,
    ) as execute_result:
        books = [book async for book in Book.objects.order_by("name")]

    assert [book.name for book in books] == ["Async", "Django", "PostgreSQL"]
    execute_result.assert_awaited_once()


@pytest.mark.asyncio
async def test_django_async_iteration_supports_values_iterables(django_books):
    executor = connections["default"].get_async_executor()

    with mock.patch.object(
        executor,
        "execute_result",
        wraps=executor.execute_result,
    ) as execute_result:
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
    assert execute_result.await_count == 4


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


@pytest.mark.asyncio
async def test_django_async_row_helpers_execute_through_native_backend(django_books):
    executor = connections["default"].get_async_executor()
    expected_names = ["Async", "Django", "PostgreSQL"]

    with mock.patch.object(
        executor,
        "execute_result",
        wraps=executor.execute_result,
    ) as execute_result:
        django = await Book.objects.aget(name="Django")
        first = await Book.objects.order_by("name").afirst()
        last = await Book.objects.order_by("name").alast()
        earliest = await Book.objects.aearliest("name")
        latest = await Book.objects.alatest("name")
        contains = await Book.objects.acontains(django)
        books_by_id = await Book.objects.ain_bulk([book.pk for book in django_books])

    assert django.name == "Django"
    assert first.name == "Async"
    assert last.name == "PostgreSQL"
    assert earliest.name == "Async"
    assert latest.name == "PostgreSQL"
    assert contains is True
    assert sorted(book.name for book in books_by_id.values()) == expected_names
    assert execute_result.await_count == 7


@pytest.mark.asyncio
async def test_django_async_scalar_queries_execute_through_native_backend(django_books):
    executor = connections["default"].get_async_executor()

    with mock.patch.object(
        executor,
        "execute_result",
        wraps=executor.execute_result,
    ) as execute_result:
        count = await Book.objects.acount()
        aggregate = await Book.objects.aaggregate(total=Count("id"))
        explanation = await Book.objects.order_by("name").aexplain()

    assert count == 3
    assert aggregate == {"total": 3}
    assert "Sort" in explanation
    assert execute_result.await_count == 3


@pytest.mark.asyncio
async def test_django_aupdate_executes_through_native_backend(django_books):
    executor = connections["default"].get_async_executor()

    with mock.patch.object(
        executor,
        "execute_result",
        wraps=executor.execute_result,
    ) as execute_result:
        rows_updated = await Book.objects.filter(name="Django").aupdate(name="Updated")

    names = [
        name
        async for name in Book.objects.order_by("name").values_list("name", flat=True)
    ]
    assert rows_updated == 1
    assert names == ["Async", "PostgreSQL", "Updated"]
    execute_result.assert_awaited_once()


@pytest.mark.asyncio
async def test_django_acreate_and_asave_execute_through_native_backend(
    transactional_db,
):
    executor = connections["default"].get_async_executor()

    with mock.patch.object(
        executor,
        "execute_result",
        wraps=executor.execute_result,
    ) as execute_result:
        book = await Book.objects.acreate(name="Created")
        book.name = "Saved"
        await book.asave(update_fields=["name"])

    saved = await Book.objects.aget(pk=book.pk)
    assert book.pk is not None
    assert saved.name == "Saved"
    assert execute_result.await_count == 2


@pytest.mark.asyncio
async def test_django_async_bulk_writes_execute_through_native_backend(
    transactional_db,
):
    executor = connections["default"].get_async_executor()
    books = [Book(name="First"), Book(name="Second"), Book(name="Third")]

    with mock.patch.object(
        executor,
        "execute_result",
        wraps=executor.execute_result,
    ) as execute_result:
        created = await Book.objects.abulk_create(books, batch_size=2)

    assert created == books
    assert all(book.pk is not None for book in books)
    assert execute_result.await_count == 2

    for book in books:
        book.name = f"Updated {book.name}"

    with mock.patch.object(
        executor,
        "execute_result",
        wraps=executor.execute_result,
    ) as execute_result:
        rows_updated = await Book.objects.abulk_update(
            books,
            ["name"],
            batch_size=2,
        )

    names = [
        name
        async for name in Book.objects.order_by("name").values_list("name", flat=True)
    ]
    assert rows_updated == 3
    assert names == ["Updated First", "Updated Second", "Updated Third"]
    assert execute_result.await_count == 2


@pytest.mark.asyncio
async def test_django_adelete_preserves_cascades_field_updates_and_signals(
    deletion_graph,
):
    parent, cascade_child, nullable_child, callable_set_child = deletion_graph
    events = []

    async def record_pre_delete(sender, instance, **kwargs):
        events.append(("pre", sender, instance.pk))

    async def record_post_delete(sender, instance, **kwargs):
        events.append(("post", sender, instance.pk))

    pre_delete.connect(record_pre_delete, sender=CascadeChild)
    post_delete.connect(record_post_delete, sender=CascadeChild)
    try:
        deleted, deleted_by_model = await DeletionParent.objects.filter(
            pk=parent.pk
        ).adelete()
    finally:
        pre_delete.disconnect(record_pre_delete, sender=CascadeChild)
        post_delete.disconnect(record_post_delete, sender=CascadeChild)

    assert deleted == 3
    assert deleted_by_model == {
        "integration_app.CascadeGrandchild": 1,
        "integration_app.CascadeChild": 1,
        "integration_app.DeletionParent": 1,
    }
    assert events == [
        ("pre", CascadeChild, cascade_child.pk),
        ("post", CascadeChild, cascade_child.pk),
    ]
    assert not await CascadeChild.objects.filter(pk=cascade_child.pk).aexists()
    assert (await NullableChild.objects.aget(pk=nullable_child.pk)).parent_id is None
    assert (
        await CallableSetChild.objects.aget(pk=callable_set_child.pk)
    ).parent_id is None


@pytest.mark.asyncio
async def test_django_model_adelete_clears_primary_key(transactional_db):
    parent = await DeletionParent.objects.acreate(name="parent")

    result = await parent.adelete()

    assert result == (1, {"integration_app.DeletionParent": 1})
    assert parent.pk is None


@pytest.mark.asyncio
async def test_django_adelete_enforces_protect_and_restrict(transactional_db):
    protected_parent = await DeletionParent.objects.acreate(name="protected")
    protected_child = await ProtectedChild.objects.acreate(parent=protected_parent)
    restricted_parent = await DeletionParent.objects.acreate(name="restricted")
    restricted_child = await RestrictedChild.objects.acreate(parent=restricted_parent)
    nested_parent = await DeletionParent.objects.acreate(name="nested protected")
    nested_child = await CascadeChild.objects.acreate(parent=nested_parent)
    protected_grandchild = await ProtectedGrandchild.objects.acreate(
        parent=nested_child
    )

    with pytest.raises(ProtectedError) as protected_error:
        await protected_parent.adelete()
    with pytest.raises(RestrictedError) as restricted_error:
        await restricted_parent.adelete()
    with pytest.raises(ProtectedError) as nested_protected_error:
        await nested_parent.adelete()

    assert protected_error.value.protected_objects == {protected_child}
    assert restricted_error.value.restricted_objects == {restricted_child}
    assert nested_protected_error.value.protected_objects == {protected_grandchild}
    assert await DeletionParent.objects.filter(pk=protected_parent.pk).aexists()
    assert await DeletionParent.objects.filter(pk=restricted_parent.pk).aexists()
    assert await DeletionParent.objects.filter(pk=nested_parent.pk).aexists()


@pytest.mark.asyncio
async def test_django_adelete_rolls_back_when_signal_fails(deletion_graph):
    parent, cascade_child, nullable_child, callable_set_child = deletion_graph

    async def fail_after_delete(**kwargs):
        raise RuntimeError("stop deletion")

    post_delete.connect(fail_after_delete, sender=DeletionParent)
    try:
        with pytest.raises(RuntimeError, match="stop deletion"):
            await parent.adelete()
    finally:
        post_delete.disconnect(fail_after_delete, sender=DeletionParent)

    assert parent.pk is not None
    assert await DeletionParent.objects.filter(pk=parent.pk).aexists()
    assert await CascadeChild.objects.filter(pk=cascade_child.pk).aexists()
    nullable_child = await NullableChild.objects.aget(pk=nullable_child.pk)
    assert nullable_child.parent_id == parent.pk
    assert (
        await CallableSetChild.objects.aget(pk=callable_set_child.pk)
    ).parent_id == parent.pk


@pytest.mark.asyncio
async def test_django_adelete_empty_queryset(transactional_db):
    result = await DeletionParent.objects.filter(name="missing").adelete()

    assert result == (0, {})
