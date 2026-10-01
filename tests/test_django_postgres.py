import os

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


def test_django_can_create_and_query_model_with_sync_backend():
    Book.objects.create(name="Django")

    assert Book.objects.filter(name="Django").exists()


@pytest.mark.asyncio
async def test_django_compiled_query_executes_through_rust(
    compiled_book_query,
):
    database_url, sql, params = compiled_book_query

    rows = await _native.execute(
        database_url=database_url,
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
