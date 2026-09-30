import os

import pytest
from django_native_postgres import _native


def test_native_exposes_execute():
    assert callable(_native.execute)


@pytest.fixture
def postgres_database_url() -> str:
    database_url = os.environ.get("DJANGO_NATIVE_POSTGRES_TEST_DATABASE_URL")
    if database_url is None:
        pytest.skip("DJANGO_NATIVE_POSTGRES_TEST_DATABASE_URL is not configured")
    return database_url


@pytest.mark.asyncio
async def test_execute_returns_text_rows_from_postgres(postgres_database_url):
    rows = await _native.execute(
        database_url=postgres_database_url,
        sql="SELECT 'django-native-postgres'::TEXT",
    )

    assert rows == [["django-native-postgres"]]


@pytest.mark.asyncio
async def test_execute_decodes_multiple_nullable_text_columns(postgres_database_url):
    rows = await _native.execute(
        database_url=postgres_database_url,
        sql="SELECT 'value'::TEXT, NULL::TEXT",
    )

    assert rows == [["value", None]]


@pytest.mark.asyncio
async def test_execute_propagates_postgres_query_errors(postgres_database_url):
    with pytest.raises(RuntimeError, match="PostgreSQL query failed"):
        await _native.execute(
            database_url=postgres_database_url,
            sql="SELECT FROM",
        )
