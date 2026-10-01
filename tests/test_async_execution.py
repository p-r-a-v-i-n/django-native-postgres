from unittest import mock

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.db import connections
from django_native_postgres.base import DatabaseWrapper
from django_native_postgres.executor import NativeExecutor
from psycopg.conninfo import conninfo_to_dict


def test_database_wrapper_builds_and_caches_native_executor():
    setting_dict = {
        "ENGINE": "django_native_postgres",
        "NAME": "example_database",
        "USER": "example_user",
        "PASSWORD": "example_password",
        "HOST": "database.example.com",
        "PORT": "5432",
        "TIME_ZONE": None,
        "OPTIONS": {
            "native_pool": {
                "max_size": 4,
                "wait_timeout_ms": 250,
            },
        },
    }
    connection = DatabaseWrapper(setting_dict, alias="default")
    pool = object()

    with mock.patch(
        "django_native_postgres.executor._native.create_pool",
        return_value=pool,
        create=True,
    ) as create_pool:
        executor = connection.get_async_executor()
        cached_executor = connection.get_async_executor()

    assert isinstance(executor, NativeExecutor)
    assert cached_executor is executor
    assert executor.pool is pool
    assert "native_pool" not in connection.get_connection_params()
    create_pool.assert_called_once()
    assert create_pool.call_args.kwargs["max_size"] == 4
    assert create_pool.call_args.kwargs["wait_timeout_ms"] == 250
    assert conninfo_to_dict(create_pool.call_args.kwargs["database_url"]) == {
        "dbname": "example_database",
        "user": "example_user",
        "password": "example_password",
        "host": "database.example.com",
        "port": "5432",
    }


def test_database_wrapper_rejects_zero_native_pool_max_size():
    setting_dict = {
        **connections["default"].settings_dict,
        "OPTIONS": {
            "native_pool": {
                "max_size": 0,
            },
        },
    }
    connection = DatabaseWrapper(setting_dict, alias="default")

    with pytest.raises(
        ImproperlyConfigured,
        match=r"native_pool\.max_size must be greater than zero",
    ):
        connection.get_async_executor()


@pytest.mark.asyncio
async def test_aexecute_forwards_query_to_process_executor():
    connection = connections["default"]
    executor = mock.Mock()
    executor.execute = mock.AsyncMock(return_value=object())
    sql = "SELECT id FROM example WHERE active = %s"
    params = (True,)

    with mock.patch.object(
        connection,
        "get_async_executor",
        return_value=executor,
        create=True,
    ):
        result = await connection.aexecute(sql, params)

    assert result is executor.execute.return_value
    executor.execute.assert_awaited_once_with(sql=sql, params=params)
    assert connection.connection is None


@pytest.mark.asyncio
async def test_native_executor_forwards_query_to_native_extension():
    pool = object()
    rows = [[42]]

    with (
        mock.patch(
            "django_native_postgres.executor._native.create_pool",
            return_value=pool,
            create=True,
        ) as create_pool,
        mock.patch(
            "django_native_postgres.executor._native.execute",
            new=mock.AsyncMock(return_value=rows),
        ) as execute,
    ):
        executor = NativeExecutor(
            database_url="postgresql://example",
            pool_max_size=4,
            pool_wait_timeout_ms=250,
        )
        result = await executor.execute(
            sql="SELECT %s::BIGINT",
            params=(42,),
        )

    assert result is rows
    create_pool.assert_called_once_with(
        database_url="postgresql://example",
        max_size=4,
        wait_timeout_ms=250,
    )
    execute.assert_awaited_once_with(
        pool=pool,
        sql="SELECT %s::BIGINT",
        params=(42,),
    )


@pytest.mark.asyncio
async def test_native_executor_closes_pool_handle():
    pool = object()

    with (
        mock.patch(
            "django_native_postgres.executor._native.create_pool",
            return_value=pool,
        ),
        mock.patch(
            "django_native_postgres.executor._native.close_pool",
            new=mock.AsyncMock(),
            create=True,
        ) as close_pool,
    ):
        executor = NativeExecutor(database_url="postgresql://example")
        await executor.close()

    close_pool.assert_awaited_once_with(pool)
