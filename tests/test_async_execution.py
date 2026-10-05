import asyncio
from unittest import mock

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.db import connections
from django_native_postgres.base import DatabaseWrapper
from django_native_postgres.cursor import NativeAsyncCursor
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


def test_database_wrapper_opts_in_to_native_async_execution():
    connection = connections["default"]

    assert connection.features.supports_async is True
    assert isinstance(connection.acursor(), NativeAsyncCursor)


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


@pytest.mark.parametrize("wait_timeout_ms", [0, -1, True, "250"])
def test_database_wrapper_rejects_invalid_pool_wait_timeout(wait_timeout_ms):
    setting_dict = {
        **connections["default"].settings_dict,
        "OPTIONS": {
            "native_pool": {
                "wait_timeout_ms": wait_timeout_ms,
            },
        },
    }
    connection = DatabaseWrapper(setting_dict, alias="default")

    with pytest.raises(
        ImproperlyConfigured,
        match=r"native_pool\.wait_timeout_ms must be a positive integer",
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
async def test_aexecute_result_forwards_query_to_process_executor():
    connection = connections["default"]
    executor = mock.Mock()
    executor.execute_result = mock.AsyncMock(return_value=([], 2))
    sql = "UPDATE example SET active = %s"
    params = (True,)

    with mock.patch.object(
        connection,
        "get_async_executor",
        return_value=executor,
        create=True,
    ):
        result = await connection.aexecute_result(sql, params)

    assert result == ([], 2)
    executor.execute_result.assert_awaited_once_with(sql=sql, params=params)
    assert connection.connection is None


@pytest.mark.asyncio
async def test_aexecute_with_metadata_forwards_query_to_process_executor():
    connection = connections["default"]
    executor = mock.Mock()
    executor.execute_with_metadata = mock.AsyncMock(return_value=([[1]], 1, ["value"]))

    with mock.patch.object(
        connection,
        "get_async_executor",
        return_value=executor,
        create=True,
    ):
        result = await connection.aexecute_with_metadata("SELECT %s", (1,))

    assert result == ([[1]], 1, ["value"])
    executor.execute_with_metadata.assert_awaited_once_with(
        sql="SELECT %s", params=(1,)
    )
    assert connection.connection is None


@pytest.mark.asyncio
async def test_async_cursor_executes_and_fetches_rows():
    connection = mock.Mock()
    connection.aexecute_with_metadata = mock.AsyncMock(
        return_value=([[1], [2]], 2, ["value"])
    )
    cursor = NativeAsyncCursor(connection)

    async with cursor as opened_cursor:
        result = await opened_cursor.aexecute("SELECT %s", (1,))

        assert result is None
        assert opened_cursor.rowcount == 2
        assert opened_cursor.description == [
            ("value", None, None, None, None, None, None)
        ]
        assert await opened_cursor.afetchone() == [1]
        assert await opened_cursor.afetchone() == [2]
        assert await opened_cursor.afetchone() is None

    connection.aexecute_with_metadata.assert_awaited_once_with("SELECT %s", (1,))


@pytest.mark.asyncio
async def test_async_cursor_fetches_remaining_rows():
    connection = mock.Mock()
    connection.aexecute_with_metadata = mock.AsyncMock(
        return_value=([[1], [2], [3]], 3, ["value"])
    )

    async with NativeAsyncCursor(connection) as cursor:
        await cursor.aexecute("SELECT value FROM example")

        assert await cursor.afetchone() == [1]
        assert await cursor.afetchall() == [[2], [3]]
        assert await cursor.afetchall() == []


@pytest.mark.asyncio
async def test_async_cursor_fetches_rows_in_chunks():
    connection = mock.Mock()
    connection.aexecute_with_metadata = mock.AsyncMock(
        return_value=([[1], [2], [3]], 3, ["value"])
    )

    async with NativeAsyncCursor(connection) as cursor:
        await cursor.aexecute("SELECT value FROM example")

        assert await cursor.afetchmany(2) == [[1], [2]]
        assert await cursor.afetchmany(2) == [[3]]
        assert await cursor.afetchmany(2) == []


@pytest.mark.asyncio
async def test_async_cursor_cannot_fetch_without_an_active_result():
    cursor = NativeAsyncCursor(mock.Mock())
    message = "No active query result on this cursor"

    with pytest.raises(RuntimeError, match=message):
        await cursor.afetchone()

    with pytest.raises(RuntimeError, match=message):
        await cursor.afetchmany(1)

    with pytest.raises(RuntimeError, match=message):
        await cursor.afetchall()

    async with cursor:
        pass

    with pytest.raises(RuntimeError, match=message):
        await cursor.afetchone()


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


@pytest.mark.asyncio
async def test_native_transaction_commits_after_successful_context():
    pool = object()
    handle = object()
    rows = [[42]]

    with (
        mock.patch(
            "django_native_postgres.executor._native.create_pool",
            return_value=pool,
        ),
        mock.patch(
            "django_native_postgres.executor._native.begin_transaction",
            new=mock.AsyncMock(return_value=handle),
            create=True,
        ) as begin_transaction,
        mock.patch(
            "django_native_postgres.executor._native.execute_transaction",
            new=mock.AsyncMock(return_value=rows),
            create=True,
        ) as execute_transaction,
        mock.patch(
            "django_native_postgres.executor._native.commit_transaction",
            new=mock.AsyncMock(),
            create=True,
        ) as commit_transaction,
        mock.patch(
            "django_native_postgres.executor._native.rollback_transaction",
            new=mock.AsyncMock(),
            create=True,
        ) as rollback_transaction,
    ):
        executor = NativeExecutor(database_url="postgresql://example")

        async with executor.transaction() as transaction:
            result = await transaction.execute(
                sql="SELECT %s::BIGINT",
                params=(42,),
            )

    assert result is rows
    begin_transaction.assert_awaited_once_with(
        pool,
        isolation_level=None,
        read_only=None,
        deferrable=None,
    )
    execute_transaction.assert_awaited_once_with(
        transaction=handle,
        sql="SELECT %s::BIGINT",
        params=(42,),
    )
    commit_transaction.assert_awaited_once_with(handle)
    rollback_transaction.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_transaction_forwards_transaction_options():
    pool = object()
    handle = object()

    with (
        mock.patch(
            "django_native_postgres.executor._native.create_pool",
            return_value=pool,
        ),
        mock.patch(
            "django_native_postgres.executor._native.begin_transaction",
            new=mock.AsyncMock(return_value=handle),
        ) as begin_transaction,
        mock.patch(
            "django_native_postgres.executor._native.commit_transaction",
            new=mock.AsyncMock(),
        ) as commit_transaction,
    ):
        executor = NativeExecutor(database_url="postgresql://example")

        async with executor.transaction(
            isolation_level="serializable",
            read_only=True,
            deferrable=True,
        ):
            pass

    begin_transaction.assert_awaited_once_with(
        pool,
        isolation_level="serializable",
        read_only=True,
        deferrable=True,
    )
    commit_transaction.assert_awaited_once_with(handle)


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"isolation_level": 1}, "isolation_level must be a string or None"),
        ({"read_only": "yes"}, "read_only must be a bool or None"),
        ({"deferrable": 1}, "deferrable must be a bool or None"),
    ],
)
def test_native_transaction_rejects_invalid_option_types(options, message):
    with mock.patch(
        "django_native_postgres.executor._native.create_pool",
        return_value=object(),
    ):
        executor = NativeExecutor(database_url="postgresql://example")

    with pytest.raises(TypeError, match=message):
        executor.transaction(**options)


@pytest.mark.asyncio
async def test_native_transaction_rolls_back_after_context_error():
    pool = object()
    handle = object()
    error = ValueError("query processing failed")

    with (
        mock.patch(
            "django_native_postgres.executor._native.create_pool",
            return_value=pool,
        ),
        mock.patch(
            "django_native_postgres.executor._native.begin_transaction",
            new=mock.AsyncMock(return_value=handle),
            create=True,
        ),
        mock.patch(
            "django_native_postgres.executor._native.commit_transaction",
            new=mock.AsyncMock(),
            create=True,
        ) as commit_transaction,
        mock.patch(
            "django_native_postgres.executor._native.rollback_transaction",
            new=mock.AsyncMock(),
            create=True,
        ) as rollback_transaction,
    ):
        executor = NativeExecutor(database_url="postgresql://example")

        with pytest.raises(ValueError) as raised:
            async with executor.transaction():
                raise error

    assert raised.value is error
    rollback_transaction.assert_awaited_once_with(handle)
    commit_transaction.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_transaction_rejects_execution_outside_context():
    pool = object()
    handle = object()

    with (
        mock.patch(
            "django_native_postgres.executor._native.create_pool",
            return_value=pool,
        ),
        mock.patch(
            "django_native_postgres.executor._native.begin_transaction",
            new=mock.AsyncMock(return_value=handle),
            create=True,
        ),
        mock.patch(
            "django_native_postgres.executor._native.commit_transaction",
            new=mock.AsyncMock(),
            create=True,
        ),
    ):
        executor = NativeExecutor(database_url="postgresql://example")
        transaction = executor.transaction()

        with pytest.raises(RuntimeError, match="Native transaction is not active"):
            await transaction.execute(sql="SELECT 1")

        async with transaction:
            pass

        with pytest.raises(RuntimeError, match="Native transaction is not active"):
            await transaction.execute(sql="SELECT 1")


@pytest.mark.asyncio
async def test_executor_routes_queries_through_active_transaction():
    pool = object()
    handle = object()
    transaction_rows = [["transaction"]]
    pool_rows = [["pool"]]

    with (
        mock.patch(
            "django_native_postgres.executor._native.create_pool",
            return_value=pool,
        ),
        mock.patch(
            "django_native_postgres.executor._native.begin_transaction",
            new=mock.AsyncMock(return_value=handle),
        ),
        mock.patch(
            "django_native_postgres.executor._native.execute_transaction",
            new=mock.AsyncMock(return_value=transaction_rows),
        ) as execute_transaction,
        mock.patch(
            "django_native_postgres.executor._native.execute",
            new=mock.AsyncMock(return_value=pool_rows),
        ) as execute,
        mock.patch(
            "django_native_postgres.executor._native.commit_transaction",
            new=mock.AsyncMock(),
        ),
    ):
        executor = NativeExecutor(database_url="postgresql://example")

        async with executor.transaction():
            active_rows = await executor.execute(sql="SELECT 'transaction'")

        inactive_rows = await executor.execute(sql="SELECT 'pool'")

    assert active_rows is transaction_rows
    assert inactive_rows is pool_rows
    execute_transaction.assert_awaited_once_with(
        transaction=handle,
        sql="SELECT 'transaction'",
        params=None,
    )
    execute.assert_awaited_once_with(
        pool=pool,
        sql="SELECT 'pool'",
        params=None,
    )


@pytest.mark.asyncio
async def test_executor_routes_metadata_queries_through_active_transaction():
    pool = object()
    handle = object()
    transaction_result = ([["transaction"]], 1, ["source"])
    pool_result = ([["pool"]], 1, ["source"])

    with (
        mock.patch(
            "django_native_postgres.executor._native.create_pool",
            return_value=pool,
        ),
        mock.patch(
            "django_native_postgres.executor._native.begin_transaction",
            new=mock.AsyncMock(return_value=handle),
        ),
        mock.patch(
            "django_native_postgres.executor._native.execute_transaction_with_metadata",
            new=mock.AsyncMock(return_value=transaction_result),
        ) as execute_transaction_with_metadata,
        mock.patch(
            "django_native_postgres.executor._native.execute_with_metadata",
            new=mock.AsyncMock(return_value=pool_result),
        ) as execute_with_metadata,
        mock.patch(
            "django_native_postgres.executor._native.commit_transaction",
            new=mock.AsyncMock(),
        ),
    ):
        executor = NativeExecutor(database_url="postgresql://example")

        async with executor.transaction():
            active_result = await executor.execute_with_metadata(
                sql="SELECT 'transaction' AS source"
            )

        inactive_result = await executor.execute_with_metadata(
            sql="SELECT 'pool' AS source"
        )

    assert active_result is transaction_result
    assert inactive_result is pool_result
    execute_transaction_with_metadata.assert_awaited_once_with(
        transaction=handle,
        sql="SELECT 'transaction' AS source",
        params=None,
    )
    execute_with_metadata.assert_awaited_once_with(
        pool=pool,
        sql="SELECT 'pool' AS source",
        params=None,
    )


@pytest.mark.asyncio
async def test_executor_keeps_transaction_isolated_between_tasks():
    pool = object()
    handle = object()
    transaction_started = asyncio.Event()
    pool_query_finished = asyncio.Event()

    with (
        mock.patch(
            "django_native_postgres.executor._native.create_pool",
            return_value=pool,
        ),
        mock.patch(
            "django_native_postgres.executor._native.begin_transaction",
            new=mock.AsyncMock(return_value=handle),
        ),
        mock.patch(
            "django_native_postgres.executor._native.execute_transaction",
            new=mock.AsyncMock(return_value=[["transaction"]]),
        ) as execute_transaction,
        mock.patch(
            "django_native_postgres.executor._native.execute",
            new=mock.AsyncMock(return_value=[["pool"]]),
        ) as execute,
        mock.patch(
            "django_native_postgres.executor._native.commit_transaction",
            new=mock.AsyncMock(),
        ),
    ):
        executor = NativeExecutor(database_url="postgresql://example")

        async def execute_inside_transaction():
            async with executor.transaction():
                transaction_started.set()
                await pool_query_finished.wait()
                return await executor.execute(sql="SELECT 'transaction'")

        async def execute_outside_transaction():
            await transaction_started.wait()
            rows = await executor.execute(sql="SELECT 'pool'")
            pool_query_finished.set()
            return rows

        transaction_rows, pool_rows = await asyncio.gather(
            execute_inside_transaction(),
            execute_outside_transaction(),
        )

    assert transaction_rows == [["transaction"]]
    assert pool_rows == [["pool"]]
    execute_transaction.assert_awaited_once_with(
        transaction=handle,
        sql="SELECT 'transaction'",
        params=None,
    )
    execute.assert_awaited_once_with(
        pool=pool,
        sql="SELECT 'pool'",
        params=None,
    )


@pytest.mark.asyncio
async def test_executor_uses_savepoint_for_nested_transaction():
    pool = object()
    handle = object()

    with (
        mock.patch(
            "django_native_postgres.executor._native.create_pool",
            return_value=pool,
        ),
        mock.patch(
            "django_native_postgres.executor._native.begin_transaction",
            new=mock.AsyncMock(return_value=handle),
        ) as begin_transaction,
        mock.patch(
            "django_native_postgres.executor._native.execute_transaction",
            new=mock.AsyncMock(return_value=[]),
        ) as execute_transaction,
        mock.patch(
            "django_native_postgres.executor._native.commit_transaction",
            new=mock.AsyncMock(),
        ) as commit_transaction,
        mock.patch(
            "django_native_postgres.executor._native.rollback_transaction",
            new=mock.AsyncMock(),
        ) as rollback_transaction,
    ):
        executor = NativeExecutor(database_url="postgresql://example")

        async with executor.transaction():
            async with executor.transaction():
                await executor.execute(sql="SELECT 1")

    begin_transaction.assert_awaited_once_with(
        pool,
        isolation_level=None,
        read_only=None,
        deferrable=None,
    )
    assert execute_transaction.await_args_list == [
        mock.call(
            transaction=handle,
            sql="SAVEPOINT django_native_postgres_savepoint_1",
            params=None,
        ),
        mock.call(
            transaction=handle,
            sql="SELECT 1",
            params=None,
        ),
        mock.call(
            transaction=handle,
            sql="RELEASE SAVEPOINT django_native_postgres_savepoint_1",
            params=None,
        ),
    ]
    commit_transaction.assert_awaited_once_with(handle)
    rollback_transaction.assert_not_awaited()


@pytest.mark.asyncio
async def test_executor_rejects_transaction_use_from_child_tasks():
    pool = object()
    handle = object()

    with (
        mock.patch(
            "django_native_postgres.executor._native.create_pool",
            return_value=pool,
        ),
        mock.patch(
            "django_native_postgres.executor._native.begin_transaction",
            new=mock.AsyncMock(return_value=handle),
        ),
        mock.patch(
            "django_native_postgres.executor._native.execute_transaction",
            new=mock.AsyncMock(return_value=[]),
        ) as execute_transaction,
        mock.patch(
            "django_native_postgres.executor._native.commit_transaction",
            new=mock.AsyncMock(),
        ),
    ):
        executor = NativeExecutor(database_url="postgresql://example")

        async def execute_query():
            return await executor.execute(sql="SELECT 1")

        async def open_savepoint():
            async with executor.transaction():
                pass

        async with executor.transaction():
            query_task = asyncio.create_task(execute_query())
            savepoint_task = asyncio.create_task(open_savepoint())
            results = await asyncio.gather(
                query_task,
                savepoint_task,
                return_exceptions=True,
            )

    for result in results:
        assert isinstance(result, RuntimeError)
        assert str(result) == (
            "Native transaction cannot be used from a different asyncio task"
        )

    execute_transaction.assert_not_awaited()


@pytest.mark.asyncio
async def test_executor_marks_transaction_for_rollback_after_savepoint_creation_error():
    pool = object()
    handle = object()

    with (
        mock.patch(
            "django_native_postgres.executor._native.create_pool",
            return_value=pool,
        ),
        mock.patch(
            "django_native_postgres.executor._native.begin_transaction",
            new=mock.AsyncMock(return_value=handle),
        ),
        mock.patch(
            "django_native_postgres.executor._native.execute_transaction",
            new=mock.AsyncMock(side_effect=RuntimeError("savepoint failed")),
        ) as execute_transaction,
        mock.patch(
            "django_native_postgres.executor._native.commit_transaction",
            new=mock.AsyncMock(),
        ) as commit_transaction,
        mock.patch(
            "django_native_postgres.executor._native.rollback_transaction",
            new=mock.AsyncMock(),
        ) as rollback_transaction,
    ):
        executor = NativeExecutor(database_url="postgresql://example")

        async with executor.transaction():
            with pytest.raises(RuntimeError, match="savepoint failed"):
                async with executor.transaction():
                    pass

            with pytest.raises(
                RuntimeError,
                match="Native transaction is marked for rollback",
            ):
                await executor.execute(sql="SELECT 'outer'")

    assert execute_transaction.await_args_list == [
        mock.call(
            transaction=handle,
            sql="SAVEPOINT django_native_postgres_savepoint_1",
            params=None,
        )
    ]
    commit_transaction.assert_not_awaited()
    rollback_transaction.assert_awaited_once_with(handle)


@pytest.mark.asyncio
async def test_executor_rolls_back_outer_transaction_after_savepoint_release_error():
    pool = object()
    handle = object()
    pool_rows = [["pool"]]

    with (
        mock.patch(
            "django_native_postgres.executor._native.create_pool",
            return_value=pool,
        ),
        mock.patch(
            "django_native_postgres.executor._native.begin_transaction",
            new=mock.AsyncMock(return_value=handle),
        ),
        mock.patch(
            "django_native_postgres.executor._native.execute_transaction",
            new=mock.AsyncMock(side_effect=[[], RuntimeError("release failed")]),
        ),
        mock.patch(
            "django_native_postgres.executor._native.execute",
            new=mock.AsyncMock(return_value=pool_rows),
        ) as execute,
        mock.patch(
            "django_native_postgres.executor._native.commit_transaction",
            new=mock.AsyncMock(),
        ) as commit_transaction,
        mock.patch(
            "django_native_postgres.executor._native.rollback_transaction",
            new=mock.AsyncMock(),
        ) as rollback_transaction,
    ):
        executor = NativeExecutor(database_url="postgresql://example")

        async with executor.transaction():
            with pytest.raises(RuntimeError, match="release failed"):
                async with executor.transaction():
                    pass

            with pytest.raises(
                RuntimeError,
                match="Native transaction is marked for rollback",
            ):
                await executor.execute(sql="SELECT 'transaction'")

        rows = await executor.execute(sql="SELECT 'pool'")

    assert rows is pool_rows
    commit_transaction.assert_not_awaited()
    rollback_transaction.assert_awaited_once_with(handle)
    execute.assert_awaited_once_with(
        pool=pool,
        sql="SELECT 'pool'",
        params=None,
    )


@pytest.mark.asyncio
async def test_executor_rolls_back_outer_transaction_after_savepoint_rollback_error():
    pool = object()
    handle = object()

    with (
        mock.patch(
            "django_native_postgres.executor._native.create_pool",
            return_value=pool,
        ),
        mock.patch(
            "django_native_postgres.executor._native.begin_transaction",
            new=mock.AsyncMock(return_value=handle),
        ),
        mock.patch(
            "django_native_postgres.executor._native.execute_transaction",
            new=mock.AsyncMock(
                side_effect=[[], RuntimeError("savepoint rollback failed")]
            ),
        ) as execute_transaction,
        mock.patch(
            "django_native_postgres.executor._native.commit_transaction",
            new=mock.AsyncMock(),
        ) as commit_transaction,
        mock.patch(
            "django_native_postgres.executor._native.rollback_transaction",
            new=mock.AsyncMock(),
        ) as rollback_transaction,
    ):
        executor = NativeExecutor(database_url="postgresql://example")

        async with executor.transaction():
            with pytest.raises(RuntimeError, match="savepoint rollback failed"):
                async with executor.transaction():
                    raise ValueError("application failed")

    assert execute_transaction.await_args_list == [
        mock.call(
            transaction=handle,
            sql="SAVEPOINT django_native_postgres_savepoint_1",
            params=None,
        ),
        mock.call(
            transaction=handle,
            sql="ROLLBACK TO SAVEPOINT django_native_postgres_savepoint_1",
            params=None,
        ),
    ]
    commit_transaction.assert_not_awaited()
    rollback_transaction.assert_awaited_once_with(handle)


@pytest.mark.asyncio
async def test_executor_rolls_back_savepoint_and_transaction_after_cancellation():
    pool = object()
    handle = object()
    transaction_started = asyncio.Event()
    wait_forever = asyncio.Event()

    with (
        mock.patch(
            "django_native_postgres.executor._native.create_pool",
            return_value=pool,
        ),
        mock.patch(
            "django_native_postgres.executor._native.begin_transaction",
            new=mock.AsyncMock(return_value=handle),
        ),
        mock.patch(
            "django_native_postgres.executor._native.execute_transaction",
            new=mock.AsyncMock(return_value=[]),
        ) as execute_transaction,
        mock.patch(
            "django_native_postgres.executor._native.commit_transaction",
            new=mock.AsyncMock(),
        ) as commit_transaction,
        mock.patch(
            "django_native_postgres.executor._native.rollback_transaction",
            new=mock.AsyncMock(),
        ) as rollback_transaction,
    ):
        executor = NativeExecutor(database_url="postgresql://example")

        async def execute_transaction_until_cancelled():
            async with executor.transaction():
                async with executor.transaction():
                    transaction_started.set()
                    await wait_forever.wait()

        task = asyncio.create_task(execute_transaction_until_cancelled())
        await transaction_started.wait()
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

    assert execute_transaction.await_args_list == [
        mock.call(
            transaction=handle,
            sql="SAVEPOINT django_native_postgres_savepoint_1",
            params=None,
        ),
        mock.call(
            transaction=handle,
            sql="ROLLBACK TO SAVEPOINT django_native_postgres_savepoint_1",
            params=None,
        ),
        mock.call(
            transaction=handle,
            sql="RELEASE SAVEPOINT django_native_postgres_savepoint_1",
            params=None,
        ),
    ]
    commit_transaction.assert_not_awaited()
    rollback_transaction.assert_awaited_once_with(handle)


@pytest.mark.asyncio
async def test_executor_rolls_back_transaction_after_caught_query_error():
    pool = object()
    handle = object()

    with (
        mock.patch(
            "django_native_postgres.executor._native.create_pool",
            return_value=pool,
        ),
        mock.patch(
            "django_native_postgres.executor._native.begin_transaction",
            new=mock.AsyncMock(return_value=handle),
        ),
        mock.patch(
            "django_native_postgres.executor._native.execute_transaction",
            new=mock.AsyncMock(side_effect=RuntimeError("query failed")),
        ),
        mock.patch(
            "django_native_postgres.executor._native.commit_transaction",
            new=mock.AsyncMock(),
        ) as commit_transaction,
        mock.patch(
            "django_native_postgres.executor._native.rollback_transaction",
            new=mock.AsyncMock(),
        ) as rollback_transaction,
    ):
        executor = NativeExecutor(database_url="postgresql://example")

        async with executor.transaction():
            with pytest.raises(RuntimeError, match="query failed"):
                await executor.execute(sql="SELECT invalid")

            with pytest.raises(
                RuntimeError,
                match="Native transaction is marked for rollback",
            ):
                await executor.execute(sql="SELECT 1")

    commit_transaction.assert_not_awaited()
    rollback_transaction.assert_awaited_once_with(handle)


@pytest.mark.asyncio
async def test_executor_restores_context_after_commit_error():
    pool = object()
    handle = object()
    pool_rows = [["pool"]]

    with (
        mock.patch(
            "django_native_postgres.executor._native.create_pool",
            return_value=pool,
        ),
        mock.patch(
            "django_native_postgres.executor._native.begin_transaction",
            new=mock.AsyncMock(return_value=handle),
        ),
        mock.patch(
            "django_native_postgres.executor._native.commit_transaction",
            new=mock.AsyncMock(side_effect=RuntimeError("commit failed")),
        ),
        mock.patch(
            "django_native_postgres.executor._native.execute",
            new=mock.AsyncMock(return_value=pool_rows),
        ) as execute,
    ):
        executor = NativeExecutor(database_url="postgresql://example")

        with pytest.raises(RuntimeError, match="commit failed"):
            async with executor.transaction():
                pass

        rows = await executor.execute(sql="SELECT 'pool'")

    assert rows is pool_rows
    execute.assert_awaited_once_with(
        pool=pool,
        sql="SELECT 'pool'",
        params=None,
    )
