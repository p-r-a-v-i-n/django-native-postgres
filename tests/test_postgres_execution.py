import asyncio
import multiprocessing
import os

import pytest
from django.db import (
    DataError,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from django_native_postgres import _native
from django_native_postgres.executor import NativeExecutor


async def _get_backend_pid(pool):
    rows = await asyncio.wait_for(
        _native.execute(
            pool=pool,
            sql="SELECT pg_backend_pid()",
        ),
        timeout=5,
    )
    return rows[0][0]


async def _wait_for_backend_count(
    pool,
    sql,
    backend_pid,
    expected_count,
):
    event_loop = asyncio.get_running_loop()
    deadline = event_loop.time() + 2

    while True:
        rows = await _native.execute(
            pool=pool,
            sql=sql,
            params=(backend_pid,),
        )
        if rows == [[expected_count]]:
            return
        if event_loop.time() >= deadline:
            pytest.fail(
                f"expected backend count {expected_count}, received {rows[0][0]}"
            )
        await asyncio.sleep(0.01)


def _get_backend_pid_in_child(pool, connection):
    backend_pid = asyncio.run(_get_backend_pid(pool))
    connection.send(backend_pid)
    connection.close()


async def _execute_transaction_with_timeout(transaction):
    return await asyncio.wait_for(
        _native.execute_transaction(
            transaction=transaction,
            sql="SELECT pg_backend_pid()",
        ),
        timeout=1,
    )


def _execute_transaction_in_child(transaction, connection):
    try:
        rows = asyncio.run(_execute_transaction_with_timeout(transaction))
    except Exception as error:
        connection.send((type(error).__name__, str(error)))
    else:
        connection.send(("result", rows))
    finally:
        connection.close()


def test_native_exposes_execute():
    assert callable(_native.execute)


@pytest.fixture
def postgres_database_url() -> str:
    database_url = os.environ.get("DJANGO_NATIVE_POSTGRES_TEST_DATABASE_URL")
    if database_url is None:
        pytest.skip("DJANGO_NATIVE_POSTGRES_TEST_DATABASE_URL is not configured")
    return database_url


@pytest.fixture
def postgres_pool(postgres_database_url):
    return _native.create_pool(database_url=postgres_database_url)


@pytest.mark.asyncio
async def test_execute_returns_text_rows_from_postgres(postgres_pool):
    rows = await _native.execute(
        pool=postgres_pool,
        sql="SELECT 'django-native-postgres'::TEXT",
    )

    assert rows == [["django-native-postgres"]]


@pytest.mark.asyncio
async def test_execute_result_returns_rows_and_affected_count(postgres_pool):
    rows, rows_affected = await _native.execute_result(
        pool=postgres_pool,
        sql="SELECT value FROM (VALUES (1), (2)) AS example(value)",
    )

    assert rows == [[1], [2]]
    assert rows_affected == 2


@pytest.mark.asyncio
async def test_execute_with_metadata_returns_column_names(postgres_pool):
    rows, rows_affected, columns = await _native.execute_with_metadata(
        pool=postgres_pool,
        sql="SELECT 1::BIGINT AS first_value, 'two'::TEXT AS second_value",
    )

    assert rows == [[1, "two"]]
    assert rows_affected == 1
    assert columns == ["first_value", "second_value"]


@pytest.mark.asyncio
async def test_transaction_execute_with_metadata_returns_column_names(postgres_pool):
    transaction = await _native.begin_transaction(postgres_pool)

    try:
        rows, rows_affected, columns = await _native.execute_transaction_with_metadata(
            transaction=transaction,
            sql="SELECT 1::BIGINT AS value",
        )
    finally:
        await _native.rollback_transaction(transaction)

    assert rows == [[1]]
    assert rows_affected == 1
    assert columns == ["value"]


@pytest.mark.asyncio
async def test_transaction_execute_accepts_named_parameters(postgres_pool):
    transaction = await _native.begin_transaction(postgres_pool)

    try:
        rows = await _native.execute_transaction(
            transaction=transaction,
            sql="SELECT %(value)s::TEXT, %(value)s::TEXT",
            params={"value": "Django"},
        )
    finally:
        await _native.rollback_transaction(transaction)

    assert rows == [["Django", "Django"]]


@pytest.mark.asyncio
async def test_transaction_execute_result_returns_write_count(postgres_pool):
    transaction = await _native.begin_transaction(postgres_pool)

    await _native.execute_transaction(
        transaction=transaction,
        sql="CREATE TEMPORARY TABLE native_result_count (value BIGINT)",
    )
    rows, rows_affected = await _native.execute_transaction_result(
        transaction=transaction,
        sql="INSERT INTO native_result_count VALUES (1), (2) RETURNING value",
    )

    assert rows == [[1], [2]]
    assert rows_affected == 2

    rows, rows_affected = await _native.execute_transaction_result(
        transaction=transaction,
        sql="UPDATE native_result_count SET value = value + 1",
    )

    assert rows == []
    assert rows_affected == 2

    await _native.rollback_transaction(transaction)


@pytest.mark.asyncio
async def test_execute_reuses_postgres_connection(postgres_pool):
    first_rows = await _native.execute(
        pool=postgres_pool,
        sql="SELECT pg_backend_pid()",
    )
    second_rows = await _native.execute(
        pool=postgres_pool,
        sql="SELECT pg_backend_pid()",
    )

    assert second_rows == first_rows


@pytest.mark.asyncio
async def test_transaction_pins_and_releases_pool_connection(
    postgres_database_url,
):
    pool = _native.create_pool(
        database_url=postgres_database_url,
        max_size=1,
    )
    transaction = await _native.begin_transaction(pool)

    first_rows = await _native.execute_transaction(
        transaction=transaction,
        sql="SELECT pg_backend_pid()",
    )
    second_rows = await _native.execute_transaction(
        transaction=transaction,
        sql="SELECT pg_backend_pid()",
    )

    assert second_rows == first_rows

    await _native.rollback_transaction(transaction)

    pool_rows = await _native.execute(
        pool=pool,
        sql="SELECT pg_backend_pid()",
    )
    assert pool_rows == first_rows


@pytest.mark.asyncio
async def test_transaction_commit_persists_changes(postgres_database_url):
    pool = _native.create_pool(
        database_url=postgres_database_url,
        max_size=1,
    )
    await _native.execute(
        pool=pool,
        sql="CREATE TEMP TABLE native_transaction_commit (value BIGINT)",
    )
    transaction = await _native.begin_transaction(pool)

    await _native.execute_transaction(
        transaction=transaction,
        sql="INSERT INTO native_transaction_commit VALUES (1)",
    )
    await _native.commit_transaction(transaction)

    rows = await _native.execute(
        pool=pool,
        sql="SELECT value FROM native_transaction_commit",
    )
    assert rows == [[1]]

    with pytest.raises(
        RuntimeError,
        match="PostgreSQL transaction handle is closed",
    ):
        await _native.execute_transaction(
            transaction=transaction,
            sql="SELECT 1",
        )


@pytest.mark.asyncio
async def test_transaction_rollback_discards_changes(postgres_database_url):
    pool = _native.create_pool(
        database_url=postgres_database_url,
        max_size=1,
    )
    await _native.execute(
        pool=pool,
        sql="CREATE TEMP TABLE native_transaction_rollback (value BIGINT)",
    )
    transaction = await _native.begin_transaction(pool)

    await _native.execute_transaction(
        transaction=transaction,
        sql="INSERT INTO native_transaction_rollback VALUES (1)",
    )
    await _native.rollback_transaction(transaction)

    rows = await _native.execute(
        pool=pool,
        sql="SELECT value FROM native_transaction_rollback",
    )
    assert rows == []

    with pytest.raises(
        RuntimeError,
        match="PostgreSQL transaction handle is closed",
    ):
        await _native.execute_transaction(
            transaction=transaction,
            sql="SELECT 1",
        )


@pytest.mark.asyncio
async def test_native_executor_transaction_commits_and_rolls_back(
    postgres_database_url,
):
    executor = NativeExecutor(
        database_url=postgres_database_url,
        pool_max_size=1,
    )

    try:
        await executor.execute(
            sql="CREATE TEMP TABLE native_executor_transaction (value BIGINT)",
        )

        async with executor.transaction():
            await executor.execute(
                sql="INSERT INTO native_executor_transaction VALUES (%s)",
                params=(1,),
            )

        with pytest.raises(ValueError, match="roll back this transaction"):
            async with executor.transaction():
                await executor.execute(
                    sql="INSERT INTO native_executor_transaction VALUES (%s)",
                    params=(2,),
                )
                raise ValueError("roll back this transaction")

        rows = await executor.execute(
            sql="SELECT value FROM native_executor_transaction ORDER BY value",
        )
        assert rows == [[1]]
    finally:
        await executor.close()


@pytest.mark.asyncio
async def test_nested_transaction_rolls_back_to_savepoint(postgres_database_url):
    executor = NativeExecutor(
        database_url=postgres_database_url,
        pool_max_size=1,
    )

    try:
        await executor.execute(
            sql="CREATE TEMP TABLE native_savepoint (value BIGINT)",
        )

        async with executor.transaction():
            await executor.execute(
                sql="INSERT INTO native_savepoint VALUES (%s)",
                params=(1,),
            )

            with pytest.raises(ValueError, match="roll back this savepoint"):
                async with executor.transaction():
                    await executor.execute(
                        sql="INSERT INTO native_savepoint VALUES (%s)",
                        params=(2,),
                    )
                    raise ValueError("roll back this savepoint")

            await executor.execute(
                sql="INSERT INTO native_savepoint VALUES (%s)",
                params=(3,),
            )

        rows = await executor.execute(
            sql="SELECT value FROM native_savepoint ORDER BY value",
        )
        assert rows == [[1], [3]]
    finally:
        await executor.close()


@pytest.mark.asyncio
async def test_released_savepoint_rolls_back_with_outer_transaction(
    postgres_database_url,
):
    executor = NativeExecutor(
        database_url=postgres_database_url,
        pool_max_size=1,
    )

    try:
        await executor.execute(
            sql="CREATE TEMP TABLE released_savepoint (value BIGINT)",
        )

        with pytest.raises(ValueError, match="roll back outer transaction"):
            async with executor.transaction():
                async with executor.transaction():
                    await executor.execute(
                        sql="INSERT INTO released_savepoint VALUES (%s)",
                        params=(1,),
                    )

                raise ValueError("roll back outer transaction")

        rows = await executor.execute(
            sql="SELECT value FROM released_savepoint",
        )
        assert rows == []
    finally:
        await executor.close()


@pytest.mark.asyncio
async def test_savepoint_recovers_after_postgres_error(postgres_database_url):
    executor = NativeExecutor(
        database_url=postgres_database_url,
        pool_max_size=1,
    )

    try:
        await executor.execute(
            sql="CREATE TEMP TABLE savepoint_error (value BIGINT UNIQUE)",
        )

        async with executor.transaction():
            await executor.execute(
                sql="INSERT INTO savepoint_error VALUES (%s)",
                params=(1,),
            )

            with pytest.raises(IntegrityError, match="PostgreSQL query failed"):
                async with executor.transaction():
                    await executor.execute(
                        sql="INSERT INTO savepoint_error VALUES (%s)",
                        params=(1,),
                    )

            await executor.execute(
                sql="INSERT INTO savepoint_error VALUES (%s)",
                params=(2,),
            )

        rows = await executor.execute(
            sql="SELECT value FROM savepoint_error ORDER BY value",
        )
        assert rows == [[1], [2]]
    finally:
        await executor.close()


@pytest.mark.asyncio
async def test_savepoint_recovers_when_postgres_error_is_caught_inside_context(
    postgres_database_url,
):
    executor = NativeExecutor(
        database_url=postgres_database_url,
        pool_max_size=1,
    )

    try:
        await executor.execute(
            sql="CREATE TEMP TABLE caught_savepoint_error (value BIGINT UNIQUE)",
        )

        async with executor.transaction():
            await executor.execute(
                sql="INSERT INTO caught_savepoint_error VALUES (%s)",
                params=(1,),
            )

            async with executor.transaction():
                with pytest.raises(IntegrityError, match="PostgreSQL query failed"):
                    await executor.execute(
                        sql="INSERT INTO caught_savepoint_error VALUES (%s)",
                        params=(1,),
                    )

            await executor.execute(
                sql="INSERT INTO caught_savepoint_error VALUES (%s)",
                params=(2,),
            )

        rows = await executor.execute(
            sql="SELECT value FROM caught_savepoint_error ORDER BY value",
        )
        assert rows == [[1], [2]]
    finally:
        await executor.close()


@pytest.mark.asyncio
async def test_deepest_savepoint_rolls_back_independently(postgres_database_url):
    executor = NativeExecutor(
        database_url=postgres_database_url,
        pool_max_size=1,
    )

    try:
        await executor.execute(
            sql="CREATE TEMP TABLE nested_savepoints (value BIGINT)",
        )

        async with executor.transaction():
            async with executor.transaction():
                await executor.execute(
                    sql="INSERT INTO nested_savepoints VALUES (%s)",
                    params=(1,),
                )

                with pytest.raises(ValueError, match="roll back deepest savepoint"):
                    async with executor.transaction():
                        await executor.execute(
                            sql="INSERT INTO nested_savepoints VALUES (%s)",
                            params=(2,),
                        )
                        raise ValueError("roll back deepest savepoint")

                await executor.execute(
                    sql="INSERT INTO nested_savepoints VALUES (%s)",
                    params=(3,),
                )

        rows = await executor.execute(
            sql="SELECT value FROM nested_savepoints ORDER BY value",
        )
        assert rows == [[1], [3]]
    finally:
        await executor.close()


@pytest.mark.asyncio
async def test_transaction_holds_pool_connection_until_rollback(
    postgres_database_url,
):
    pool = _native.create_pool(
        database_url=postgres_database_url,
        max_size=1,
        wait_timeout_ms=50,
    )
    transaction = await _native.begin_transaction(pool)

    with pytest.raises(
        RuntimeError,
        match="failed to acquire PostgreSQL connection",
    ):
        await _native.execute(
            pool=pool,
            sql="SELECT 1",
        )

    await _native.rollback_transaction(transaction)

    rows = await _native.execute(
        pool=pool,
        sql="SELECT 1::BIGINT",
    )
    assert rows == [[1]]


@pytest.mark.asyncio
async def test_transaction_can_rollback_after_query_error(
    postgres_database_url,
):
    pool = _native.create_pool(
        database_url=postgres_database_url,
        max_size=1,
    )
    transaction = await _native.begin_transaction(pool)
    transaction_rows = await _native.execute_transaction(
        transaction=transaction,
        sql="SELECT pg_backend_pid()",
    )

    with pytest.raises(RuntimeError, match="PostgreSQL query failed"):
        await _native.execute_transaction(
            transaction=transaction,
            sql="SELECT FROM",
        )

    await _native.rollback_transaction(transaction)

    pool_rows = await _native.execute(
        pool=pool,
        sql="SELECT pg_backend_pid()",
    )
    assert pool_rows == transaction_rows


@pytest.mark.asyncio
async def test_transaction_can_only_be_finalized_once(postgres_database_url):
    pool = _native.create_pool(database_url=postgres_database_url)
    transaction = await _native.begin_transaction(pool)

    results = await asyncio.gather(
        _native.commit_transaction(transaction),
        _native.rollback_transaction(transaction),
        return_exceptions=True,
    )

    successes = [result for result in results if result is None]
    errors = [result for result in results if isinstance(result, Exception)]

    assert len(successes) == 1
    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)
    assert "PostgreSQL transaction handle is closed" in str(errors[0])


@pytest.mark.asyncio
async def test_dropping_transaction_rolls_back_and_releases_connection(
    postgres_database_url,
):
    pool = _native.create_pool(
        database_url=postgres_database_url,
        max_size=1,
        wait_timeout_ms=1_000,
    )
    await _native.execute(
        pool=pool,
        sql="CREATE TEMP TABLE native_transaction_drop (value BIGINT)",
    )
    transaction = await _native.begin_transaction(pool)
    await _native.execute_transaction(
        transaction=transaction,
        sql="INSERT INTO native_transaction_drop VALUES (1)",
    )

    del transaction

    rows = await _native.execute(
        pool=pool,
        sql="SELECT value FROM native_transaction_drop",
    )
    assert rows == []


@pytest.mark.asyncio
async def test_begin_transaction_rejects_closed_pool(postgres_database_url):
    pool = _native.create_pool(database_url=postgres_database_url)
    await _native.close_pool(pool)

    with pytest.raises(
        RuntimeError,
        match="PostgreSQL pool handle is closed",
    ):
        await _native.begin_transaction(pool)


@pytest.mark.asyncio
async def test_close_pool_allows_active_transaction_to_finish(
    postgres_database_url,
):
    target_pool = _native.create_pool(
        database_url=postgres_database_url,
        max_size=1,
    )
    observer_pool = _native.create_pool(database_url=postgres_database_url)

    try:
        transaction = await _native.begin_transaction(target_pool)
        first_rows = await _native.execute_transaction(
            transaction=transaction,
            sql="SELECT pg_backend_pid()",
        )
        backend_pid = first_rows[0][0]

        await _native.close_pool(target_pool)

        second_rows = await _native.execute_transaction(
            transaction=transaction,
            sql="SELECT pg_backend_pid()",
        )
        assert second_rows == first_rows

        with pytest.raises(
            RuntimeError,
            match="PostgreSQL pool handle is closed",
        ):
            await _native.execute(
                pool=target_pool,
                sql="SELECT 1",
            )

        await _native.commit_transaction(transaction)

        await _wait_for_backend_count(
            observer_pool,
            """
                SELECT COUNT(*)::BIGINT
                FROM pg_stat_activity
                WHERE pid = %s
            """,
            backend_pid,
            0,
        )
    finally:
        await _native.close_pools()


@pytest.mark.asyncio
async def test_close_pools_releases_postgres_connection(postgres_pool):
    first_rows = await _native.execute(
        pool=postgres_pool,
        sql="SELECT pg_backend_pid()",
    )

    await _native.close_pools()

    second_rows = await _native.execute(
        pool=postgres_pool,
        sql="SELECT pg_backend_pid()",
    )

    assert second_rows != first_rows


@pytest.mark.asyncio
async def test_pool_replaces_broken_postgres_connection(postgres_pool):
    await _native.close_pools()

    try:
        first_rows = await _native.execute(
            pool=postgres_pool,
            sql="SELECT pg_backend_pid()",
        )

        with pytest.raises(RuntimeError, match="PostgreSQL query failed"):
            await _native.execute(
                pool=postgres_pool,
                sql="SELECT pg_terminate_backend(pg_backend_pid())",
            )

        second_rows = await _native.execute(
            pool=postgres_pool,
            sql="SELECT pg_backend_pid()",
        )

        assert second_rows != first_rows
    finally:
        await _native.close_pools()


@pytest.mark.asyncio
async def test_pool_keeps_connection_after_safe_query_error(postgres_pool):
    await _native.close_pools()

    try:
        first_rows = await _native.execute(
            pool=postgres_pool,
            sql="SELECT pg_backend_pid()",
        )

        with pytest.raises(RuntimeError, match="PostgreSQL query failed"):
            await _native.execute(
                pool=postgres_pool,
                sql="SELECT FROM",
            )

        second_rows = await _native.execute(
            pool=postgres_pool,
            sql="SELECT pg_backend_pid()",
        )

        assert second_rows == first_rows
    finally:
        await _native.close_pools()


@pytest.mark.asyncio
async def test_execute_decodes_multiple_nullable_text_columns(postgres_pool):
    rows = await _native.execute(
        pool=postgres_pool,
        sql="SELECT 'value'::TEXT, NULL::TEXT",
    )

    assert rows == [["value", None]]


@pytest.mark.asyncio
async def test_execute_propagates_postgres_query_errors(postgres_pool):
    with pytest.raises(RuntimeError, match="PostgreSQL query failed"):
        await _native.execute(
            pool=postgres_pool,
            sql="SELECT FROM",
        )


@pytest.mark.asyncio
async def test_execute_accepts_django_text_parameter(postgres_pool):
    rows = await _native.execute(
        pool=postgres_pool,
        sql="SELECT %s::TEXT",
        params=("Django",),
    )

    assert rows == [["Django"]]


@pytest.mark.asyncio
async def test_execute_accepts_multiple_django_text_parameters(
    postgres_pool,
):
    rows = await _native.execute(
        pool=postgres_pool,
        sql="SELECT %s::TEXT, %s::TEXT",
        params=("Django", "Rust"),
    )

    assert rows == [["Django", "Rust"]]


@pytest.mark.asyncio
async def test_execute_accepts_named_parameters_in_sql_order(postgres_pool):
    rows = await _native.execute(
        pool=postgres_pool,
        sql=("SELECT 5 %% 2, %(second)s::TEXT, %(first)s::TEXT, %(second)s::TEXT"),
        params={
            "first": "Django",
            "second": "Rust",
            "unused": "ignored",
        },
    )

    assert rows == [[1, "Rust", "Django", "Rust"]]


@pytest.mark.asyncio
async def test_execute_rejects_missing_named_parameter(postgres_pool):
    with pytest.raises(
        RuntimeError,
        match='SQL placeholder "missing" has no matching parameter',
    ):
        await _native.execute(
            pool=postgres_pool,
            sql="SELECT %(missing)s::TEXT",
            params={"unused": "Django"},
        )


@pytest.mark.asyncio
async def test_execute_rejects_parameter_count_mismatch(
    postgres_pool,
):
    with pytest.raises(
        RuntimeError,
        match="SQL contains 1 placeholders but received 2 parameters",
    ):
        await _native.execute(
            pool=postgres_pool,
            sql="SELECT %s::TEXT",
            params=("Django", "Rust"),
        )


@pytest.mark.asyncio
async def test_execute_accepts_integer_parameter(postgres_pool):
    rows = await _native.execute(
        pool=postgres_pool,
        sql="SELECT %s::BIGINT",
        params=(42,),
    )
    assert rows == [[42]]


@pytest.mark.asyncio
async def test_execute_assigns_type_to_untyped_integer_parameter(
    postgres_pool,
):
    rows = await _native.execute(
        pool=postgres_pool,
        sql="SELECT %s",
        params=(42,),
    )

    assert rows == [[42]]


@pytest.mark.asyncio
async def test_execute_accepts_null_parameters(postgres_pool):
    rows = await _native.execute(
        pool=postgres_pool,
        sql="SELECT %s::BIGINT, %s::TEXT",
        params=(None, None),
    )

    assert rows == [[None, None]]


@pytest.mark.asyncio
async def test_execute_exposes_postgres_integrity_errors(postgres_pool):
    transaction = await _native.begin_transaction(postgres_pool)
    try:
        await _native.execute_transaction(
            transaction,
            "CREATE TEMPORARY TABLE native_unique (value BIGINT UNIQUE)",
        )
        await _native.execute_transaction(
            transaction,
            "INSERT INTO native_unique VALUES (%s)",
            (1,),
        )

        with pytest.raises(_native.PostgresIntegrityError) as error:
            await _native.execute_transaction(
                transaction,
                "INSERT INTO native_unique VALUES (%s)",
                (1,),
            )

        assert isinstance(error.value, _native.PostgresDatabaseError)
        assert error.value.sqlstate == "23505"
        assert error.value.severity == "ERROR"
        assert "duplicate key value" in error.value.message_primary
        assert "(value)=(1)" in error.value.detail
        assert error.value.hint is None
        assert error.value.schema_name.startswith("pg_temp")
        assert error.value.table_name == "native_unique"
        assert error.value.column_name is None
        assert error.value.datatype_name is None
        assert error.value.constraint_name == "native_unique_value_key"
    finally:
        await _native.rollback_transaction(transaction)


@pytest.mark.asyncio
async def test_execute_exposes_postgres_sqlstate(postgres_pool):
    with pytest.raises(_native.PostgresDatabaseError) as error:
        await _native.execute(
            pool=postgres_pool,
            sql="SELECT * FROM missing_native_error_mapping_table",
        )

    assert error.value.sqlstate == "42P01"


@pytest.mark.asyncio
async def test_executor_maps_postgres_programming_errors(postgres_database_url):
    executor = NativeExecutor(database_url=postgres_database_url)

    try:
        with pytest.raises(ProgrammingError) as error:
            await executor.execute(
                sql="SELECT * FROM missing_error_mapping_table",
            )
    finally:
        await executor.close()

    assert isinstance(error.value.__cause__, _native.PostgresDatabaseError)
    assert error.value.__cause__.sqlstate == "42P01"


@pytest.mark.asyncio
async def test_executor_maps_postgres_data_errors(postgres_database_url):
    executor = NativeExecutor(database_url=postgres_database_url)

    try:
        with pytest.raises(DataError) as error:
            await executor.execute(sql="SELECT 'not-an-integer'::BIGINT")
    finally:
        await executor.close()

    assert isinstance(error.value.__cause__, _native.PostgresDatabaseError)
    assert error.value.__cause__.sqlstate == "22P02"


@pytest.mark.asyncio
async def test_executor_maps_deferred_constraint_errors_on_commit(
    postgres_database_url,
):
    executor = NativeExecutor(
        database_url=postgres_database_url,
        pool_max_size=1,
    )

    try:
        await executor.execute(
            sql=(
                "CREATE TEMP TABLE deferred_unique ("
                "value BIGINT, "
                "CONSTRAINT deferred_unique_value UNIQUE (value) "
                "DEFERRABLE INITIALLY DEFERRED)"
            )
        )

        with pytest.raises(IntegrityError) as error:
            async with executor.transaction():
                await executor.execute(
                    sql="INSERT INTO deferred_unique VALUES (%s), (%s)",
                    params=(1, 1),
                )
    finally:
        await executor.close()

    assert isinstance(error.value.__cause__, _native.PostgresIntegrityError)
    assert error.value.__cause__.sqlstate == "23505"
    assert error.value.__cause__.constraint_name == "deferred_unique_value"


@pytest.mark.asyncio
@pytest.mark.parametrize("sqlstate", ["40001", "40P01"])
async def test_executor_maps_retryable_transaction_errors(
    postgres_database_url,
    sqlstate,
):
    executor = NativeExecutor(database_url=postgres_database_url)

    try:
        with pytest.raises(OperationalError) as error:
            await executor.execute(
                sql=(
                    "DO $error$ BEGIN "
                    "RAISE EXCEPTION 'forced transaction failure' "
                    f"USING ERRCODE = '{sqlstate}'; "
                    "END $error$"
                )
            )
    finally:
        await executor.close()

    assert isinstance(error.value.__cause__, _native.PostgresDatabaseError)
    assert error.value.__cause__.sqlstate == sqlstate


@pytest.mark.asyncio
async def test_executor_maps_connection_failures_without_exposing_url():
    password = "secret-password"
    executor = NativeExecutor(
        database_url=(f"postgresql://missing:{password}@127.0.0.1:1/missing_database"),
        pool_wait_timeout_ms=100,
    )

    try:
        with pytest.raises(OperationalError) as error:
            await executor.execute(sql="SELECT 1")
    finally:
        await executor.close()

    assert isinstance(error.value.__cause__, _native.PostgresOperationalError)
    assert password not in str(error.value)


@pytest.mark.asyncio
async def test_executor_maps_placeholder_errors(postgres_database_url):
    executor = NativeExecutor(database_url=postgres_database_url)

    try:
        with pytest.raises(ProgrammingError) as error:
            await executor.execute(
                sql="SELECT %s::TEXT",
                params=("first", "second"),
            )
    finally:
        await executor.close()

    assert isinstance(error.value.__cause__, _native.PostgresProgrammingError)


@pytest.mark.asyncio
async def test_executor_maps_unsupported_postgres_types(postgres_database_url):
    executor = NativeExecutor(database_url=postgres_database_url)

    try:
        with pytest.raises(NotSupportedError) as error:
            await executor.execute(sql="SELECT TRUE")
    finally:
        await executor.close()

    assert isinstance(error.value.__cause__, _native.PostgresNotSupportedError)


@pytest.mark.asyncio
async def test_executor_maps_closed_pool_errors(postgres_database_url):
    executor = NativeExecutor(database_url=postgres_database_url)
    await executor.close()

    with pytest.raises(InterfaceError) as error:
        await executor.execute(sql="SELECT 1")

    assert isinstance(error.value.__cause__, _native.PostgresInterfaceError)


@pytest.mark.asyncio
async def test_execute_limits_pool_connections(postgres_database_url):
    await _native.close_pools()

    pool_max_size = 2
    query_count = pool_max_size * 3
    pool = _native.create_pool(
        database_url=postgres_database_url,
        max_size=pool_max_size,
    )

    queries = [
        _native.execute(
            pool=pool,
            sql="SELECT pg_backend_pid() FROM pg_sleep(0.05)",
        )
        for _ in range(query_count)
    ]

    results = await asyncio.gather(*queries)
    backend_pids = {rows[0][0] for rows in results}
    assert len(backend_pids) == pool_max_size


def test_create_pool_rejects_zero_max_size(postgres_database_url):
    with pytest.raises(
        RuntimeError,
        match="PostgreSQL pool max size must be greater than zero",
    ):
        _native.create_pool(
            database_url=postgres_database_url,
            max_size=0,
        )


@pytest.mark.parametrize("wait_timeout_ms", [0, -1])
def test_create_pool_rejects_invalid_wait_timeout(
    postgres_database_url,
    wait_timeout_ms,
):
    with pytest.raises(
        RuntimeError,
        match="PostgreSQL pool wait timeout must be a positive integer",
    ):
        _native.create_pool(
            database_url=postgres_database_url,
            wait_timeout_ms=wait_timeout_ms,
        )


@pytest.mark.asyncio
async def test_execute_times_out_when_pool_is_exhausted(postgres_database_url):
    await _native.close_pools()
    pool = _native.create_pool(
        database_url=postgres_database_url,
        max_size=1,
        wait_timeout_ms=50,
    )

    try:
        first_query = _native.execute(
            pool=pool,
            sql="SELECT pg_backend_pid() FROM pg_sleep(0.2)",
        )
        second_query = _native.execute(
            pool=pool,
            sql="SELECT pg_backend_pid() FROM pg_sleep(0.2)",
        )

        results = await asyncio.gather(
            first_query,
            second_query,
            return_exceptions=True,
        )
    finally:
        await _native.close_pools()

    rows = [result for result in results if isinstance(result, list)]
    errors = [result for result in results if isinstance(result, Exception)]

    assert len(rows) == 1
    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)
    assert "failed to acquire PostgreSQL connection" in str(errors[0])


@pytest.mark.asyncio
async def test_separate_pool_handles_do_not_share_connection(
    postgres_database_url,
):
    await _native.close_pools()
    first_pool = _native.create_pool(database_url=postgres_database_url)
    second_pool = _native.create_pool(database_url=postgres_database_url)

    try:
        first_rows = await _native.execute(
            pool=first_pool,
            sql="SELECT pg_backend_pid()",
        )
        second_rows = await _native.execute(
            pool=second_pool,
            sql="SELECT pg_backend_pid()",
        )

        assert second_rows != first_rows
    finally:
        await _native.close_pools()


@pytest.mark.asyncio
async def test_pool_handles_keep_their_own_configuration(postgres_database_url):
    await _native.close_pools()
    small_pool = _native.create_pool(
        database_url=postgres_database_url,
        max_size=1,
    )
    large_pool = _native.create_pool(
        database_url=postgres_database_url,
        max_size=2,
    )

    try:
        small_pool_rows = await _native.execute(
            pool=small_pool,
            sql="SELECT pg_backend_pid()",
        )
        large_pool_results = await asyncio.gather(
            _native.execute(
                pool=large_pool,
                sql="SELECT pg_backend_pid() FROM pg_sleep(0.05)",
            ),
            _native.execute(
                pool=large_pool,
                sql="SELECT pg_backend_pid() FROM pg_sleep(0.05)",
            ),
        )

        small_pool_pid = small_pool_rows[0][0]
        large_pool_pids = {rows[0][0] for rows in large_pool_results}

        assert len(large_pool_pids) == 2
        assert small_pool_pid not in large_pool_pids
    finally:
        await _native.close_pools()


@pytest.mark.asyncio
async def test_close_pool_only_closes_selected_handle(postgres_database_url):
    await _native.close_pools()
    first_pool = _native.create_pool(database_url=postgres_database_url)
    second_pool = _native.create_pool(database_url=postgres_database_url)

    try:
        await _native.execute(
            pool=first_pool,
            sql="SELECT pg_backend_pid()",
        )
        second_pool_rows = await _native.execute(
            pool=second_pool,
            sql="SELECT pg_backend_pid()",
        )

        await _native.close_pool(first_pool)

        with pytest.raises(
            RuntimeError,
            match="PostgreSQL pool handle is closed",
        ):
            await _native.execute(
                pool=first_pool,
                sql="SELECT 1",
            )

        current_second_pool_rows = await _native.execute(
            pool=second_pool,
            sql="SELECT pg_backend_pid()",
        )
        assert current_second_pool_rows == second_pool_rows
    finally:
        await _native.close_pools()


@pytest.mark.asyncio
async def test_close_pool_before_first_execution(postgres_database_url):
    pool = _native.create_pool(database_url=postgres_database_url)

    await _native.close_pool(pool)

    with pytest.raises(
        RuntimeError,
        match="PostgreSQL pool handle is closed",
    ):
        await _native.execute(
            pool=pool,
            sql="SELECT 1",
        )


@pytest.mark.asyncio
async def test_close_pool_is_idempotent(postgres_database_url):
    pool = _native.create_pool(database_url=postgres_database_url)

    await asyncio.gather(
        _native.close_pool(pool),
        _native.close_pool(pool),
    )

    with pytest.raises(
        RuntimeError,
        match="PostgreSQL pool handle is closed",
    ):
        await _native.execute(
            pool=pool,
            sql="SELECT 1",
        )


@pytest.mark.asyncio
async def test_close_pool_releases_idle_postgres_connection(
    postgres_database_url,
):
    target_pool = _native.create_pool(database_url=postgres_database_url)
    observer_pool = _native.create_pool(database_url=postgres_database_url)

    try:
        target_backend_pid = await _get_backend_pid(target_pool)

        await _native.close_pool(target_pool)

        await _wait_for_backend_count(
            observer_pool,
            """
                SELECT COUNT(*)::BIGINT
                FROM pg_stat_activity
                WHERE pid = %s
            """,
            target_backend_pid,
            0,
        )
    finally:
        await _native.close_pools()


@pytest.mark.asyncio
async def test_close_pool_allows_active_query_to_finish(postgres_database_url):
    target_pool = _native.create_pool(
        database_url=postgres_database_url,
        max_size=1,
    )
    observer_pool = _native.create_pool(
        database_url=postgres_database_url,
        max_size=1,
    )
    lock_id = os.getpid()
    query = None

    try:
        await _native.execute(
            pool=observer_pool,
            sql="SELECT 1::BIGINT FROM pg_advisory_lock(%s)",
            params=(lock_id,),
        )
        target_backend_pid = await _get_backend_pid(target_pool)
        query = asyncio.create_task(
            _native.execute(
                pool=target_pool,
                sql="SELECT pg_backend_pid() FROM pg_advisory_lock(%s)",
                params=(lock_id,),
            )
        )

        await _wait_for_backend_count(
            observer_pool,
            """
                SELECT COUNT(*)::BIGINT
                FROM pg_stat_activity
                WHERE pid = %s AND wait_event_type = 'Lock'
            """,
            target_backend_pid,
            1,
        )

        await _native.close_pool(target_pool)
        await _native.execute(
            pool=observer_pool,
            sql="SELECT 1::BIGINT FROM pg_advisory_unlock(%s)",
            params=(lock_id,),
        )

        rows = await asyncio.wait_for(query, timeout=2)
        assert rows == [[target_backend_pid]]

        with pytest.raises(
            RuntimeError,
            match="PostgreSQL pool handle is closed",
        ):
            await _native.execute(
                pool=target_pool,
                sql="SELECT 1",
            )
    finally:
        if query is not None and not query.done():
            query.cancel()
            await asyncio.gather(query, return_exceptions=True)
        await _native.close_pools()


@pytest.mark.asyncio
async def test_cancelling_query_releases_pool_connection(postgres_database_url):
    target_pool = _native.create_pool(
        database_url=postgres_database_url,
        max_size=1,
        wait_timeout_ms=1_000,
    )
    observer_pool = _native.create_pool(database_url=postgres_database_url)
    lock_id = os.getpid()
    query = None

    try:
        await _native.execute(
            pool=observer_pool,
            sql="SELECT 1::BIGINT FROM pg_advisory_lock(%s)",
            params=(lock_id,),
        )
        target_backend_pid = await _get_backend_pid(target_pool)
        query = asyncio.create_task(
            _native.execute(
                pool=target_pool,
                sql="SELECT pg_backend_pid() FROM pg_advisory_lock(%s)",
                params=(lock_id,),
            )
        )

        await _wait_for_backend_count(
            observer_pool,
            """
                SELECT COUNT(*)::BIGINT
                FROM pg_stat_activity
                WHERE pid = %s AND wait_event_type = 'Lock'
            """,
            target_backend_pid,
            1,
        )

        query.cancel()
        with pytest.raises(asyncio.CancelledError):
            await query

        rows = await _native.execute(
            pool=target_pool,
            sql="SELECT pg_backend_pid()",
        )
        assert rows == [[target_backend_pid]]
    finally:
        await _native.execute(
            pool=observer_pool,
            sql="SELECT 1::BIGINT FROM pg_advisory_unlock(%s)",
            params=(lock_id,),
        )
        if query is not None and not query.done():
            await asyncio.gather(query, return_exceptions=True)
        await _native.close_pool(target_pool)
        await _native.close_pool(observer_pool)


@pytest.mark.asyncio
async def test_cancelling_query_waiting_for_pool_does_not_execute(
    postgres_database_url,
):
    target_pool = _native.create_pool(
        database_url=postgres_database_url,
        max_size=1,
        wait_timeout_ms=1_000,
    )
    observer_pool = _native.create_pool(
        database_url=postgres_database_url,
        max_size=1,
    )
    lock_id = os.getpid()
    blocked_query = None
    queued_query = None

    try:
        await _native.execute(
            pool=target_pool,
            sql="CREATE TEMP TABLE cancelled_pool_query (value BIGINT)",
        )
        await _native.execute(
            pool=observer_pool,
            sql="SELECT 1::BIGINT FROM pg_advisory_lock(%s)",
            params=(lock_id,),
        )
        target_backend_pid = await _get_backend_pid(target_pool)
        blocked_query = asyncio.create_task(
            _native.execute(
                pool=target_pool,
                sql="SELECT 1::BIGINT FROM pg_advisory_lock(%s)",
                params=(lock_id,),
            )
        )

        await _wait_for_backend_count(
            observer_pool,
            """
                SELECT COUNT(*)::BIGINT
                FROM pg_stat_activity
                WHERE pid = %s AND wait_event_type = 'Lock'
            """,
            target_backend_pid,
            1,
        )

        queued_query = asyncio.create_task(
            _native.execute(
                pool=target_pool,
                sql="INSERT INTO cancelled_pool_query VALUES (1)",
            )
        )
        await asyncio.sleep(0)

        queued_query.cancel()
        with pytest.raises(asyncio.CancelledError):
            await queued_query

        await _native.execute(
            pool=observer_pool,
            sql="SELECT 1::BIGINT FROM pg_advisory_unlock(%s)",
            params=(lock_id,),
        )
        await asyncio.wait_for(blocked_query, timeout=1)

        rows = await _native.execute(
            pool=target_pool,
            sql="SELECT value FROM cancelled_pool_query",
        )
        assert rows == []
    finally:
        await _native.execute(
            pool=observer_pool,
            sql="SELECT 1::BIGINT FROM pg_advisory_unlock(%s)",
            params=(lock_id,),
        )
        for query in (blocked_query, queued_query):
            if query is not None and not query.done():
                query.cancel()
                await asyncio.gather(query, return_exceptions=True)
        await _native.close_pool(target_pool)
        await _native.close_pool(observer_pool)


@pytest.mark.asyncio
async def test_cancelling_transaction_query_releases_connection(
    postgres_database_url,
):
    executor = NativeExecutor(
        database_url=postgres_database_url,
        pool_max_size=1,
        pool_wait_timeout_ms=1_000,
    )
    observer_pool = _native.create_pool(database_url=postgres_database_url)
    lock_id = os.getpid()
    transaction_started = asyncio.get_running_loop().create_future()
    transaction_task = None

    async def execute_blocked_transaction():
        async with executor.transaction():
            rows = await executor.execute(sql="SELECT pg_backend_pid()")
            transaction_started.set_result(rows[0][0])
            await executor.execute(
                sql="INSERT INTO cancelled_transaction VALUES (1)",
            )
            await executor.execute(
                sql="SELECT 1::BIGINT FROM pg_advisory_lock(%s)",
                params=(lock_id,),
            )

    try:
        await executor.execute(
            sql="CREATE TEMP TABLE cancelled_transaction (value BIGINT)",
        )
        await _native.execute(
            pool=observer_pool,
            sql="SELECT 1::BIGINT FROM pg_advisory_lock(%s)",
            params=(lock_id,),
        )
        transaction_task = asyncio.create_task(execute_blocked_transaction())
        target_backend_pid = await transaction_started

        await _wait_for_backend_count(
            observer_pool,
            """
                SELECT COUNT(*)::BIGINT
                FROM pg_stat_activity
                WHERE pid = %s AND wait_event_type = 'Lock'
            """,
            target_backend_pid,
            1,
        )

        transaction_task.cancel()
        done, _ = await asyncio.wait({transaction_task}, timeout=1)

        assert transaction_task in done
        with pytest.raises(asyncio.CancelledError):
            transaction_task.result()

        rows = await executor.execute(sql="SELECT pg_backend_pid()")
        assert rows == [[target_backend_pid]]

        rows = await executor.execute(
            sql="SELECT value FROM cancelled_transaction",
        )
        assert rows == []
    finally:
        await _native.execute(
            pool=observer_pool,
            sql="SELECT 1::BIGINT FROM pg_advisory_unlock(%s)",
            params=(lock_id,),
        )
        if transaction_task is not None and not transaction_task.done():
            await asyncio.gather(transaction_task, return_exceptions=True)
        await executor.close()
        await _native.close_pool(observer_pool)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("isolation_level", "postgres_value"),
    [
        ("read_uncommitted", "read uncommitted"),
        ("read_committed", "read committed"),
        ("repeatable_read", "repeatable read"),
        ("serializable", "serializable"),
    ],
)
async def test_transaction_uses_requested_isolation_level(
    postgres_database_url,
    isolation_level,
    postgres_value,
):
    executor = NativeExecutor(database_url=postgres_database_url)

    try:
        async with executor.transaction(isolation_level=isolation_level):
            rows = await executor.execute(sql="SHOW transaction_isolation")

        assert rows == [[postgres_value]]
    finally:
        await executor.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("read_only", "postgres_value"),
    [(True, "on"), (False, "off")],
)
async def test_transaction_uses_requested_read_only_mode(
    postgres_database_url,
    read_only,
    postgres_value,
):
    executor = NativeExecutor(database_url=postgres_database_url)

    try:
        async with executor.transaction(read_only=read_only):
            rows = await executor.execute(sql="SHOW transaction_read_only")

        assert rows == [[postgres_value]]
    finally:
        await executor.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("deferrable", "postgres_value"),
    [(True, "on"), (False, "off")],
)
async def test_transaction_uses_requested_deferrable_mode(
    postgres_database_url,
    deferrable,
    postgres_value,
):
    executor = NativeExecutor(database_url=postgres_database_url)

    try:
        async with executor.transaction(deferrable=deferrable):
            rows = await executor.execute(sql="SHOW transaction_deferrable")

        assert rows == [[postgres_value]]
    finally:
        await executor.close()


@pytest.mark.asyncio
async def test_transaction_combines_all_options(postgres_database_url):
    executor = NativeExecutor(database_url=postgres_database_url)

    try:
        async with executor.transaction(
            isolation_level="serializable",
            read_only=True,
            deferrable=True,
        ):
            rows = await executor.execute(
                sql="""
                    SELECT
                        current_setting('transaction_isolation'),
                        current_setting('transaction_read_only'),
                        current_setting('transaction_deferrable')
                """
            )

        assert rows == [["serializable", "on", "on"]]
    finally:
        await executor.close()


@pytest.mark.asyncio
async def test_transaction_without_options_preserves_postgres_defaults(
    postgres_database_url,
):
    executor = NativeExecutor(
        database_url=postgres_database_url,
        pool_max_size=1,
    )

    try:
        await executor.execute(
            sql="SET default_transaction_isolation = 'repeatable read'"
        )
        await executor.execute(sql="SET default_transaction_read_only = on")
        await executor.execute(sql="SET default_transaction_deferrable = on")

        async with executor.transaction():
            rows = await executor.execute(
                sql="""
                    SELECT
                        current_setting('transaction_isolation'),
                        current_setting('transaction_read_only'),
                        current_setting('transaction_deferrable')
                """
            )

        assert rows == [["repeatable read", "on", "on"]]
    finally:
        await executor.execute(sql="RESET default_transaction_isolation")
        await executor.execute(sql="RESET default_transaction_read_only")
        await executor.execute(sql="RESET default_transaction_deferrable")
        await executor.close()


@pytest.mark.asyncio
async def test_transaction_options_override_postgres_defaults(
    postgres_database_url,
):
    executor = NativeExecutor(
        database_url=postgres_database_url,
        pool_max_size=1,
    )

    try:
        await executor.execute(
            sql="SET default_transaction_isolation = 'repeatable read'"
        )
        await executor.execute(sql="SET default_transaction_read_only = on")
        await executor.execute(sql="SET default_transaction_deferrable = on")

        async with executor.transaction(
            isolation_level="read_committed",
            read_only=False,
            deferrable=False,
        ):
            rows = await executor.execute(
                sql="""
                    SELECT
                        current_setting('transaction_isolation'),
                        current_setting('transaction_read_only'),
                        current_setting('transaction_deferrable')
                """
            )

        assert rows == [["read committed", "off", "off"]]
    finally:
        await executor.execute(sql="RESET default_transaction_isolation")
        await executor.execute(sql="RESET default_transaction_read_only")
        await executor.execute(sql="RESET default_transaction_deferrable")
        await executor.close()


@pytest.mark.asyncio
async def test_transaction_rejects_invalid_isolation_level(
    postgres_database_url,
):
    executor = NativeExecutor(database_url=postgres_database_url)

    try:
        with pytest.raises(
            ValueError,
            match="unsupported transaction isolation level",
        ):
            async with executor.transaction(isolation_level="snapshot"):
                pass

        rows = await executor.execute(sql="SELECT 1::BIGINT")
        assert rows == [[1]]
    finally:
        await executor.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "options",
    [
        {"isolation_level": "serializable"},
        {"read_only": True},
        {"deferrable": True},
    ],
)
async def test_nested_transaction_rejects_transaction_options(
    postgres_database_url,
    options,
):
    executor = NativeExecutor(database_url=postgres_database_url)

    try:
        async with executor.transaction():
            with pytest.raises(
                ValueError,
                match=(
                    "transaction options can only be used on the outermost transaction"
                ),
            ):
                async with executor.transaction(**options):
                    pass

            rows = await executor.execute(sql="SELECT 1::BIGINT")

        assert rows == [[1]]
    finally:
        await executor.close()


@pytest.mark.asyncio
async def test_read_only_transaction_rejects_writes(postgres_database_url):
    executor = NativeExecutor(database_url=postgres_database_url)

    try:
        await executor.execute(sql="DROP TABLE IF EXISTS native_read_only_transaction")
        await executor.execute(
            sql="CREATE TABLE native_read_only_transaction (value BIGINT)"
        )

        with pytest.raises(InternalError, match="PostgreSQL query failed"):
            async with executor.transaction(read_only=True):
                await executor.execute(
                    sql="INSERT INTO native_read_only_transaction VALUES (1)"
                )

        rows = await executor.execute(
            sql="SELECT value FROM native_read_only_transaction"
        )
        assert rows == []
    finally:
        await executor.execute(sql="DROP TABLE IF EXISTS native_read_only_transaction")
        await executor.close()


@pytest.mark.asyncio
async def test_cancelling_transaction_start_waiting_for_pool(
    postgres_database_url,
):
    executor = NativeExecutor(
        database_url=postgres_database_url,
        pool_max_size=1,
        pool_wait_timeout_ms=5_000,
    )
    blocking_transaction = await _native.begin_transaction(executor.pool)
    transaction_task = None

    async def start_transaction():
        async with executor.transaction(
            isolation_level="serializable",
            read_only=True,
            deferrable=True,
        ):
            pass

    try:
        transaction_task = asyncio.create_task(start_transaction())
        await asyncio.sleep(0)

        transaction_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await transaction_task

        await _native.rollback_transaction(blocking_transaction)
        blocking_transaction = None

        rows = await asyncio.wait_for(
            executor.execute(sql="SELECT 1::BIGINT"),
            timeout=1,
        )
        assert rows == [[1]]
    finally:
        if transaction_task is not None and not transaction_task.done():
            transaction_task.cancel()
            await asyncio.gather(transaction_task, return_exceptions=True)
        if blocking_transaction is not None:
            await _native.rollback_transaction(blocking_transaction)
        await executor.close()


@pytest.mark.skipif(
    "fork" not in multiprocessing.get_all_start_methods(),
    reason="fork is not supported",
)
@pytest.mark.filterwarnings(
    "ignore:This process .* is multi-threaded.*:DeprecationWarning",
)
def test_transaction_handle_is_rejected_after_fork(postgres_database_url):
    asyncio.run(_native.close_pools())
    pool = _native.create_pool(
        database_url=postgres_database_url,
        max_size=1,
    )
    transaction = asyncio.run(_native.begin_transaction(pool))
    parent_rows = asyncio.run(
        _native.execute_transaction(
            transaction=transaction,
            sql="SELECT pg_backend_pid()",
        )
    )

    context = multiprocessing.get_context("fork")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(
        target=_execute_transaction_in_child,
        args=(transaction, sender),
    )

    try:
        process.start()
        sender.close()
        process.join(timeout=5)

        if process.is_alive():
            process.kill()
            process.join()

        assert process.exitcode == 0
        assert receiver.poll()

        error_type, message = receiver.recv()
        assert error_type == "PostgresInterfaceError"
        assert message == (
            "PostgreSQL transaction handle belongs to a different process"
        )

        current_parent_rows = asyncio.run(
            _native.execute_transaction(
                transaction=transaction,
                sql="SELECT pg_backend_pid()",
            )
        )
        assert current_parent_rows == parent_rows
    finally:
        receiver.close()
        sender.close()
        asyncio.run(_native.rollback_transaction(transaction))
        asyncio.run(_native.close_pools())


@pytest.mark.skipif(
    "fork" not in multiprocessing.get_all_start_methods(),
    reason="fork is not supported",
)
@pytest.mark.filterwarnings(
    "ignore:This process .* is multi-threaded.*:DeprecationWarning",
)
def test_pool_is_recreated_after_fork(postgres_database_url):
    asyncio.run(_native.close_pools())
    pool = _native.create_pool(database_url=postgres_database_url)
    parent_backend_pid = asyncio.run(_get_backend_pid(pool))

    context = multiprocessing.get_context("fork")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(
        target=_get_backend_pid_in_child,
        args=(pool, sender),
    )

    try:
        process.start()
        sender.close()
        process.join(timeout=10)

        if process.is_alive():
            process.kill()
            process.join()

        assert process.exitcode == 0
        assert receiver.poll()

        child_backend_pid = receiver.recv()
        assert child_backend_pid != parent_backend_pid

        current_parent_backend_pid = asyncio.run(_get_backend_pid(pool))
        assert current_parent_backend_pid == parent_backend_pid
    finally:
        receiver.close()
        sender.close()
        asyncio.run(_native.close_pools())
