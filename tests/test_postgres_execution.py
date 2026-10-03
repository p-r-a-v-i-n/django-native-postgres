import asyncio
import multiprocessing
import os

import pytest
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

            with pytest.raises(RuntimeError, match="PostgreSQL query failed"):
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
                with pytest.raises(RuntimeError, match="PostgreSQL query failed"):
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
        assert error_type == "RuntimeError"
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
