import asyncio
import multiprocessing
import os

import pytest
from django_native_postgres import _native


async def _get_backend_pid(database_url):
    rows = await asyncio.wait_for(
        _native.execute(
            database_url=database_url,
            sql="SELECT pg_backend_pid()",
        ),
        timeout=5,
    )
    return rows[0][0]


def _get_backend_pid_in_child(database_url, connection):
    backend_pid = asyncio.run(_get_backend_pid(database_url))
    connection.send(backend_pid)
    connection.close()


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
async def test_execute_reuses_postgres_connection(postgres_database_url):
    first_rows = await _native.execute(
        database_url=postgres_database_url,
        sql="SELECT pg_backend_pid()",
    )
    second_rows = await _native.execute(
        database_url=postgres_database_url,
        sql="SELECT pg_backend_pid()",
    )

    assert second_rows == first_rows


@pytest.mark.asyncio
async def test_close_pools_releases_postgres_connection(postgres_database_url):
    first_rows = await _native.execute(
        database_url=postgres_database_url,
        sql="SELECT pg_backend_pid()",
    )

    await _native.close_pools()

    second_rows = await _native.execute(
        database_url=postgres_database_url,
        sql="SELECT pg_backend_pid()",
    )

    assert second_rows != first_rows


@pytest.mark.asyncio
async def test_pool_replaces_broken_postgres_connection(postgres_database_url):
    await _native.close_pools()

    try:
        first_rows = await _native.execute(
            database_url=postgres_database_url,
            sql="SELECT pg_backend_pid()",
        )

        with pytest.raises(RuntimeError, match="PostgreSQL query failed"):
            await _native.execute(
                database_url=postgres_database_url,
                sql="SELECT pg_terminate_backend(pg_backend_pid())",
            )

        second_rows = await _native.execute(
            database_url=postgres_database_url,
            sql="SELECT pg_backend_pid()",
        )

        assert second_rows != first_rows
    finally:
        await _native.close_pools()


@pytest.mark.asyncio
async def test_pool_keeps_connection_after_safe_query_error(postgres_database_url):
    await _native.close_pools()

    try:
        first_rows = await _native.execute(
            database_url=postgres_database_url,
            sql="SELECT pg_backend_pid()",
        )

        with pytest.raises(RuntimeError, match="PostgreSQL query failed"):
            await _native.execute(
                database_url=postgres_database_url,
                sql="SELECT FROM",
            )

        second_rows = await _native.execute(
            database_url=postgres_database_url,
            sql="SELECT pg_backend_pid()",
        )

        assert second_rows == first_rows
    finally:
        await _native.close_pools()


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


@pytest.mark.asyncio
async def test_execute_accepts_django_text_parameter(postgres_database_url):
    rows = await _native.execute(
        database_url=postgres_database_url,
        sql="SELECT %s::TEXT",
        params=("Django",),
    )

    assert rows == [["Django"]]


@pytest.mark.asyncio
async def test_execute_accepts_multiple_django_text_parameters(
    postgres_database_url,
):
    rows = await _native.execute(
        database_url=postgres_database_url,
        sql="SELECT %s::TEXT, %s::TEXT",
        params=("Django", "Rust"),
    )

    assert rows == [["Django", "Rust"]]


@pytest.mark.asyncio
async def test_execute_rejects_parameter_count_mismatch(
    postgres_database_url,
):
    with pytest.raises(
        RuntimeError,
        match="SQL contains 1 placeholders but received 2 parameters",
    ):
        await _native.execute(
            database_url=postgres_database_url,
            sql="SELECT %s::TEXT",
            params=("Django", "Rust"),
        )


@pytest.mark.asyncio
async def test_execute_accepts_integer_parameter(postgres_database_url):
    rows = await _native.execute(
        database_url=postgres_database_url,
        sql="SELECT %s::BIGINT",
        params=(42,),
    )
    assert rows == [[42]]


@pytest.mark.asyncio
async def test_execute_assigns_type_to_untyped_integer_parameter(
    postgres_database_url,
):
    rows = await _native.execute(
        database_url=postgres_database_url,
        sql="SELECT %s",
        params=(42,),
    )

    assert rows == [[42]]


@pytest.mark.asyncio
async def test_execute_limits_pool_connections(postgres_database_url):
    await _native.close_pools()

    pool_max_size = 2
    query_count = pool_max_size * 3

    queries = [
        _native.execute(
            database_url=postgres_database_url,
            sql="SELECT pg_backend_pid() FROM pg_sleep(0.05)",
            pool_max_size=pool_max_size,
        )
        for _ in range(query_count)
    ]

    results = await asyncio.gather(*queries)
    backend_pids = {rows[0][0] for rows in results}
    assert len(backend_pids) == pool_max_size


@pytest.mark.asyncio
async def test_execute_rejects_zero_pool_max_size(postgres_database_url):
    await _native.close_pools()

    try:
        with pytest.raises(
            RuntimeError,
            match="PostgreSQL pool max size must be greater than zero",
        ):
            await asyncio.wait_for(
                _native.execute(
                    database_url=postgres_database_url,
                    sql="SELECT 1",
                    pool_max_size=0,
                ),
                timeout=0.5,
            )
    finally:
        await _native.close_pools()


@pytest.mark.asyncio
async def test_execute_times_out_when_pool_is_exhausted(postgres_database_url):
    await _native.close_pools()

    try:
        first_query = _native.execute(
            database_url=postgres_database_url,
            sql="SELECT pg_backend_pid() FROM pg_sleep(0.2)",
            pool_max_size=1,
            pool_wait_timeout_ms=50,
        )
        second_query = _native.execute(
            database_url=postgres_database_url,
            sql="SELECT pg_backend_pid() FROM pg_sleep(0.2)",
            pool_max_size=1,
            pool_wait_timeout_ms=50,
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


@pytest.mark.skipif(
    "fork" not in multiprocessing.get_all_start_methods(),
    reason="fork is not supported",
)
@pytest.mark.filterwarnings(
    "ignore:This process .* is multi-threaded.*:DeprecationWarning",
)
def test_pool_is_recreated_after_fork(postgres_database_url):
    asyncio.run(_native.close_pools())
    parent_backend_pid = asyncio.run(_get_backend_pid(postgres_database_url))

    context = multiprocessing.get_context("fork")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(
        target=_get_backend_pid_in_child,
        args=(postgres_database_url, sender),
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

        current_parent_backend_pid = asyncio.run(
            _get_backend_pid(postgres_database_url)
        )
        assert current_parent_backend_pid == parent_backend_pid
    finally:
        receiver.close()
        sender.close()
        asyncio.run(_native.close_pools())
