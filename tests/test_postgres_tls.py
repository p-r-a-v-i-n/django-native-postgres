import asyncio
import os

import pytest
from django_native_postgres import _native


@pytest.fixture
def tls_database_url() -> str:
    database_url = os.environ.get("DNP_TLS_DATABASE_URL")
    if database_url is None:
        pytest.skip("DNP_TLS_DATABASE_URL is not configured")
    return database_url


@pytest.fixture
def tls_root_cert() -> str:
    root_cert = os.environ.get("DNP_TLS_ROOT_CERT")
    if root_cert is None:
        pytest.skip("DNP_TLS_ROOT_CERT is not configured")
    return root_cert


@pytest.fixture
def tls_untrusted_root_cert() -> str:
    root_cert = os.environ.get("DNP_TLS_UNTRUSTED_ROOT_CERT")
    if root_cert is None:
        pytest.skip("DNP_TLS_UNTRUSTED_ROOT_CERT is not configured")
    return root_cert


def create_tls_pool(database_url, root_cert, **options):
    return _native.create_pool(
        database_url=database_url,
        ssl_mode="require",
        ssl_root_cert=root_cert,
        **options,
    )


@pytest.mark.asyncio
async def test_tls_connection_uses_configured_root_certificate(
    tls_database_url,
    tls_root_cert,
):
    pool = create_tls_pool(tls_database_url, tls_root_cert)

    try:
        rows = await _native.execute(
            pool=pool,
            sql="SELECT ssl::TEXT FROM pg_stat_ssl WHERE pid = pg_backend_pid()",
        )

        assert rows == [["true"]]
    finally:
        await _native.close_pool(pool)


@pytest.mark.asyncio
async def test_tls_connection_rejects_untrusted_root_certificate(
    tls_database_url,
    tls_untrusted_root_cert,
):
    pool = create_tls_pool(tls_database_url, tls_untrusted_root_cert)

    try:
        with pytest.raises(_native.PostgresOperationalError):
            await _native.execute(pool=pool, sql="SELECT 1::BIGINT")
    finally:
        await _native.close_pool(pool)


@pytest.mark.asyncio
async def test_tls_connection_rejects_hostname_mismatch(
    tls_database_url,
    tls_root_cert,
):
    mismatched_database_url = tls_database_url.replace(
        "@localhost:",
        "@127.0.0.1:",
        1,
    )
    pool = create_tls_pool(mismatched_database_url, tls_root_cert)

    try:
        with pytest.raises(_native.PostgresOperationalError):
            await _native.execute(pool=pool, sql="SELECT 1::BIGINT")
    finally:
        await _native.close_pool(pool)


async def wait_until_query_is_blocked(pool, backend_pid):
    event_loop = asyncio.get_running_loop()
    deadline = event_loop.time() + 2

    while event_loop.time() < deadline:
        rows = await _native.execute(
            pool=pool,
            sql="""
                SELECT COUNT(*)::BIGINT
                FROM pg_stat_activity
                WHERE pid = %s AND wait_event_type = 'Lock'
            """,
            params=(backend_pid,),
        )
        if rows == [[1]]:
            return
        await asyncio.sleep(0.01)

    pytest.fail("TLS query did not reach PostgreSQL")


@pytest.mark.asyncio
async def test_cancelling_tls_query_reuses_pool_connection(
    tls_database_url,
    tls_root_cert,
):
    target_pool = create_tls_pool(
        tls_database_url,
        tls_root_cert,
        max_size=1,
        wait_timeout_ms=1000,
    )
    observer_pool = create_tls_pool(tls_database_url, tls_root_cert)
    lock_id = os.getpid()
    query = None

    try:
        await _native.execute(
            pool=observer_pool,
            sql="SELECT 1::BIGINT FROM pg_advisory_lock(%s)",
            params=(lock_id,),
        )
        rows = await _native.execute(
            pool=target_pool,
            sql="SELECT pg_backend_pid()",
        )
        backend_pid = rows[0][0]

        query = asyncio.create_task(
            _native.execute(
                pool=target_pool,
                sql="SELECT 1::BIGINT FROM pg_advisory_lock(%s)",
                params=(lock_id,),
            )
        )
        await wait_until_query_is_blocked(observer_pool, backend_pid)

        query.cancel()
        with pytest.raises(asyncio.CancelledError):
            await query

        rows = await _native.execute(
            pool=target_pool,
            sql="""
                SELECT pg_backend_pid(), ssl::TEXT
                FROM pg_stat_ssl
                WHERE pid = pg_backend_pid()
            """,
        )
        assert rows == [[backend_pid, "true"]]
    finally:
        await _native.execute(
            pool=observer_pool,
            sql="SELECT 1::BIGINT FROM pg_advisory_unlock(%s)",
            params=(lock_id,),
        )
        if query is not None and not query.done():
            query.cancel()
            await asyncio.gather(query, return_exceptions=True)
        await _native.close_pool(target_pool)
        await _native.close_pool(observer_pool)
