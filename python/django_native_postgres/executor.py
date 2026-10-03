from __future__ import annotations

from collections.abc import Sequence
from contextvars import ContextVar, Token
from types import TracebackType

from django_native_postgres import _native


class NativeTransaction:
    def __init__(
        self,
        pool: _native.PoolHandle,
        active_transaction: ContextVar[NativeTransaction | None],
    ):
        self._pool = pool
        self._active_transaction = active_transaction
        self._handle: _native.TransactionHandle | None = None
        self._token: Token | None = None
        self._used = False

    async def __aenter__(self) -> NativeTransaction:
        if self._used:
            raise RuntimeError("Native transaction context cannot be reused")

        if self._active_transaction.get() is not None:
            raise RuntimeError("Nested native transactions are not supported")

        self._used = True
        self._handle = await _native.begin_transaction(self._pool)
        self._token = self._active_transaction.set(self)
        return self

    async def execute(
        self,
        sql: str,
        params: Sequence[str | int] | None = None,
    ) -> list[list[str | int | None]]:
        handle = self._handle

        if handle is None:
            raise RuntimeError("Native transaction is not active")

        return await _native.execute_transaction(
            transaction=handle,
            sql=sql,
            params=params,
        )

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        handle = self._handle
        token = self._token

        if handle is None or token is None:
            raise RuntimeError("Native transaction is not active")

        self._handle = None
        self._token = None

        try:
            if exception_type is None:
                await _native.commit_transaction(handle)
            else:
                await _native.rollback_transaction(handle)
        finally:
            self._active_transaction.reset(token)


class NativeExecutor:
    def __init__(
        self,
        database_url: str,
        pool_max_size: int = 16,
        pool_wait_timeout_ms: int = 30_000,
    ):
        self.pool = _native.create_pool(
            database_url=database_url,
            max_size=pool_max_size,
            wait_timeout_ms=pool_wait_timeout_ms,
        )
        self._active_transaction: ContextVar[NativeTransaction | None] = ContextVar(
            "django_native_postgres_active_transaction",
            default=None,
        )

    async def execute(
        self,
        sql: str,
        params: Sequence[str | int] | None = None,
    ) -> list[list[str | int | None]]:
        transaction = self._active_transaction.get()

        if transaction is not None:
            return await transaction.execute(sql=sql, params=params)

        return await _native.execute(
            pool=self.pool,
            sql=sql,
            params=params,
        )

    async def close(self) -> None:
        await _native.close_pool(self.pool)

    def transaction(self) -> NativeTransaction:
        return NativeTransaction(
            self.pool,
            self._active_transaction,
        )
