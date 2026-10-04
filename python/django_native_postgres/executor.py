from __future__ import annotations

from asyncio import Task, current_task
from collections.abc import Sequence
from contextvars import ContextVar, Token
from types import TracebackType
from typing import Literal

from django_native_postgres import _native

type TransactionIsolationLevel = Literal[
    "read_uncommitted",
    "read_committed",
    "repeatable_read",
    "serializable",
]


class NativeTransaction:
    def __init__(
        self,
        pool: _native.PoolHandle,
        active_transaction: ContextVar[NativeTransaction | None],
        *,
        isolation_level: TransactionIsolationLevel | None = None,
        read_only: bool | None = None,
        deferrable: bool | None = None,
    ):
        self._pool = pool
        self._active_transaction = active_transaction
        self._handle: _native.TransactionHandle | None = None
        self._token: Token | None = None
        self._used = False
        self._root: NativeTransaction = self
        self._savepoint_name: str | None = None
        self._savepoint_number = 0
        self._owner_task: Task[object] | None = None
        self._rollback_only = False
        self._isolation_level = isolation_level
        self._read_only = read_only
        self._deferrable = deferrable

    def _validate_task(self) -> None:
        if self._root._owner_task is not current_task():
            raise RuntimeError(
                "Native transaction cannot be used from a different asyncio task"
            )

    def _validate_usable(self) -> None:
        self._validate_task()

        if self._rollback_only or self._root._rollback_only:
            raise RuntimeError("Native transaction is marked for rollback")

    async def __aenter__(self) -> NativeTransaction:
        if self._used:
            raise RuntimeError("Native transaction context cannot be reused")

        owner_task = current_task()

        if owner_task is None:
            raise RuntimeError("Native transaction requires an asyncio task")

        self._used = True
        active_transaction = self._active_transaction.get()

        if active_transaction is None:
            self._handle = await _native.begin_transaction(
                self._pool,
                isolation_level=self._isolation_level,
                read_only=self._read_only,
                deferrable=self._deferrable,
            )
            self._owner_task = owner_task
        else:
            handle = active_transaction._handle

            if handle is None:
                raise RuntimeError("Native transaction is not active")

            active_transaction._validate_usable()

            if any(
                option is not None
                for option in (
                    self._isolation_level,
                    self._read_only,
                    self._deferrable,
                )
            ):
                raise ValueError(
                    "transaction options can only be used on the outermost transaction"
                )

            self._root = active_transaction._root
            self._root._savepoint_number += 1
            self._savepoint_name = (
                f"django_native_postgres_savepoint_{self._root._savepoint_number}"
            )

            try:
                await _native.execute_transaction(
                    transaction=handle,
                    sql=f"SAVEPOINT {self._savepoint_name}",
                    params=None,
                )
            except BaseException:
                self._root._rollback_only = True
                raise

            self._handle = handle

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

        self._validate_usable()

        try:
            return await _native.execute_transaction(
                transaction=handle,
                sql=sql,
                params=params,
            )
        except BaseException:
            self._rollback_only = True
            raise

    async def execute_result(
        self,
        sql: str,
        params: Sequence[str | int] | None = None,
    ) -> tuple[list[list[str | int | None]], int]:
        handle = self._handle

        if handle is None:
            raise RuntimeError("Native transaction is not active")

        self._validate_usable()

        try:
            return await _native.execute_transaction_result(
                transaction=handle,
                sql=sql,
                params=params,
            )
        except BaseException:
            self._rollback_only = True
            raise

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        handle = self._handle
        token = self._token
        savepoint_name = self._savepoint_name

        if handle is None or token is None:
            raise RuntimeError("Native transaction is not active")

        self._validate_task()

        should_rollback = (
            exception_type is not None
            or self._rollback_only
            or self._root._rollback_only
        )

        self._handle = None
        self._token = None
        self._savepoint_name = None

        try:
            if savepoint_name is None:
                if should_rollback:
                    await _native.rollback_transaction(handle)
                else:
                    await _native.commit_transaction(handle)
            else:
                try:
                    if should_rollback:
                        await _native.execute_transaction(
                            transaction=handle,
                            sql=f"ROLLBACK TO SAVEPOINT {savepoint_name}",
                            params=None,
                        )

                    await _native.execute_transaction(
                        transaction=handle,
                        sql=f"RELEASE SAVEPOINT {savepoint_name}",
                        params=None,
                    )
                except BaseException:
                    self._root._rollback_only = True
                    raise
        finally:
            self._active_transaction.reset(token)

            if savepoint_name is None:
                self._owner_task = None


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

    async def execute_result(
        self,
        sql: str,
        params: Sequence[str | int] | None = None,
    ) -> tuple[list[list[str | int | None]], int]:
        transaction = self._active_transaction.get()

        if transaction is not None:
            return await transaction.execute_result(sql=sql, params=params)

        return await _native.execute_result(
            pool=self.pool,
            sql=sql,
            params=params,
        )

    async def close(self) -> None:
        await _native.close_pool(self.pool)

    def transaction(
        self,
        *,
        isolation_level: TransactionIsolationLevel | None = None,
        read_only: bool | None = None,
        deferrable: bool | None = None,
    ) -> NativeTransaction:
        if isolation_level is not None and not isinstance(isolation_level, str):
            raise TypeError("isolation_level must be a string or None")
        if read_only is not None and not isinstance(read_only, bool):
            raise TypeError("read_only must be a bool or None")
        if deferrable is not None and not isinstance(deferrable, bool):
            raise TypeError("deferrable must be a bool or None")

        return NativeTransaction(
            self.pool,
            self._active_transaction,
            isolation_level=isolation_level,
            read_only=read_only,
            deferrable=deferrable,
        )
