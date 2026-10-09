from __future__ import annotations

import logging
from asyncio import Task, current_task
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextvars import ContextVar, Token
from types import TracebackType
from typing import Literal

from django.db import (
    DatabaseError,
    DataError,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from django.db.transaction import TransactionManagementError

from django_native_postgres import _native

logger = logging.getLogger("django.db.backends.base")

type TransactionIsolationLevel = Literal[
    "read_uncommitted",
    "read_committed",
    "repeatable_read",
    "serializable",
]
type QueryParameter = str | int | None
type QueryParameters = Sequence[QueryParameter] | Mapping[str, QueryParameter]


_SQLSTATE_EXCEPTION_CLASSES: dict[str, type[DatabaseError]] = {
    "08": OperationalError,
    "0A": NotSupportedError,
    "10": ProgrammingError,
    "20": ProgrammingError,
    "21": ProgrammingError,
    "22": DataError,
    "23": IntegrityError,
    "24": InternalError,
    "25": InternalError,
    "26": ProgrammingError,
    "27": OperationalError,
    "28": OperationalError,
    "2B": InternalError,
    "2D": InternalError,
    "2F": OperationalError,
    "34": ProgrammingError,
    "38": OperationalError,
    "39": OperationalError,
    "3B": OperationalError,
    "3D": ProgrammingError,
    "3F": ProgrammingError,
    "40": OperationalError,
    "42": ProgrammingError,
    "44": ProgrammingError,
    "53": OperationalError,
    "54": OperationalError,
    "55": OperationalError,
    "57": OperationalError,
    "58": OperationalError,
    "F0": OperationalError,
    "HV": OperationalError,
    "P0": ProgrammingError,
    "XX": InternalError,
}


def _django_database_error_class(sqlstate: str) -> type[DatabaseError]:
    return _SQLSTATE_EXCEPTION_CLASSES.get(sqlstate[:2], DatabaseError)


async def _translate_database_error[T](operation: Awaitable[T]) -> T:
    try:
        return await operation
    except _native.PostgresDatabaseError as error:
        error_class = _django_database_error_class(error.sqlstate)
        raise error_class(str(error)) from error
    except _native.PostgresDataError as error:
        raise DataError(str(error)) from error
    except _native.PostgresInterfaceError as error:
        raise InterfaceError(str(error)) from error
    except _native.PostgresNotSupportedError as error:
        raise NotSupportedError(str(error)) from error
    except _native.PostgresOperationalError as error:
        raise OperationalError(str(error)) from error
    except _native.PostgresProgrammingError as error:
        raise ProgrammingError(str(error)) from error


class NativeTransaction:
    def __init__(
        self,
        pool: _native.PoolHandle,
        active_transaction: ContextVar[NativeTransaction | None],
        pending_on_commit_callbacks: ContextVar[
            tuple[tuple[Callable[[], object], bool], ...]
        ],
        *,
        savepoint: bool = True,
        isolation_level: TransactionIsolationLevel | None = None,
        read_only: bool | None = None,
        deferrable: bool | None = None,
    ):
        self._pool = pool
        self._active_transaction = active_transaction
        self._pending_on_commit_callbacks = pending_on_commit_callbacks
        self._handle: _native.TransactionHandle | None = None
        self._token: Token | None = None
        self._used = False
        self._root: NativeTransaction = self
        self._parent: NativeTransaction | None = None
        self._nested = False
        self._savepoint = savepoint
        self._savepoint_name: str | None = None
        self._savepoint_number = 0
        self._owner_task: Task[object] | None = None
        self._rollback_only = False
        self._on_commit_callbacks: list[tuple[Callable[[], object], bool]] = []
        self._isolation_level = isolation_level
        self._read_only = read_only
        self._deferrable = deferrable

    def _mark_for_rollback(self) -> None:
        self._rollback_only = True
        if self._nested and not self._savepoint:
            self._root._rollback_only = True

    @property
    def needs_rollback(self) -> bool:
        return self._rollback_only or self._root._rollback_only

    def set_rollback(self, rollback: bool) -> None:
        self._validate_task()
        if rollback:
            self._mark_for_rollback()
            return
        self._rollback_only = False
        if not self._nested or not self._savepoint:
            self._root._rollback_only = False

    def on_commit(
        self,
        func: Callable[[], object],
        robust: bool = False,
    ) -> None:
        self._validate_task()
        if not callable(func):
            raise TypeError("on_commit()'s callback must be a callable.")
        self._on_commit_callbacks.append((func, robust))

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
            self._handle = await _translate_database_error(
                _native.begin_transaction(
                    self._pool,
                    isolation_level=self._isolation_level,
                    read_only=self._read_only,
                    deferrable=self._deferrable,
                )
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
            self._parent = active_transaction
            self._nested = True

            if self._savepoint:
                self._root._savepoint_number += 1
                self._savepoint_name = (
                    f"django_native_postgres_savepoint_{self._root._savepoint_number}"
                )

                try:
                    await _translate_database_error(
                        _native.execute_transaction(
                            transaction=handle,
                            sql=f"SAVEPOINT {self._savepoint_name}",
                            params=None,
                        )
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
        params: QueryParameters | None = None,
    ) -> list[list[str | int | None]]:
        handle = self._handle

        if handle is None:
            raise RuntimeError("Native transaction is not active")

        self._validate_usable()

        try:
            return await _translate_database_error(
                _native.execute_transaction(
                    transaction=handle,
                    sql=sql,
                    params=params,
                )
            )
        except BaseException:
            self._mark_for_rollback()
            raise

    async def execute_result(
        self,
        sql: str,
        params: QueryParameters | None = None,
    ) -> tuple[list[list[str | int | None]], int]:
        handle = self._handle

        if handle is None:
            raise RuntimeError("Native transaction is not active")

        self._validate_usable()

        try:
            return await _translate_database_error(
                _native.execute_transaction_result(
                    transaction=handle,
                    sql=sql,
                    params=params,
                )
            )
        except BaseException:
            self._mark_for_rollback()
            raise

    async def execute_with_metadata(
        self,
        sql: str,
        params: QueryParameters | None = None,
    ) -> tuple[list[list[str | int | None]], int, list[str]]:
        handle = self._handle

        if handle is None:
            raise RuntimeError("Native transaction is not active")

        self._validate_usable()

        try:
            return await _translate_database_error(
                _native.execute_transaction_with_metadata(
                    transaction=handle,
                    sql=sql,
                    params=params,
                )
            )
        except BaseException:
            self._mark_for_rollback()
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

        if (
            self._nested
            and self._savepoint_name is None
            and (exception_type is not None or self._rollback_only)
        ):
            self._root._rollback_only = True

        should_rollback = (
            exception_type is not None
            or self._rollback_only
            or self._root._rollback_only
        )

        self._handle = None
        self._token = None
        self._savepoint_name = None
        committed = False

        try:
            if self._nested and savepoint_name is None:
                committed = not should_rollback
            elif savepoint_name is None:
                if should_rollback:
                    await _translate_database_error(
                        _native.rollback_transaction(handle)
                    )
                else:
                    await _translate_database_error(_native.commit_transaction(handle))
                    committed = True
            else:
                try:
                    if should_rollback:
                        await _translate_database_error(
                            _native.execute_transaction(
                                transaction=handle,
                                sql=f"ROLLBACK TO SAVEPOINT {savepoint_name}",
                                params=None,
                            )
                        )

                    await _translate_database_error(
                        _native.execute_transaction(
                            transaction=handle,
                            sql=f"RELEASE SAVEPOINT {savepoint_name}",
                            params=None,
                        )
                    )
                    committed = not should_rollback
                except BaseException:
                    self._root._rollback_only = True
                    raise
        finally:
            self._active_transaction.reset(token)

            if not self._nested:
                self._owner_task = None

        if committed:
            if self._parent is None:
                self._pending_on_commit_callbacks.set(tuple(self._on_commit_callbacks))
                self._on_commit_callbacks = []
            else:
                self._parent._on_commit_callbacks.extend(self._on_commit_callbacks)
                self._on_commit_callbacks = []


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
        self._pending_on_commit_callbacks: ContextVar[
            tuple[tuple[Callable[[], object], bool], ...]
        ] = ContextVar(
            "django_native_postgres_pending_on_commit_callbacks",
            default=(),
        )

    @property
    def in_transaction(self) -> bool:
        return self._active_transaction.get() is not None

    @property
    def needs_rollback(self) -> bool:
        transaction = self._active_transaction.get()
        if transaction is None:
            raise TransactionManagementError(
                "The rollback flag doesn't work outside of an 'atomic' block."
            )
        return transaction.needs_rollback

    def set_rollback(self, rollback: bool) -> None:
        transaction = self._active_transaction.get()
        if transaction is None:
            raise TransactionManagementError(
                "The rollback flag doesn't work outside of an 'atomic' block."
            )
        transaction.set_rollback(rollback)

    def on_commit(
        self,
        func: Callable[[], object],
        robust: bool = False,
    ) -> None:
        transaction = self._active_transaction.get()
        if transaction is None:
            if not callable(func):
                raise TypeError("on_commit()'s callback must be a callable.")
            if robust:
                try:
                    func()
                except Exception as error:
                    name = getattr(func, "__qualname__", func)
                    logger.exception(
                        "Error calling %s in on_commit() (%s).",
                        name,
                        error,
                    )
            else:
                func()
            return
        transaction.on_commit(func, robust)

    def run_on_commit_callbacks(self) -> None:
        callbacks = self._pending_on_commit_callbacks.get()
        self._pending_on_commit_callbacks.set(())
        for func, robust in callbacks:
            if robust:
                try:
                    func()
                except Exception as error:
                    name = getattr(func, "__qualname__", func)
                    logger.exception(
                        "Error calling %s in on_commit() during transaction (%s).",
                        name,
                        error,
                    )
            else:
                func()

    async def execute(
        self,
        sql: str,
        params: QueryParameters | None = None,
    ) -> list[list[str | int | None]]:
        transaction = self._active_transaction.get()

        if transaction is not None:
            return await transaction.execute(sql=sql, params=params)

        return await _translate_database_error(
            _native.execute(
                pool=self.pool,
                sql=sql,
                params=params,
            )
        )

    async def execute_result(
        self,
        sql: str,
        params: QueryParameters | None = None,
    ) -> tuple[list[list[str | int | None]], int]:
        transaction = self._active_transaction.get()

        if transaction is not None:
            return await transaction.execute_result(sql=sql, params=params)

        return await _translate_database_error(
            _native.execute_result(
                pool=self.pool,
                sql=sql,
                params=params,
            )
        )

    async def execute_with_metadata(
        self,
        sql: str,
        params: QueryParameters | None = None,
    ) -> tuple[list[list[str | int | None]], int, list[str]]:
        transaction = self._active_transaction.get()

        if transaction is not None:
            return await transaction.execute_with_metadata(sql=sql, params=params)

        return await _translate_database_error(
            _native.execute_with_metadata(
                pool=self.pool,
                sql=sql,
                params=params,
            )
        )

    async def close(self) -> None:
        await _translate_database_error(_native.close_pool(self.pool))

    def transaction(
        self,
        *,
        savepoint: bool = True,
        isolation_level: TransactionIsolationLevel | None = None,
        read_only: bool | None = None,
        deferrable: bool | None = None,
    ) -> NativeTransaction:
        if not isinstance(savepoint, bool):
            raise TypeError("savepoint must be a bool")
        if isolation_level is not None and not isinstance(isolation_level, str):
            raise TypeError("isolation_level must be a string or None")
        if read_only is not None and not isinstance(read_only, bool):
            raise TypeError("read_only must be a bool or None")
        if deferrable is not None and not isinstance(deferrable, bool):
            raise TypeError("deferrable must be a bool or None")

        return NativeTransaction(
            self.pool,
            self._active_transaction,
            self._pending_on_commit_callbacks,
            savepoint=savepoint,
            isolation_level=isolation_level,
            read_only=read_only,
            deferrable=deferrable,
        )
