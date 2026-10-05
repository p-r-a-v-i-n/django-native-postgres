from __future__ import annotations

from collections.abc import Sequence
from types import TracebackType
from typing import Protocol

type QueryParameter = str | int | None
type QueryRow = list[str | int | None]
type CursorDescription = tuple[str, None, None, None, None, None, None]


class AsyncConnection(Protocol):
    async def aexecute_with_metadata(
        self,
        sql: str,
        params: Sequence[QueryParameter] | None = None,
    ) -> tuple[list[QueryRow], int, list[str]]: ...


class NativeAsyncCursor:
    def __init__(self, connection: AsyncConnection):
        self.connection = connection
        self._rows: list[QueryRow] | None = None
        self._position = 0
        self.rowcount = -1
        self.description: list[CursorDescription] | None = None

    async def __aenter__(self) -> NativeAsyncCursor:
        self._rows = None
        self._position = 0
        self.rowcount = -1
        self.description = None
        return self

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._rows = None
        self._position = 0
        self.rowcount = -1
        self.description = None

    async def aexecute(
        self,
        sql: str,
        params: Sequence[QueryParameter] | None = None,
    ) -> None:
        self._rows = None
        self._position = 0
        self.rowcount = -1
        self.description = None
        (
            self._rows,
            self.rowcount,
            columns,
        ) = await self.connection.aexecute_with_metadata(sql, params)
        self.description = [
            (column, None, None, None, None, None, None) for column in columns
        ]

    async def afetchone(self) -> QueryRow | None:
        if self._rows is None:
            raise RuntimeError("No active query result on this cursor")

        if self._position >= len(self._rows):
            return None

        row = self._rows[self._position]
        self._position += 1
        return row

    async def afetchmany(self, size: int = 1) -> list[QueryRow]:
        if self._rows is None:
            raise RuntimeError("No active query result on this cursor")

        rows = self._rows[self._position : self._position + size]
        self._position += len(rows)
        return rows

    async def afetchall(self) -> list[QueryRow]:
        if self._rows is None:
            raise RuntimeError("No active query result on this cursor")

        rows = self._rows[self._position :]
        self._position = len(self._rows)
        return rows
