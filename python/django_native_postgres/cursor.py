from __future__ import annotations

from collections.abc import Sequence
from types import TracebackType
from typing import Protocol

type QueryParameter = str | int
type QueryRow = list[str | int | None]


class AsyncConnection(Protocol):
    async def aexecute(
        self,
        sql: str,
        params: Sequence[QueryParameter] | None = None,
    ) -> list[QueryRow]: ...


class NativeAsyncCursor:
    def __init__(self, connection: AsyncConnection):
        self.connection = connection
        self._rows: list[QueryRow] | None = None
        self._position = 0

    async def __aenter__(self) -> NativeAsyncCursor:
        self._rows = None
        self._position = 0
        return self

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._rows = None
        self._position = 0

    async def aexecute(
        self,
        sql: str,
        params: Sequence[QueryParameter] | None = None,
    ) -> None:
        self._rows = None
        self._position = 0
        self._rows = await self.connection.aexecute(sql, params)

    async def afetchone(self) -> QueryRow | None:
        if self._rows is None:
            raise RuntimeError("No active query result on this cursor")

        if self._position >= len(self._rows):
            return None

        row = self._rows[self._position]
        self._position += 1
        return row
