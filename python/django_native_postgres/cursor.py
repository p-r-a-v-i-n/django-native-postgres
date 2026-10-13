from __future__ import annotations

from collections.abc import Mapping, Sequence
from types import TracebackType
from typing import Protocol

type QueryParameter = str | int | None
type QueryParameters = Sequence[QueryParameter] | Mapping[str, QueryParameter]
type QueryRow = list[str | int | None]
type CursorDescription = tuple[str, None, None, None, None, None, None]

_FETCH_ALL_CHUNK_SIZE = 2000


class AsyncQueryStream(Protocol):
    rows_affected: int | None
    columns: list[str]

    async def fetchmany(self, size: int) -> list[QueryRow]: ...

    async def close(self) -> None: ...


class AsyncConnection(Protocol):
    async def aopen_query_stream(
        self,
        sql: str,
        params: QueryParameters | None = None,
    ) -> AsyncQueryStream: ...


class NativeAsyncCursor:
    def __init__(self, connection: AsyncConnection):
        self.connection = connection
        self._stream: AsyncQueryStream | None = None
        self.rowcount = -1
        self.description: list[CursorDescription] | None = None

    async def __aenter__(self) -> NativeAsyncCursor:
        await self._close_stream()
        self.rowcount = -1
        self.description = None
        return self

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            await self._close_stream()
        finally:
            self.rowcount = -1
            self.description = None

    async def _close_stream(self) -> None:
        stream = self._stream
        self._stream = None
        if stream is not None:
            await stream.close()

    async def aexecute(
        self,
        sql: str,
        params: QueryParameters | None = None,
    ) -> None:
        await self._close_stream()
        self.rowcount = -1
        self.description = None

        stream = await self.connection.aopen_query_stream(sql, params)
        self._stream = stream
        if stream.rows_affected is not None:
            self.rowcount = stream.rows_affected
        self.description = [
            (column, None, None, None, None, None, None) for column in stream.columns
        ]

    async def afetchone(self) -> QueryRow | None:
        if self._stream is None:
            raise RuntimeError("No active query result on this cursor")

        rows = await self._stream.fetchmany(1)
        return rows[0] if rows else None

    async def afetchmany(self, size: int = 1) -> list[QueryRow]:
        if self._stream is None:
            raise RuntimeError("No active query result on this cursor")

        return await self._stream.fetchmany(size)

    async def afetchall(self) -> list[QueryRow]:
        if self._stream is None:
            raise RuntimeError("No active query result on this cursor")

        rows = []
        while batch := await self._stream.fetchmany(_FETCH_ALL_CHUNK_SIZE):
            rows.extend(batch)
        return rows
