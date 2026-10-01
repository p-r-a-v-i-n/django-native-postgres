from collections.abc import Sequence

from django_native_postgres import _native


class NativeExecutor:
    def __init__(self, database_url: str):
        self.database_url = database_url

    async def execute(
        self,
        sql: str,
        params: Sequence[str | int] | None = None,
    ) -> list[list[str | int | None]]:
        return await _native.execute(
            database_url=self.database_url, sql=sql, params=params
        )
