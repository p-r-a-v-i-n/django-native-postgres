from collections.abc import Sequence

from django_native_postgres import _native


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

    async def execute(
        self,
        sql: str,
        params: Sequence[str | int] | None = None,
    ) -> list[list[str | int | None]]:
        return await _native.execute(
            pool=self.pool,
            sql=sql,
            params=params,
        )
