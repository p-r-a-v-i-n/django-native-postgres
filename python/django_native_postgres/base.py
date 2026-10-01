from django.core.exceptions import ImproperlyConfigured
from django.db.backends.postgresql.base import (
    DatabaseWrapper as PostgreSQLDatabaseWrapper,
)
from psycopg.conninfo import make_conninfo

from django_native_postgres.executor import NativeExecutor


class DatabaseWrapper(PostgreSQLDatabaseWrapper):
    display_name = "PostgreSQL (native async)"

    def get_async_executor(self):
        executor = getattr(self, "_native_executor", None)

        if executor is None:
            settings = self.settings_dict
            pool_options = settings.get("OPTIONS", {}).get("native_pool", {})
            pool_max_size = pool_options.get("max_size", 16)
            pool_wait_timeout_ms = pool_options.get("wait_timeout_ms", 30_000)
            database_url = make_conninfo(
                dbname=settings.get("NAME") or "postgres",
                user=settings.get("USER") or None,
                password=settings.get("PASSWORD") or None,
                host=settings.get("HOST") or None,
                port=settings.get("PORT") or None,
            )

            if (
                isinstance(pool_max_size, bool)
                or not isinstance(pool_max_size, int)
                or pool_max_size <= 0
            ):
                raise ImproperlyConfigured(
                    "native_pool.max_size must be greater than zero"
                )

            if (
                isinstance(pool_wait_timeout_ms, bool)
                or not isinstance(pool_wait_timeout_ms, int)
                or pool_wait_timeout_ms <= 0
            ):
                raise ImproperlyConfigured(
                    "native_pool.wait_timeout_ms must be a positive integer"
                )

            executor = NativeExecutor(
                database_url=database_url,
                pool_max_size=pool_max_size,
                pool_wait_timeout_ms=pool_wait_timeout_ms,
            )
            self._native_executor = executor
        return executor

    async def aexecute(self, sql, params=None):
        executor = self.get_async_executor()
        return await executor.execute(sql=sql, params=params)

    def get_connection_params(self):
        connection_params = super().get_connection_params()
        connection_params.pop("native_pool", None)
        return connection_params
