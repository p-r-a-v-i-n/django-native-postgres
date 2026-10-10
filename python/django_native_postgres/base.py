from django.core.exceptions import ImproperlyConfigured
from django.db.backends.postgresql.base import (
    DatabaseWrapper as PostgreSQLDatabaseWrapper,
)
from psycopg.conninfo import make_conninfo

from django_native_postgres.cursor import NativeAsyncCursor
from django_native_postgres.django_compatibility import require_native_async_django
from django_native_postgres.executor import NativeExecutor
from django_native_postgres.features import DatabaseFeatures
from django_native_postgres.operations import DatabaseOperations

require_native_async_django()


class DatabaseWrapper(PostgreSQLDatabaseWrapper):
    display_name = "PostgreSQL (native async)"
    features_class = DatabaseFeatures
    ops_class = DatabaseOperations

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

    async def aexecute_result(self, sql, params=None):
        executor = self.get_async_executor()
        return await executor.execute_result(sql=sql, params=params)

    async def aexecute_with_metadata(self, sql, params=None):
        executor = self.get_async_executor()
        return await executor.execute_with_metadata(sql=sql, params=params)

    def acursor(self):
        return NativeAsyncCursor(self)

    def atransaction(
        self,
        *,
        savepoint=True,
        isolation_level=None,
        read_only=None,
        deferrable=None,
    ):
        return self.get_async_executor().transaction(
            savepoint=savepoint,
            isolation_level=isolation_level,
            read_only=read_only,
            deferrable=deferrable,
        )

    def get_async_autocommit(self):
        return not self.get_async_executor().in_transaction

    def get_async_rollback(self):
        return self.get_async_executor().needs_rollback

    def set_async_rollback(self, rollback):
        self.get_async_executor().set_rollback(rollback)

    def on_async_commit(self, func, robust=False):
        self.get_async_executor().on_commit(func, robust)

    def run_async_commit_hooks(self):
        self.get_async_executor().run_on_commit_callbacks()

    def get_connection_params(self):
        connection_params = super().get_connection_params()
        connection_params.pop("native_pool", None)
        return connection_params
