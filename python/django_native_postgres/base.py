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
            database_url = make_conninfo(
                dbname=settings.get("NAME") or "postgres",
                user=settings.get("USER") or None,
                password=settings.get("PASSWORD") or None,
                host=settings.get("HOST") or None,
                port=settings.get("PORT") or None,
            )

            executor = NativeExecutor(database_url=database_url)
            self._native_executor = executor
        return executor

    async def aexecute(self, sql, params=None):
        executor = self.get_async_executor()
        return await executor.execute(sql=sql, params=params)
