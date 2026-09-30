from django.db.backends.postgresql.base import (
    DatabaseWrapper as PostgreSQLDatabaseWrapper,
)


class DatabaseWrapper(PostgreSQLDatabaseWrapper):
    display_name = "PostgreSQL (native async)"

    def get_async_executor(self):
        raise NotImplementedError("Native async executor is not configured yet.")

    async def aexecute(self, sql, params=None):
        executor = self.get_async_executor()
        return await executor.execute(sql=sql, params=params)
