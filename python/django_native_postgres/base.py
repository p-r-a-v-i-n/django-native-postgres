from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.base.client import BaseDatabaseClient
from django.db.backends.base.creation import BaseDatabaseCreation
from django.db.backends.base.features import BaseDatabaseFeatures
from django.db.backends.base.introspection import BaseDatabaseIntrospection
from django.db.backends.base.operations import BaseDatabaseOperations


class DatabaseClient(BaseDatabaseClient):
    pass


class DatabaseCreation(BaseDatabaseCreation):
    pass


class DatabaseFeatures(BaseDatabaseFeatures):
    pass


class DatabaseIntrospection(BaseDatabaseIntrospection):
    pass


class DatabaseOperations(BaseDatabaseOperations):
    pass


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = "postgresql"
    display_name = "PostgreSQL (native async)"

    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations

    def get_async_executor(self):
        raise NotImplementedError("Native async executor is not configured yet.")

    async def aexecute(self, sql, params=None):
        executor = self.get_async_executor()
        return await executor.execute(sql=sql, params=params)
