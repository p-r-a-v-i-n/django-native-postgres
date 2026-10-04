from django.db.backends.postgresql.features import (
    DatabaseFeatures as PostgreSQLDatabaseFeatures,
)


class DatabaseFeatures(PostgreSQLDatabaseFeatures):
    supports_async = True
