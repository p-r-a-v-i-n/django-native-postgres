from django.db.backends.postgresql.operations import (
    DatabaseOperations as PostgreSQLDatabaseOperations,
)


class DatabaseOperations(PostgreSQLDatabaseOperations):
    # PostgreSQL's optional UNNEST bulk-insert optimization requires array
    # parameters. Use standard multi-row VALUES until native arrays are
    # supported.
    compiler_module = "django_native_postgres.compiler"
