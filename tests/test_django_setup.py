from django.db import connections
from django_native_postgres.base import DatabaseWrapper


def test_django_connection_registry_loads_native_backend():
    connection = connections["default"]

    assert isinstance(connection, DatabaseWrapper)
    assert connection.settings_dict["ENGINE"] == "django_native_postgres"
    assert connection.vendor == "postgresql"
    assert connection.connection is None
