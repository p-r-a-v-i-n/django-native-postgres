from django_native_postgres.base import DatabaseWrapper


def test_database_wrapper_initializes_backend_components():
    settings_dict = {
        "ENGINE": "django_native_postgres",
        "NAME": "test_database",
    }

    connection = DatabaseWrapper(settings_dict, alias="default")

    assert connection.connection is None
    assert connection.settings_dict is settings_dict
    assert connection.alias == "default"
    for component in (
        connection.client,
        connection.creation,
        connection.features,
        connection.introspection,
        connection.ops,
        connection.validation,
    ):
        assert component.connection is connection
