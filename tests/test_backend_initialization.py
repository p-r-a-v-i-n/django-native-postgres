import pytest
from django.core.exceptions import ImproperlyConfigured
from django_native_postgres.base import DatabaseWrapper
from django_native_postgres.django_compatibility import require_native_async_django


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


def test_backend_rejects_django_without_native_async_contract(monkeypatch):
    monkeypatch.delattr(
        "django_native_postgres.django_compatibility.SQLCompiler.aexecute_sql"
    )

    with pytest.raises(
        ImproperlyConfigured,
        match="requires its tested Django fork.*SQLCompiler.aexecute_sql",
    ):
        require_native_async_django()
