from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.utils import load_backend


def test_django_loads_native_postgres_backend():
    backend = load_backend("django_native_postgres")

    assert issubclass(backend.DatabaseWrapper, BaseDatabaseWrapper)
    assert backend.DatabaseWrapper.vendor == "postgresql"
