from importlib.metadata import requires
from importlib.metadata import version as distribution_version

from django.db.backends.base.features import BaseDatabaseFeatures
from django_native_postgres import build_info
from django_native_postgres.base import DatabaseWrapper


def main() -> None:
    name, version = build_info()
    requirements = requires("django-native-postgres") or []

    assert name == "django-native-postgres"
    assert version == distribution_version("django-native-postgres")
    assert BaseDatabaseFeatures.supports_async is False
    assert DatabaseWrapper.features_class.supports_async is True
    assert "django==6.2.dev20261009125329" in requirements


if __name__ == "__main__":
    main()
