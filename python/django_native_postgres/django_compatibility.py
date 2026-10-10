from django.core.exceptions import ImproperlyConfigured
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.base.features import BaseDatabaseFeatures
from django.db.models.sql.compiler import SQLCompiler
from django.db.transaction import Atomic


def require_native_async_django() -> None:
    required_attributes = (
        (BaseDatabaseFeatures, "supports_async"),
        (BaseDatabaseWrapper, "acursor"),
        (SQLCompiler, "aexecute_sql"),
        (Atomic, "__aenter__"),
        (Atomic, "__aexit__"),
    )
    missing = [
        f"{owner.__name__}.{attribute}"
        for owner, attribute in required_attributes
        if not hasattr(owner, attribute)
    ]

    if missing:
        missing_contract = ", ".join(missing)
        raise ImproperlyConfigured(
            "django-native-postgres requires its tested Django fork; "
            f"the installed Django is missing: {missing_contract}. "
            "See the Django fork requirement in the project documentation."
        )
