import os

SECRET_KEY = "django-native-postgres-tests"

INSTALLED_APPS = ["tests.integration_app"]

DATABASES = {
    "default": {
        "ENGINE": "django_native_postgres",
        "NAME": os.getenv("DNP_POSTGRES_DB", "django_native_postgres_tests"),
        "USER": os.getenv("DNP_POSTGRES_USER", "django_native"),
        "PASSWORD": os.getenv("DNP_POSTGRES_PASSWORD", "django_native"),
        "HOST": os.getenv("DNP_POSTGRES_HOST", "127.0.0.1"),
        "PORT": os.getenv("DNP_POSTGRES_PORT", "55432"),
        "CONN_MAX_AGE": 0,
    },
}

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
USE_TZ = True
