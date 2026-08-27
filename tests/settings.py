SECRET_KEY = "django-native-postgres-tests"

INSTALLED_APPS = []

DATABASES = {
    "default": {
        "ENGINE": "django_native_postgres",
        "NAME": "django_native_postgres_tests",
    },
}

USE_TZ = True
