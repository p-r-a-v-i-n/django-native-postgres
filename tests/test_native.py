from django_native_postgres import build_info


def test_build_info() -> None:
    name, version = build_info()

    assert name == "django-native-postgres"
    assert version == "0.1.0"
