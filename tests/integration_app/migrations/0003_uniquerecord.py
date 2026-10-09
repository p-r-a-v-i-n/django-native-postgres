from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("integration_app", "0002_deletion_models"),
    ]

    operations = [
        migrations.CreateModel(
            name="UniqueRecord",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("key", models.CharField(max_length=200, unique=True)),
                ("value", models.CharField(max_length=200)),
            ],
        ),
    ]
