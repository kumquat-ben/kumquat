from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("scrapers", "0004_alter_jobposting_unique_together"),
    ]

    operations = [
        migrations.AddField(
            model_name="scraper",
            name="timeout_seconds",
            field=models.PositiveIntegerField(default=180),
        ),
    ]
