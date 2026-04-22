from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("scrapers", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="scraper",
            name="interval_hours",
            field=models.PositiveIntegerField(default=24),
        ),
    ]
