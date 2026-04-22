from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("scrapers", "0003_scraperrun"),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name="jobposting",
            unique_together={("scraper", "link")},
        ),
    ]
