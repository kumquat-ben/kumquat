import django.db.models.deletion
from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):
    dependencies = [
        ("scrapers", "0002_scraper_interval_hours"),
    ]

    operations = [
        migrations.CreateModel(
            name="ScraperRun",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("started_at", models.DateTimeField(default=django.utils.timezone.now)),
                ("finished_at", models.DateTimeField(blank=True, null=True)),
                ("duration_ms", models.PositiveIntegerField(blank=True, null=True)),
                (
                    "status",
                    models.CharField(
                        choices=[("success", "Success"), ("error", "Error")],
                        max_length=20,
                    ),
                ),
                ("payload", models.JSONField(blank=True, null=True)),
                ("error", models.TextField(blank=True)),
                (
                    "triggered_by",
                    models.CharField(
                        choices=[("manual", "Manual"), ("scheduler", "Scheduler"), ("api", "API"), ("management", "Management Command")],
                        default="manual",
                        max_length=20,
                    ),
                ),
                (
                    "scraper",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="runs",
                        to="scrapers.scraper",
                    ),
                ),
            ],
            options={
                "ordering": ["-started_at"],
            },
        ),
    ]
