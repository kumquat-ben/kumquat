from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="Scraper",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("company", models.CharField(max_length=255)),
                ("url", models.URLField()),
                ("code", models.TextField()),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("last_run", models.DateTimeField(blank=True, null=True)),
                ("active", models.BooleanField(default=True)),
            ],
        ),
        migrations.CreateModel(
            name="JobPosting",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("title", models.CharField(max_length=255)),
                ("location", models.CharField(blank=True, max_length=255, null=True)),
                ("date", models.CharField(blank=True, max_length=100, null=True)),
                ("link", models.URLField()),
                ("description", models.TextField()),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                (
                    "scraper",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="job_postings",
                        to="scrapers.scraper",
                    ),
                ),
            ],
        ),
    ]
