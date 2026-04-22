from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("api", "0009_searchcommandanalytics"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="WebsiteCrawlerDefinition",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(max_length=160)),
                ("slug", models.SlugField(max_length=180, unique=True)),
                (
                    "source_type",
                    models.CharField(
                        choices=[("design_time", "Design Time"), ("runtime", "Runtime")],
                        default="runtime",
                        max_length=20,
                    ),
                ),
                (
                    "vertical",
                    models.CharField(
                        choices=[
                            ("small_business_law", "Small Business Law"),
                            ("home_improvement", "Home Improvement"),
                            ("general_local", "General Local Business"),
                        ],
                        default="general_local",
                        max_length=40,
                    ),
                ),
                ("seed_url", models.URLField(max_length=500)),
                ("scope_netloc", models.CharField(blank=True, db_index=True, max_length=255)),
                ("prompt", models.TextField(blank=True)),
                ("generated_code", models.TextField(blank=True)),
                ("config", models.JSONField(blank=True, default=dict)),
                ("is_active", models.BooleanField(default=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "created_by",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="website_crawler_definitions",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "ordering": ["name", "-updated_at"],
            },
        ),
    ]
