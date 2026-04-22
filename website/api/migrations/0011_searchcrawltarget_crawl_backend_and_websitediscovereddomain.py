from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ("api", "0010_websitecrawlerdefinition"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.AddField(
            model_name="searchcrawltarget",
            name="crawl_backend",
            field=models.CharField(
                choices=[("basic", "Basic"), ("scrapy", "Scrapy")],
                default="basic",
                max_length=16,
            ),
        ),
        migrations.CreateModel(
            name="WebsiteDiscoveredDomain",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("source_url", models.URLField(blank=True, max_length=500)),
                ("domain", models.CharField(db_index=True, max_length=255)),
                ("normalized_url", models.URLField(max_length=500)),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("new", "New"),
                            ("queued", "Queued"),
                            ("crawled", "Crawled"),
                            ("failed", "Failed"),
                            ("ignored", "Ignored"),
                        ],
                        default="new",
                        max_length=16,
                    ),
                ),
                ("discovery_count", models.PositiveIntegerField(default=1)),
                ("last_error", models.TextField(blank=True)),
                ("discovered_at", models.DateTimeField(default=django.utils.timezone.now)),
                ("last_seen_at", models.DateTimeField(default=django.utils.timezone.now)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "crawl_target",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="discovered_domains",
                        to="api.searchcrawltarget",
                    ),
                ),
                (
                    "crawler_definition",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="discovered_domains",
                        to="api.websitecrawlerdefinition",
                    ),
                ),
            ],
            options={
                "ordering": ["domain", "-last_seen_at"],
            },
        ),
        migrations.AddConstraint(
            model_name="websitediscovereddomain",
            constraint=models.UniqueConstraint(
                fields=("crawler_definition", "domain"),
                name="unique_discovered_domain_per_crawler",
            ),
        ),
    ]
