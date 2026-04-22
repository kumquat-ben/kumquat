# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
import django.db.models.deletion
import django.db.models.functions.text
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("api", "0007_searchcrawltarget_searchdocument"),
    ]

    operations = [
        migrations.CreateModel(
            name="CompanyProfile",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(max_length=200)),
                ("slug", models.SlugField(max_length=220, unique=True)),
                ("website", models.URLField(blank=True, max_length=300)),
                ("info", models.TextField(blank=True)),
                ("source", models.CharField(blank=True, max_length=120)),
                ("source_url", models.URLField(blank=True, max_length=400)),
                ("yc_url", models.URLField(blank=True, max_length=200)),
                ("batch", models.CharField(blank=True, max_length=20)),
                ("status", models.CharField(blank=True, max_length=60)),
                ("employees", models.CharField(blank=True, max_length=50)),
                ("location", models.CharField(blank=True, max_length=150)),
                ("tags", models.CharField(blank=True, max_length=200)),
                ("linkedin_url", models.URLField(blank=True, max_length=400)),
                ("twitter_url", models.URLField(blank=True, max_length=400)),
                ("cb_url", models.URLField(blank=True, max_length=400)),
                ("careers_url", models.URLField(blank=True, max_length=400)),
                ("collected_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "ordering": ["name"],
            },
        ),
        migrations.CreateModel(
            name="JobListing",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("title", models.CharField(max_length=255)),
                ("slug", models.SlugField(max_length=280, unique=True)),
                ("location", models.CharField(blank=True, max_length=255)),
                ("normalized_location", models.CharField(blank=True, max_length=255)),
                ("employment_type", models.CharField(blank=True, max_length=120)),
                ("salary", models.CharField(blank=True, max_length=120)),
                ("excerpt", models.TextField(blank=True)),
                ("description", models.TextField(blank=True)),
                ("apply_url", models.URLField(blank=True, max_length=500)),
                ("source", models.CharField(blank=True, max_length=120)),
                ("metadata", models.JSONField(blank=True, default=dict)),
                ("posted_at", models.DateTimeField(blank=True, null=True)),
                ("is_active", models.BooleanField(default=True)),
                ("view_count", models.PositiveIntegerField(default=0)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "company",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="job_listings",
                        to="api.companyprofile",
                    ),
                ),
            ],
            options={
                "ordering": ["-posted_at", "-created_at", "title"],
            },
        ),
        migrations.AddConstraint(
            model_name="companyprofile",
            constraint=models.UniqueConstraint(
                django.db.models.functions.text.Lower("name"),
                name="unique_company_profile_name_ci",
            ),
        ),
    ]
