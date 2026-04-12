# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
import django.db.models.deletion
import django.utils.timezone
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("api", "0002_vonageinboundsms"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="ManagedNode",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.SlugField(max_length=63, unique=True)),
                ("display_name", models.CharField(max_length=120)),
                ("container_name", models.CharField(blank=True, max_length=120)),
                ("container_id", models.CharField(blank=True, db_index=True, max_length=128)),
                ("image", models.CharField(max_length=255)),
                ("network_name", models.CharField(default="dev", max_length=32)),
                ("chain_id", models.PositiveBigIntegerField(default=1337)),
                ("enable_mining", models.BooleanField(default=False)),
                ("mining_threads", models.PositiveIntegerField(default=1)),
                ("api_port", models.PositiveIntegerField(unique=True)),
                ("p2p_port", models.PositiveIntegerField(unique=True)),
                ("metrics_port", models.PositiveIntegerField(unique=True)),
                ("status", models.CharField(choices=[("pending", "Pending"), ("running", "Running"), ("exited", "Exited"), ("stopped", "Stopped"), ("failed", "Failed")], default="pending", max_length=16)),
                ("last_error", models.TextField(blank=True)),
                ("last_logs", models.TextField(blank=True)),
                ("launched_at", models.DateTimeField(default=django.utils.timezone.now)),
                ("last_status_at", models.DateTimeField(blank=True, null=True)),
                ("stopped_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("launched_by", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="managed_nodes", to=settings.AUTH_USER_MODEL)),
            ],
            options={"ordering": ["-created_at"]},
        ),
    ]
