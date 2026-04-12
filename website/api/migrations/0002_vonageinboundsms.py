# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):
    dependencies = [
        ("api", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="VonageInboundSms",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("api_key", models.CharField(blank=True, max_length=64)),
                ("message_id", models.CharField(blank=True, db_index=True, max_length=120)),
                ("from_number", models.CharField(blank=True, max_length=32)),
                ("to_number", models.CharField(blank=True, max_length=32)),
                ("text", models.TextField(blank=True)),
                ("message_type", models.CharField(blank=True, max_length=32)),
                ("keyword", models.CharField(blank=True, max_length=120)),
                ("message_timestamp", models.DateTimeField(blank=True, null=True)),
                ("message_timestamp_raw", models.CharField(blank=True, max_length=64)),
                ("event_timestamp", models.DateTimeField(blank=True, null=True)),
                ("event_timestamp_raw", models.CharField(blank=True, max_length=64)),
                ("nonce", models.CharField(blank=True, max_length=120)),
                ("signature", models.CharField(blank=True, max_length=255)),
                ("signature_valid", models.BooleanField(blank=True, null=True)),
                ("signature_error", models.CharField(blank=True, max_length=255)),
                ("is_concatenated", models.BooleanField(default=False)),
                ("concat_ref", models.CharField(blank=True, max_length=64)),
                ("concat_total", models.PositiveIntegerField(blank=True, null=True)),
                ("concat_part", models.PositiveIntegerField(blank=True, null=True)),
                ("data", models.TextField(blank=True)),
                ("udh", models.TextField(blank=True)),
                ("content_type", models.CharField(blank=True, max_length=255)),
                ("request_method", models.CharField(blank=True, max_length=16)),
                ("remote_addr", models.CharField(blank=True, max_length=64)),
                ("user_agent", models.CharField(blank=True, max_length=255)),
                ("payload", models.JSONField(blank=True, default=dict)),
                ("raw_body", models.TextField(blank=True)),
                ("received_at", models.DateTimeField(db_index=True, default=django.utils.timezone.now)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "ordering": ["-received_at", "-created_at"],
            },
        ),
    ]
