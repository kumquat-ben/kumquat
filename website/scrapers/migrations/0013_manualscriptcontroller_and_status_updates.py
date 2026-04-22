from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("scrapers", "0012_jobapplicationsubmissionmanager_and_more"),
    ]

    operations = [
        migrations.AlterField(
            model_name="manualscriptqueue",
            name="status",
            field=models.CharField(
                choices=[
                    ("pending", "Pending"),
                    ("running", "Running"),
                    ("success", "Success"),
                    ("error", "Error"),
                    ("stopped", "Stopped"),
                ],
                default="pending",
                max_length=20,
            ),
        ),
        migrations.AlterField(
            model_name="manualscriptrun",
            name="status",
            field=models.CharField(
                choices=[
                    ("pending", "Pending"),
                    ("running", "Running"),
                    ("success", "Success"),
                    ("error", "Error"),
                    ("cancelled", "Cancelled"),
                ],
                default="pending",
                max_length=20,
            ),
        ),
        migrations.CreateModel(
            name="ManualScriptController",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("is_enabled", models.BooleanField(default=False)),
                ("loop_mode", models.BooleanField(default=True)),
                ("queue_concurrency", models.PositiveIntegerField(default=2)),
                ("desired_worker_replicas", models.PositiveIntegerField(default=1)),
                ("last_started_at", models.DateTimeField(blank=True, null=True)),
                ("last_stopped_at", models.DateTimeField(blank=True, null=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "ordering": ["id"],
            },
        ),
    ]
