from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("scrapers", "0009_jobposting_location_latitude_and_more"),
    ]

    operations = [
        migrations.CreateModel(
            name="ManualScriptQueue",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("started_at", models.DateTimeField(blank=True, null=True)),
                ("finished_at", models.DateTimeField(blank=True, null=True)),
                (
                    "status",
                    models.CharField(
                        choices=[("pending", "Pending"), ("running", "Running"), ("success", "Success"), ("error", "Error")],
                        default="pending",
                        max_length=20,
                    ),
                ),
                ("current_script_name", models.CharField(blank=True, max_length=255)),
                ("total_scripts", models.PositiveIntegerField(default=0)),
                ("completed_scripts", models.PositiveIntegerField(default=0)),
                ("error", models.TextField(blank=True)),
            ],
            options={"ordering": ["-created_at"]},
        ),
        migrations.AddField(
            model_name="manualscriptrun",
            name="queue",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="runs",
                to="scrapers.manualscriptqueue",
            ),
        ),
        migrations.AddField(
            model_name="manualscriptrun",
            name="queue_position",
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
    ]
