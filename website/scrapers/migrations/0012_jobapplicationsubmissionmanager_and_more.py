from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("scrapers", "0011_manualscriptsourceurl"),
    ]

    operations = [
        migrations.CreateModel(
            name="JobApplicationSubmissionManager",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("status", models.CharField(choices=[("candidate_script_found", "Candidate Script Found"), ("design_time_script_needed", "Design-Time Script Needed")], max_length=40)),
                ("browser_session_status", models.CharField(choices=[("pending", "Pending"), ("completed", "Completed")], default="pending", max_length=20)),
                ("matched_script_name", models.CharField(blank=True, max_length=255)),
                ("candidate_script_names", models.JSONField(blank=True, default=list)),
                ("apply_url", models.URLField(blank=True, max_length=1000)),
                ("apply_domain", models.CharField(blank=True, max_length=255)),
                ("form_requirements", models.JSONField(blank=True, default=dict)),
                ("requirements_summary", models.TextField(blank=True)),
                ("notes", models.TextField(blank=True)),
                ("request_count", models.PositiveIntegerField(default=0)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("first_requested_by", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="job_submission_managers_first_requested", to=settings.AUTH_USER_MODEL)),
                ("job", models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, related_name="submission_manager", to="scrapers.jobposting")),
                ("last_requested_by", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="job_submission_managers_last_requested", to=settings.AUTH_USER_MODEL)),
            ],
            options={
                "ordering": ["-updated_at", "-id"],
            },
        ),
        migrations.CreateModel(
            name="JobApplicationSubmissionRequest",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("status", models.CharField(choices=[("requested", "Requested")], default="requested", max_length=20)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("job", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="submission_requests", to="scrapers.jobposting")),
                ("manager", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="requests", to="scrapers.jobapplicationsubmissionmanager")),
                ("user", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="job_submission_requests", to=settings.AUTH_USER_MODEL)),
            ],
            options={
                "ordering": ["-created_at", "-id"],
            },
        ),
        migrations.AddConstraint(
            model_name="jobapplicationsubmissionrequest",
            constraint=models.UniqueConstraint(fields=("job", "user"), name="unique_job_submission_request_per_user"),
        ),
    ]
