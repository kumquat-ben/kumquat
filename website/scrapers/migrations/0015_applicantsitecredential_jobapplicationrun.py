from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("scrapers", "0014_jobposting_duplicate_tracking"),
    ]

    operations = [
        migrations.CreateModel(
            name="ApplicantSiteCredential",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("apply_domain", models.CharField(max_length=255)),
                ("login_url", models.URLField(blank=True, max_length=1000)),
                ("username", models.CharField(max_length=255)),
                ("password", models.TextField()),
                ("metadata", models.JSONField(blank=True, default=dict)),
                ("last_used_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("user", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="applicant_site_credentials", to=settings.AUTH_USER_MODEL)),
            ],
            options={
                "ordering": ["apply_domain", "-updated_at", "-id"],
            },
        ),
        migrations.CreateModel(
            name="JobApplicationRun",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("status", models.CharField(choices=[("pending", "Pending"), ("running", "Running"), ("awaiting_email_verification", "Awaiting Email Verification"), ("action_required", "Action Required"), ("applied", "Applied"), ("incomplete", "Incomplete"), ("error", "Error")], default="pending", max_length=40)),
                ("selected_script_name", models.CharField(blank=True, max_length=255)),
                ("apply_url", models.URLField(blank=True, max_length=1000)),
                ("apply_domain", models.CharField(blank=True, max_length=255)),
                ("form_classification", models.CharField(blank=True, max_length=100)),
                ("verification_prompt", models.TextField(blank=True)),
                ("last_result", models.JSONField(blank=True, default=dict)),
                ("runtime_state", models.JSONField(blank=True, default=dict)),
                ("submitted_at", models.DateTimeField(blank=True, null=True)),
                ("last_error", models.TextField(blank=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("credential", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="application_runs", to="scrapers.applicantsitecredential")),
                ("job", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="application_runs", to="scrapers.jobposting")),
                ("manager", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="application_runs", to="scrapers.jobapplicationsubmissionmanager")),
                ("user", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="job_application_runs", to=settings.AUTH_USER_MODEL)),
            ],
            options={
                "ordering": ["-updated_at", "-id"],
            },
        ),
        migrations.AddConstraint(
            model_name="applicantsitecredential",
            constraint=models.UniqueConstraint(fields=("user", "apply_domain"), name="unique_applicant_site_credential_per_user_domain"),
        ),
    ]
