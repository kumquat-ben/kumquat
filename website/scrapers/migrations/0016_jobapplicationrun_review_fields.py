from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("scrapers", "0015_applicantsitecredential_jobapplicationrun"),
    ]

    operations = [
        migrations.AddField(
            model_name="jobapplicationrun",
            name="apply_method",
            field=models.CharField(
                choices=[
                    ("generic_script", "Generic Script"),
                    ("site_specific_script", "Site-Specific Script"),
                    ("unknown", "Unknown"),
                ],
                default="unknown",
                max_length=40,
            ),
        ),
        migrations.AddField(
            model_name="jobapplicationrun",
            name="current_step",
            field=models.CharField(blank=True, max_length=100),
        ),
        migrations.AddField(
            model_name="jobapplicationrun",
            name="reprocess_count",
            field=models.PositiveIntegerField(default=0),
        ),
        migrations.AddField(
            model_name="jobapplicationrun",
            name="review_notes",
            field=models.TextField(blank=True),
        ),
        migrations.AddField(
            model_name="jobapplicationrun",
            name="reviewed_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="jobapplicationrun",
            name="reviewed_by",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="reviewed_job_application_runs",
                to=settings.AUTH_USER_MODEL,
            ),
        ),
        migrations.AlterField(
            model_name="jobapplicationrun",
            name="status",
            field=models.CharField(
                choices=[
                    ("pending", "Pending"),
                    ("running", "Running"),
                    ("awaiting_email_verification", "Awaiting Email Verification"),
                    ("action_required", "Action Required"),
                    ("applied", "Applied"),
                    ("needs_review", "Needs Systems Review"),
                    ("failed", "Failed"),
                    ("incomplete", "Incomplete"),
                    ("error", "Error"),
                ],
                default="pending",
                max_length=40,
            ),
        ),
    ]
