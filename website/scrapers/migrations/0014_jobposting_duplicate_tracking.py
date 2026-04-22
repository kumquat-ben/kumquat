from django.db import migrations, models
from django.utils import timezone


def backfill_last_crawled_at(apps, schema_editor):
    JobPosting = apps.get_model("scrapers", "JobPosting")
    for job in JobPosting.objects.filter(last_crawled_at__isnull=True).only("id", "created_at").iterator(chunk_size=500):
        JobPosting.objects.filter(pk=job.pk).update(last_crawled_at=job.created_at)


class Migration(migrations.Migration):

    dependencies = [
        ("scrapers", "0013_manualscriptcontroller_and_status_updates"),
    ]

    operations = [
        migrations.AddField(
            model_name="jobposting",
            name="duplicate_hit_count",
            field=models.PositiveIntegerField(default=0),
        ),
        migrations.AddField(
            model_name="jobposting",
            name="last_crawled_at",
            field=models.DateTimeField(blank=True, db_index=True, null=True),
        ),
        migrations.AddField(
            model_name="jobposting",
            name="last_duplicate_seen_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.RunPython(backfill_last_crawled_at, migrations.RunPython.noop),
        migrations.AlterField(
            model_name="jobposting",
            name="last_crawled_at",
            field=models.DateTimeField(db_index=True, default=timezone.now),
        ),
    ]
