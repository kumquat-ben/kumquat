from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("scrapers", "0010_manualscriptqueue_manualscriptrun_queue_and_more"),
    ]

    operations = [
        migrations.CreateModel(
            name="ManualScriptSourceURL",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("script_name", models.CharField(max_length=255)),
                ("source_name", models.CharField(blank=True, max_length=255)),
                ("url", models.URLField(max_length=1000)),
                ("url_digest", models.CharField(editable=False, max_length=64)),
                ("file_modified_at", models.DateTimeField()),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "ordering": ["script_name", "source_name", "url"],
            },
        ),
        migrations.AddConstraint(
            model_name="manualscriptsourceurl",
            constraint=models.UniqueConstraint(
                fields=("script_name", "url_digest"),
                name="unique_manual_script_source_url",
            ),
        ),
    ]
