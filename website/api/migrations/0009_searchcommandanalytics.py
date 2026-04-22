from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("api", "0008_companyprofile_joblisting"),
    ]

    operations = [
        migrations.CreateModel(
            name="SearchCommandAnalytics",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("channel", models.CharField(max_length=32, unique=True)),
                ("command_count", models.PositiveBigIntegerField(default=0)),
                ("last_command_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "ordering": ["channel"],
            },
        ),
    ]
