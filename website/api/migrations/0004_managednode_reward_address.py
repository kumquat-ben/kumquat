# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("api", "0003_managednode"),
    ]

    operations = [
        migrations.AddField(
            model_name="managednode",
            name="reward_address",
            field=models.CharField(blank=True, max_length=64),
        ),
    ]
