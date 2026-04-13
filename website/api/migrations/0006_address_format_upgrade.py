# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
from django.db import migrations, models


def normalize_existing_addresses(apps, _schema_editor):
    from api.address_codec import normalize_address

    ManagedNode = apps.get_model("api", "ManagedNode")
    UserWallet = apps.get_model("api", "UserWallet")

    for node in ManagedNode.objects.exclude(reward_address=""):
        normalized = normalize_address(node.reward_address)
        if normalized != node.reward_address:
            node.reward_address = normalized
            node.save(update_fields=["reward_address"])

    for wallet in UserWallet.objects.all():
        normalized = normalize_address(wallet.address)
        if normalized != wallet.address:
            wallet.address = normalized
            wallet.save(update_fields=["address"])


class Migration(migrations.Migration):
    dependencies = [
        ("api", "0005_userwallet"),
    ]

    operations = [
        migrations.AlterField(
            model_name="managednode",
            name="reward_address",
            field=models.CharField(blank=True, max_length=96),
        ),
        migrations.AlterField(
            model_name="userwallet",
            name="address",
            field=models.CharField(db_index=True, max_length=96, unique=True),
        ),
        migrations.RunPython(normalize_existing_addresses, migrations.RunPython.noop),
    ]
