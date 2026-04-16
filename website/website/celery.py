# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
import os

from celery import Celery


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "website.settings")

app = Celery("website")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()
