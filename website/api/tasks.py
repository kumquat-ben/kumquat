# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
from celery import shared_task
from django.conf import settings

from .search import crawl_target


@shared_task(name="api.crawl_search_target")
def crawl_search_target(target_id):
    return crawl_target(target_id).pk


def schedule_crawl_search_target(target_id):
    broker_url = getattr(settings, "CELERY_BROKER_URL", "")
    if getattr(settings, "CELERY_TASK_ALWAYS_EAGER", False) or broker_url.startswith("memory://"):
        crawl_search_target.apply(args=[target_id])
        return "inline"

    crawl_search_target.delay(target_id)
    return "queued"
