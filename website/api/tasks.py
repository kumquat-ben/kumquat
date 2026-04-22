# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
import subprocess
import sys
from pathlib import Path

from celery import shared_task
from django.conf import settings

from .models import SearchCrawlTarget, WebsiteDiscoveredDomain
from .search import crawl_target


def _run_scrapy_management_command(*, mode, identifier):
    manage_py = Path(settings.BASE_DIR) / "manage.py"
    completed = subprocess.run(
        [sys.executable, str(manage_py), "run_scrapy_crawl", "--mode", mode, "--id", str(identifier)],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        stderr = (completed.stderr or completed.stdout or "").strip()
        raise RuntimeError(stderr or f"Scrapy crawl subprocess failed with exit code {completed.returncode}.")


@shared_task(name="api.crawl_search_target")
def crawl_search_target(target_id):
    target = SearchCrawlTarget.objects.get(pk=target_id)
    if target.crawl_backend == SearchCrawlTarget.BACKEND_SCRAPY:
        try:
            _run_scrapy_management_command(mode="target", identifier=target_id)
        except Exception as exc:
            target.status = SearchCrawlTarget.STATUS_FAILED
            target.last_error = str(exc)
            target.save(update_fields=["status", "last_error", "updated_at"])
            WebsiteDiscoveredDomain.objects.filter(crawl_target=target).update(
                status=WebsiteDiscoveredDomain.STATUS_FAILED,
                last_error=str(exc),
            )
            raise
        target.refresh_from_db()
    else:
        target = crawl_target(target_id)

    discovered_status = (
        WebsiteDiscoveredDomain.STATUS_CRAWLED
        if target.status == SearchCrawlTarget.STATUS_COMPLETED
        else WebsiteDiscoveredDomain.STATUS_FAILED
    )
    WebsiteDiscoveredDomain.objects.filter(crawl_target=target).update(
        status=discovered_status,
        last_error=target.last_error,
    )
    return target.pk


@shared_task(name="api.discover_domains_with_scrapy")
def discover_domains_for_crawler(crawler_definition_id):
    _run_scrapy_management_command(mode="discovery", identifier=crawler_definition_id)
    return crawler_definition_id


def schedule_crawl_search_target(target_id):
    broker_url = getattr(settings, "CELERY_BROKER_URL", "")
    if getattr(settings, "CELERY_TASK_ALWAYS_EAGER", False) or broker_url.startswith("memory://"):
        crawl_search_target.apply(args=[target_id])
        return "inline"

    crawl_search_target.delay(target_id)
    return "queued"


def schedule_domain_discovery(crawler_definition_id):
    broker_url = getattr(settings, "CELERY_BROKER_URL", "")
    if getattr(settings, "CELERY_TASK_ALWAYS_EAGER", False) or broker_url.startswith("memory://"):
        discover_domains_for_crawler.apply(args=[crawler_definition_id])
        return "inline"

    discover_domains_for_crawler.delay(crawler_definition_id)
    return "queued"
