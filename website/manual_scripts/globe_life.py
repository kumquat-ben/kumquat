#!/usr/bin/env python3
"""Standalone scraper for the Globe Life careers portal."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "website.settings")

import django  # noqa: E402

django.setup()

from django.utils import timezone  # noqa: E402

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

BASE_URL = "https://careers.globelifeinsurance.com"
LISTING_URL = f"{BASE_URL}/jobs/jobs-by-category"
SCRAPER_COMPANY = "Globe Life"
SCRAPER_TIMEOUT_SECONDS = 300
REQUEST_TIMEOUT: Tuple[int, int] = (10, 30)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": BASE_URL,
}


def emit(event: str, data: Dict[str, object]) -> None:
    print(json.dumps({"event": event, "data": data}, ensure_ascii=False))
    sys.stdout.flush()


def clean_text(fragment: Optional[str]) -> str:
    soup = BeautifulSoup(fragment or "", "html.parser")
    return soup.get_text("\n", strip=True).strip()


def fetch_listing(session: requests.Session) -> BeautifulSoup:
    response = session.get(LISTING_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def fetch_detail(session: requests.Session, url: str) -> Dict[str, object]:
    response = session.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    job_body = soup.select_one("div.job-body")

    description_html = ""
    if job_body:
        span_block = job_body.select_one("span[id$='lbDescription']")
        if span_block:
            description_html = str(span_block)
        else:
            text_block = job_body.find("div")
            description_html = str(text_block) if text_block else ""

    job_number_node = soup.select_one("div.job-number")
    job_number = None
    if job_number_node:
        job_number = job_number_node.get_text(strip=True).replace("Job number:", "").strip() or None

    apply_links = []
    for anchor in soup.select("div.apply-buttons a[href]"):
        href = anchor.get("href", "").strip()
        if href:
            apply_links.append(href)

    detail: Dict[str, object] = {
        "description_html": description_html,
        "description_text": clean_text(description_html),
        "job_number": job_number,
        "apply_links": apply_links,
    }
    return detail


def collect_jobs(session: requests.Session) -> List[Dict[str, object]]:
    soup = fetch_listing(session)
    results: List[Dict[str, object]] = []
    seen_links = set()

    for entry in soup.select("div.jobs-list-entry"):
        anchor = entry.find("a")
        if not anchor:
            continue

        raw_href = anchor.get("href") or ""
        if "/jobs/job-details/" not in raw_href:
            continue

        link = urljoin(BASE_URL, raw_href)
        if link in seen_links:
            continue
        seen_links.add(link)

        title_node = anchor.find("div", class_="jobs-list-title")
        location_node = anchor.find("div", class_="jobs-list-location")

        tracking_code = None
        if location_node:
            tracking_span = location_node.find("span", class_="job-tracking-code")
            if tracking_span:
                tracking_code = tracking_span.get_text(strip=True)
                tracking_span.extract()

        title = title_node.get_text(strip=True) if title_node else ""
        location = location_node.get_text(" ", strip=True) if location_node else ""

        category = ""
        category_node = entry.find_previous("div", class_="jobs-list-category")
        if category_node:
            category = category_node.get_text(strip=True)

        query = parse_qs(urlparse(link).query)
        job_id = query.get("jobid", [""])[0] or None

        detail = fetch_detail(session, link)

        metadata: Dict[str, object] = {
            "category": category or None,
            "job_id": job_id,
            "tracking_code": tracking_code,
            "job_number": detail.get("job_number"),
            "apply_links": detail.get("apply_links"),
            "description_html": detail.get("description_html"),
        }

        job_record = {
            "title": title,
            "location": location,
            "date": "",
            "link": link,
            "description": detail.get("description_text") or "",
            "metadata": {k: v for k, v in metadata.items() if v},
        }
        results.append(job_record)

    return results


def get_scraper() -> Scraper:
    qs = Scraper.objects.filter(company=SCRAPER_COMPANY, url=LISTING_URL).order_by("id")
    matches = list(qs[:2])
    if matches:
        scraper = matches[0]
        if len(matches) > 1:
            emit(
                "log",
                {
                    "step": "scraper",
                    "detail": f"Multiple Scraper rows matched; using id={scraper.id}.",
                },
            )
        return scraper

    scraper = Scraper.objects.create(
        company=SCRAPER_COMPANY,
        url=LISTING_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=SCRAPER_TIMEOUT_SECONDS,
    )
    emit(
        "log",
        {"step": "scraper_created", "detail": f"Created Scraper row id={scraper.id}."},
    )
    return scraper


def persist_jobs(scraper: Scraper, jobs: List[Dict[str, object]]) -> Dict[str, object]:
    summary: Dict[str, object] = {
        "total": len(jobs),
        "created": 0,
        "updated": 0,
        "skipped": 0,
        "errors": 0,
    }
    for job in jobs:
        link = (job.get("link") or "").strip()
        title = (job.get("title") or "").strip()
        if not link or not title:
            summary["skipped"] = summary.get("skipped", 0) + 1
            continue

        metadata = job.get("metadata") or None
        if isinstance(metadata, dict):
            metadata = {k: v for k, v in metadata.items() if v not in (None, "", [], {})}

        location_value = job.get("location")
        if isinstance(location_value, str):
            location_clean = location_value.strip()
            location_field = location_clean[:255] if location_clean else None
        elif location_value is None:
            location_field = None
        else:
            location_field = str(location_value)[:255]

        defaults = {
            "title": title[:255],
            "location": location_field,
            "date": (job.get("date") or "").strip()[:100] or None,
            "description": (job.get("description") or "")[:10000],
            "metadata": metadata,
        }

        try:
            _, created = JobPosting.objects.update_or_create(
                scraper=scraper,
                link=link,
                defaults=defaults,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            summary["errors"] = summary.get("errors", 0) + 1
            emit(
                "log",
                {"step": "persist_error", "detail": str(exc), "link": link},
            )
            continue

        if created:
            summary["created"] = summary.get("created", 0) + 1
        else:
            summary["updated"] = summary.get("updated", 0) + 1

    dedupe_summary = deduplicate_job_postings(scraper=scraper)
    scraper.last_run = timezone.now()
    scraper.save(update_fields=["last_run"])
    summary["deduplicated"] = dedupe_summary
    return summary


def main() -> None:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    emit("log", {"step": "start", "detail": f"Fetching listings from {LISTING_URL}"})
    try:
        jobs = collect_jobs(session)
    except Exception as exc:  # pragma: no cover - defensive for runtime usage
        emit("log", {"step": "error", "detail": str(exc)})
        raise

    emit(
        "result",
        {
            "company": "Globe Life",
            "url": LISTING_URL,
            "jobs": jobs,
            "count": len(jobs),
        },
    )

    scraper = get_scraper()
    persistence_summary = persist_jobs(scraper, jobs)
    emit("log", {"step": "persisted", "detail": persistence_summary})


if __name__ == "__main__":
    main()
