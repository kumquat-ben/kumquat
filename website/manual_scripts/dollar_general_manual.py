#!/usr/bin/env python3
"""Manual scraper for Dollar General careers.

This script walks the public JSON API that powers https://careers.dollargeneral.com,
normalizes each job posting, and persists the results through the existing
``JobPosting`` model that is associated with the Dollar General scraper entry.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Django bootstrap
# ---------------------------------------------------------------------------
BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "website.settings")

import django  # noqa: E402

django.setup()

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CAREERS_ROOT = "https://careers.dollargeneral.com"
CAREERS_URL = f"{CAREERS_ROOT}/jobs"
JOBS_ENDPOINT = f"{CAREERS_ROOT}/api/jobs"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": CAREERS_URL,
}

REQUEST_TIMEOUT = (10, 45)

SCRAPER_QS = Scraper.objects.filter(company="Dollar General", url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched Dollar General; using id=%s.", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Dollar General",
        url=CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=900,
    )


class ScraperError(Exception):
    """Raised when the scraper cannot recover from a failure."""


@dataclass
class DollarGeneralJob:
    slug: str
    title: str
    link: str
    location: Optional[str]
    posted_date: Optional[str]
    description_text: str
    metadata: Dict[str, object]


def _html_to_text(value: Optional[str]) -> str:
    if not value:
        return ""
    soup = BeautifulSoup(value, "html.parser")
    return soup.get_text("\n", strip=True).strip()


def _normalize_location(job: Dict[str, object]) -> Optional[str]:
    full_location = (job.get("full_location") or "").strip()
    if full_location:
        return full_location

    city = (job.get("city") or "").strip()
    state = (job.get("state") or "").strip()
    country = (job.get("country") or "").strip()

    parts = [part for part in (city, state, country) if part]
    return ", ".join(parts) if parts else None


class DollarGeneralClient:
    def __init__(
        self,
        *,
        session: Optional[requests.Session] = None,
        delay: float = 0.0,
        page_size: int = 100,
    ) -> None:
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.delay = max(0.0, delay)
        self.page_size = max(1, min(page_size, 100))
        self.logger = logging.getLogger(self.__class__.__name__)

    def iter_jobs(
        self,
        *,
        start_page: int = 1,
        max_pages: Optional[int] = None,
        max_results: Optional[int] = None,
        sort_by: str = "posted_date",
    ) -> Iterator[DollarGeneralJob]:
        if start_page < 1:
            raise ValueError("start_page must be at least 1.")

        page = start_page
        processed_pages = 0
        yielded = 0

        while True:
            payload = self._fetch_page(page=page, sort_by=sort_by)
            jobs = payload.get("jobs") or []
            if not jobs:
                self.logger.info("No jobs returned on page %s; stopping pagination.", page)
                break

            for entry in jobs:
                job_data = entry.get("data") if isinstance(entry, dict) else None
                if not isinstance(job_data, dict):
                    continue
                try:
                    job = self._build_job(job_data)
                except ScraperError as exc:
                    self.logger.warning("Skipping job due to error: %s", exc)
                    continue
                yield job
                yielded += 1
                if max_results and yielded >= max_results:
                    self.logger.info("Reached max_results=%s; stopping.", max_results)
                    return

            processed_pages += 1
            if max_pages and processed_pages >= max_pages:
                self.logger.info("Reached max_pages=%s; stopping.", max_pages)
                break

            page += 1
            if self.delay:
                time.sleep(self.delay)

    def _fetch_page(self, *, page: int, sort_by: str) -> Dict[str, object]:
        params = {
            "page": page,
            "limit": self.page_size,
            "sortBy": sort_by,
        }
        try:
            response = self.session.get(JOBS_ENDPOINT, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch jobs page {page}: {exc}") from exc

        try:
            return response.json()
        except ValueError as exc:  # JSONDecodeError is a subclass
            raise ScraperError(f"Invalid JSON payload on page {page}: {exc}") from exc

    def _build_job(self, job: Dict[str, object]) -> DollarGeneralJob:
        slug = (job.get("slug") or "").strip()
        title = (job.get("title") or "").strip()
        if not slug or not title:
            raise ScraperError("Missing slug or title.")

        link = urljoin(CAREERS_ROOT, f"/jobs/{slug}")
        location = _normalize_location(job)
        posted_date = (job.get("posted_date") or "").strip() or None

        description_html = job.get("description") or ""
        description_text = _html_to_text(description_html)
        if not description_text and description_html:
            description_text = description_html

        metadata = {
            "slug": slug,
            "req_id": job.get("req_id"),
            "apply_url": job.get("apply_url"),
            "city": job.get("city"),
            "state": job.get("state"),
            "country": job.get("country"),
            "postal_code": job.get("postal_code"),
            "latitude": job.get("latitude"),
            "longitude": job.get("longitude"),
            "employment_type": job.get("employment_type"),
            "department": job.get("department"),
            "categories": job.get("categories"),
            "tags": {
                "tags1": job.get("tags1"),
                "tags2": job.get("tags2"),
                "tags3": job.get("tags3"),
                "tags4": job.get("tags4"),
            },
            "update_date": job.get("update_date"),
            "create_date": job.get("create_date"),
            "multiple_locations": job.get("multipleLocations"),
            "hiring_flow_name": job.get("hiring_flow_name"),
            "hiring_organization": job.get("hiring_organization"),
            "raw_location_name": job.get("location_name"),
            "location_type": job.get("location_type"),
            "full_location": job.get("full_location"),
            "short_location": job.get("short_location"),
        }

        return DollarGeneralJob(
            slug=slug,
            title=title,
            link=link,
            location=location,
            posted_date=posted_date,
            description_text=description_text,
            metadata=metadata,
        )


def persist_job(listing: DollarGeneralJob) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255] or None,
        "date": (listing.posted_date or "")[:100] or None,
        "description": listing.description_text[:10000],
        "metadata": listing.metadata,
    }

    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.link,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Persisted Dollar General job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Dollar General job listings.")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of job records to process.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=100,
        help="Number of jobs to request per API page (max 100).",
    )
    parser.add_argument(
        "--start-page",
        type=int,
        default=1,
        help="Page number to start from (1-indexed).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Maximum number of API pages to traverse.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Seconds to sleep between page fetches.",
    )
    parser.add_argument(
        "--sort-by",
        default="posted_date",
        choices=["posted_date", "relevance"],
        help="Sort order applied by the API (default: posted_date).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and display jobs without writing to the database.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s: %(message)s")

    client = DollarGeneralClient(delay=args.delay, page_size=args.page_size)
    totals = {"fetched": 0, "created": 0, "updated": 0, "skipped": 0, "errors": 0}

    try:
        for listing in client.iter_jobs(
            start_page=args.start_page,
            max_pages=args.max_pages,
            max_results=args.limit,
            sort_by=args.sort_by,
        ):
            totals["fetched"] += 1
            if args.dry_run:
                print(json.dumps(listing.__dict__, default=str))
                continue

            try:
                created = persist_job(listing)
            except Exception as exc:  # pragma: no cover - defensive persistence guard
                logging.error("Failed to persist job %s: %s", listing.link, exc)
                totals["errors"] += 1
                continue

            if created:
                totals["created"] += 1
            else:
                totals["updated"] += 1
    except KeyboardInterrupt:  # pragma: no cover - manual interruption
        logging.warning("Interrupted by user; exiting early.")
        return 1
    except Exception as exc:
        logging.error("Unexpected error during scrape: %s", exc)
        return 1

    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["dedupe"] = dedupe_summary

    logging.info(
        "Dollar General scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )
    if "dedupe" in totals:
        logging.debug("Deduplication summary: %s", totals["dedupe"])
    return 0 if not totals["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
