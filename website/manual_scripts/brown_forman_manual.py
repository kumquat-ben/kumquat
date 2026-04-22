#!/usr/bin/env python3
"""Manual scraper for Brown-Forman careers (https://careers.brown-forman.com)."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Django bootstrap
# ---------------------------------------------------------------------------
CURRENT_FILE = Path(__file__).resolve()
parents = list(CURRENT_FILE.parents)
default_backend_dir = parents[2] if len(parents) > 2 else parents[-1]
BACKEND_DIR = next(
    (candidate for candidate in parents if (candidate / "manage.py").exists()),
    default_backend_dir,
)
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "website.settings")

import django  # noqa: E402

django.setup()

from django.conf import settings  # noqa: E402

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CAREERS_BASE_URL = "https://careers.brown-forman.com"
SEARCH_PAGE_URL = f"{CAREERS_BASE_URL}/jobs"
API_BASE_URL = "https://prod-search-api.jobsyn.org/api"
SEARCH_ENDPOINT = "/v1/solr/search"
DEFAULT_BUID = "27241"
DEFAULT_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "X-Origin": "careers.brown-forman.com",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}
DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 900), 60)
DEFAULT_REQUEST_TIMEOUT = max(getattr(settings, "MANUAL_SCRIPT_REQUEST_TIMEOUT", 30), 10)

SCRAPER_QS = Scraper.objects.filter(
    company="Brown-Forman",
    url=SEARCH_PAGE_URL,
).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Brown-Forman scrapers found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Brown-Forman",
        url=SEARCH_PAGE_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
class ScraperError(RuntimeError):
    """Raised when the scraper encounters an unrecoverable issue."""


@dataclass
class JobRecord:
    title: str
    link: str
    location: str
    date_posted: Optional[str]
    description_text: str
    description_html: Optional[str]
    metadata: Dict[str, Any]


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
def slugify(value: str) -> str:
    """Return a URL-safe slug similar to the site's client logic."""
    normalized = unicodedata.normalize("NFKD", value or "")
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    tokens = re.findall(r"[A-Za-z0-9]+", ascii_text)
    return "-".join(token.lower() for token in tokens if token) or "job"


def html_to_text(html: Optional[str]) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    return text.strip()


def build_job_url(job: Dict[str, Any]) -> Optional[str]:
    guid = (job.get("guid") or "").strip()
    if not guid:
        return None
    title_slug = (job.get("title_slug") or "").strip().strip("/")
    if not title_slug:
        title_slug = slugify(job.get("title_exact") or job.get("title") or "")
    location_raw = (job.get("location_exact") or job.get("city_exact") or job.get("country_exact") or "").strip()
    location_slug = slugify(location_raw or "global")
    return f"{CAREERS_BASE_URL}/{location_slug}/{title_slug}/{guid}/job/"


def compact_metadata(job: Dict[str, Any], job_url: Optional[str], page: int) -> Dict[str, Any]:
    payload = dict(job)
    description = payload.pop("description", None)
    metadata: Dict[str, Any] = {
        "guid": payload.get("guid"),
        "reqid": payload.get("reqid"),
        "job_url": job_url,
        "api_page": page,
        "api_payload": payload,
    }
    if description:
        metadata["description_length"] = len(description)
    return metadata


# ---------------------------------------------------------------------------
# Scraper implementation
# ---------------------------------------------------------------------------
class BrownFormanJobScraper:
    def __init__(
        self,
        *,
        request_timeout: int = DEFAULT_REQUEST_TIMEOUT,
        delay: float = 0.0,
        session: Optional[requests.Session] = None,
        page_size: int = 10,
    ) -> None:
        self.request_timeout = max(request_timeout, 5)
        self.delay = max(delay, 0.0)
        self.page_size = max(int(page_size), 1)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.total_available: Optional[int] = None
        self.total_pages: Optional[int] = None
        self.pages_processed = 0
        self.jobs_seen = 0

    def scrape(self, *, max_pages: Optional[int] = None, limit: Optional[int] = None) -> Iterator[JobRecord]:
        page = 1
        emitted = 0
        while True:
            if max_pages is not None and page > max_pages:
                logging.info("Reached max_pages=%s; stopping", max_pages)
                break

            logging.debug("Fetching API page %s", page)
            data = self._fetch_page(page)
            jobs = data.get("jobs") or []
            pagination = data.get("pagination") or {}

            if self.total_available is None:
                self.total_available = int(pagination.get("total") or 0)
                self.total_pages = int(pagination.get("total_pages") or 0)
                logging.info(
                    "Brown-Forman API reports %s total jobs across %s pages",
                    self.total_available,
                    self.total_pages,
                )

            self.pages_processed += 1
            self.jobs_seen += len(jobs)

            for job in jobs:
                record = self._transform_job(job, page=page)
                if not record:
                    continue
                yield record
                emitted += 1
                if limit is not None and emitted >= limit:
                    logging.info("Reached limit=%s; stopping after API page %s", limit, page)
                    return

            has_more = bool(pagination.get("has_more_pages"))
            if not has_more:
                logging.debug("API indicates no more pages after %s", page)
                break

            page += 1
            if self.delay:
                time.sleep(self.delay)

    def _fetch_page(self, page: int) -> Dict[str, Any]:
        params = {
            "buids": DEFAULT_BUID,
            "page": page,
        }
        if self.page_size != 10:
            params["page_size"] = self.page_size
        url = f"{API_BASE_URL}{SEARCH_ENDPOINT}"
        try:
            response = self.session.get(url, params=params, timeout=self.request_timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch page {page}: {exc}") from exc
        try:
            data: Dict[str, Any] = response.json()
        except ValueError as exc:
            raise ScraperError(f"Invalid JSON payload for page {page}") from exc
        if not isinstance(data, dict) or "jobs" not in data:
            raise ScraperError(f"Unexpected API response structure for page {page}")
        return data

    def _transform_job(self, job: Dict[str, Any], *, page: int) -> Optional[JobRecord]:
        title = (job.get("title_exact") or job.get("title") or "").strip()
        guid = (job.get("guid") or "").strip()
        if not title or not guid:
            logging.debug("Skipping job missing mandatory fields: %s", job)
            return None

        link = build_job_url(job)
        if not link:
            logging.debug("Skipping job %s due to missing link data", guid)
            return None

        location = (job.get("location_exact") or job.get("city_exact") or job.get("country_exact") or "").strip()
        description_html = (job.get("description") or "").strip()
        description_text = html_to_text(description_html)

        metadata = compact_metadata(job, link, page)
        metadata["location_exact"] = job.get("location_exact")
        metadata["city_exact"] = job.get("city_exact")
        metadata["country_exact"] = job.get("country_exact")

        date_posted = (
            (job.get("date_added") or "").strip()
            or (job.get("date_updated") or "").strip()
            or (job.get("date_new") or "").strip()
        )

        return JobRecord(
            title=title,
            link=link,
            location=location,
            date_posted=date_posted or None,
            description_text=description_text,
            description_html=description_html or None,
            metadata=metadata,
        )

    def summary(self) -> Dict[str, Any]:
        return {
            "total_available": self.total_available,
            "total_pages": self.total_pages,
            "pages_processed": self.pages_processed,
            "jobs_seen": self.jobs_seen,
        }


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------
def store_job(record: JobRecord) -> None:
    metadata = record.metadata or {}
    if record.description_html:
        metadata.setdefault("description_html", record.description_html)
    JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=record.link,
        defaults={
            "title": (record.title or "")[:255],
            "location": (record.location or "")[:255],
            "date": (record.date_posted or "")[:100],
            "description": (record.description_text or "")[:10000],
            "metadata": metadata or None,
        },
    )


def run_scrape(args: argparse.Namespace) -> Dict[str, Any]:
    scraper = BrownFormanJobScraper(
        request_timeout=args.request_timeout,
        delay=args.delay,
        page_size=args.page_size,
    )
    count = 0
    for record in scraper.scrape(max_pages=args.max_pages, limit=args.limit):
        store_job(record)
        count += 1
    summary = scraper.summary()
    summary.update(
        {
            "company": "Brown-Forman",
            "jobs_written": count,
            "search_url": SEARCH_PAGE_URL,
            "max_pages": args.max_pages,
            "limit": args.limit,
            "page_size": args.page_size,
        }
    )
    return summary


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Brown-Forman careers manual scraper")
    parser.add_argument("--max-pages", type=int, default=None, help="Stop after processing this many API pages")
    parser.add_argument("--limit", type=int, default=None, help="Stop after storing this many jobs")
    parser.add_argument("--delay", type=float, default=0.0, help="Delay (seconds) between API page fetches")
    parser.add_argument(
        "--request-timeout",
        type=int,
        default=DEFAULT_REQUEST_TIMEOUT,
        help="Per-request timeout in seconds",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=10,
        help="Requested page size (API currently defaults to 10)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    start_time = time.time()

    try:
        summary = run_scrape(args)
    except ScraperError as exc:
        logging.error("Brown-Forman scrape failed: %s", exc)
        return 1

    elapsed = time.time() - start_time
    dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)

    output = {
        "summary": summary,
        "elapsed_seconds": elapsed,
        "dedupe": dedupe_summary,
    }
    logging.info("Scrape summary: %s", json.dumps(output))
    print(json.dumps(output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
