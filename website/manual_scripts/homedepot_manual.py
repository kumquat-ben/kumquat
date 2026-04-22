#!/usr/bin/env python3
"""Manual scraper for The Home Depot careers portal (Workday-powered).

This script queries the public Google Talent Solutions endpoint backing
https://careers.homedepot.com/job-search-results and stores the results in the
shared ``JobPosting`` table. It mirrors the conventions used by the other
manual scrapers in this repository so operations staff can schedule or run it
ad hoc.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Django setup
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
RESULTS_URL = "https://careers.homedepot.com/job-search-results"
API_ENDPOINT = "https://jobsapi-google.m-cloud.io/api/job/search"
COMPANY_NAME = "companies/8454851f-07b7-4e4c-9b5f-00e0ffbfcb09"
ATTRIBUTE_FILTER = '(ats_portalid="KBR-5032" OR ats_portalid="Workday")'
ORDER_BY = "posting_publish_time desc"
DEFAULT_PAGE_SIZE = 100
DEFAULT_DELAY = 0.15
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/127.0.0.0 Safari/537.36"
)
METADATA_KEYS = [
    "id",
    "ref",
    "ats_portalid",
    "clientid",
    "primary_category",
    "addtnl_categories",
    "department",
    "job_type",
    "employment_type",
    "brand",
    "location_type",
    "open_date",
    "close_date",
    "seo_url",
    "store_id",
    "primary_address",
    "primary_city",
    "primary_state",
    "primary_country",
    "google_categories",
    "addtnl_locations",
    "custom_fields",
]

SCRAPER_QS = Scraper.objects.filter(company="Home Depot", url=RESULTS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning(
            "Multiple Scraper rows matched Home Depot; using id=%s.", SCRAPER.id
        )
else:
    SCRAPER = Scraper.objects.create(
        company="Home Depot",
        url=RESULTS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=6000,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def html_to_text(html: Optional[str]) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = soup.get_text("\n", strip=True)
    return text.strip()


def format_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        normalized = value.rstrip("Z")
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return value[:10] or value
    return parsed.date().isoformat()


def build_metadata(job: Dict[str, object]) -> Dict[str, object]:
    metadata: Dict[str, object] = {}
    for key in METADATA_KEYS:
        if key in job:
            value = job[key]
            if value not in (None, "", []):
                metadata[key] = value
    return metadata


def join_location(job: Dict[str, object]) -> str:
    parts: List[str] = []
    for key in ("primary_city", "primary_state", "primary_country"):
        value = job.get(key)
        if isinstance(value, str) and value.strip():
            parts.append(value.strip())
    return ", ".join(parts)


def pick_link(job: Dict[str, object]) -> Optional[str]:
    for key in ("url", "seo_url"):
        value = job.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    job_id = job.get("id")
    if job_id:
        return f"https://careers.homedepot.com/job/{job_id}"
    return None


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
@dataclass
class HomeDepotJob:
    job_id: str
    title: str
    link: str
    location: str
    posted_on: Optional[str]
    description: str
    metadata: Dict[str, object]


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------
class HomeDepotJobScraper:
    def __init__(
        self,
        *,
        page_size: int = DEFAULT_PAGE_SIZE,
        delay: float = DEFAULT_DELAY,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.page_size = max(1, min(int(page_size), 500))
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.setdefault("User-Agent", DEFAULT_USER_AGENT)
        self.session.headers.setdefault("Accept", "application/json")
        self.logger = logging.getLogger(self.__class__.__name__)

    def _build_params(self, offset: int) -> Dict[str, str]:
        return {
            "pageSize": str(self.page_size),
            "offset": str(offset),
            "companyName": COMPANY_NAME,
            "customAttributeFilter": ATTRIBUTE_FILTER,
            "orderBy": ORDER_BY,
        }

    def _request_page(self, offset: int) -> Dict[str, object]:
        params = self._build_params(offset)
        self.logger.debug("Requesting jobs offset=%s pageSize=%s", offset, self.page_size)
        response = self.session.get(API_ENDPOINT, params=params, timeout=40)
        response.raise_for_status()
        return response.json()

    def _to_listing(self, job: Dict[str, object]) -> Optional[HomeDepotJob]:
        title = job.get("title")
        link = pick_link(job)
        job_id = job.get("id")
        if not (title and link and job_id):
            return None
        location = join_location(job)
        description = html_to_text(job.get("description"))
        posted_on = format_date(job.get("open_date"))
        metadata = build_metadata(job)
        metadata.setdefault("company_name", job.get("company_name"))
        metadata.setdefault("brand", job.get("brand"))
        return HomeDepotJob(
            job_id=str(job_id),
            title=str(title).strip(),
            link=link,
            location=location,
            posted_on=posted_on,
            description=description,
            metadata=metadata,
        )

    def scrape(self, *, limit: Optional[int] = None) -> Iterator[HomeDepotJob]:
        offset = 0
        fetched = 0
        total_hits: Optional[int] = None

        while True:
            payload = self._request_page(offset)
            if total_hits is None:
                total_hits = int(payload.get("totalHits") or 0)
                self.logger.info(
                    "Detected %s total Home Depot postings.", total_hits
                )

            results = payload.get("searchResults") or []
            if not results:
                self.logger.debug("No results at offset=%s; stopping.", offset)
                break

            for result in results:
                job = (result or {}).get("job") or {}
                listing = self._to_listing(job)
                if not listing:
                    self.logger.debug("Skipping job with insufficient data: %s", job)
                    continue
                yield listing
                fetched += 1
                if limit is not None and fetched >= limit:
                    self.logger.info("Limit of %s reached; stopping.", limit)
                    return

            offset += self.page_size
            if total_hits is not None and offset >= total_hits:
                break
            if self.delay:
                time.sleep(self.delay)


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------
def persist_listing(listing: HomeDepotJob) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": listing.location[:255] if listing.location else "",
        "date": listing.posted_on or "",
        "description": listing.description[:10000],
        "metadata": listing.metadata,
    }
    _, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.link,
        defaults=defaults,
    )
    return created


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape Home Depot careers listings."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of jobs to process.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help="Number of jobs to request per API page (max 500).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help="Seconds to sleep between API pages.",
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
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(levelname)s: %(message)s",
    )

    scraper = HomeDepotJobScraper(page_size=args.page_size, delay=args.delay)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    for listing in scraper.scrape(limit=args.limit):
        totals["fetched"] += 1
        if args.dry_run:
            print(
                json.dumps(
                    {
                        "job_id": listing.job_id,
                        "title": listing.title,
                        "link": listing.link,
                        "location": listing.location,
                        "posted_on": listing.posted_on,
                    },
                    ensure_ascii=False,
                )
            )
            continue

        try:
            created = persist_listing(listing)
            if created:
                totals["created"] += 1
            else:
                totals["updated"] += 1
        except Exception as exc:  # pragma: no cover - persistence failure path
            logging.error("Failed to persist job %s: %s", listing.link, exc)
            totals["errors"] += 1

    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["dedupe"] = dedupe_summary

    logging.info(
        "Home Depot scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )
    return 0 if not totals["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
