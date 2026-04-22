#!/usr/bin/env python3
"""Manual scraper for General Mills careers (Jibe/iCIMS-powered)."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests

# ---------------------------------------------------------------------------
# Django bootstrap
# ---------------------------------------------------------------------------
BACKEND_DIR = Path(__file__).resolve().parents[1]
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
CAREERS_ROOT = "https://careers.generalmills.com/careers"
API_URL = "https://careers.generalmills.com/api/jobs"
DETAIL_TEMPLATE = "https://careers.generalmills.com/careers/jobs/{slug}?lang={lang}"
REQUEST_TIMEOUT = (15, 45)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": CAREERS_ROOT,
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 1800), 30)
SCRAPER_QS = Scraper.objects.filter(company="General Mills", url=CAREERS_ROOT).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched General Mills careers; using id=%s.", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="General Mills",
        url=CAREERS_ROOT,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(RuntimeError):
    """Raised when the General Mills scrape pipeline cannot proceed."""


@dataclass
class JobListing:
    link: str
    title: str
    location: Optional[str]
    date: Optional[str]
    description: str
    metadata: Dict[str, object]


class GeneralMillsClient:
    def __init__(
        self,
        *,
        lang: str = "en-US",
        page_size: int = 50,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.lang = lang
        self.page_size = max(1, page_size)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

    def iter_listings(self, limit: Optional[int] = None) -> Iterable[JobListing]:
        fetched = 0
        total: Optional[int] = None
        page = 1

        while True:
            payload = self._fetch_page(page)
            jobs = payload.get("jobs") or []
            if not jobs:
                break

            for raw_job in jobs:
                listing = self._transform_job(raw_job)
                yield listing
                fetched += 1
                if limit and fetched >= limit:
                    return

            if total is None:
                total = payload.get("totalCount") or payload.get("count")
            page += 1

            if total and fetched >= total:
                break

    def _fetch_page(self, page: int) -> Dict[str, object]:
        params = {
            "lang": self.lang,
            "limit": self.page_size,
            "page": page,
        }
        try:
            response = self.session.get(API_URL, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:  # ValueError covers json decoding
            raise ScraperError(f"Failed to fetch jobs page {page}: {exc}") from exc

    def _transform_job(self, raw_job: Dict[str, object]) -> JobListing:
        data = raw_job.get("data") if isinstance(raw_job, dict) else None
        if not isinstance(data, dict):
            raise ScraperError("Encountered malformed job payload without 'data'.")

        slug = data.get("slug") or data.get("req_id")
        lang = (data.get("language") or self.lang or "en-US").lower()
        detail_link = (
            DETAIL_TEMPLATE.format(slug=slug, lang=lang)
            if slug
            else data.get("apply_url") or CAREERS_ROOT
        )

        title = (data.get("title") or "").strip()
        location = (
            (data.get("full_location") or data.get("short_location") or data.get("location_name") or "")
            .strip()
            or None
        )
        posted_date = (data.get("posted_date") or "").strip() or None
        description = (data.get("description") or "").strip()

        categories = []
        raw_categories = data.get("categories") or []
        if isinstance(raw_categories, list):
            categories = [
                (entry.get("name") or "").strip()
                for entry in raw_categories
                if isinstance(entry, dict) and entry.get("name")
            ]

        metadata: Dict[str, object] = {
            "req_id": data.get("req_id"),
            "slug": slug,
            "categories": categories,
            "tags": data.get("tags"),
            "tags2": data.get("tags2"),
            "employment_type": data.get("employment_type"),
            "apply_url": data.get("apply_url"),
            "city": data.get("city"),
            "country": data.get("country"),
            "country_code": data.get("country_code"),
            "postal_code": data.get("postal_code"),
            "multiple_locations": data.get("multipleLocations"),
            "latitude": data.get("latitude"),
            "longitude": data.get("longitude"),
            "external": data.get("external"),
            "internal": data.get("internal"),
            "searchable": data.get("searchable"),
            "applyable": data.get("applyable"),
            "li_easy_applyable": data.get("li_easy_applyable"),
            "ats_code": data.get("ats_code"),
            "update_date": data.get("update_date"),
            "create_date": data.get("create_date"),
            "meta_data": data.get("meta_data"),
        }

        return JobListing(
            link=detail_link,
            title=title,
            location=location,
            date=posted_date,
            description=description,
            metadata=metadata,
        )


def store_listing(listing: JobListing) -> None:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255],
        "date": (listing.date or "")[:100],
        "description": listing.description[:10000],
        "metadata": listing.metadata,
    }
    JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.link,
        defaults=defaults,
    )


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="General Mills careers manual scraper")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of job records to process")
    parser.add_argument(
        "--page-size",
        type=int,
        default=50,
        help="Number of records to request per API call (default: 50)",
    )
    parser.add_argument(
        "--lang",
        type=str,
        default="en-US",
        help="Locale to request from the API (default: en-US)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity",
    )
    return parser.parse_args(argv)


def run_scrape(args: argparse.Namespace) -> Dict[str, object]:
    client = GeneralMillsClient(lang=args.lang, page_size=args.page_size)
    processed = 0
    for listing in client.iter_listings(limit=args.limit):
        store_listing(listing)
        processed += 1
    dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
    return {"processed_jobs": processed, "deduplicated": dedupe_summary}


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    start_time = time.time()
    try:
        outcome = run_scrape(args)
    except ScraperError as exc:
        logging.error("Scrape failed: %s", exc)
        return 1

    duration = time.time() - start_time
    summary = {
        "company": "General Mills",
        "site": CAREERS_ROOT,
        "elapsed_seconds": round(duration, 2),
        **outcome,
    }
    logging.info("Summary: %s", json.dumps(summary))
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

