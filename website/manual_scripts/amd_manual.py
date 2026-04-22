#!/usr/bin/env python3
"""Manual scraper for AMD's careers API.

AMD publishes its job catalogue at https://careers.amd.com/api/jobs. This
script pages through that API, normalises each entry, and stores the resulting
records through the Django ORM for manual/on-demand ingestion.
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
# Constants & configuration
# ---------------------------------------------------------------------------
CAREERS_URL = "https://careers.amd.com/careers-home"
API_URL = "https://careers.amd.com/api/jobs"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 600), 30)
SCRAPER_QS = Scraper.objects.filter(company="AMD", url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched AMD; using id=%s.", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="AMD",
        url=CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the AMD scraper encounters unrecoverable issues."""


@dataclass
class JobListing:
    title: str
    link: str
    location: Optional[str]
    date: Optional[str]
    description: str
    metadata: Dict[str, object]


def _clean(value: Optional[str]) -> str:
    return (value or "").strip()


def _build_job_url(data: Dict[str, object]) -> str:
    slug = _clean(str(data.get("slug") or data.get("req_id") or ""))
    if not slug:
        return ""

    language = _clean(str(data.get("language") or "en-us")).lower()
    if language:
        return f"https://careers.amd.com/jobs/{slug}?lang={language}"
    return f"https://careers.amd.com/jobs/{slug}"


class AMDJobScraper:
    def __init__(
        self,
        *,
        page_size: int = 100,
        delay: float = 0.1,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.page_size = max(1, min(int(page_size), 100))
        self.delay = max(0.0, float(delay))
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

    def scrape(self, *, limit: Optional[int] = None) -> Iterable[JobListing]:
        page = 1
        fetched = 0
        total_count: Optional[int] = None

        while True:
            payload = self._fetch_page(page)
            jobs_payload = payload.get("jobs") or []
            if not jobs_payload:
                logging.info("No jobs returned on page %s; stopping.", page)
                break

            if total_count is None:
                total_count = self._parse_total(payload)
                logging.info("Discovered totalCount=%s.", total_count)

            for entry in jobs_payload:
                listing = self._build_listing(entry)
                if listing is None:
                    continue
                yield listing
                fetched += 1
                if limit is not None and fetched >= limit:
                    logging.info("Reached limit=%s; stopping.", limit)
                    return

            page += 1
            if total_count is not None and fetched >= total_count:
                logging.info("Reached totalCount=%s; pagination complete.", total_count)
                return
            if self.delay:
                time.sleep(self.delay)

    def _fetch_page(self, page: int) -> Dict[str, object]:
        params = {"page": str(page), "limit": str(self.page_size)}
        try:
            response = self.session.get(API_URL, params=params, timeout=40)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch AMD jobs page={page}: {exc}") from exc
        return response.json()

    def _parse_total(self, payload: Dict[str, object]) -> Optional[int]:
        total = payload.get("totalCount") or payload.get("count")
        try:
            return int(total)
        except (TypeError, ValueError):
            return None

    def _build_listing(self, entry: Dict[str, object]) -> Optional[JobListing]:
        data = entry.get("data") or {}
        title = _clean(data.get("title"))
        link = _build_job_url(data)
        apply_url = _clean(data.get("apply_url"))
        if not title or not link:
            return None

        location_candidates: List[str] = []
        for key in ("short_location", "full_location", "location_name"):
            value = _clean(data.get(key))
            if value and value not in location_candidates:
                location_candidates.append(value)
        location = "; ".join(location_candidates) or None

        metadata = {
            "req_id": data.get("req_id"),
            "employment_type": data.get("employment_type"),
            "country": data.get("country"),
            "state": data.get("state"),
            "city": data.get("city"),
            "postal_code": data.get("postal_code"),
            "category_names": [
                cat.get("name")
                for cat in (data.get("categories") or [])
                if isinstance(cat, dict) and cat.get("name")
            ],
            "hiring_organization": data.get("hiring_organization"),
            "latitude": data.get("latitude"),
            "longitude": data.get("longitude"),
            "apply_url": apply_url or None,
        }

        return JobListing(
            title=title,
            link=link,
            location=location,
            date=_clean(data.get("update_date") or data.get("posted_date")),
            description=_clean(data.get("description")),
            metadata={k: v for k, v in metadata.items() if v},
        )


def persist_listing(listing: JobListing) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255] or None,
        "date": (listing.date or "")[:100] or None,
        "description": listing.description[:10000],
        "metadata": listing.metadata,
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.link,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Persisted AMD job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape AMD careers listings.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum jobs to process.")
    parser.add_argument(
        "--page-size",
        type=int,
        default=100,
        help="Number of records requested per page (max 100).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.1,
        help="Seconds to sleep between page requests (default: 0.1).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print jobs without persisting.")
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

    scraper = AMDJobScraper(page_size=args.page_size, delay=args.delay)
    totals = {"fetched": 0, "created": 0}

    for listing in scraper.scrape(limit=args.limit):
        totals["fetched"] += 1
        if args.dry_run:
            print(json.dumps(listing.__dict__, default=str, ensure_ascii=False))
            continue
        created = persist_listing(listing)
        if created:
            totals["created"] += 1

    if not args.dry_run and totals["fetched"]:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        logging.info("Deduplication summary: %s", dedupe_summary)

    logging.info("AMD scraper finished - fetched=%(fetched)s created=%(created)s", totals)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
