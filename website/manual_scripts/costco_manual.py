#!/usr/bin/env python3
"""Manual scraper for https://www.costco.com/jobs.html.

This script consumes the public JSON API that powers Costco's careers site
(`https://careers.costco.com/api/jobs`) and upserts the results into the shared
`JobPosting` table. The implementation mirrors the patterns used by the other
manual scrapers so operations staff can schedule or run it from the dashboard.
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
from typing import Dict, Generator, Iterable, Optional

import requests

# ---------------------------------------------------------------------------
# Django setup (keeps parity with existing manual scripts)
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
CAREERS_LANDING_URL = "https://www.costco.com/jobs.html"
JOB_SEARCH_URL = "https://careers.costco.com/jobs"
JOBS_API_ENDPOINT = "https://careers.costco.com/api/jobs"
DETAIL_URL_TEMPLATE = "https://careers.costco.com/jobs/{slug}"
APPLY_URL_TEMPLATE = "https://careers-costco.icims.com/jobs/{slug}/login"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": JOB_SEARCH_URL,
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 60)
SCRAPER_QS = Scraper.objects.filter(company="Costco", url=CAREERS_LANDING_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.getLogger(__name__).warning(
            "Multiple Scraper rows matched Costco; using id=%s.", SCRAPER.id
        )
else:
    SCRAPER = Scraper.objects.create(
        company="Costco",
        url=CAREERS_LANDING_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the scraper encounters an unrecoverable error."""


@dataclass
class JobListing:
    slug: str
    title: str
    detail_url: str
    apply_url: str
    location: Optional[str]
    city: Optional[str]
    state: Optional[str]
    country: Optional[str]
    postal_code: Optional[str]
    posted_date: Optional[str]
    description: str
    metadata: Dict[str, object]


class CostcoJobScraper:
    """Thin wrapper around the public Costco careers JSON API."""

    def __init__(
        self,
        *,
        page_size: int = 100,
        language: str = "en-us",
        country: Optional[str] = None,
        state: Optional[str] = None,
        city: Optional[str] = None,
        search: Optional[str] = None,
        delay: float = 0.25,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.page_size = page_size
        self.language = language
        self.country = country
        self.state = state
        self.city = city
        self.search = search
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    def scrape(
        self,
        *,
        limit: Optional[int] = None,
        max_pages: Optional[int] = None,
    ) -> Generator[JobListing, None, None]:
        """Iterate over paginated API results and yield normalized job listings."""
        page = 1
        yielded = 0

        while True:
            if max_pages is not None and page > max_pages:
                self.logger.info("Reached max_pages=%s; stopping.", max_pages)
                return

            payload = self._build_query_params(page)
            data = self._fetch_page(payload)
            jobs = data.get("jobs") or []
            if not jobs:
                self.logger.info("No jobs returned for page %s; stopping.", page)
                return

            self.logger.debug(
                "Fetched %s jobs on page %s (limit=%s)", len(jobs), page, limit
            )

            for raw in jobs:
                job_data = raw.get("data") or {}
                try:
                    listing = self._transform(job_data)
                except ScraperError as exc:
                    self.logger.error("Failed to transform job payload: %s", exc)
                    continue

                yield listing
                yielded += 1
                if limit is not None and yielded >= limit:
                    self.logger.info("Limit %s reached; stopping scrape.", limit)
                    return

            page += 1
            if self.delay:
                time.sleep(self.delay)

    def _build_query_params(self, page: int) -> Dict[str, object]:
        params: Dict[str, object] = {
            "page": page,
            "language": self.language,
            "limit": self.page_size,
        }
        if self.country:
            params["country"] = self.country
        if self.state:
            params["state"] = self.state
        if self.city:
            params["city"] = self.city
        if self.search:
            params["search"] = self.search
        return params

    def _fetch_page(self, params: Dict[str, object]) -> Dict[str, object]:
        self.logger.debug("Requesting %s with params=%s", JOBS_API_ENDPOINT, params)
        try:
            resp = self.session.get(JOBS_API_ENDPOINT, params=params, timeout=40)
        except requests.RequestException as exc:  # pragma: no cover - network failure
            raise ScraperError(f"Request failed: {exc}") from exc

        if resp.status_code >= 400:
            snippet = resp.text[:200].strip()
            raise ScraperError(
                f"Jobs API returned {resp.status_code} for params={params}: {snippet or 'no body'}"
            )

        try:
            return resp.json()
        except ValueError as exc:
            raise ScraperError(f"Failed to decode JSON: {exc}") from exc

    def _transform(self, job: Dict[str, object]) -> JobListing:
        slug = str(job.get("slug") or job.get("req_id") or "").strip()
        if not slug:
            raise ScraperError("Job payload missing slug/req_id.")

        title = (job.get("title") or "").strip()
        if not title:
            raise ScraperError(f"Job {slug} missing title.")

        description = (job.get("description") or "").strip() or "Description unavailable."

        city = _clean_str(job.get("city"))
        state = _clean_str(job.get("state"))
        country = _clean_str(job.get("country"))
        location = _derive_location(job, city=city, state=state, fallback=country)
        postal_code = _clean_str(job.get("postal_code"))
        posted_date = _clean_str(job.get("posted_date"))

        detail_url = DETAIL_URL_TEMPLATE.format(slug=slug)
        apply_url = _clean_str(job.get("apply_url")) or APPLY_URL_TEMPLATE.format(slug=slug)

        metadata: Dict[str, object] = {
            "req_id": job.get("req_id"),
            "location_name": job.get("location_name"),
            "full_location": job.get("full_location"),
            "short_location": job.get("short_location"),
            "street_address": job.get("street_address"),
            "latitude": job.get("latitude"),
            "longitude": job.get("longitude"),
            "country_code": job.get("country_code"),
            "location_type": job.get("location_type"),
            "language": job.get("language"),
            "languages": job.get("languages"),
            "promotion_value": job.get("promotion_value"),
            "hiring_organization": job.get("hiring_organization"),
            "hiring_organization_logo": job.get("hiring_organization_logo"),
            "applyable": job.get("applyable"),
            "searchable": job.get("searchable"),
            "li_easy_applyable": job.get("li_easy_applyable"),
            "ats_code": job.get("ats_code"),
            "update_date": job.get("update_date"),
            "create_date": job.get("create_date"),
            "category": job.get("category"),
            "multipleLocations": job.get("multipleLocations"),
            "meta_data": job.get("meta_data"),
            "apply_url": apply_url,
        }

        return JobListing(
            slug=slug,
            title=title,
            detail_url=detail_url,
            apply_url=apply_url,
            location=location,
            city=city,
            state=state,
            country=country,
            postal_code=postal_code,
            posted_date=posted_date,
            description=description,
            metadata=metadata,
        )


def _clean_str(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned or None
    return str(value).strip() or None


def _derive_location(
    job: Dict[str, object],
    *,
    city: Optional[str],
    state: Optional[str],
    fallback: Optional[str],
) -> Optional[str]:
    location_name = _clean_str(job.get("location_name"))
    full_location = _clean_str(job.get("full_location"))
    parts = [city, state]
    if any(parts):
        derived = ", ".join([part for part in parts if part])
        return derived
    return location_name or full_location or fallback


def persist_listing(listing: JobListing) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255] or None,
        "date": (listing.posted_date or "")[:100] or None,
        "description": listing.description[:10000],
        "metadata": listing.metadata,
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Persisted job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Costco careers job listings.")
    parser.add_argument("--language", default="en-us", help="Language code accepted by the API (default: en-us).")
    parser.add_argument("--country", default=None, help="Optional country filter (exact match required).")
    parser.add_argument("--state", default=None, help="Optional state/province filter (exact match required).")
    parser.add_argument("--city", default=None, help="Optional city filter (exact match required).")
    parser.add_argument("--search", default=None, help="Optional search keyword string.")
    parser.add_argument(
        "--page-size",
        type=int,
        default=100,
        help="Number of jobs to request per API page (max 100, default 100).",
    )
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument("--max-pages", type=int, default=None, help="Maximum number of API pages to traverse.")
    parser.add_argument("--delay", type=float, default=0.25, help="Seconds to sleep between page requests.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch jobs and emit JSON without touching the database.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO).",
    )
    args = parser.parse_args(argv)
    if args.page_size < 1 or args.page_size > 100:
        parser.error("--page-size must be between 1 and 100.")
    return args


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s: %(message)s")
    logger = logging.getLogger("costco")

    scraper = CostcoJobScraper(
        page_size=args.page_size,
        language=args.language,
        country=args.country,
        state=args.state,
        city=args.city,
        search=args.search,
        delay=args.delay,
    )

    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    for listing in scraper.scrape(limit=args.limit, max_pages=args.max_pages):
        totals["fetched"] += 1
        if args.dry_run:
            payload = {
                "slug": listing.slug,
                "title": listing.title,
                "detail_url": listing.detail_url,
                "apply_url": listing.apply_url,
                "location": listing.location,
                "city": listing.city,
                "state": listing.state,
                "country": listing.country,
                "postal_code": listing.postal_code,
                "posted_date": listing.posted_date,
                "description": listing.description,
            }
            print(json.dumps(payload, ensure_ascii=False))
            continue

        try:
            created = persist_listing(listing)
        except Exception as exc:  # pragma: no cover - persistence error handling
            logger.error("Failed to persist %s: %s", listing.detail_url, exc)
            totals["errors"] += 1
            continue

        if created:
            totals["created"] += 1
        else:
            totals["updated"] += 1

    exit_code = 0
    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        logger.info("Deduplication summary: %s", dedupe_summary)
        if totals["errors"]:
            exit_code = 1

    logger.info(
        "Costco scraper finished - fetched=%(fetched)s created=%(created)s updated=%(updated)s errors=%(errors)s",
        totals,
    )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
