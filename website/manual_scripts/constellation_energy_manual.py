#!/usr/bin/env python3
"""Manual scraper for Constellation Energy careers (Jibe-powered portal)."""
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
from typing import Dict, Iterable, Iterator, Optional

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

from django.conf import settings  # noqa: E402
from django.db import IntegrityError  # noqa: E402

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

# ---------------------------------------------------------------------------
# Constants & configuration
# ---------------------------------------------------------------------------
CAREERS_LANDING_URL = "https://www.constellationenergy.com/careers/careers-search/view-all-jobs.html"
JOBS_BASE_URL = "https://jobs.constellationenergy.com"
API_ENDPOINT = f"{JOBS_BASE_URL}/api/jobs"
JOB_DETAIL_TEMPLATE = JOBS_BASE_URL + "/jobs/{slug}"
REQUEST_TIMEOUT = (10, 30)
DEFAULT_DELAY = 0.25
DEFAULT_PAGE_SIZE = 10

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": CAREERS_LANDING_URL,
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 2400), 120)
SCRAPER_QS = Scraper.objects.filter(company="Constellation Energy", url=CAREERS_LANDING_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning(
            "Multiple Scraper rows matched Constellation Energy; using id=%s.",
            SCRAPER.id,
        )
else:
    SCRAPER = Scraper.objects.create(
        company="Constellation Energy",
        url=CAREERS_LANDING_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


# ---------------------------------------------------------------------------
# Data structures & utilities
# ---------------------------------------------------------------------------
class ScraperError(Exception):
    """Raised when the Constellation Energy scrape cannot proceed."""


@dataclass
class JobListing:
    slug: str
    title: str
    detail_url: str
    location: Optional[str]
    posted_date: Optional[str]
    description: str
    apply_url: Optional[str]
    metadata: Dict[str, object]
    latitude: Optional[float]
    longitude: Optional[float]


def _html_fragment_to_text(fragment: Optional[str]) -> str:
    if not fragment:
        return ""

    soup = BeautifulSoup(fragment, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()

    text = soup.get_text("\n", strip=True)
    lines = [line.strip() for line in text.splitlines()]
    compact_lines = [line for line in lines if line]
    return "\n".join(compact_lines)


def _format_posted_date(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None

    value = raw.strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(value, fmt)
        except ValueError:
            continue
        return dt.date().isoformat()

    # If parsing fails, return the raw string (up to 100 chars to keep it bounded)
    return value[:100]


def _compact_dict(data: Dict[str, object]) -> Dict[str, object]:
    return {
        key: value
        for key, value in data.items()
        if value not in (None, "", [], {}, ())
    }


def _safe_float(value: object) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _compose_description(job: Dict[str, object]) -> str:
    sections = []
    for key in ("description", "responsibilities", "qualifications"):
        cleaned = _html_fragment_to_text(job.get(key))
        if cleaned and cleaned not in sections:
            sections.append(cleaned)

    combined = "\n\n".join(sections).strip()
    if not combined:
        return "Description unavailable."

    return combined[:10000]


def _build_metadata(job: Dict[str, object], posted_iso: Optional[str]) -> Dict[str, object]:
    salary = _compact_dict(
        {
            "value": job.get("salary_value"),
            "min": job.get("salary_min_value"),
            "max": job.get("salary_max_value"),
        }
    )

    metadata = _compact_dict(
        {
            "req_id": job.get("req_id"),
            "apply_url": job.get("apply_url"),
            "city": job.get("city"),
            "state": job.get("state"),
            "country": job.get("country"),
            "postal_code": job.get("postal_code"),
            "street_address": job.get("street_address"),
            "location_type": job.get("location_type"),
            "location_name": job.get("location_name"),
            "full_location": job.get("full_location"),
            "short_location": job.get("short_location"),
            "department": job.get("department"),
            "employment_type": job.get("employment_type"),
            "categories": job.get("categories"),
            "tags": job.get("tags"),
            "tags1": job.get("tags1"),
            "tags2": job.get("tags2"),
            "tags3": job.get("tags3"),
            "tags5": job.get("tags5"),
            "tags7": job.get("tags7"),
            "tags8": job.get("tags8"),
            "tags9": job.get("tags9"),
            "posted_date_iso": posted_iso,
            "posting_expiry_date": job.get("posting_expiry_date"),
            "update_date": job.get("update_date"),
            "create_date": job.get("create_date"),
            "multiple_locations": job.get("multipleLocations"),
        }
    )
    if salary:
        metadata["salary"] = salary
    return metadata


# ---------------------------------------------------------------------------
# HTTP client
# ---------------------------------------------------------------------------
class ConstellationCareersClient:
    def __init__(
        self,
        *,
        delay: float = DEFAULT_DELAY,
        page_size: int = DEFAULT_PAGE_SIZE,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.delay = max(0.0, delay)
        self.page_size = max(1, page_size)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    def iter_jobs(self, *, limit: Optional[int] = None) -> Iterator[JobListing]:
        seen = 0
        page = 1
        reported_total: Optional[int] = None
        resolved_page_size: Optional[int] = None

        while True:
            payload = {
                "fields": "job",
                "type": "all",
                "page": page,
                "per_page": self.page_size,
            }
            data = self._fetch_page(payload)
            jobs = data.get("jobs") or []
            if not jobs:
                if page == 1:
                    self.logger.warning("First page returned zero jobs.")
                else:
                    self.logger.info("No jobs returned for page %s; stopping.", page)
                break

            if reported_total is None:
                try:
                    reported_total = int(data.get("totalCount") or 0)
                except (TypeError, ValueError):
                    reported_total = None

            if resolved_page_size is None:
                resolved_page_size = len(jobs) or self.page_size

            for wrapper in jobs:
                job = wrapper.get("data") or {}
                try:
                    listing = self._transform_job(job)
                except ScraperError as exc:
                    self.logger.warning("Skipping job due to error: %s", exc)
                    continue

                yield listing
                seen += 1
                if limit is not None and seen >= limit:
                    return

            page += 1
            if reported_total is not None and resolved_page_size:
                if (page - 1) * resolved_page_size >= reported_total:
                    break

            if self.delay:
                time.sleep(self.delay)

    def _fetch_page(self, params: Dict[str, object]) -> Dict[str, object]:
        try:
            response = self.session.get(API_ENDPOINT, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch jobs API page: {exc}") from exc

        try:
            return response.json()
        except ValueError as exc:
            raise ScraperError("Jobs API returned invalid JSON.") from exc

    def _transform_job(self, job: Dict[str, object]) -> JobListing:
        slug = str(job.get("slug") or job.get("req_id") or "").strip()
        title = (job.get("title") or "").strip()
        if not slug or not title:
            raise ScraperError("Job missing slug or title.")

        location_text = (job.get("short_location") or job.get("full_location") or "").strip() or None
        posted_iso = job.get("posted_date") or job.get("update_date") or job.get("create_date")
        posted_date = _format_posted_date(posted_iso if isinstance(posted_iso, str) else None)
        description = _compose_description(job)
        apply_url = job.get("apply_url") or None

        metadata = _build_metadata(job, posted_iso if isinstance(posted_iso, str) else None)
        lat = _safe_float(job.get("latitude"))
        lon = _safe_float(job.get("longitude"))

        detail_url = JOB_DETAIL_TEMPLATE.format(slug=slug)
        return JobListing(
            slug=slug,
            title=title,
            detail_url=detail_url,
            location=location_text,
            posted_date=posted_date,
            description=description,
            apply_url=apply_url,
            metadata=metadata,
            latitude=lat,
            longitude=lon,
        )


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------
def persist_listing(listing: JobListing) -> bool:
    metadata = dict(listing.metadata)
    if listing.apply_url and "apply_url" not in metadata:
        metadata["apply_url"] = listing.apply_url

    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255] or None,
        "date": (listing.posted_date or "")[:100] or None,
        "description": listing.description[:10000],
        "metadata": metadata,
    }

    if listing.latitude is not None:
        defaults["location_latitude"] = listing.latitude
    if listing.longitude is not None:
        defaults["location_longitude"] = listing.longitude

    normalized_location = metadata.get("full_location") or metadata.get("short_location")
    if normalized_location:
        defaults["normalized_location"] = str(normalized_location)[:255]

    try:
        obj, created = JobPosting.objects.update_or_create(
            scraper=SCRAPER,
            link=listing.detail_url,
            defaults=defaults,
        )
    except IntegrityError as exc:
        raise ScraperError(f"Failed to persist job {listing.slug}: {exc}") from exc

    logging.getLogger("persist").debug(
        "Persisted Constellation job '%s' (created=%s, id=%s)",
        obj.title,
        created,
        obj.id,
    )
    return created


# ---------------------------------------------------------------------------
# CLI orchestration
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Constellation Energy job listings.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help="Number of jobs to request per API page.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help="Seconds to sleep between pagination requests.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch jobs without persisting them to the database.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(levelname)s: %(message)s",
    )

    client = ConstellationCareersClient(delay=args.delay, page_size=args.page_size)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    try:
        job_iter = client.iter_jobs(limit=args.limit)
    except ScraperError as exc:
        logging.error("Failed to initialize job iterator: %s", exc)
        return 1

    for listing in job_iter:
        totals["fetched"] += 1

        if args.dry_run:
            print(json.dumps(listing.__dict__, default=str))
            continue

        try:
            created = persist_listing(listing)
        except ScraperError as exc:
            logging.error("Failed to persist job %s: %s", listing.detail_url, exc)
            totals["errors"] += 1
            continue

        if created:
            totals["created"] += 1
        else:
            totals["updated"] += 1

    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["deduplicated"] = dedupe_summary

    logging.info(
        "Constellation Energy scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )

    summary = {
        "company": "Constellation Energy",
        "jobs_base": JOBS_BASE_URL,
        **totals,
    }
    print(json.dumps(summary))
    return 0 if totals["errors"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
