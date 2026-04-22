#!/usr/bin/env python3
"""Manual scraper for Fifth Third Bank careers (53.com).

The public Fifth Third marketing site at https://53.com/careers embeds a
client-side powered job feed that ultimately pulls from the Jobsyn Solr API
(`https://prod-search-api.jobsyn.org/api/v1/solr/search`). This script mirrors
that flow: it pages through the API, normalises the payload, and stores the
results via the shared Django models so operations can schedule or trigger it
like the other manual scrapers.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
import unicodedata
from dataclasses import dataclass, asdict
from typing import Dict, Iterable, List, Optional
from urllib.parse import urljoin

import requests
from pathlib import Path

# ---------------------------------------------------------------------------
# Django bootstrap (keeps parity with existing manual scripts)
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
CAREERS_LANDING_URL = "https://53.com/careers"
JOB_SEARCH_URL = "https://53bank.dejobs.org/jobs/"
DEJOBS_BASE_URL = "https://53bank.dejobs.org"
API_ENDPOINT = "https://prod-search-api.jobsyn.org/api/v1/solr/search"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Origin": DEJOBS_BASE_URL,
    "Referer": JOB_SEARCH_URL,
    "x-origin": "53bank.dejobs.org",
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 60)
REQUEST_TIMEOUT_SECONDS = 45

SCRAPER_QS = Scraper.objects.filter(company="Fifth Third Bank", url=CAREERS_LANDING_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.getLogger(__name__).warning(
            "Multiple Scraper rows matched Fifth Third Bank; using id=%s.", SCRAPER.id
        )
else:  # pragma: no cover - creation path
    SCRAPER = Scraper.objects.create(
        company="Fifth Third Bank",
        url=CAREERS_LANDING_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the scraper encounters a non-recoverable error."""


@dataclass
class JobListing:
    guid: str
    reqid: Optional[str]
    title: str
    location: Optional[str]
    link: str
    date_posted: Optional[str]
    description: Optional[str]
    metadata: Dict[str, object]


def slugify(value: str) -> str:
    """Produce a URL slug similar to the jobs front-end."""
    if not value:
        return ""
    text = unicodedata.normalize("NFKD", value)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-z0-9]+", "-", text.lower())
    return text.strip("-")


def normalize_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return value.split("T", 1)[0]


class FifthThirdJobScraper:
    def __init__(
        self,
        *,
        delay: float = 0.25,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def scrape(
        self,
        *,
        max_pages: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Iterable[JobListing]:
        page = 1
        yielded = 0

        while True:
            if max_pages is not None and page > max_pages:
                self.logger.info("Reached max_pages=%s; stopping.", max_pages)
                break

            data = self._fetch_page(page)
            jobs = data.get("jobs") or []
            if not jobs:
                self.logger.info("API returned no jobs for page %s; stopping.", page)
                break

            for raw in jobs:
                listing = self._transform(raw)
                yield listing
                yielded += 1
                if limit is not None and yielded >= limit:
                    self.logger.info("Reached limit=%s; stopping.", limit)
                    return

            pagination = data.get("pagination") or {}
            has_more = pagination.get("has_more_pages")
            self.logger.debug(
                "Processed page %s (jobs=%s, has_more=%s)",
                page,
                len(jobs),
                has_more,
            )

            if not has_more:
                break

            page += 1
            if self.delay:
                time.sleep(self.delay)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _fetch_page(self, page: int) -> Dict[str, object]:
        params = {"page": page}
        self.logger.debug("Fetching Jobsyn Solr page %s", page)
        try:
            response = self.session.get(API_ENDPOINT, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        except requests.RequestException as exc:  # pragma: no cover - network failure
            raise ScraperError(f"Request failed for page {page}: {exc}") from exc

        if response.status_code >= 400:
            snippet = response.text[:200].strip()
            raise ScraperError(
                f"Jobs API returned {response.status_code} for page={page}: {snippet or 'no body'}"
            )

        try:
            return response.json()
        except ValueError as exc:
            raise ScraperError(f"Failed to decode JSON for page {page}: {exc}") from exc

    def _transform(self, job: Dict[str, object]) -> JobListing:
        guid = str(job.get("guid") or "").strip()
        title = str(job.get("title_exact") or "").strip()
        if not guid or not title:
            raise ScraperError("Job payload missing mandatory guid/title fields.")

        location_text = job.get("location_exact")
        location_slug = slugify(str(location_text or ""))
        if not location_slug:
            # Fall back to city/state if available for jobs that omit location_exact.
            city = slugify(str(job.get("city_exact") or ""))
            state = str(job.get("state_short") or "").lower()
            components = [part for part in (city, state) if part]
            location_slug = "-".join(components)

        title_slug = slugify(str(job.get("title_slug") or title))
        relative_path = f"/{location_slug}/{title_slug}/{guid}/job/"
        link = urljoin(DEJOBS_BASE_URL, relative_path)

        metadata: Dict[str, object] = {
            "reqid": job.get("reqid"),
            "date_added": job.get("date_added"),
            "date_updated": job.get("date_updated"),
            "date_new": job.get("date_new"),
            "raw": job,
        }

        return JobListing(
            guid=guid,
            reqid=job.get("reqid"),
            title=title,
            location=str(location_text or "") or None,
            link=link,
            date_posted=normalize_date(job.get("date_new") or job.get("date_added")),
            description=str(job.get("description") or "").strip() or None,
            metadata=metadata,
        )


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------
def persist_listing(listing: JobListing) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255] or None,
        "date": (listing.date_posted or "")[:100] or None,
        "description": (listing.description or "")[:10000],
        "metadata": listing.metadata,
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.link,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Persisted Fifth Third job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Fifth Third Bank job listings.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Maximum number of API pages to fetch.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.25,
        help="Optional delay (seconds) between API requests.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and print jobs without touching the database.",
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
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s: %(message)s")

    scraper = FifthThirdJobScraper(delay=args.delay)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    try:
        for listing in scraper.scrape(limit=args.limit, max_pages=args.max_pages):
            totals["fetched"] += 1
            if args.dry_run:
                print(json.dumps(asdict(listing), default=str, ensure_ascii=False))
                continue
            try:
                created = persist_listing(listing)
                if created:
                    totals["created"] += 1
                else:
                    totals["updated"] += 1
            except Exception as exc:  # pragma: no cover - persistence failure
                logging.error("Failed to persist job %s: %s", listing.guid, exc)
                totals["errors"] += 1
    except ScraperError as exc:
        logging.error("Fifth Third scraper stopped due to error: %s", exc)
        totals["errors"] += 1

    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["dedupe"] = dedupe_summary

    logging.info(
        "Fifth Third scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )
    return 0 if not totals["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
