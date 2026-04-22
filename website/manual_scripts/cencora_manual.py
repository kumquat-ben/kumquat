#!/usr/bin/env python3
"""Manual scraper for https://careers.cencora.com/us/en."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional
from urllib.parse import urljoin

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
BASE_URL = "https://careers.cencora.com"
SEARCH_PATH = "/us/en/search-results"
SEARCH_URL = urljoin(BASE_URL, SEARCH_PATH)
DETAIL_PATH_TEMPLATE = "/us/en/job/{job_id}"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.8",
    "Referer": SEARCH_URL,
}

PAGE_SIZE_FALLBACK = 10
REQUEST_TIMEOUT = 45
DEFAULT_DELAY_SECONDS = 0.25
DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 3600), 60)

SCRAPER_QS = Scraper.objects.filter(
    company="Cencora",
    url=SEARCH_URL,
).order_by("id")

if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Cencora scrapers found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Cencora",
        url=SEARCH_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the scraper encounters an unrecoverable issue."""


@dataclass
class JobListing:
    job_id: str
    job_seq_no: str
    title: str
    detail_url: str
    location: Optional[str]
    posted_date: Optional[str]
    description_text: str
    description_html: Optional[str]
    apply_url: Optional[str]
    workday_url: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    metadata: Dict[str, Any]


class CencoraJobScraper:
    def __init__(self, *, delay: float = DEFAULT_DELAY_SECONDS, session: Optional[requests.Session] = None) -> None:
        self.delay = max(delay, 0.0)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)
        self._total_hits: Optional[int] = None
        self._page_size: Optional[int] = None

    def scrape(
        self,
        *,
        max_pages: Optional[int] = None,
        limit: Optional[int] = None,
        start_offset: int = 0,
    ) -> Iterator[JobListing]:
        offset = max(start_offset, 0)
        pages_processed = 0
        processed_jobs = 0

        while True:
            if max_pages is not None and pages_processed >= max_pages:
                self.logger.info("Max pages reached (%s); stopping scrape", max_pages)
                return

            search_data = self._fetch_search_page(offset)
            jobs = search_data.get("jobs", [])
            self._total_hits = search_data.get("total_hits", self._total_hits)
            self._page_size = search_data.get("hits", self._page_size) or self._page_size or PAGE_SIZE_FALLBACK

            if not jobs:
                self.logger.info("No jobs returned at offset=%s; ending scrape", offset)
                return

            for job in jobs:
                listing = self._build_listing(job)
                if not listing:
                    continue

                yield listing
                processed_jobs += 1

                if limit is not None and processed_jobs >= limit:
                    self.logger.info("Reached limit=%s; stopping scrape", limit)
                    return

                if self.delay:
                    time.sleep(self.delay)

            pages_processed += 1
            offset += len(jobs)

            if self._total_hits is not None and offset >= self._total_hits:
                self.logger.info("Reached total hits (%s); stopping scrape", self._total_hits)
                return

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _fetch_search_page(self, offset: int) -> Dict[str, Any]:
        params = {"from": offset} if offset else {}
        response = self.session.get(SEARCH_URL, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        ddo = _extract_phapp_ddo(response.text)
        refine = (ddo.get("eagerLoadRefineSearch") or {})

        status = refine.get("status")
        if status and status != 200:
            raise ScraperError(f"Unexpected status from search payload: {status}")

        data = refine.get("data") or {}
        jobs = data.get("jobs") or []

        return {
            "jobs": jobs,
            "total_hits": refine.get("totalHits"),
            "hits": refine.get("hits"),
        }

    def _build_listing(self, job: Dict[str, Any]) -> Optional[JobListing]:
        job_id = (job.get("jobId") or "").strip()
        job_seq_no = (job.get("jobSeqNo") or "").strip()
        title = (job.get("title") or "").strip()
        if not job_id or not job_seq_no or not title:
            self.logger.debug("Skipping job with missing identifiers: %s", job)
            return None

        detail_url = urljoin(BASE_URL, DETAIL_PATH_TEMPLATE.format(job_id=job_id))

        try:
            detail = self._fetch_job_detail(job_id)
        except ScraperError as exc:
            self.logger.error("Failed to fetch detail for %s: %s", job_id, exc)
            return None

        description_html = detail.get("description")
        description_text = _html_to_text(description_html) if description_html else ""

        latitude = _float_or_none(job.get("latitude")) or _float_or_none(detail.get("latitude"))
        longitude = _float_or_none(job.get("longitude")) or _float_or_none(detail.get("longitude"))

        metadata = _compact_metadata(
            (
                ("job_id", job_id),
                ("job_seq_no", job_seq_no),
                ("apply_url", detail.get("applyUrl") or job.get("applyUrl")),
                ("workday_url", detail.get("workdayURL")),
                ("job_visibility", detail.get("jobVisibility")),
                ("worker_type", detail.get("workerType")),
                ("employee_type", detail.get("employeeType") or job.get("type")),
                ("job_family", detail.get("jobFamily")),
                ("job_family_group", detail.get("jobFamilyGroup")),
                ("department", detail.get("departmentName") or job.get("department")),
                ("category", detail.get("category") or job.get("category")),
                ("multi_category", detail.get("multi_category") or job.get("multi_category")),
                ("multi_location", detail.get("multi_location") or job.get("multi_location")),
                ("skills", detail.get("ml_skills") or job.get("ml_skills")),
                ("source", detail.get("source")),
                ("description_html", description_html),
            )
        )

        return JobListing(
            job_id=job_id,
            job_seq_no=job_seq_no,
            title=title,
            detail_url=detail_url,
            location=job.get("location") or detail.get("location"),
            posted_date=job.get("postedDate") or detail.get("postedDate"),
            description_text=description_text,
            description_html=description_html,
            apply_url=detail.get("applyUrl") or job.get("applyUrl"),
            workday_url=detail.get("workdayURL"),
            latitude=latitude,
            longitude=longitude,
            metadata=metadata,
        )

    def _fetch_job_detail(self, job_id: str) -> Dict[str, Any]:
        detail_url = urljoin(BASE_URL, DETAIL_PATH_TEMPLATE.format(job_id=job_id))
        response = self.session.get(detail_url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        ddo = _extract_phapp_ddo(response.text)
        payload = ddo.get("jobDetail") or {}
        status = payload.get("status")
        if status and status != 200:
            raise ScraperError(f"Unexpected status from job detail payload: {status}")

        data = payload.get("data") or {}
        job = data.get("job")
        if not isinstance(job, dict):
            raise ScraperError(f"Malformed job detail payload for {job_id}")
        return job


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------
PHAPP_PATTERN = re.compile(r"phApp\.ddo\s*=\s*(\{.*?\});\s*phApp", re.S)


def _extract_phapp_ddo(html: str) -> Dict[str, Any]:
    match = PHAPP_PATTERN.search(html)
    if not match:
        raise ScraperError("Unable to locate phApp.ddo payload in response.")
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError as exc:  # pragma: no cover
        raise ScraperError(f"Failed to decode phApp.ddo JSON: {exc}") from exc


def _float_or_none(value: Any) -> Optional[float]:
    if value in (None, "", "None"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n", strip=True)
    return text or ""


def _compact_metadata(pairs: Iterable[tuple[str, Any]]) -> Dict[str, Any]:
    metadata: Dict[str, Any] = {}
    for key, value in pairs:
        if value is None:
            continue
        if isinstance(value, str):
            trimmed = value.strip()
            if not trimmed:
                continue
            metadata[key] = trimmed
            continue
        if isinstance(value, (list, dict)) and not value:
            continue
        metadata[key] = value
    return metadata


def store_listing(listing: JobListing) -> None:
    metadata = dict(listing.metadata or {})
    metadata.setdefault("detail_url", listing.detail_url)
    metadata.setdefault("apply_url", listing.apply_url)
    metadata.setdefault("workday_url", listing.workday_url)

    defaults: Dict[str, Any] = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255],
        "date": (listing.posted_date or "")[:100],
        "description": listing.description_text[:10000],
        "metadata": metadata,
    }

    if listing.latitude is not None:
        defaults["location_latitude"] = listing.latitude
    if listing.longitude is not None:
        defaults["location_longitude"] = listing.longitude

    JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults=defaults,
    )


def run_scrape(max_pages: Optional[int], limit: Optional[int], delay: float, start_offset: int) -> int:
    scraper = CencoraJobScraper(delay=delay)
    stored = 0
    for listing in scraper.scrape(max_pages=max_pages, limit=limit, start_offset=start_offset):
        store_listing(listing)
        stored += 1
    return stored


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cencora careers manual scraper")
    parser.add_argument("--max-pages", type=int, default=None, help="Stop after processing this many search result pages")
    parser.add_argument("--limit", type=int, default=None, help="Stop after processing this many job postings")
    parser.add_argument("--start-offset", type=int, default=0, help="Initial search offset (multiples of page size)")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY_SECONDS, help="Delay (seconds) between detail requests")
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
        stored = run_scrape(args.max_pages, args.limit, args.delay, args.start_offset)
    except ScraperError as exc:
        logging.error("Scrape failed: %s", exc)
        return 1

    duration = time.time() - start_time
    dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
    summary = {
        "company": "Cencora",
        "url": SEARCH_URL,
        "count": stored,
        "elapsed_seconds": duration,
        "dedupe": dedupe_summary,
    }
    logging.info("Summary: %s", json.dumps(summary))
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
