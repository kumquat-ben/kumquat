#!/usr/bin/env python3
"""Manual scraper for https://careers.cfindustries.com/search-jobs."""

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

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_URL = "https://careers.cfindustries.com"
SEARCH_URL = f"{BASE_URL}/search-jobs"
API_URL = f"{BASE_URL}/api/v1/jobsearch/results"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.8",
    "Origin": BASE_URL,
    "Referer": SEARCH_URL,
}
REQUEST_TIMEOUT = (10, 45)
DEFAULT_PAGE_SIZE = 100
DEFAULT_DELAY = 0.0
DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 1800), 120)

SCRAPER_QS = Scraper.objects.filter(company="CF Industries", url=SEARCH_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple CF Industries scraper rows found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="CF Industries",
        url=SEARCH_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
class ScraperError(Exception):
    """Raised when the scraper cannot continue."""


@dataclass
class JobListing:
    job_id: str
    title: str
    detail_url: str
    location: Optional[str]
    posted_date: Optional[str]
    description_text: str
    description_html: Optional[str]
    metadata: Dict[str, object]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _coalesce_location(*parts: Optional[str]) -> Optional[str]:
    tokens = [token.strip() for token in parts if token and token.strip()]
    return ", ".join(tokens) if tokens else None


def _clean_html(html: Optional[str]) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    return text.strip()


def _normalize_date(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    cleaned = raw.strip()
    if not cleaned:
        return None
    try:
        normalized = datetime.fromisoformat(cleaned.replace("Z", "")).date().isoformat()
        return normalized
    except ValueError:
        return cleaned


def _ensure_json(data: str) -> Dict[str, object]:
    try:
        parsed = json.loads(data)
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive guard
        raise ScraperError(f"Unable to parse response payload: {exc}") from exc

    if isinstance(parsed, str):
        try:
            return json.loads(parsed)
        except json.JSONDecodeError as exc:  # pragma: no cover - defensive guard
            raise ScraperError(f"Unable to parse nested payload: {exc}") from exc
    if isinstance(parsed, dict):
        return parsed
    raise ScraperError("Unexpected payload type received from API.")


def _bounded(value: int, *, minimum: int, maximum: int) -> int:
    return max(minimum, min(maximum, value))


def _chunk_jobs(jobs: Iterable[dict]) -> Iterator[dict]:
    for job in jobs:
        if not isinstance(job, dict):
            continue
        job_id = job.get("id")
        title = job.get("title")
        detail_url = job.get("detailPageUrl")
        if not job_id or not title or not detail_url:
            continue
        yield job


# ---------------------------------------------------------------------------
# Scraper implementation
# ---------------------------------------------------------------------------
class CFIndustriesJobScraper:
    def __init__(
        self,
        *,
        session: Optional[requests.Session] = None,
        page_size: int = DEFAULT_PAGE_SIZE,
        delay: float = DEFAULT_DELAY,
    ) -> None:
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.page_size = _bounded(page_size, minimum=1, maximum=200)
        self.delay = max(0.0, delay)
        self.logger = logging.getLogger(self.__class__.__name__)

    def _fetch_page(self, page_number: int) -> Dict[str, object]:
        payload = {
            "pageNumber": page_number,
            "pageSize": self.page_size,
            "searchText": "",
            "city": "",
            "state": "",
            "country": "",
            "businessArea": "",
        }
        self.logger.debug("Fetching page %s with payload=%s", page_number, payload)
        response = self.session.post(API_URL, json=payload, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = _ensure_json(response.text)
        if "results" not in data:
            raise ScraperError("Payload missing results key.")
        return data

    def scrape(self, *, max_pages: Optional[int] = None, limit: Optional[int] = None) -> Iterator[JobListing]:
        page = 1
        processed = 0
        total_pages: Optional[int] = None

        while True:
            if max_pages is not None and page > max_pages:
                self.logger.info("Reached max_pages=%s; stopping scrape", max_pages)
                return

            payload = self._fetch_page(page)
            results = list(_chunk_jobs(payload.get("results") or []))
            if not results:
                self.logger.info("No results returned for page %s; stopping scrape", page)
                return

            if total_pages is None:
                total = payload.get("totalResults")
                if isinstance(total, int) and total > 0:
                    total_pages = max(1, (total + self.page_size - 1) // self.page_size)
                    self.logger.info("Detected %s total results across ~%s pages", total, total_pages)

            for job in results:
                listing = self._build_listing(job)
                yield listing
                processed += 1
                if limit is not None and processed >= limit:
                    self.logger.info("Reached limit=%s; stopping scrape", limit)
                    return

            page += 1
            if total_pages is not None and page > total_pages:
                self.logger.info("Processed all pages (%s); stopping scrape", total_pages)
                return
            if self.delay:
                time.sleep(self.delay)

    def _build_listing(self, job: Dict[str, object]) -> JobListing:
        job_id = str(job.get("id"))
        title = str(job.get("title") or "").strip()
        detail_url = str(job.get("detailPageUrl") or "").strip()

        location = _coalesce_location(job.get("city"), job.get("state"), job.get("country"))
        posted_date = _normalize_date(job.get("postingDate"))
        description_html = job.get("description") or ""
        description_text = _clean_html(description_html)

        metadata: Dict[str, object] = {
            "job_id": job_id,
            "business_area": job.get("businessArea"),
            "city": job.get("city"),
            "state": job.get("state"),
            "country": job.get("country"),
            "posting_date": job.get("postingDate"),
            "expire_date": job.get("expireDate"),
            "detail_page_url": detail_url,
            "apply_url": job.get("linkUrl"),
        }

        return JobListing(
            job_id=job_id,
            title=title,
            detail_url=detail_url,
            location=location,
            posted_date=posted_date,
            description_text=description_text,
            description_html=description_html,
            metadata=metadata,
        )


# ---------------------------------------------------------------------------
# Persistence & CLI
# ---------------------------------------------------------------------------
def store_listing(listing: JobListing) -> None:
    metadata = dict(listing.metadata)
    if listing.description_html:
        metadata["description_html"] = listing.description_html
    metadata["source"] = "cf_industries_manual"

    JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults={
            "title": listing.title[:255],
            "location": (listing.location or "")[:255],
            "date": (listing.posted_date or "")[:100],
            "description": listing.description_text[:10000],
            "metadata": metadata,
        },
    )


def run_scrape(max_pages: Optional[int], limit: Optional[int], page_size: int, delay: float) -> int:
    scraper = CFIndustriesJobScraper(page_size=page_size, delay=delay)
    count = 0
    for listing in scraper.scrape(max_pages=max_pages, limit=limit):
        store_listing(listing)
        count += 1
    return count


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CF Industries careers manual scraper")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Stop after processing this many search result pages",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after processing this many job postings",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help="Number of results to request per page (default: %(default)s)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help="Delay (seconds) between page fetches",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        default="INFO",
        help="Logging verbosity",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))

    start = time.time()
    try:
        count = run_scrape(
            max_pages=args.max_pages,
            limit=args.limit,
            page_size=args.page_size,
            delay=args.delay,
        )
    except Exception as exc:
        logging.exception("Scrape failed")
        return 1

    duration = time.time() - start
    dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
    summary = {
        "company": "CF Industries",
        "url": SEARCH_URL,
        "count": count,
        "elapsed_seconds": duration,
        "dedupe": dedupe_summary,
    }
    logging.info("Summary: %s", json.dumps(summary))
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

