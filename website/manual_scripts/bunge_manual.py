#!/usr/bin/env python3
"""Manual scraper for Bunge careers (https://jobs.bunge.com).

The public careers site is powered by SAP SuccessFactors / Jobs2Web.
This script paginates through the HTML search results, visits each job
detail page for rich metadata, and upserts the records into the shared
`JobPosting` Django model so they surface inside Kumquat dashboards.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag

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
# Constants & configuration
# ---------------------------------------------------------------------------
BASE_URL = "https://jobs.bunge.com"
SEARCH_PATH = "/search/"
SEARCH_URL = urljoin(BASE_URL, SEARCH_PATH)
PAGE_SIZE = 100
REQUEST_TIMEOUT = (10, 45)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Referer": SEARCH_URL,
}

PAGINATION_TOTAL_RE = re.compile(r"of\s+([\d,]+)")
JOB_ID_RE = re.compile(r"/(\d+)/?$")

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 1800), 120)

SCRAPER_QS = Scraper.objects.filter(company="Bunge", url=SEARCH_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Bunge scrapers found; using id=%s", SCRAPER.id)
else:  # pragma: no cover - bootstrap path
    SCRAPER = Scraper.objects.create(
        company="Bunge",
        url=SEARCH_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
class ScraperError(Exception):
    """Raised when the scraper encounters an unrecoverable condition."""


@dataclass
class JobSummary:
    title: str
    detail_url: str
    job_id: Optional[str]
    location: Optional[str]
    date_posted: Optional[str]


@dataclass
class JobListing(JobSummary):
    description_text: Optional[str]
    description_html: Optional[str]
    metadata: Dict[str, object]


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------
def _clean_text(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    cleaned = value.replace("\xa0", " ").strip()
    return cleaned or None


def _strip_labelled_value(node: Optional[Tag]) -> Optional[str]:
    if node is None:
        return None
    text = node.get_text("\n", strip=True)
    if not text:
        return None
    parts = text.split(":", 1)
    if len(parts) == 2:
        return _clean_text(parts[1])
    return _clean_text(text)


def _normalize_metadata(data: Dict[str, object]) -> Dict[str, object]:
    normalized: Dict[str, object] = {}
    for key, value in data.items():
        if value in (None, "", [], {}, ()):
            continue
        if isinstance(value, str):
            cleaned = value.strip()
            if cleaned:
                normalized[key] = cleaned
            continue
        normalized[key] = value
    return normalized


# ---------------------------------------------------------------------------
# Scraper implementation
# ---------------------------------------------------------------------------
class BungeJobScraper:
    def __init__(self, *, delay: float = 0.35, session: Optional[requests.Session] = None) -> None:
        self.delay = max(delay, 0.0)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    # Public API ---------------------------------------------------------
    def scrape(
        self,
        *,
        max_pages: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Iterator[JobListing]:
        page_index = 0
        processed = 0
        startrow = 0
        total_results: Optional[int] = None

        while True:
            if max_pages is not None and page_index >= max_pages:
                self.logger.info("Reached max_pages=%s, stopping pagination.", max_pages)
                break

            soup = self._fetch_search_page(startrow=startrow)
            if soup is None:
                self.logger.info("No search result page returned for startrow=%s; stopping.", startrow)
                break

            if total_results is None:
                total_results = self._extract_total_results(soup)
                if total_results is not None:
                    self.logger.info("Detected %s total search results.", total_results)

            summaries = list(self._parse_job_summaries(soup))
            if not summaries:
                self.logger.info("No job summaries found at startrow=%s; stopping.", startrow)
                break

            for summary in summaries:
                try:
                    detail = self._fetch_job_detail(summary.detail_url)
                except ScraperError as exc:
                    self.logger.error("Skipping %s (%s)", summary.detail_url, exc)
                    continue

                listing = JobListing(
                    title=summary.title,
                    detail_url=summary.detail_url,
                    job_id=summary.job_id,
                    location=detail.pop("location_override", summary.location),
                    date_posted=detail.pop("date_override", summary.date_posted),
                    description_text=detail.pop("description_text"),
                    description_html=detail.pop("description_html"),
                    metadata=detail.get("metadata", {}),
                )

                yield listing
                processed += 1

                if limit is not None and processed >= limit:
                    self.logger.info("Reached limit=%s jobs; stopping scrape.", limit)
                    return

                if self.delay:
                    time.sleep(self.delay)

            page_index += 1
            startrow += PAGE_SIZE

            if total_results is not None and startrow >= total_results:
                self.logger.info("Processed all %s results; stopping.", total_results)
                break

    # Internal helpers ---------------------------------------------------
    def _fetch_search_page(self, *, startrow: int) -> Optional[BeautifulSoup]:
        params = {
            "q": "",
            "sortColumn": "referencedate",
            "sortDirection": "desc",
        }
        if startrow:
            params["startrow"] = startrow

        try:
            response = self.session.get(SEARCH_URL, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.RequestException as exc:  # pragma: no cover - network safeguards
            raise ScraperError(f"Search page request failed at startrow={startrow}: {exc}") from exc

        return BeautifulSoup(response.text, "html.parser")

    def _extract_total_results(self, soup: BeautifulSoup) -> Optional[int]:
        label = soup.select_one(".paginationLabel")
        if not label:
            return None
        match = PAGINATION_TOTAL_RE.search(label.get_text(" ", strip=True))
        if not match:
            return None
        try:
            return int(match.group(1).replace(",", ""))
        except ValueError:
            return None

    def _parse_job_summaries(self, soup: BeautifulSoup) -> Iterable[JobSummary]:
        for row in soup.select("tr.data-row"):
            link_tag = row.select_one("a.jobTitle-link")
            if link_tag is None or not link_tag.get("href"):
                continue
            title = _clean_text(link_tag.get_text(strip=True))
            if not title:
                continue
            detail_url = urljoin(BASE_URL, link_tag["href"])
            job_id = self._extract_job_id(detail_url)

            location_tag = row.select_one(".jobLocation")
            location = _clean_text(location_tag.get_text(" ", strip=True)) if location_tag else None

            date_tag = row.select_one(".jobDate")
            date_posted = _clean_text(date_tag.get_text(" ", strip=True)) if date_tag else None

            yield JobSummary(
                title=title,
                detail_url=detail_url,
                job_id=job_id,
                location=location,
                date_posted=date_posted,
            )

    def _extract_job_id(self, detail_url: str) -> Optional[str]:
        match = JOB_ID_RE.search(detail_url)
        if not match:
            return None
        return match.group(1)

    def _fetch_job_detail(self, url: str) -> Dict[str, object]:
        try:
            response = self.session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.RequestException as exc:  # pragma: no cover - network safeguards
            raise ScraperError(f"Detail request failed: {exc}") from exc

        soup = BeautifulSoup(response.text, "html.parser")

        description_node = soup.select_one(".jobdescription")
        description_html = description_node.decode_contents().strip() if description_node else ""
        description_text = (
            description_node.get_text("\n", strip=True) if description_node else ""
        )

        if not description_text and not description_html:
            self.logger.debug("Empty description detected for %s", url)

        detail_date = _strip_labelled_value(soup.select_one("#job-date"))
        detail_location = _strip_labelled_value(soup.select_one("#job-location"))
        company_name = _strip_labelled_value(soup.select_one("#job-company"))

        segments_raw = soup.select_one(".jobsegments")
        job_segments = None
        if segments_raw:
            job_segments = _strip_labelled_value(segments_raw)

        markets_raw = soup.select_one(".jobmarkets")
        job_markets = _strip_labelled_value(markets_raw) if markets_raw else None

        metadata = _normalize_metadata(
            {
                "detail_date": detail_date,
                "detail_location": detail_location,
                "job_company": company_name,
                "job_segments": job_segments,
                "job_markets": job_markets,
                "source": "jobs.bunge.com",
            }
        )

        return {
            "description_html": description_html or None,
            "description_text": description_text or None,
            "date_override": detail_date or None,
            "location_override": detail_location or None,
            "metadata": metadata,
        }


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------
def persist_listing(listing: JobListing) -> bool:
    metadata = dict(listing.metadata or {})
    metadata.setdefault("job_id", listing.job_id)
    metadata.setdefault("detail_url", listing.detail_url)
    metadata.setdefault("source", "jobs.bunge.com")

    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255] or None,
        "date": (listing.date_posted or "")[:100] or None,
        "description": (listing.description_text or "")[:10000],
        "metadata": _normalize_metadata(metadata),
    }

    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Persisted Bunge job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape jobs.bunge.com manual script")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Stop after this many search pages (default: all).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after processing this many job postings.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.35,
        help="Seconds to sleep between detail requests (default: 0.35).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print job payloads to stdout instead of storing them.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity (default: INFO).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(levelname)s: %(message)s",
    )

    scraper = BungeJobScraper(delay=args.delay)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    for listing in scraper.scrape(max_pages=args.max_pages, limit=args.limit):
        totals["fetched"] += 1

        if args.dry_run:
            print(json.dumps(asdict(listing), ensure_ascii=False))
            continue

        try:
            created = persist_listing(listing)
        except Exception as exc:  # pragma: no cover - persistence safeguards
            logging.error("Failed to persist %s: %s", listing.detail_url, exc)
            totals["errors"] += 1
            continue

        if created:
            totals["created"] += 1
        else:
            totals["updated"] += 1

    if not args.dry_run and totals["errors"] == 0:
        totals["dedupe"] = deduplicate_job_postings(scraper=SCRAPER)

    logging.info(
        "Bunge scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )

    return 0 if totals["errors"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

