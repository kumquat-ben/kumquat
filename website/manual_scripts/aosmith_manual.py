#!/usr/bin/env python3
"""Manual scraper for A. O. Smith careers (SuccessFactors-hosted site).

This script paginates the public search results on https://jobs.aosmith.com,
visits each job detail page for richer metadata (apply link, description,
structured fields), and persists/upserts entries into the shared JobPosting
table tied to the "A. O. Smith" scraper record.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, Generator, Iterable, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, NavigableString, Tag

# ---------------------------------------------------------------------------
# Django bootstrap (enables invocation via management dashboard)
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
BASE_URL = "https://jobs.aosmith.com"
SEARCH_PATH = "/search/"
SOURCE_URL = "https://www.aosmith.com/about-us/careers.html"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": urljoin(BASE_URL, SEARCH_PATH),
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 30)
SCRAPER_QS = Scraper.objects.filter(company="A. O. Smith", url=urljoin(BASE_URL, SEARCH_PATH)).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched A. O. Smith; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="A. O. Smith",
        url=urljoin(BASE_URL, SEARCH_PATH),
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the SuccessFactors scraper encounters an unrecoverable issue."""


@dataclass
class JobSummary:
    title: str
    detail_url: str
    job_id: Optional[str]
    location: Optional[str]
    job_function: Optional[str]
    business_unit: Optional[str]
    summary_fields: Dict[str, str] = field(default_factory=dict)


@dataclass
class JobListing(JobSummary):
    date_posted: Optional[str] = None
    apply_url: Optional[str] = None
    description_text: Optional[str] = None
    description_html: Optional[str] = None
    detail_fields: Dict[str, str] = field(default_factory=dict)
    structured_location: Dict[str, str] = field(default_factory=dict)


class AOSmithJobScraper:
    def __init__(
        self,
        *,
        page_size: int = 25,
        delay: float = 0.3,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.page_size = max(1, int(page_size))
        self.delay = max(0.0, float(delay))
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    def scrape(self, *, limit: Optional[int] = None) -> Generator[JobListing, None, None]:
        fetched = 0
        start_row = 0
        total_jobs: Optional[int] = None

        while True:
            soup = self._fetch_search_page(start_row)
            if total_jobs is None:
                total_jobs = self._extract_total_jobs(soup)
                if total_jobs is not None:
                    self.logger.info("Discovered %s total jobs on A. O. Smith search page.", total_jobs)

            summaries = list(self._parse_job_tiles(soup))
            if not summaries:
                self.logger.info("No job tiles found at startrow=%s; stopping pagination.", start_row)
                return

            for summary in summaries:
                detail_payload = self._fetch_job_detail(summary.detail_url)
                listing = JobListing(**asdict(summary), **detail_payload)
                yield listing
                fetched += 1
                if limit is not None and fetched >= limit:
                    self.logger.info("Reached limit=%s; halting scrape.", limit)
                    return

            start_row += self.page_size
            if total_jobs is not None and start_row >= total_jobs:
                self.logger.info("Reached reported total jobs (%s); pagination complete.", total_jobs)
                return

            if self.delay:
                time.sleep(self.delay)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _fetch_search_page(self, start_row: int) -> BeautifulSoup:
        params = {
            "q": "",
            "locationsearch": "",
            "startrow": max(0, int(start_row)),
            "sortColumn": "referencedate",
            "sortDirection": "desc",
        }
        response = self.session.get(urljoin(BASE_URL, SEARCH_PATH), params=params, timeout=45)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")

    def _extract_total_jobs(self, soup: BeautifulSoup) -> Optional[int]:
        label = soup.select_one("#tile-search-results-label")
        if not label:
            return None
        text = label.get_text(" ", strip=True)
        match = re.search(r"of\s+([0-9,]+)\s+Jobs", text, re.IGNORECASE)
        if not match:
            return None
        try:
            return int(match.group(1).replace(",", ""))
        except ValueError:
            return None

    def _parse_job_tiles(self, soup: BeautifulSoup) -> Iterable[JobSummary]:
        for item in soup.select("ul#job-tile-list li.job-tile"):
            link = item.select_one("a.jobTitle-link")
            if not link:
                continue

            detail_path = item.get("data-url") or link.get("href") or ""
            detail_url = urljoin(BASE_URL, detail_path)
            job_id = self._extract_job_id(item)
            summary_fields = self._extract_summary_fields(item)
            location = summary_fields.get("Location(s)") or summary_fields.get("Location") or summary_fields.get("City")

            yield JobSummary(
                title=_clean_text(link) or "Untitled Role",
                detail_url=detail_url,
                job_id=job_id,
                location=location,
                job_function=summary_fields.get("Job Function"),
                business_unit=summary_fields.get("Business Unit"),
                summary_fields=summary_fields,
            )

    def _extract_job_id(self, item: Tag) -> Optional[str]:
        for cls in item.get("class", []):
            if cls.startswith("job-id-"):
                return cls.replace("job-id-", "", 1)
        return item.get("data-job-id")

    def _extract_summary_fields(self, item: Tag) -> Dict[str, str]:
        fields: Dict[str, str] = {}
        for field in item.select("div.section-field"):
            label_elem = field.select_one(".section-label")
            label = _clean_text(label_elem)
            if not label:
                continue
            value_elem = self._first_value_element(field, label_elem)
            value = _clean_text(value_elem) or _clean_text(field)
            if value:
                fields[label] = value
        return fields

    def _first_value_element(self, field: Tag, label_elem: Optional[Tag]) -> Optional[Tag]:
        for child in field.children:
            if isinstance(child, NavigableString):
                continue
            if child is label_elem:
                continue
            return child  # first non-label child carries the value text
        return None

    def _fetch_job_detail(self, url: str) -> Dict[str, Optional[str]]:
        response = self.session.get(url, timeout=45)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        description_block = soup.select_one("span.jobdescription")
        meta_date = soup.select_one('meta[itemprop="datePosted"]')
        apply_link = soup.select_one("a.apply")

        detail_fields: Dict[str, str] = {}
        job_location = soup.select_one("p.job-location")
        if job_location:
            text_val = _clean_text(job_location)
            if text_val:
                detail_fields["Job Location Text"] = text_val
            job_segment = job_location.select_one("span.jobsegments")
            segment_text = _clean_text(job_segment)
            if segment_text:
                detail_fields["Job Segment"] = segment_text

        structured_location = self._extract_structured_location(soup)

        return {
            "date_posted": meta_date.get("content") if meta_date and meta_date.has_attr("content") else None,
            "apply_url": urljoin(BASE_URL, apply_link.get("href")) if apply_link and apply_link.get("href") else None,
            "description_text": _clean_text(description_block, separator="\n\n"),
            "description_html": str(description_block) if description_block else None,
            "detail_fields": detail_fields,
            "structured_location": structured_location,
        }

    def _extract_structured_location(self, soup: BeautifulSoup) -> Dict[str, str]:
        location: Dict[str, str] = {}
        for prop in ("addressLocality", "addressRegion", "postalCode", "addressCountry"):
            meta_tag = soup.select_one(f'meta[itemprop="{prop}"]')
            if meta_tag and meta_tag.has_attr("content") and meta_tag["content"]:
                location[prop] = meta_tag["content"]
        return location


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------
def persist_listing(listing: JobListing) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255] or None,
        "date": (listing.date_posted or "")[:100] or None,
        "description": (listing.description_text or "")[:10000],
        "metadata": _build_metadata(listing),
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Persisted A. O. Smith job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def _build_metadata(listing: JobListing) -> Dict[str, object]:
    metadata: Dict[str, object] = {
        "job_id": listing.job_id,
        "job_function": listing.job_function,
        "business_unit": listing.business_unit,
        "search_fields": listing.summary_fields,
        "detail_fields": listing.detail_fields,
        "structured_location": listing.structured_location,
        "source_url": SOURCE_URL,
    }
    if listing.apply_url:
        metadata["apply_url"] = listing.apply_url
    return metadata


# ---------------------------------------------------------------------------
# CLI / entrypoint
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape A. O. Smith (SuccessFactors) job listings.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument(
        "--page-size",
        type=int,
        default=25,
        help="Number of jobs to advance per pagination chunk (controls startrow increments).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.3,
        help="Seconds to sleep between pagination requests (default: 0.3).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and print listings without writing to the database.",
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
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s: %(message)s")

    scraper = AOSmithJobScraper(page_size=args.page_size, delay=args.delay)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    try:
        for listing in scraper.scrape(limit=args.limit):
            totals["fetched"] += 1
            if args.dry_run:
                print(json.dumps(listing.__dict__, default=str, ensure_ascii=False))
                continue

            try:
                created = persist_listing(listing)
                if created:
                    totals["created"] += 1
                else:
                    totals["updated"] += 1
            except Exception as exc:  # pragma: no cover - defensive persistence path
                logging.error("Failed to persist job %s: %s", listing.detail_url, exc)
                totals["errors"] += 1
    except (requests.RequestException, ScraperError) as exc:
        logging.error("Scraper encountered an error: %s", exc)
        return 1

    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["dedupe"] = dedupe_summary

    logging.info(
        "A. O. Smith scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )
    return 0 if not totals["errors"] else 1


def _clean_text(element: Optional[Tag], separator: str = " ") -> Optional[str]:
    if not element:
        return None
    text = element.get_text(separator=separator, strip=True)
    return text or None


if __name__ == "__main__":
    raise SystemExit(main())
