#!/usr/bin/env python3
"""Manual scraper for https://jobs.disneycareers.com/search-jobs.

This script mimics the proven Codex-style scraper: it iterates through
search-result pages, collects row summaries under `#search-results`
(table structure), then walks each job detail page to capture extended
fields (apply URL, description HTML/text, job metadata). Results are
stored directly in the Django database via `JobPosting`.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, asdict
from typing import Dict, Generator, Iterable, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, NavigableString, Tag

from pathlib import Path

# ---------------------------------------------------------------------------
# Django setup (makes script runnable via management dashboard)
# ---------------------------------------------------------------------------
BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "website.settings")

import django
from django.db import IntegrityError

django.setup()

from django.conf import settings

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

# ---------------------------------------------------------------------------
# Constants & defaults
# ---------------------------------------------------------------------------
BASE_URL = "https://jobs.disneycareers.com"
SEARCH_PATH = "/search-jobs"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": BASE_URL + SEARCH_PATH,
}

# Create / fetch scraper record to associate postings with
DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 30)
SCRAPER_QS = Scraper.objects.filter(company="Disney", url=urljoin(BASE_URL, SEARCH_PATH)).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched; using the earliest (id=%s).", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Disney",
        url=urljoin(BASE_URL, SEARCH_PATH),
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Custom error for scraper issues."""


@dataclass
class JobSummary:
    title: str
    detail_url: str
    job_id: Optional[str]
    requisition_id: Optional[str]
    location: Optional[str]
    brand: Optional[str]
    date_posted: Optional[str]


@dataclass
class JobListing(JobSummary):
    apply_url: Optional[str]
    description_text: Optional[str]
    description_html: Optional[str]
    job_info: Dict[str, str]


class DisneyJobScraper:
    def __init__(self, delay: float = 0.25, session: Optional[requests.Session] = None) -> None:
        self.delay = delay
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    def scrape(self, max_pages: Optional[int] = None, limit: Optional[int] = None) -> Generator[JobListing, None, None]:
        first_page = self._fetch_search_page(1)
        total_pages = self._extract_total_pages(first_page)
        if total_pages is None:
            raise ScraperError("Could not determine the total number of result pages.")

        self.logger.info("Discovered %s result pages", total_pages)

        yielded = 0
        page = 1
        while True:
            soup = first_page if page == 1 else self._fetch_search_page(page)
            for summary in self._parse_job_summaries(soup):
                detail = self._fetch_job_detail(summary.detail_url)
                listing = JobListing(**asdict(summary), **detail)
                yield listing
                yielded += 1
                if limit is not None and yielded >= limit:
                    self.logger.info("Reached limit %s; stopping scrape", limit)
                    return
            page += 1
            if max_pages is not None and page > max_pages:
                self.logger.info("Reached page limit %s; stopping scrape", max_pages)
                return
            if total_pages is not None and page > total_pages:
                self.logger.info("Reached final page %s; stopping scrape", total_pages)
                return
            time.sleep(self.delay)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _fetch_search_page(self, page: int) -> BeautifulSoup:
        params = {"p": page} if page > 1 else None
        response = self.session.get(urljoin(BASE_URL, SEARCH_PATH), params=params, timeout=30)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")

    def _extract_total_pages(self, soup: BeautifulSoup) -> Optional[int]:
        container = soup.select_one("#search-results")
        if not container:
            return None
        value = container.get("data-total-pages")
        try:
            return int(value) if value else None
        except (TypeError, ValueError):
            return None

    def _parse_job_summaries(self, soup: BeautifulSoup) -> Iterable[JobSummary]:
        for row in soup.select("#search-results table tbody tr"):
            link = row.find("a", attrs={"data-job-id": True})
            if not link:
                continue
            yield JobSummary(
                title=_text_or_none(link),
                detail_url=urljoin(BASE_URL, link.get("href", "")),
                job_id=link.get("data-job-id"),
                requisition_id=link.get("data-job-secondary-id"),
                location=_text_or_none(row.select_one(".job-location")),
                brand=_text_or_none(row.select_one(".job-brand")),
                date_posted=_text_or_none(row.select_one(".job-date-posted")),
            )

    def _fetch_job_detail(self, url: str) -> Dict[str, Optional[str]]:
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        apply_link = soup.select_one("a#applybutton")
        description_block = soup.select_one(".ats-description")

        job_info = {}
        for block in soup.select("span.job-info"):
            label_elem = block.find("b")
            if not label_elem:
                continue
            label = label_elem.get_text(strip=True).rstrip(":")
            value = _text_or_none(_first_non_label_child(block, label_elem)) or _text_or_none(block)
            job_info[label] = value or ""

        return {
            "apply_url": apply_link.get("href") if apply_link else None,
            "description_text": _text_or_none(description_block, separator="\n\n"),
            "description_html": str(description_block) if description_block else None,
            "job_info": job_info,
        }


# ---------------------------------------------------------------------------
# Utility helpers (copied from reference)
# ---------------------------------------------------------------------------

def _text_or_none(element: Optional[Tag], separator: str = " ") -> Optional[str]:
    if not element:
        return None
    text = element.get_text(separator=separator, strip=True)
    return text or None


def _first_non_label_child(block: Tag, label_element: Tag) -> Optional[Tag]:
    for child in block.children:
        if child is label_element:
            continue
        if isinstance(child, NavigableString):
            if child.strip():
                return block
            continue
        if isinstance(child, Tag):
            return child
    return None


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

def store_listing(listing: JobListing) -> None:
    JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults={
            "title": listing.title,
            "location": listing.location or "",
            "date": listing.date_posted or "",
            "description": listing.description_text or "",
            "metadata": listing.job_info,
        },
    )


def run_scrape(max_pages: Optional[int], limit: Optional[int], delay: float) -> int:
    scraper = DisneyJobScraper(delay=delay)
    count = 0
    for job in scraper.scrape(max_pages=max_pages, limit=limit):
        store_listing(job)
        count += 1
    return count


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Disney careers manual scraper")
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--delay", type=float, default=0.25)
    parser.add_argument("--log-level", type=str, default="INFO", choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"])
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    start = time.time()
    try:
        count = run_scrape(args.max_pages, args.limit, args.delay)
    except ScraperError as exc:
        logging.error("Scrape failed: %s", exc)
        return 1
    dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
    duration = time.time() - start
    summary = {
        "company": "Disney",
        "url": urljoin(BASE_URL, SEARCH_PATH),
        "count": count,
        "elapsed_seconds": duration,
        "dedupe": dedupe_summary,
    }
    logging.info("Summary: %s", json.dumps(summary))
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
