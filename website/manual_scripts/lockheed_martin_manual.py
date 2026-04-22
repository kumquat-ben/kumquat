#!/usr/bin/env python3
"""Manual scraper for https://www.lockheedmartinjobs.com/search-jobs.

This script mirrors the Disney manual scraper pattern: it paginates through
the public search results, extracts job rows rendered under `#search-results`,
visits each job detail page for richer metadata (apply URL, description,
structured fields), and upserts rows into `JobPosting`.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
import time
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
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
BASE_URL = "https://www.lockheedmartinjobs.com"
SEARCH_PATH = "/search-jobs"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": urljoin(BASE_URL, SEARCH_PATH),
}

# Create / fetch scraper record to associate postings with
DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 30)
SCRAPER_QS = Scraper.objects.filter(company="Lockheed Martin", url=urljoin(BASE_URL, SEARCH_PATH)).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched; using the earliest (id=%s).", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Lockheed Martin",
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
    date_posted: Optional[str]


@dataclass
class JobListing(JobSummary):
    apply_url: Optional[str]
    description_text: Optional[str]
    description_html: Optional[str]
    job_info: Dict[str, str]


class LockheedJobScraper:
    def __init__(
        self,
        delay: float = 0.25,
        session: Optional[requests.Session] = None,
        max_workers: int = 8,
    ) -> None:
        self.delay = delay
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.max_workers = max(1, int(max_workers))
        self._thread_local = threading.local()
        self.logger = logging.getLogger(self.__class__.__name__)

    def scrape(self, max_pages: Optional[int] = None, limit: Optional[int] = None) -> Generator[JobListing, None, None]:
        first_page = self._fetch_search_page(1)
        total_pages = self._extract_total_pages(first_page)
        if total_pages is None:
            raise ScraperError("Could not determine the total number of result pages.")

        self.logger.info("Discovered %s result pages", total_pages)

        yielded = 0
        page = 1
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            while True:
                soup = first_page if page == 1 else self._fetch_search_page(page)
                summaries = list(self._parse_job_summaries(soup))
                if not summaries:
                    self.logger.info("No job summaries found on page %s; stopping scrape", page)
                    return

                future_map = {
                    executor.submit(self._fetch_job_detail, summary.detail_url): summary for summary in summaries
                }
                for future in as_completed(future_map):
                    summary = future_map[future]
                    try:
                        detail = future.result()
                    except Exception as exc:
                        raise ScraperError(f"Failed to fetch details for {summary.detail_url}: {exc}") from exc
                    listing = JobListing(**asdict(summary), **detail)
                    yield listing
                    yielded += 1
                    if limit is not None and yielded >= limit:
                        self.logger.info("Reached limit %s; stopping scrape", limit)
                        for pending_future in future_map:
                            if pending_future is not future and not pending_future.done():
                                pending_future.cancel()
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
        for item in soup.select("#search-results ul li"):
            link = item.find("a", attrs={"data-job-id": True})
            if not link:
                continue
            job_id_attr = link.get("data-job-id")
            requisition_text = _text_or_none(link.select_one(".job-id"))
            requisition_id = _extract_requisition_id(requisition_text)
            yield JobSummary(
                title=_text_or_none(link.select_one(".job-title")) or "",
                detail_url=urljoin(BASE_URL, link.get("href", "")),
                job_id=job_id_attr,
                requisition_id=requisition_id,
                location=_text_or_none(link.select_one(".job-location")),
                date_posted=_normalize_date_label(_text_or_none(link.select_one(".job-date-posted"))),
            )

    def _fetch_job_detail(self, url: str) -> Dict[str, Optional[str]]:
        response = self._get_detail_session().get(url, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        apply_link = soup.select_one("a.ajd_btn__apply")
        description_block = soup.select_one(".ats-description")

        job_info: Dict[str, str] = {}
        for block in soup.select(".ajd_header__job-heading span.job-info, .ats-description span.job-info"):
            label_elem = block.find("b")
            if not label_elem:
                continue
            label = label_elem.get_text(strip=True).rstrip(":")
            value = _text_or_none(_first_non_label_child(block, label_elem)) or _text_or_none(block)
            if label and value:
                job_info[label] = value

        meta_fields = {
            "ATS Requisition ID": _meta_content(soup, "job-ats-req-id"),
            "Feed ID": _meta_content(soup, "search-job-feed-id"),
            "Job Category IDs": _meta_content(soup, "job-category-ids"),
            "Job Location IDs": _meta_content(soup, "job-location-ids"),
        }
        for key, value in meta_fields.items():
            if value and key not in job_info:
                job_info[key] = value

        return {
            "apply_url": apply_link.get("href") if apply_link else _meta_content(soup, "search-job-apply-url"),
            "description_text": _text_or_none(description_block, separator="\n\n"),
            "description_html": str(description_block) if description_block else None,
            "job_info": job_info,
        }

    def _get_detail_session(self) -> requests.Session:
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update(DEFAULT_HEADERS)
            session.cookies.update(self.session.cookies)
            self._thread_local.session = session
        return session


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


def _extract_requisition_id(raw_text: Optional[str]) -> Optional[str]:
    if not raw_text:
        return None
    parts = raw_text.split(":", 1)
    if len(parts) == 2:
        return parts[1].strip() or None
    return raw_text.strip() or None


def _normalize_date_label(raw_text: Optional[str]) -> Optional[str]:
    if not raw_text:
        return None
    if ":" in raw_text:
        return raw_text.split(":", 1)[1].strip() or None
    return raw_text.strip() or None


def _meta_content(soup: BeautifulSoup, name: str) -> Optional[str]:
    tag = soup.find("meta", attrs={"name": name})
    content = tag.get("content") if tag else None
    return content or None


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


def run_scrape(max_pages: Optional[int], limit: Optional[int], delay: float, max_workers: int) -> int:
    scraper = LockheedJobScraper(delay=delay, max_workers=max_workers)
    count = 0
    for job in scraper.scrape(max_pages=max_pages, limit=limit):
        store_listing(job)
        count += 1
    return count


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lockheed Martin careers manual scraper")
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--delay", type=float, default=0.25)
    parser.add_argument("--max-workers", type=int, default=8)
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    start = time.time()
    try:
        count = run_scrape(args.max_pages, args.limit, args.delay, args.max_workers)
    except ScraperError as exc:
        logging.error("Scrape failed: %s", exc)
        return 1
    dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
    duration = time.time() - start
    summary = {
        "company": "Lockheed Martin",
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
