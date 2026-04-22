#!/usr/bin/env python3
"""Manual scraper for ExxonMobil careers (SuccessFactors platform)."""

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
# Django setup
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
CAREERS_LANDING_URL = "https://corporate.exxonmobil.com/careers"
BASE_URL = "https://jobs.exxonmobil.com"
SEARCH_PATH = "/search/"
DEFAULT_LISTING_PARAMS = {
    "createNewAlert": "false",
    "q": "",
    "locationsearch": "",
    "sortColumn": "referencedate",
    "sortDirection": "desc",
}
PAGE_SIZE = 25
REQUEST_TIMEOUT = (15, 45)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": CAREERS_LANDING_URL,
}
TOTAL_RESULTS_PATTERN = re.compile(r"of\s+([\d,]+)")
DEFAULT_DELAY = 0.3
DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 3600), 60)

SCRAPER_QS = Scraper.objects.filter(company="ExxonMobil", url=CAREERS_LANDING_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple ExxonMobil scraper rows found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="ExxonMobil",
        url=CAREERS_LANDING_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the scrape pipeline cannot proceed."""


@dataclass
class JobSummary:
    title: str
    detail_url: str
    job_id: Optional[str]
    location: Optional[str]
    department: Optional[str]
    shift_type: Optional[str]
    date_posted: Optional[str]


@dataclass
class JobListing(JobSummary):
    apply_url: Optional[str]
    description_text: str
    description_html: Optional[str]
    metadata: Dict[str, object]


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
def _text_or_none(element: Optional[Tag], *, separator: str = " ") -> Optional[str]:
    if not element:
        return None
    text = element.get_text(separator=separator, strip=True)
    return text or None


def _clean_text(html_fragment: Optional[str]) -> str:
    if not html_fragment:
        return ""
    soup = BeautifulSoup(html_fragment, "html.parser")
    return soup.get_text("\n", strip=True).strip()


def _extract_job_id(detail_url: str) -> Optional[str]:
    if not detail_url:
        return None
    parts = detail_url.rstrip("/").split("/")
    if not parts:
        return None
    candidate = parts[-1]
    return candidate if candidate.isdigit() else None


def _compact_metadata(items: Iterable[tuple[str, object]]) -> Dict[str, object]:
    result: Dict[str, object] = {}
    for key, value in items:
        if value is None:
            continue
        if isinstance(value, str):
            trimmed = value.strip()
            if not trimmed:
                continue
            result[key] = trimmed
            continue
        if isinstance(value, (list, dict)) and not value:
            continue
        result[key] = value
    return result


# ---------------------------------------------------------------------------
# Scraper implementation
# ---------------------------------------------------------------------------
class ExxonMobilJobScraper:
    def __init__(
        self,
        *,
        delay: float = DEFAULT_DELAY,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.page_size = PAGE_SIZE
        self.logger = logging.getLogger(self.__class__.__name__)

    def scrape(
        self,
        *,
        max_pages: Optional[int] = None,
        limit: Optional[int] = None,
        start_row: int = 0,
    ) -> Iterator[JobListing]:
        offset = max(0, start_row)
        yielded = 0
        page_count = 0
        total_results: Optional[int] = None

        while True:
            soup = self._fetch_search_page(offset)
            if total_results is None:
                total_results = self._extract_total_results(soup)
                if total_results:
                    self.logger.info("Total results reported: %s", total_results)

            summaries = list(self._parse_job_summaries(soup))
            if not summaries:
                self.logger.info("No job summaries found at offset %s; stopping.", offset)
                break

            for summary in summaries:
                listing = self._hydrate_listing(summary)
                yield listing
                yielded += 1
                if limit is not None and yielded >= limit:
                    self.logger.info("Reached limit=%s; ending scrape.", limit)
                    return
                time.sleep(self.delay)

            page_count += 1
            if max_pages is not None and page_count >= max_pages:
                self.logger.info("Reached max_pages=%s; stopping.", max_pages)
                break

            offset += self.page_size
            if total_results is not None and offset >= total_results:
                self.logger.info("Consumed all reported results (%s); stopping.", total_results)
                break

            time.sleep(self.delay)

    # ------------------------------------------------------------------ #
    # Network helpers                                                    #
    # ------------------------------------------------------------------ #
    def _fetch_search_page(self, offset: int) -> BeautifulSoup:
        params = dict(DEFAULT_LISTING_PARAMS)
        if offset:
            params["startrow"] = str(offset)
        response = self.session.get(
            urljoin(BASE_URL, SEARCH_PATH),
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        if response.status_code >= 400:
            raise ScraperError(f"Listing request failed ({response.status_code}) for offset={offset}")
        return BeautifulSoup(response.text, "html.parser")

    def _hydrate_listing(self, summary: JobSummary) -> JobListing:
        response = self.session.get(summary.detail_url, timeout=REQUEST_TIMEOUT)
        if response.status_code >= 400:
            raise ScraperError(f"Detail request failed ({response.status_code}) for url={summary.detail_url}")
        soup = BeautifulSoup(response.text, "html.parser")

        description_node = soup.select_one("span.jobdescription")
        description_html = str(description_node) if description_node else None
        description_text = _clean_text(description_node.decode_contents() if description_node else None)

        apply_elem = soup.select_one("a.apply, a.dialogApplyBtn, a.applylink")
        apply_href = apply_elem.get("href") if apply_elem else None
        apply_url = urljoin(BASE_URL, apply_href) if apply_href else None

        job_segment = _text_or_none(soup.select_one("span.jobsegments span[itemprop='industry']"))
        job_markets = [
            item.get_text(strip=True)
            for item in soup.select("span.jobmarkets a")
            if item.get_text(strip=True)
        ]
        geo_location = _text_or_none(soup.select_one("span.jobGeoLocation"))

        schema_meta: Dict[str, Optional[str]] = {}
        for prop in ("addressLocality", "addressRegion", "addressCountry", "datePosted", "hiringOrganization"):
            tag = soup.select_one(f"meta[itemprop='{prop}']")
            schema_meta[prop] = tag.get("content") if tag and tag.get("content") else None

        metadata = _compact_metadata(
            (
                ("job_id", summary.job_id),
                ("department", summary.department),
                ("shift_type", summary.shift_type),
                ("job_segment", job_segment),
                ("job_markets", job_markets),
                ("detail_location", geo_location),
                ("apply_url", apply_url),
                ("detail_url", summary.detail_url),
                *(("schema_" + key, value) for key, value in schema_meta.items()),
            )
        )

        return JobListing(
            **asdict(summary),
            apply_url=apply_url,
            description_text=description_text,
            description_html=description_html,
            metadata=metadata,
        )

    # ------------------------------------------------------------------ #
    # Parsing helpers                                                    #
    # ------------------------------------------------------------------ #
    def _extract_total_results(self, soup: BeautifulSoup) -> Optional[int]:
        label = soup.select_one(".pagination-label-row .paginationLabel")
        if not label:
            return None
        text = label.get_text(" ", strip=True)
        match = TOTAL_RESULTS_PATTERN.search(text)
        if not match:
            return None
        try:
            return int(match.group(1).replace(",", ""))
        except ValueError:
            return None

    def _parse_job_summaries(self, soup: BeautifulSoup) -> Iterable[JobSummary]:
        for row in soup.select("tr.data-row"):
            link_elem = row.select_one("a.jobTitle-link")
            if not link_elem or not link_elem.get("href"):
                continue
            detail_url = urljoin(BASE_URL, link_elem["href"])
            title = link_elem.get_text(strip=True)
            location = _text_or_none(row.select_one("span.jobLocation"))
            department = _text_or_none(row.select_one("span.jobDepartment"))
            shift_type = _text_or_none(row.select_one("span.jobShifttype"))
            date_posted = _text_or_none(row.select_one("span.jobDate"))
            job_id = _extract_job_id(detail_url)

            yield JobSummary(
                title=title,
                detail_url=detail_url,
                job_id=job_id,
                location=location,
                department=department,
                shift_type=shift_type,
                date_posted=date_posted,
            )


# ---------------------------------------------------------------------------
# Persistence & CLI
# ---------------------------------------------------------------------------
def store_listing(listing: JobListing) -> None:
    JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults={
            "title": listing.title,
            "location": listing.location or "",
            "date": listing.date_posted or "",
            "description": listing.description_text,
            "metadata": listing.metadata,
        },
    )


def run_scrape(max_pages: Optional[int], limit: Optional[int], delay: float, start_row: int) -> int:
    scraper = ExxonMobilJobScraper(delay=delay)
    count = 0
    for job in scraper.scrape(max_pages=max_pages, limit=limit, start_row=start_row):
        store_listing(job)
        count += 1
    return count


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ExxonMobil manual careers scraper")
    parser.add_argument("--max-pages", type=int, default=None, help="Limit the number of result pages processed.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to ingest.")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Delay (seconds) between requests.")
    parser.add_argument("--start-row", type=int, default=0, help="Offset into the result set (multiples of 25).")
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    start_time = time.time()

    try:
        count = run_scrape(
            max_pages=args.max_pages,
            limit=args.limit,
            delay=args.delay,
            start_row=args.start_row,
        )
    except ScraperError as exc:
        logging.error("Scrape failed: %s", exc)
        return 1

    dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
    elapsed = time.time() - start_time
    summary = {
        "company": "ExxonMobil",
        "landing_url": CAREERS_LANDING_URL,
        "search_url": urljoin(BASE_URL, SEARCH_PATH),
        "jobs_processed": count,
        "elapsed_seconds": elapsed,
        "dedupe": dedupe_summary,
    }
    logging.info("Summary: %s", json.dumps(summary))
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
