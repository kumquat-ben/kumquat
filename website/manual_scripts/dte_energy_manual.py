#!/usr/bin/env python3
"""Manual scraper for DTE Energy's career listings.

The public careers catalog at https://careers.dteenergy.com/go/View-All-Jobs/4476200/
renders a paginated HTML table. This script walks the list pages, hydrates
each posting via its detail page, and upserts records into `JobPosting`.
It is intended for one-off/manual execution from the operations dashboard.
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
from typing import Dict, Iterable, Iterator, Optional, Set
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag

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
BASE_DOMAIN = "https://careers.dteenergy.com"
LISTING_PATH = "/go/View-All-Jobs/4476200/"
JOB_LIST_URL = urljoin(BASE_DOMAIN, LISTING_PATH)
LISTING_QUERY = {
    "q": "",
    "sortColumn": "referencedate",
    "sortDirection": "desc",
}
DEFAULT_PAGE_SIZE = 25
REQUEST_TIMEOUT = (15, 45)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": JOB_LIST_URL,
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 60)
SCRAPER_QS = Scraper.objects.filter(company="DTE Energy", url=JOB_LIST_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.getLogger(__name__).warning(
            "Multiple Scraper rows matched DTE Energy; using id=%s.", SCRAPER.id
        )
else:
    SCRAPER = Scraper.objects.create(
        company="DTE Energy",
        url=JOB_LIST_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
class ScraperError(RuntimeError):
    """Raised when the scraper pipeline cannot proceed."""


def collapse_whitespace(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = " ".join(value.replace("\xa0", " ").split())
    return cleaned or None


def html_to_text(value: Optional[str]) -> str:
    if not value:
        return ""
    soup = BeautifulSoup(value, "html.parser")
    text = soup.get_text("\n", strip=True)
    return text.strip()


def clean_label(value: Optional[str]) -> Optional[str]:
    cleaned = collapse_whitespace(value)
    if not cleaned:
        return None
    return cleaned.rstrip(":")


# ---------------------------------------------------------------------------
# Data containers
# ---------------------------------------------------------------------------
@dataclass
class ListingSummary:
    title: str
    link: str
    location: Optional[str]
    job_id: Optional[str]
    department: Optional[str]
    facility: Optional[str]


@dataclass
class JobListing:
    title: str
    link: str
    location: Optional[str]
    job_id: Optional[str]
    department: Optional[str]
    facility: Optional[str]
    company: Optional[str]
    posted_date: Optional[str]
    description_text: str
    description_html: Optional[str]
    apply_url: Optional[str]
    metadata: Dict[str, object]


# ---------------------------------------------------------------------------
# Client implementation
# ---------------------------------------------------------------------------
class DTECareersClient:
    def __init__(
        self,
        *,
        delay: float = 0.5,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.page_size = DEFAULT_PAGE_SIZE
        self.logger = logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------------ #
    # Public API                                                         #
    # ------------------------------------------------------------------ #
    def iter_listings(
        self,
        *,
        limit: Optional[int] = None,
        max_pages: Optional[int] = None,
        start_page: int = 0,
    ) -> Iterator[JobListing]:
        produced = 0
        page_index = max(0, start_page)
        seen_links: Set[str] = set()

        while True:
            if max_pages is not None and (page_index - start_page) >= max_pages:
                self.logger.info(
                    "Reached page cap (%s pages from offset %s).", max_pages, start_page
                )
                break

            offset = page_index * self.page_size
            page_url = self._build_page_url(offset)
            soup = self._fetch_page(page_url)
            rows = soup.select("tr.data-row")
            if not rows:
                if page_index == start_page:
                    self.logger.warning(
                        "No job rows discovered at %s; site structure might have changed.",
                        page_url,
                    )
                else:
                    self.logger.info("Pagination exhausted at offset %s.", offset)
                break

            page_label = self._extract_pagination_label(soup)
            if page_label:
                self.logger.debug("Processing page %s (%s)", page_index, page_label)

            for row in rows:
                summary = self._parse_row(row)
                if summary is None:
                    continue

                if summary.link in seen_links:
                    continue
                seen_links.add(summary.link)

                try:
                    listing = self._hydrate_summary(summary)
                except ScraperError as exc:
                    self.logger.error("Failed to hydrate %s: %s", summary.link, exc)
                    continue

                yield listing
                produced += 1
                if limit is not None and produced >= limit:
                    self.logger.info("Result limit %s reached; stopping.", limit)
                    return

            page_index += 1
            if self.delay:
                time.sleep(self.delay)

    # ------------------------------------------------------------------ #
    # Internal helpers                                                   #
    # ------------------------------------------------------------------ #
    def _build_page_url(self, offset: int) -> str:
        path = LISTING_PATH if offset <= 0 else f"{LISTING_PATH}{offset}/"
        return urljoin(BASE_DOMAIN, path)

    def _fetch_page(self, url: str) -> BeautifulSoup:
        self.logger.debug("Fetching listing page: %s", url)
        try:
            response = self.session.get(url, params=LISTING_QUERY, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch listing page {url}: {exc}") from exc
        return BeautifulSoup(response.text, "html.parser")

    def _extract_pagination_label(self, soup: BeautifulSoup) -> Optional[str]:
        label = soup.select_one("span.paginationLabel")
        return collapse_whitespace(label.get_text(" ", strip=True) if label else None)

    def _parse_row(self, row: Tag) -> Optional[ListingSummary]:
        link_el = row.select_one("a.jobTitle-link")
        if not link_el or not link_el.get("href"):
            return None
        title = collapse_whitespace(link_el.get_text(" ", strip=True))
        href = link_el["href"].strip()
        link = urljoin(BASE_DOMAIN, href)
        location_el = row.select_one("span.jobLocation")
        location = collapse_whitespace(location_el.get_text(" ", strip=True) if location_el else None)
        department_el = row.select_one("span.jobDepartment")
        department = collapse_whitespace(
            department_el.get_text(" ", strip=True) if department_el else None
        )
        facility_el = row.select_one("span.jobFacility")
        facility = collapse_whitespace(facility_el.get_text(" ", strip=True) if facility_el else None)
        job_id_el = row.select_one("span.jobShifttype")
        job_id = collapse_whitespace(job_id_el.get_text(" ", strip=True) if job_id_el else None)

        if not title:
            return None

        return ListingSummary(
            title=title,
            link=link,
            location=location,
            job_id=job_id,
            department=department,
            facility=facility,
        )

    def _fetch_detail(self, url: str) -> BeautifulSoup:
        self.logger.debug("Fetching detail page: %s", url)
        try:
            response = self.session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch job detail {url}: {exc}") from exc
        return BeautifulSoup(response.text, "html.parser")

    def _hydrate_summary(self, summary: ListingSummary) -> JobListing:
        soup = self._fetch_detail(summary.link)
        desc_el = soup.select_one("span.jobdescription")
        description_html = desc_el.decode_contents().strip() if desc_el else None
        description_text = html_to_text(description_html)
        if not description_text and desc_el:
            description_text = html_to_text(desc_el.get_text(" ", strip=True))
        if not description_text:
            description_text = "Description unavailable."

        posted_date = None
        posted_node = soup.select_one("[itemprop=datePosted]")
        if posted_node:
            posted_date = collapse_whitespace(
                posted_node.get("content") or posted_node.get_text(" ", strip=True)
            )
        if not posted_date:
            alt_node = soup.select_one("span.job-date") or soup.select_one("span.jobdate")
            if alt_node:
                posted_date = collapse_whitespace(alt_node.get_text(" ", strip=True))

        apply_link = soup.select_one("a#applyJob, a.apply, a[class*=apply], a[id*=apply]")
        apply_url = None
        if apply_link and apply_link.get("href"):
            apply_url = urljoin(BASE_DOMAIN, apply_link["href"])

        token_data: Dict[str, Optional[str]] = {}
        for label in soup.select("div.jobDisplay span.joblayouttoken-label"):
            value = label.find_next_sibling("span")
            key = clean_label(label.get_text(" ", strip=True))
            val = collapse_whitespace(value.get_text(" ", strip=True) if value else None)
            if key:
                token_data[key.lower()] = val

        company = token_data.get("company")
        detail_location = token_data.get("location")
        detail_job_id = token_data.get("job id")

        metadata: Dict[str, object] = {
            "job_id": summary.job_id or detail_job_id,
            "department": summary.department,
            "facility": summary.facility,
            "company": company,
            "detail_tokens": token_data,
            "detail_location": detail_location,
            "apply_url": apply_url,
        }
        if description_html:
            metadata["description_html"] = description_html

        return JobListing(
            title=summary.title,
            link=summary.link,
            location=summary.location or detail_location,
            job_id=summary.job_id or detail_job_id,
            department=summary.department,
            facility=summary.facility,
            company=company,
            posted_date=posted_date,
            description_text=description_text,
            description_html=description_html,
            apply_url=apply_url,
            metadata=metadata,
        )


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------
def persist_listing(listing: JobListing) -> bool:
    metadata = {k: v for k, v in listing.metadata.items() if v is not None}
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255] or None,
        "date": (listing.posted_date or "")[:100] or None,
        "description": listing.description_text[:10000],
        "metadata": metadata,
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.link,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Stored job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape DTE Energy job postings and persist them via the Django ORM."
    )
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Maximum number of list pages to traverse.",
    )
    parser.add_argument(
        "--start-page",
        type=int,
        default=0,
        help="Page index offset to begin from (0 = first page).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Seconds to sleep between page fetches (default: 0.5).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Emit JSON instead of persisting results.")
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
    logger = logging.getLogger("dte_energy")

    client = DTECareersClient(delay=args.delay)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    for listing in client.iter_listings(
        limit=args.limit,
        max_pages=args.max_pages,
        start_page=args.start_page,
    ):
        totals["fetched"] += 1
        if args.dry_run:
            payload = {
                "title": listing.title,
                "link": listing.link,
                "location": listing.location,
                "job_id": listing.job_id,
                "department": listing.department,
                "facility": listing.facility,
                "company": listing.company,
                "posted_date": listing.posted_date,
                "apply_url": listing.apply_url,
            }
            print(json.dumps(payload, ensure_ascii=False))
            continue

        try:
            created = persist_listing(listing)
        except Exception as exc:  # pragma: no cover - defensive persistence logging
            totals["errors"] += 1
            logger.error("Failed to persist %s: %s", listing.link, exc)
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
        "DTE Energy scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
