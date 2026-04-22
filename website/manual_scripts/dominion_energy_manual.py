#!/usr/bin/env python3
"""Manual scraper for the Dominion Energy careers portal.

This script walks the public search results under
https://careers.dominionenergy.com/search-jobs, hydrates each summary with the
corresponding job-detail page, and persists the data via Django's ORM so that
operations staff can run it on demand from the dashboard.
"""
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
from typing import Dict, Iterable, Iterator, List, Optional
from urllib.parse import parse_qsl, urljoin, urlsplit

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
from django.conf import settings  # noqa: E402
from django.db import IntegrityError  # noqa: E402

django.setup()

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

# ---------------------------------------------------------------------------
# Constants & configuration
# ---------------------------------------------------------------------------
BASE_URL = "https://careers.dominionenergy.com"
SEARCH_PATH = "/search-jobs"
SEARCH_URL = urljoin(BASE_URL, SEARCH_PATH)
REQUEST_TIMEOUT = (10, 30)
DEFAULT_DELAY = 0.2
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": SEARCH_URL,
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 30)
SCRAPER_QS = Scraper.objects.filter(company="Dominion Energy", url=SEARCH_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning(
            "Multiple Scraper rows matched Dominion Energy; using id=%s.", SCRAPER.id
        )
else:
    SCRAPER = Scraper.objects.create(
        company="Dominion Energy",
        url=SEARCH_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


# ---------------------------------------------------------------------------
# Data structures & helpers
# ---------------------------------------------------------------------------
class ScraperError(Exception):
    """Raised when the Dominion Energy scrape cannot proceed."""


@dataclass
class JobSummary:
    title: str
    link: str
    location: Optional[str]
    date_posted: Optional[str]
    req_id: Optional[str]
    job_id: Optional[str]


@dataclass
class JobListing(JobSummary):
    apply_url: Optional[str]
    description_text: str
    description_html: Optional[str]
    metadata: Dict[str, object]


def _clean_text(fragment: Optional[str]) -> str:
    """Convert HTML fragments into normalized text."""
    if not fragment:
        return ""
    soup = BeautifulSoup(fragment, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    return soup.get_text("\n", strip=True).strip()


def _strip_join(node: Optional[Tag]) -> Optional[str]:
    if not node:
        return None
    text = node.get_text(" ", strip=True)
    return text or None


def _trim_metadata(data: Dict[str, object]) -> Dict[str, object]:
    return {k: v for k, v in data.items() if v not in (None, "", [], {})}


# ---------------------------------------------------------------------------
# Scraper implementation
# ---------------------------------------------------------------------------
class DominionEnergyScraper:
    def __init__(
        self,
        *,
        delay: float = DEFAULT_DELAY,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.base_params: Optional[Dict[str, str]] = None
        self.logger = logging.getLogger(self.__class__.__name__)

    def scrape(
        self,
        *,
        max_pages: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Iterator[JobListing]:
        """Iterate over hydrated job listings."""
        page_number = 0
        startrow = 0
        total_results: Optional[int] = None
        per_page: Optional[int] = None
        processed = 0

        while True:
            page_number += 1
            if max_pages is not None and page_number > max_pages:
                self.logger.info("Reached max_pages=%s; stopping pagination.", max_pages)
                break

            soup = self._fetch_search_page(startrow)
            summaries = list(self._parse_rows(soup))
            if not summaries:
                if page_number == 1:
                    self.logger.warning("No job rows found on the first page.")
                else:
                    self.logger.info("No rows returned at startrow=%s; ending pagination.", startrow)
                break

            if per_page is None:
                per_page = len(summaries) or 25
                self.logger.debug("Detected per_page=%s", per_page)

            if total_results is None:
                total_results = self._extract_total_results(soup)
                if total_results is not None:
                    self.logger.info("Discovered %s total jobs.", total_results)

            for summary in summaries:
                detail = self._fetch_job_detail(summary.link)
                listing = JobListing(
                    **summary.__dict__,
                    apply_url=detail.get("apply_url"),
                    description_text=detail.get("description_text") or "",
                    description_html=detail.get("description_html"),
                    metadata=_trim_metadata(
                        {
                            "job_id": summary.job_id,
                            "req_id": summary.req_id,
                            "apply_url": detail.get("apply_url"),
                            "detail_url": summary.link,
                            "search_location": summary.location,
                            "search_date": summary.date_posted,
                            "startrow": startrow,
                            "page": page_number,
                            "total_results": total_results,
                        }
                    ),
                )
                yield listing
                processed += 1

                if limit is not None and processed >= limit:
                    self.logger.info("Reached limit=%s; stopping scrape.", limit)
                    return

            if per_page is None or per_page <= 0:
                break

            startrow += per_page
            if total_results is not None and startrow >= total_results:
                break

            if self.delay:
                time.sleep(self.delay)

    # ------------------------------------------------------------------ #
    # Internal helpers                                                   #
    # ------------------------------------------------------------------ #
    def _fetch_search_page(self, startrow: int) -> BeautifulSoup:
        params: Optional[Dict[str, str]] = None
        if self.base_params is None and startrow == 0:
            params = None
        else:
            base_params = self.base_params or {}
            params = dict(base_params)
            if startrow:
                params["startrow"] = str(startrow)
            else:
                params.pop("startrow", None)

        response = self.session.get(SEARCH_URL, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        if self.base_params is None:
            parsed = urlsplit(response.url)
            self.base_params = dict(parse_qsl(parsed.query, keep_blank_values=True))

        return BeautifulSoup(response.text, "html.parser")

    def _parse_rows(self, soup: BeautifulSoup) -> Iterable[JobSummary]:
        for row in soup.select("table.searchResults tr.data-row"):
            anchor = row.select_one("a.jobTitle-link")
            if not anchor or not anchor.get("href"):
                continue

            link = urljoin(BASE_URL, anchor["href"])
            title = anchor.get_text(" ", strip=True)
            location = _strip_join(row.select_one("td.colLocation .jobLocation"))
            if not location:
                location = _strip_join(row.select_one(".jobdetail-phone .jobLocation"))
            date_posted = _strip_join(row.select_one("td.colDate .jobDate"))
            if not date_posted:
                date_posted = _strip_join(row.select_one(".jobdetail-phone .jobDate"))
            req_id = _strip_join(row.select_one("td.colFacility .jobFacility"))

            job_id = link.rstrip("/").split("/")[-1] if link else None
            yield JobSummary(
                title=title,
                link=link,
                location=location,
                date_posted=date_posted,
                req_id=req_id,
                job_id=job_id,
            )

    def _fetch_job_detail(self, url: str) -> Dict[str, Optional[str]]:
        response = self.session.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        desc_node = soup.select_one(".jobdescription")
        description_html = str(desc_node) if desc_node else None
        description_text = _clean_text(description_html)

        apply_node = soup.select_one("div.applylink a[href]")
        apply_href = urljoin(BASE_URL, apply_node["href"]) if apply_node else None

        return {
            "apply_url": apply_href,
            "description_html": description_html,
            "description_text": description_text,
        }

    def _extract_total_results(self, soup: BeautifulSoup) -> Optional[int]:
        label = soup.select_one(".paginationLabel")
        if not label:
            return None
        text = label.get_text(" ", strip=True)
        match = re.search(r"of\s+([\d,]+)", text)
        if match:
            try:
                return int(match.group(1).replace(",", ""))
            except ValueError:
                pass

        numbers: List[int] = []
        for token in text.replace(",", " ").split():
            cleaned = token.replace("\u2013", "").replace("-", "")
            if cleaned.isdigit():
                try:
                    numbers.append(int(token.replace(",", "")))
                except ValueError:
                    continue
        if numbers:
            return max(numbers)
        return None


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------
def store_listing(listing: JobListing) -> None:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255],
        "date": (listing.date_posted or "")[:100],
        "description": listing.description_text[:10000],
        "metadata": listing.metadata,
    }
    try:
        JobPosting.objects.update_or_create(
            scraper=SCRAPER,
            link=listing.link,
            defaults=defaults,
        )
    except IntegrityError as exc:
        raise ScraperError(f"Failed to store listing {listing.job_id or listing.link}: {exc}") from exc


# ---------------------------------------------------------------------------
# CLI orchestration
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dominion Energy careers manual scraper")
    parser.add_argument("--max-pages", type=int, default=None, help="Maximum number of listing pages to fetch")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of job records to process")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Delay between page requests (seconds)")
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity",
    )
    return parser.parse_args(argv)


def run_scrape(args: argparse.Namespace) -> Dict[str, object]:
    scraper = DominionEnergyScraper(delay=args.delay)
    processed = 0
    for listing in scraper.scrape(max_pages=args.max_pages, limit=args.limit):
        store_listing(listing)
        processed += 1

    dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
    return {
        "processed_jobs": processed,
        "deduplicated": dedupe_summary,
    }


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    start_time = time.time()

    try:
        outcome = run_scrape(args)
    except requests.RequestException as exc:
        logging.error("HTTP error during scrape: %s", exc)
        return 1
    except ScraperError as exc:
        logging.error("Scraper aborted: %s", exc)
        return 1

    duration = time.time() - start_time
    summary = {
        "company": "Dominion Energy",
        "site": SEARCH_URL,
        "elapsed_seconds": round(duration, 2),
        **outcome,
    }
    logging.info("Scrape summary: %s", json.dumps(summary))
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
