#!/usr/bin/env python3
"""Manual scraper for https://www.gd.com/careers/job-search.

The public careers site for General Dynamics renders job results via a JSON API
that requires per-request HMAC headers exposed in the search page. This manual
scraper reproduces that flow: it boots the search page to collect the nonce,
signature, and timestamp values, pages through the API with a configurable
page size, enriches each result with detail-page content, and persists the
postings into ``scrapers.JobPosting``.
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Generator, Iterable, List, Optional
from urllib.parse import urljoin

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
from django.db import IntegrityError  # noqa: E402

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

# ---------------------------------------------------------------------------
# Constants & configuration
# ---------------------------------------------------------------------------
BASE_URL = "https://www.gd.com"
JOB_SEARCH_URL = f"{BASE_URL}/careers/job-search"
REQUEST_TIMEOUT = (15, 45)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": JOB_SEARCH_URL,
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 1800), 30)
SCRAPER_QS = Scraper.objects.filter(company="General Dynamics", url=JOB_SEARCH_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched; using the earliest (id=%s).", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="General Dynamics",
        url=JOB_SEARCH_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(RuntimeError):
    """Raised when the scraper cannot proceed."""


@dataclass
class JobSummary:
    reference_code: Optional[str]
    title: str
    detail_url: str
    location: Optional[str]
    posted_date: Optional[str]
    raw_api_record: Dict[str, object]


@dataclass
class JobListing(JobSummary):
    description_text: str
    description_html: str
    metadata: Dict[str, object]


def _clean_text(html_fragment: str) -> str:
    soup = BeautifulSoup(html_fragment or "", "html.parser")
    return soup.get_text("\n", strip=True).strip()


class GeneralDynamicsClient:
    def __init__(
        self,
        *,
        page_size: int = 100,
        delay: float = 0.25,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.page_size = max(1, min(page_size, 200))
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)
        self._api_endpoint: Optional[str] = None
        self._auth_headers: Dict[str, str] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def scrape(self, limit: Optional[int] = None) -> Generator[JobListing, None, None]:
        total_yielded = 0
        for summary in self._iter_summaries(limit=limit):
            detail = self._fetch_detail(summary.detail_url)
            listing = JobListing(
                **summary.__dict__,
                description_text=_clean_text(detail["description_html"]),
                description_html=detail["description_html"],
                metadata={
                    "apply_links": detail["apply_links"],
                    "sidebar": detail["sidebar"],
                    "api_record": summary.raw_api_record,
                },
            )
            yield listing
            total_yielded += 1
            if limit and total_yielded >= limit:
                break
            if self.delay:
                time.sleep(self.delay)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _iter_summaries(self, limit: Optional[int]) -> Iterable[JobSummary]:
        self._bootstrap()

        page = 0
        remaining = limit if limit is not None else None

        while True:
            payload = {
                "page": page,
                "pageSize": self.page_size,
                "facets": [],
                "address": [],
                "usedPlacesApi": False,
            }
            data = self._call_api(payload)
            results: List[Dict[str, object]] = data.get("Results") or []

            if not results:
                self.logger.info("No results returned at page %s; stopping.", page)
                break

            for record in results:
                link = (record.get("Link") or {}).get("Url")
                if not link:
                    continue
                detail_url = urljoin(BASE_URL, link)
                location_names = record.get("LocationNames") or []
                location = location_names[0] if location_names else None
                summary = JobSummary(
                    reference_code=record.get("ReferenceCode"),
                    title=str(record.get("Title") or "").strip(),
                    detail_url=detail_url,
                    location=location,
                    posted_date=record.get("FormattedDate") or record.get("Date"),
                    raw_api_record=record,
                )
                yield summary
                if remaining is not None:
                    remaining -= 1
                    if remaining <= 0:
                        return

            page += 1
            page_count = data.get("PageCount")
            if page_count is not None and page >= page_count:
                break

    def _bootstrap(self) -> None:
        if self._api_endpoint and self._auth_headers:
            return

        try:
            resp = self.session.get(JOB_SEARCH_URL, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to load job search page: {exc}") from exc

        soup = BeautifulSoup(resp.text, "html.parser")

        auth_block = soup.select_one(".js-api-authentication")
        if not auth_block:
            raise ScraperError("Missing API authentication block on job search page.")

        form = soup.select_one(".js-career-search__form")
        if not form:
            raise ScraperError("Missing search form configuration on job search page.")

        endpoint = form.get("data-search-endpoint")
        if not endpoint:
            raise ScraperError("Search form did not expose an API endpoint.")

        self._api_endpoint = urljoin(BASE_URL, endpoint)
        self._auth_headers = {
            "api-auth-nonce": auth_block.get("data-nonce") or "",
            "api-auth-signature": auth_block.get("data-signature") or "",
            "api-auth-timestamp": auth_block.get("data-timestamp") or "",
        }

        if not all(self._auth_headers.values()):
            raise ScraperError("Incomplete API authentication headers discovered on job search page.")

        self.logger.debug("Bootstrapped API endpoint %s", self._api_endpoint)

    def _call_api(self, payload: Dict[str, object]) -> Dict[str, object]:
        if not self._api_endpoint:
            raise ScraperError("API endpoint was not initialised before calling _call_api.")

        encoded = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("utf-8")

        try:
            resp = self.session.get(
                self._api_endpoint,
                params={"request": encoded},
                headers=self._auth_headers,
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"API request failed at page {payload.get('page')}: {exc}") from exc

        try:
            data = resp.json()
        except json.JSONDecodeError as exc:
            raise ScraperError(f"Failed to decode API response: {exc}") from exc

        if data.get("IsLogicError"):
            raise ScraperError("API reported a logic error; check request payload.")

        return data

    def _fetch_detail(self, url: str) -> Dict[str, object]:
        try:
            resp = self.session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch job detail page {url}: {exc}") from exc

        soup = BeautifulSoup(resp.text, "html.parser")

        description_block = soup.select_one("div.career-detail-description")
        description_html = str(description_block) if description_block else ""

        sidebar_entries = []
        for inset in soup.select("div.career-detail__sidebar div.career-detail__inset"):
            heading = inset.find("h5")
            body = inset.find("div", class_="career-detail__copy")
            sidebar_entries.append(
                {
                    "heading": heading.get_text(strip=True) if heading else None,
                    "body": _clean_text(str(body)) if body else _clean_text(str(inset)),
                }
            )

        apply_links = []
        for anchor in soup.select("a"):
            href = anchor.get("href")
            if href and "apply" in href.lower():
                apply_links.append(href.strip())

        return {
            "description_html": description_html,
            "sidebar": sidebar_entries,
            "apply_links": apply_links,
        }


def store_listing(listing: JobListing) -> None:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255],
        "date": (listing.posted_date or "")[:100],
        "description": listing.description_text[:10000],
        "metadata": listing.metadata,
    }
    try:
        JobPosting.objects.update_or_create(
            scraper=SCRAPER,
            link=listing.detail_url,
            defaults=defaults,
        )
    except IntegrityError as exc:
        raise ScraperError(f"Failed to persist job at {listing.detail_url}: {exc}") from exc


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="General Dynamics careers manual scraper")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process")
    parser.add_argument("--page-size", type=int, default=100, help="API page size (max 200)")
    parser.add_argument("--delay", type=float, default=0.1, help="Delay between detail page fetches (seconds)")
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity",
    )
    return parser.parse_args(argv)


def run_scrape(args: argparse.Namespace) -> Dict[str, object]:
    client = GeneralDynamicsClient(page_size=args.page_size, delay=args.delay)
    processed = 0
    for listing in client.scrape(limit=args.limit):
        store_listing(listing)
        processed += 1
    dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
    return {"processed_jobs": processed, "deduplicated": dedupe_summary}


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    start = time.time()
    try:
        outcome = run_scrape(args)
    except ScraperError as exc:
        logging.error("Scrape failed: %s", exc)
        return 1
    duration = time.time() - start
    summary = {
        "company": "General Dynamics",
        "site": JOB_SEARCH_URL,
        "elapsed_seconds": round(duration, 2),
        **outcome,
    }
    logging.info("Summary: %s", json.dumps(summary))
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
