#!/usr/bin/env python3
"""Manual scraper for Carrier careers (jobs.carrier.com).

The public careers page at https://www.corporate.carrier.com/careers routes to
the TalentBrew job board hosted at https://jobs.carrier.com/en/search-jobs.
This script mirrors the same flow the browser performs:

1. Load the search landing page to capture the dynamic pagination payload.
2. Paginate the `/en/search-jobs/resultspost` endpoint for job summaries.
3. Visit each job detail page to collect rich fields (description, metadata).
4. Persist the results through the shared Django `JobPosting` model.
"""
from __future__ import annotations

import argparse
import copy
import json
import logging
import os
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Generator, Iterable, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag

# ---------------------------------------------------------------------------
# Django bootstrap (keeps parity with other manual scripts)
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
CAREERS_LANDING_URL = "https://www.corporate.carrier.com/careers/"
BASE_URL = "https://jobs.carrier.com"
SEARCH_PATH = "/en/search-jobs"
RESULTS_POST_PATH = "/en/search-jobs/resultspost"
SEARCH_URL = urljoin(BASE_URL, SEARCH_PATH)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.8",
    "Origin": BASE_URL,
    "Referer": SEARCH_URL,
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 3600), 60)

SCRAPER_QS = Scraper.objects.filter(company="Carrier", url=CAREERS_LANDING_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.getLogger(__name__).warning(
            "Multiple Carrier scraper rows detected; using id=%s", SCRAPER.id
        )
else:  # pragma: no cover - creation path
    SCRAPER = Scraper.objects.create(
        company="Carrier",
        url=CAREERS_LANDING_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the Carrier scraper encounters an unrecoverable issue."""


@dataclass
class JobSummary:
    title: str
    detail_url: str
    job_id: Optional[str]
    location: Optional[str]


@dataclass
class JobListing(JobSummary):
    description_text: Optional[str]
    description_html: Optional[str]
    date_posted: Optional[str]
    metadata: Dict[str, object]


class CarrierJobScraper:
    def __init__(
        self,
        *,
        delay: float = 0.25,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.delay = max(delay, 0.0)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)
        self._search_payload: Optional[Dict[str, object]] = None
        self._total_pages: Optional[int] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def scrape(
        self,
        *,
        max_pages: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Generator[JobListing, None, None]:
        initial_soup = self._fetch_initial_page()
        self._initialize_search_payload(initial_soup)

        if not self._search_payload:
            raise ScraperError("Failed to derive search payload from initial page.")

        total_pages = self._total_pages or 1
        self.logger.info("Detected %s result pages", total_pages)

        page = 1
        processed = 0
        while True:
            soup = initial_soup if page == 1 else self._fetch_results_page(page)

            for summary in self._parse_job_summaries(soup):
                try:
                    detail = self._fetch_job_detail(summary.detail_url)
                except Exception as exc:  # pragma: no cover - defensive logging
                    self.logger.error("Failed to enrich %s: %s", summary.detail_url, exc)
                    continue

                detail_location = detail.pop("location", None)
                if detail_location:
                    summary.location = detail_location

                listing = JobListing(**asdict(summary), **detail)
                yield listing
                processed += 1

                if limit is not None and processed >= limit:
                    self.logger.info("Reached limit=%s; stopping scrape", limit)
                    return

                if self.delay:
                    time.sleep(self.delay)

            page += 1
            if max_pages is not None and page > max_pages:
                self.logger.info("Max pages reached (%s); stopping scrape", max_pages)
                return
            if self._total_pages is not None and page > self._total_pages:
                self.logger.info("Reached last page (%s); stopping scrape", self._total_pages)
                return

    # ------------------------------------------------------------------
    # Fetch helpers
    # ------------------------------------------------------------------
    def _fetch_initial_page(self) -> BeautifulSoup:
        response = self.session.get(SEARCH_URL, timeout=45)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")

    def _initialize_search_payload(self, soup: BeautifulSoup) -> None:
        container = soup.select_one("#search-results")
        filters = soup.select_one("#search-filters")
        if not container:
            raise ScraperError("Unable to locate search results container.")

        def container_data(attr: str, default: Optional[str] = None) -> Optional[str]:
            value = container.get(attr)
            return value if value is not None else default

        def filter_data(attr: str, default: Optional[str] = None) -> Optional[str]:
            if not filters:
                return default
            value = filters.get(attr)
            return value if value is not None else default

        refined_raw = container.get("data-refined-keywords") or "[]"
        try:
            refined_keywords = json.loads(refined_raw)
        except json.JSONDecodeError:
            refined_keywords = []

        payload: Dict[str, object] = {
            "ActiveFacetID": _int_or_none(container_data("data-active-facet-id")),
            "Distance": _int_or_none(container_data("data-distance")),
            "RadiusUnitType": _int_or_none(filter_data("data-radius-unit-type")),
            "RecordsPerPage": _int_or_none(container_data("data-records-per-page")) or 10,
            "CurrentPage": 1,
            "TotalPages": _int_or_none(container_data("data-total-pages")),
            "TotalResults": _int_or_none(container_data("data-total-results")),
            "Keywords": container_data("data-keywords", "") or "",
            "Location": container_data("data-location", "") or "",
            "Latitude": container_data("data-latitude"),
            "Longitude": container_data("data-longitude"),
            "ShowRadius": container_data("data-show-radius") or "False",
            "FacetTerm": container_data("data-facet-term", "") or "",
            "FacetType": _int_or_none(container_data("data-facet-type")),
            "SearchResultsModuleName": container_data("data-search-results-module-name"),
            "SearchFiltersModuleName": filter_data("data-search-filters-module-name"),
            "SortCriteria": _int_or_none(container_data("data-sort-criteria")) or 0,
            "SortDirection": _int_or_none(container_data("data-sort-direction")) or 1,
            "SearchType": _int_or_none(container_data("data-search-type")) or 5,
            "CategoryFacetTerm": container_data("data-category-facet-term"),
            "CategoryFacetType": _int_or_none(container_data("data-category-facet-type")),
            "LocationFacetTerm": container_data("data-location-facet-term"),
            "LocationFacetType": _int_or_none(container_data("data-location-facet-type")),
            "KeywordType": container_data("data-keyword-type"),
            "LocationType": container_data("data-location-type"),
            "LocationPath": container_data("data-location-path"),
            "OrganizationIds": container_data("data-organization-ids"),
            "RefinedKeywords": refined_keywords,
            "PostalCode": container_data("data-postal-code"),
            "ResultsType": _int_or_none(container_data("data-results-type")) or 0,
            "IsPagination": "True",
            "fc": filter_data("data-filtered-categories"),
            "fl": filter_data("data-filtered-locations"),
            "fcf": filter_data("data-filtered-custom-facet"),
            "afc": filter_data("data-filtered-advanced-categories"),
            "afl": filter_data("data-filtered-advanced-locations"),
            "afcf": filter_data("data-filtered-advanced-custom-facet"),
        }

        payload = {
            key: value
            for key, value in payload.items()
            if value not in (None, "", [])
            or key in {"Keywords", "Location", "FacetTerm", "RefinedKeywords"}
        }

        self._search_payload = payload
        self._total_pages = _int_or_none(container_data("data-total-pages"))

    def _fetch_results_page(self, page: int) -> BeautifulSoup:
        if not self._search_payload:
            raise ScraperError("Search payload is not initialized.")

        payload = copy.deepcopy(self._search_payload)
        payload["CurrentPage"] = page

        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/json; charset=UTF-8",
            "Referer": SEARCH_URL,
            "Origin": BASE_URL,
            "User-Agent": self.session.headers.get("User-Agent", DEFAULT_HEADERS["User-Agent"]),
        }

        response = self.session.post(
            urljoin(BASE_URL, RESULTS_POST_PATH),
            json=payload,
            headers=headers,
            timeout=45,
        )
        response.raise_for_status()

        data = response.json()
        if not isinstance(data, dict) or "results" not in data:
            raise ScraperError(f"Unexpected response for page {page}: {data!r}")

        if data.get("hasJobs") is False:
            self.logger.info("No jobs reported for page %s", page)
            return BeautifulSoup("", "html.parser")

        results_html = data.get("results") or ""
        soup = BeautifulSoup(results_html, "html.parser")

        meta = soup.select_one("#search-results")
        if meta:
            total_pages = meta.get("data-total-pages")
            self._total_pages = _int_or_none(total_pages) or self._total_pages

        return soup

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------
    def _parse_job_summaries(self, soup: BeautifulSoup) -> Iterable[JobSummary]:
        for anchor in soup.select("#search-results-list li a[data-job-id]"):
            href = anchor.get("href")
            if not href:
                continue

            detail_url = urljoin(BASE_URL, href)
            title = _text_or_none(anchor.select_one("h2")) or _text_or_none(anchor)
            if not title:
                continue

            job_id = anchor.get("data-job-id")
            location = _text_or_none(anchor.select_one(".job-location"))

            yield JobSummary(
                title=title,
                detail_url=detail_url,
                job_id=job_id,
                location=location,
            )

    def _fetch_job_detail(self, url: str) -> Dict[str, Optional[str]]:
        response = self.session.get(url, timeout=45)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        json_ld = _extract_jobposting_jsonld(soup)
        description_html = None
        description_text = None
        date_posted = None

        if json_ld:
            description_html = json_ld.get("description")
            if description_html:
                description_text = _clean_html_text(description_html)
            date_posted = json_ld.get("datePosted")

        description_section = soup.select_one(".ats-description")
        if description_section:
            if description_html is None:
                description_html = str(description_section)
            if description_text is None:
                description_text = _text_or_none(description_section, separator="\n\n")

        location_override = _text_or_none(soup.select_one(".job-location"))
        job_info = _extract_job_info(soup)

        if not date_posted:
            date_posted = job_info.get("Date Posted") or job_info.get("Posting Date")

        apply_url = None
        apply_button = soup.select_one("a.job-apply")
        if apply_button:
            apply_url = apply_button.get("data-apply-url") or apply_button.get("href")

        metadata: Dict[str, object] = {
            "job_info": job_info,
            "apply_url": apply_url,
        }
        if json_ld:
            metadata["json_ld"] = json_ld

        metadata = {key: value for key, value in metadata.items() if value not in (None, "", {})}

        return {
            "description_text": description_text,
            "description_html": description_html,
            "date_posted": date_posted,
            "metadata": metadata,
            "location": location_override,
        }

    # ------------------------------------------------------------------
    # Internal state helpers
    # ------------------------------------------------------------------
    def _fetch_results_page_count(self) -> Optional[int]:
        return self._total_pages


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _int_or_none(value: Optional[str]) -> Optional[int]:
    if value in (None, "", "None"):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _text_or_none(node: Optional[Tag], separator: str = " ") -> Optional[str]:
    if not node:
        return None
    text = node.get_text(separator=separator, strip=True)
    return text or None


def _clean_html_text(html_fragment: str) -> Optional[str]:
    soup = BeautifulSoup(html_fragment, "html.parser")
    text = soup.get_text("\n", strip=True)
    lines = [line.rstrip() for line in text.splitlines() if line.strip()]
    return "\n".join(lines) if lines else None


def _extract_jobposting_jsonld(soup: BeautifulSoup) -> Optional[Dict[str, object]]:
    for script in soup.select("script[type='application/ld+json']"):
        raw = script.string or ""
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict) and data.get("@type") == "JobPosting":
            return data
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and item.get("@type") == "JobPosting":
                    return item
    return None


def _extract_job_info(soup: BeautifulSoup) -> Dict[str, str]:
    info: Dict[str, str] = {}
    for node in soup.select(".job-description__info .job-info"):
        label_el = node.find("b")
        label = _text_or_none(label_el)
        value = _text_or_none(node, separator=" ")
        if not label or not value:
            continue

        cleaned_value = value.strip()
        for prefix in (f"{label}:", label):
            if cleaned_value.startswith(prefix):
                cleaned_value = cleaned_value[len(prefix) :].strip(" -:\n\r\t")
                break

        if cleaned_value:
            info[label] = cleaned_value
    return info


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------

def store_listing(listing: JobListing) -> None:
    metadata = dict(listing.metadata or {})
    if listing.job_id:
        metadata.setdefault("job_id", listing.job_id)
    if listing.description_html:
        metadata.setdefault("description_html", listing.description_html)

    metadata = {key: value for key, value in metadata.items() if value not in (None, "", {})}

    JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults={
            "title": listing.title[:255],
            "location": (listing.location or "")[:255],
            "date": (listing.date_posted or "")[:100],
            "description": (listing.description_text or "")[:10000],
            "metadata": metadata,
        },
    )


def run_scrape(max_pages: Optional[int], limit: Optional[int], delay: float) -> int:
    scraper = CarrierJobScraper(delay=delay)
    count = 0
    for job in scraper.scrape(max_pages=max_pages, limit=limit):
        store_listing(job)
        count += 1
    return count


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Carrier careers manual scraper")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Limit the number of search result pages processed",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after processing this many job listings",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.25,
        help="Delay (in seconds) between job detail requests",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data but do not persist to the database",
    )
    parser.add_argument(
        "--dedupe",
        action="store_true",
        help="Run the job deduper after ingest",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger("carrier_manual")

    scraper = CarrierJobScraper(delay=args.delay)

    jobs: List[JobListing] = []
    start = time.time()
    try:
        for job in scraper.scrape(max_pages=args.max_pages, limit=args.limit):
            jobs.append(job)
    except ScraperError as exc:
        logger.error("Scrape failed: %s", exc)
        return 1

    elapsed = time.time() - start
    logger.info("Collected %s Carrier jobs in %.2fs", len(jobs), elapsed)

    if not jobs:
        logger.warning("No Carrier jobs collected; aborting.")
        return 1

    if args.dry_run:
        summary = {
            "company": "Carrier",
            "url": SEARCH_URL,
            "count": len(jobs),
            "elapsed_seconds": elapsed,
            "dry_run": True,
        }
        print(json.dumps(summary))
        return 0

    for job in jobs:
        store_listing(job)

    dedupe_summary = None
    if args.dedupe:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        logger.info("Deduplication summary: %s", dedupe_summary)

    summary = {
        "company": "Carrier",
        "url": SEARCH_URL,
        "count": len(jobs),
        "elapsed_seconds": elapsed,
        "dedupe": dedupe_summary,
    }
    logger.info("Summary: %s", summary)
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
