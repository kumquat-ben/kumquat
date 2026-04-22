#!/usr/bin/env python3
"""Manual scraper for https://www.careers.fiserv.com.

It emulates the in-browser TalentBrew search flow by:

1. Loading the landing search page to capture the dynamic payload metadata.
2. Paginating through the `/search-jobs/resultspost` endpoint to gather job
   summaries.
3. Visiting each job detail page for richer fields (description, metadata,
   apply URL, structured header information).
4. Persisting results directly via the shared Django `JobPosting` model.
"""
from __future__ import annotations

import argparse
import copy
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Generator, Iterable, List, Optional, Tuple, Union
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, NavigableString, Tag

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
BASE_URL = "https://www.careers.fiserv.com"
SEARCH_PATH = "/search-jobs"
RESULTS_POST_PATH = "/search-jobs/resultspost"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.8",
    "Referer": urljoin(BASE_URL, SEARCH_PATH),
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 60)

SCRAPER_QS = Scraper.objects.filter(
    company="Fiserv",
    url=urljoin(BASE_URL, SEARCH_PATH),
).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Fiserv scrapers found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Fiserv",
        url=urljoin(BASE_URL, SEARCH_PATH),
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the scraper encounters an unrecoverable issue."""


@dataclass
class JobSummary:
    title: str
    detail_url: str
    job_id: Optional[str]
    location: Optional[str]
    additional_locations: Optional[str]
    date_posted: Optional[str]


@dataclass
class JobListing(JobSummary):
    description_text: Optional[str]
    description_html: Optional[str]
    metadata: Dict[str, object]


class FiservJobScraper:
    def __init__(self, *, delay: float = 0.25, session: Optional[requests.Session] = None) -> None:
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

                date_override = detail.pop("date_posted", None)
                if date_override:
                    summary.date_posted = date_override

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
        response = self.session.get(urljoin(BASE_URL, SEARCH_PATH), timeout=40)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")

    def _initialize_search_payload(self, soup: BeautifulSoup) -> None:
        container = soup.select_one("#search-results")
        filters = soup.select_one("#search-filters")
        if not container:
            raise ScraperError("Unable to locate search results container.")

        def data(attr: str, default: Optional[str] = None) -> Optional[str]:
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
            "ActiveFacetID": _int_or_none(data("data-active-facet-id")),
            "Distance": _int_or_none(data("data-distance")),
            "RadiusUnitType": _int_or_none(filter_data("data-radius-unit-type")),
            "RecordsPerPage": _int_or_none(data("data-records-per-page")) or 20,
            "CurrentPage": 1,
            "TotalPages": _int_or_none(data("data-total-pages")),
            "TotalResults": _int_or_none(data("data-total-results")),
            "Keywords": data("data-keywords", "") or "",
            "Location": data("data-location", "") or "",
            "Latitude": data("data-latitude"),
            "Longitude": data("data-longitude"),
            "ShowRadius": data("data-show-radius") or "False",
            "FacetTerm": data("data-facet-term", "") or "",
            "FacetType": _int_or_none(data("data-facet-type")),
            "SearchResultsModuleName": data("data-search-results-module-name"),
            "SearchFiltersModuleName": filter_data("data-search-filters-module-name"),
            "SortCriteria": _int_or_none(data("data-sort-criteria")) or 0,
            "SortDirection": _int_or_none(data("data-sort-direction")) or 0,
            "SearchType": _int_or_none(data("data-search-type")) or 5,
            "CategoryFacetTerm": data("data-category-facet-term"),
            "CategoryFacetType": _int_or_none(data("data-category-facet-type")),
            "LocationFacetTerm": data("data-location-facet-term"),
            "LocationFacetType": _int_or_none(data("data-location-facet-type")),
            "KeywordType": data("data-keyword-type"),
            "LocationType": data("data-location-type"),
            "LocationPath": data("data-location-path"),
            "OrganizationIds": data("data-organization-ids"),
            "RefinedKeywords": refined_keywords,
            "PostalCode": data("data-postal-code"),
            "ResultsType": _int_or_none(data("data-results-type")) or 0,
            "IsPagination": "True",
            "fc": filter_data("data-filtered-categories"),
            "fl": filter_data("data-filtered-locations"),
            "fcf": filter_data("data-filtered-custom-facet"),
            "afc": filter_data("data-filtered-advanced-categories"),
            "afl": filter_data("data-filtered-advanced-locations"),
            "afcf": filter_data("data-filtered-advanced-custom-facet"),
        }

        payload = {k: v for k, v in payload.items() if v not in (None, "") or k in {"Keywords", "Location", "FacetTerm"}}

        self._search_payload = payload
        self._total_pages = _int_or_none(str(payload.get("TotalPages"))) if payload.get("TotalPages") is not None else None

    def _fetch_results_page(self, page: int) -> BeautifulSoup:
        if not self._search_payload:
            raise ScraperError("Search payload is not initialized.")

        payload = copy.deepcopy(self._search_payload)
        payload["CurrentPage"] = page

        response = self.session.post(
            urljoin(BASE_URL, RESULTS_POST_PATH),
            json=payload,
            timeout=40,
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
        for anchor in soup.select("#search-results-list ul > li > a"):
            href = anchor.get("href")
            if not href:
                continue

            detail_url = urljoin(BASE_URL, href)
            title = _text_or_none(anchor.select_one("h3")) or ""
            job_id = anchor.get("data-job-id")

            location = _text_or_none(anchor.select_one(".job-location"))
            additional = _text_or_none(anchor.select_one(".job-multi-loc"))
            date_posted = _text_or_none(anchor.select_one(".job-date-posted"))

            yield JobSummary(
                title=title,
                detail_url=detail_url,
                job_id=job_id,
                location=location,
                additional_locations=additional,
                date_posted=date_posted,
            )

    def _fetch_job_detail(self, url: str) -> Dict[str, Optional[Union[str, Dict[str, object], List[str]]]]:
        response = self.session.get(url, timeout=45)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        description_elem = soup.select_one(".ats-description")
        description_html = str(description_elem) if description_elem else None
        description_text = _text_or_none(description_elem, separator="\n\n")

        apply_link = soup.select_one("a.job-apply")
        apply_url = None
        if apply_link:
            apply_url = apply_link.get("data-apply-url") or apply_link.get("href")

        job_header = _extract_job_header(soup.select_one(".job-header"))
        quick_facts = _extract_quick_facts(soup.select_one("#anchor-overview .quick-facts"))
        overview_text = _text_or_none(soup.select_one("#anchor-overview"))

        if description_text is None:
            fallback = soup.select_one("[data-selector-name='jobdetails']")
            description_text = _text_or_none(fallback, separator="\n\n")
            if fallback and description_html is None:
                description_html = str(fallback)

        metadata: Dict[str, object] = {
            "job_header": job_header,
            "quick_facts": quick_facts,
            "overview": overview_text,
            "apply_url": apply_url,
        }

        if "Date posted" in job_header and not job_header.get("Date posted"):
            metadata["raw_job_header"] = job_header

        date_posted = job_header.get("Date posted") or job_header.get("Date Posted")
        result: Dict[str, Optional[Union[str, Dict[str, object], List[str]]]] = {
            "description_text": description_text,
            "description_html": description_html,
            "metadata": metadata,
        }
        if date_posted:
            result["date_posted"] = date_posted
        return result


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


def _extract_job_header(container: Optional[Tag]) -> Dict[str, str]:
    if not container:
        return {}
    data: Dict[str, str] = {}
    for item in container.select("span.job-info"):
        label, value = _split_label_value(item)
        if label:
            data[label] = value or ""
        elif value:
            key = f"field_{len(data) + 1}"
            data[key] = value
    return data


def _split_label_value(node: Tag) -> Tuple[Optional[str], Optional[str]]:
    label_elem = node.find("b")
    label = label_elem.get_text(" ", strip=True) if label_elem else None
    value_parts: List[str] = []
    for child in node.children:
        if isinstance(child, NavigableString):
            piece = str(child).strip()
            if piece:
                value_parts.append(piece)
        elif isinstance(child, Tag) and child is not label_elem:
            value_parts.append(child.get_text(" ", strip=True))
    value = " ".join(part for part in value_parts if part).strip(":- ")
    return label, value or None


def _extract_quick_facts(list_elem: Optional[Tag]) -> List[str]:
    if not list_elem:
        return []
    facts: List[str] = []
    for item in list_elem.select("li"):
        text = _text_or_none(item)
        if text:
            facts.append(text)
    return facts


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------

def store_listing(listing: JobListing) -> None:
    metadata = dict(listing.metadata or {})
    metadata.setdefault("job_id", listing.job_id)
    if listing.additional_locations:
        metadata["additional_locations"] = listing.additional_locations

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
    scraper = FiservJobScraper(delay=delay)
    count = 0
    for job in scraper.scrape(max_pages=max_pages, limit=limit):
        store_listing(job)
        count += 1
    return count


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fiserv careers manual scraper")
    parser.add_argument("--max-pages", type=int, default=None, help="Stop after processing this many search result pages")
    parser.add_argument("--limit", type=int, default=None, help="Stop after processing this many job postings")
    parser.add_argument("--delay", type=float, default=0.25, help="Delay (seconds) between detail page fetches")
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity",
    )
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
    duration = time.time() - start
    dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
    summary = {
        "company": "Fiserv",
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
