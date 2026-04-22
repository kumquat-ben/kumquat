#!/usr/bin/env python3
"""Manual scraper for https://jobs.chipotle.com/search-jobs."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional
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
# Constants
# ---------------------------------------------------------------------------
BASE_URL = "https://jobs.chipotle.com"
SEARCH_PATH = "/search-jobs"
RESULTS_PATH = "/search-jobs/results"
RESULTS_POST_PATH = "/search-jobs/resultspost"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.8",
    "Referer": urljoin(BASE_URL, SEARCH_PATH),
}
DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 3600), 60)
DEFAULT_REQUEST_TIMEOUT = max(getattr(settings, "MANUAL_SCRIPT_REQUEST_TIMEOUT", 30), 5)

SCRAPER_QS = Scraper.objects.filter(
    company="Chipotle",
    url=urljoin(BASE_URL, SEARCH_PATH),
).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Chipotle scrapers found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Chipotle",
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
    address: Optional[str]


@dataclass
class JobListing(JobSummary):
    date_posted: Optional[str]
    description_text: Optional[str]
    description_html: Optional[str]
    metadata: Dict[str, Any]


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------
def _parse_data_attributes(node: Optional[Tag]) -> Dict[str, str]:
    data: Dict[str, str] = {}
    if not node:
        return data
    for key, value in node.attrs.items():
        if key.startswith("data-"):
            data[key[5:]] = value
    return data


def _to_int(value: Optional[str], default: Optional[int] = None) -> Optional[int]:
    if value in (None, "", "None"):
        return default
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return default


def _to_float(value: Optional[str], default: Optional[float] = None) -> Optional[float]:
    if value in (None, "", "None"):
        return default
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return default


def _to_bool(value: Optional[str], default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    lowered = str(value).strip().lower()
    if lowered in {"true", "t", "1", "yes"}:
        return True
    if lowered in {"false", "f", "0", "no"}:
        return False
    return default


def _load_json_array(raw: Optional[str]) -> List[Any]:
    if not raw:
        return []
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(value, list):
        return []
    return [item for item in value if item not in (None, "", "null")]


def _extract_custom_fields(soup: BeautifulSoup) -> Dict[str, str]:
    fields: Dict[str, str] = {}
    for meta in soup.select("meta[name^='custom_fields.']"):
        name = meta.get("name")
        value = meta.get("content")
        if not name or value is None:
            continue
        key = name.split("custom_fields.", 1)[-1]
        if key and value:
            fields[key] = value
    return fields


def _extract_job_meta(soup: BeautifulSoup) -> Dict[str, str]:
    meta_fields: Dict[str, str] = {}
    for meta in soup.select("meta[name^='job-'], meta[name^='search-job'], meta[name^='gtm_']"):
        name = meta.get("name")
        value = meta.get("content")
        if not name or not value:
            continue
        meta_fields[name] = value
    return meta_fields


def _extract_json_ld(soup: BeautifulSoup) -> Optional[Any]:
    script = soup.find("script", type="application/ld+json")
    if not script or not script.string:
        return None
    try:
        return json.loads(script.string)
    except json.JSONDecodeError:
        return None


def _description_to_text(html_fragment: Optional[str]) -> Optional[str]:
    if not html_fragment:
        return None
    fragment = BeautifulSoup(html_fragment, "html.parser")
    text = fragment.get_text(" ", strip=True)
    return text or None


def _compact_metadata(pairs: Iterable[tuple[str, Any]]) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    for key, value in pairs:
        if value is None:
            continue
        if isinstance(value, str):
            trimmed = value.strip()
            if not trimmed:
                continue
            data[key] = trimmed
            continue
        if isinstance(value, (list, dict)) and not value:
            continue
        data[key] = value
    return data


# ---------------------------------------------------------------------------
# Scraper implementation
# ---------------------------------------------------------------------------
class ChipotleJobScraper:
    def __init__(self, *, delay: float = 0.25, session: Optional[requests.Session] = None) -> None:
        self.delay = max(delay, 0.0)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.request_timeout = DEFAULT_REQUEST_TIMEOUT

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def scrape(
        self,
        *,
        max_pages: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Iterator[JobListing]:
        page = 1
        processed = 0

        search_data, filters_data, summaries = self._fetch_initial_page()
        total_pages = _to_int(search_data.get("total-pages"), 1) or 1
        total_results = _to_int(search_data.get("total-results"), 0) or 0
        self.logger.info(
            "Initial state: total_results=%s total_pages=%s records_per_page=%s",
            total_results,
            total_pages,
            search_data.get("records-per-page"),
        )

        while True:
            if not summaries:
                self.logger.debug("No job summaries on page %s", page)
            for summary in summaries:
                try:
                    listing = self._enrich_summary(summary)
                except Exception as exc:  # pragma: no cover
                    self.logger.error("Failed to enrich %s: %s", summary.detail_url, exc)
                    continue

                yield listing
                processed += 1

                if limit is not None and processed >= limit:
                    self.logger.info("Limit reached (%s); stopping scrape", limit)
                    return

                if self.delay:
                    time.sleep(self.delay)

            page += 1
            if max_pages is not None and page > max_pages:
                self.logger.info("Max pages reached (%s); stopping scrape", max_pages)
                return
            if page > (total_pages or 1):
                self.logger.info("Completed all pages (%s)", total_pages)
                return

            search_data, filters_data, summaries = self._fetch_results_page(page, search_data, filters_data)
            total_pages = _to_int(search_data.get("total-pages"), total_pages) or total_pages

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _fetch_initial_page(self) -> tuple[Dict[str, str], Dict[str, str], List[JobSummary]]:
        url = urljoin(BASE_URL, SEARCH_PATH)
        try:
            response = self.session.get(url, timeout=self.request_timeout)
            response.raise_for_status()
        except requests.RequestException as exc:  # pragma: no cover
            raise ScraperError(f"Failed to fetch search page: {exc}") from exc

        soup = BeautifulSoup(response.text, "html.parser")
        search_section = soup.find(id="search-results")
        if not search_section:
            raise ScraperError("Missing search results section on initial page.")
        filters_section = soup.find(id="search-filters")

        search_data = _parse_data_attributes(search_section)
        filters_data = _parse_data_attributes(filters_section)
        summaries = self._parse_job_summaries(search_section)
        return search_data, filters_data, summaries

    def _fetch_results_page(
        self,
        page: int,
        current_search_data: Dict[str, str],
        current_filters_data: Dict[str, str],
    ) -> tuple[Dict[str, str], Dict[str, str], List[JobSummary]]:
        payload = self._build_search_criteria(page, current_search_data, current_filters_data)
        url = urljoin(BASE_URL, RESULTS_POST_PATH)
        try:
            response = self.session.post(url, json=payload, timeout=self.request_timeout)
            response.raise_for_status()
        except requests.RequestException as exc:  # pragma: no cover
            raise ScraperError(f"Failed to fetch results for page {page}: {exc}") from exc

        try:
            data = response.json()
        except json.JSONDecodeError as exc:  # pragma: no cover
            raise ScraperError(f"Invalid JSON response for page {page}: {exc}") from exc

        results_html = data.get("results")
        if not results_html:
            raise ScraperError(f"Empty results payload for page {page}")

        filters_html = data.get("filters")
        search_data, filters_data, summaries = self._extract_page_state(
            results_html,
            filters_html,
            current_filters_data,
        )
        return search_data, filters_data, summaries

    def _build_search_criteria(
        self,
        page: int,
        search_data: Dict[str, str],
        filters_data: Dict[str, str],
    ) -> Dict[str, Any]:
        refined_keywords = _load_json_array(search_data.get("refined-keywords"))
        criteria: Dict[str, Any] = {
            "ActiveFacetID": _to_int(search_data.get("active-facet-id"), 0),
            "CurrentPage": page,
            "RecordsPerPage": _to_int(search_data.get("records-per-page"), 20),
            "Distance": _to_int(search_data.get("distance"), 0),
            "RadiusUnitType": _to_int(filters_data.get("radius-unit-type"), 0),
            "Keywords": search_data.get("keywords") or "",
            "Location": search_data.get("location") or "",
            "Latitude": _to_float(search_data.get("latitude")),
            "Longitude": _to_float(search_data.get("longitude")),
            "ShowRadius": _to_bool(search_data.get("show-radius"), False),
            "IsPagination": page > 1,
            "CustomFacetName": search_data.get("custom-facet-name") or "",
            "FacetTerm": search_data.get("facet-term") or "",
            "FacetType": _to_int(search_data.get("facet-type"), 0),
            "FacetFilters": [],
            "SearchResultsModuleName": search_data.get("search-results-module-name") or "Search Results",
            "SearchFiltersModuleName": filters_data.get("search-filters-module-name"),
            "SortCriteria": _to_int(search_data.get("sort-criteria"), 0),
            "SortDirection": _to_int(search_data.get("sort-direction"), 0),
            "SearchType": _to_int(search_data.get("search-type"), 0),
            "CategoryFacetTerm": search_data.get("category-facet-term") or "",
            "CategoryFacetType": _to_int(search_data.get("category-facet-type")),
            "LocationFacetTerm": search_data.get("location-facet-term") or "",
            "LocationFacetType": _to_int(search_data.get("location-facet-type")),
            "KeywordType": search_data.get("keyword-type") or "",
            "LocationType": search_data.get("location-type") or "",
            "LocationPath": search_data.get("location-path") or "",
            "OrganizationIds": search_data.get("organization-ids") or "",
            "RefinedKeywords": refined_keywords,
            "PostalCode": search_data.get("postal-code") or "",
            "ResultsType": _to_int(search_data.get("results-type"), 0),
            "fc": filters_data.get("filtered-categories") or "",
            "fl": filters_data.get("filtered-locations") or "",
            "fcf": filters_data.get("filtered-custom-facet") or "",
            "afc": filters_data.get("filtered-advanced-categories") or "",
            "afl": filters_data.get("filtered-advanced-locations") or "",
            "afcf": filters_data.get("filtered-advanced-custom-facet") or "",
        }
        return criteria

    def _extract_page_state(
        self,
        results_html: str,
        filters_html: Optional[str],
        fallback_filters: Dict[str, str],
    ) -> tuple[Dict[str, str], Dict[str, str], List[JobSummary]]:
        results_soup = BeautifulSoup(results_html, "html.parser")
        search_section = results_soup.find(id="search-results")
        if not search_section:
            raise ScraperError("Missing search results section in payload.")
        search_data = _parse_data_attributes(search_section)
        summaries = self._parse_job_summaries(search_section)

        filters_data = dict(fallback_filters)
        if filters_html:
            filters_soup = BeautifulSoup(filters_html, "html.parser")
            filters_section = filters_soup.find(id="search-filters")
            parsed = _parse_data_attributes(filters_section)
            if parsed:
                filters_data = parsed

        return search_data, filters_data, summaries

    def _parse_job_summaries(self, search_section: Tag) -> List[JobSummary]:
        listings: List[JobSummary] = []
        results_list = search_section.find(id="search-results-list")
        if not results_list:
            return listings
        for item in results_list.select("ul > li"):
            anchor = item.find("a", href=True)
            title_node = anchor.find("h2") if anchor else None
            if not anchor or not title_node:
                continue
            title = title_node.get_text(strip=True)
            detail_url = urljoin(BASE_URL, anchor["href"])
            job_id = anchor.get("data-job-id")
            location_node = anchor.find("span", class_="job-location")
            address_node = anchor.find("span", class_="job-address")
            listing = JobSummary(
                title=title,
                detail_url=detail_url,
                job_id=job_id,
                location=location_node.get_text(strip=True) if location_node else None,
                address=address_node.get_text(strip=True) if address_node else None,
            )
            listings.append(listing)
        return listings

    def _enrich_summary(self, summary: JobSummary) -> JobListing:
        try:
            response = self.session.get(summary.detail_url, timeout=self.request_timeout)
            response.raise_for_status()
        except requests.RequestException as exc:  # pragma: no cover
            raise ScraperError(f"Failed to fetch job detail {summary.detail_url}: {exc}") from exc

        soup = BeautifulSoup(response.text, "html.parser")
        json_ld = _extract_json_ld(soup)
        description_html: Optional[str] = None
        date_posted: Optional[str] = None

        if isinstance(json_ld, dict):
            description_html = json_ld.get("description")
            date_posted = json_ld.get("datePosted")
        elif isinstance(json_ld, list):
            for entry in json_ld:
                if isinstance(entry, dict) and entry.get("@type") == "JobPosting":
                    description_html = entry.get("description")
                    date_posted = entry.get("datePosted")
                    break

        if not description_html:
            container = soup.select_one(".description")
            if container:
                description_html = str(container)

        description_text = _description_to_text(description_html)

        json_ld_meta: Any = None
        if isinstance(json_ld, dict):
            json_ld_meta = dict(json_ld)
            json_ld_meta.pop("description", None)
        elif isinstance(json_ld, list):
            json_ld_meta = []
            for entry in json_ld:
                if isinstance(entry, dict):
                    copy_entry = dict(entry)
                    copy_entry.pop("description", None)
                    json_ld_meta.append(copy_entry)
                else:
                    json_ld_meta.append(entry)
        else:
            json_ld_meta = json_ld

        metadata = _compact_metadata(
            [
                ("job_id", summary.job_id),
                ("address", summary.address),
                ("json_ld", json_ld_meta),
                ("custom_fields", _extract_custom_fields(soup)),
                ("job_meta", _extract_job_meta(soup)),
                ("detail_url", summary.detail_url),
            ]
        )

        return JobListing(
            title=summary.title,
            detail_url=summary.detail_url,
            job_id=summary.job_id,
            location=summary.location,
            address=summary.address,
            date_posted=date_posted,
            description_text=description_text,
            description_html=description_html,
            metadata=metadata,
        )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def store_listing(listing: JobListing) -> None:
    metadata = dict(listing.metadata or {})
    if listing.description_html:
        metadata.setdefault("description_html", listing.description_html)
    JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults={
            "title": (listing.title or "")[:255],
            "location": (listing.location or "")[:255],
            "date": (listing.date_posted or "")[:100],
            "description": (listing.description_text or "")[:10000],
            "metadata": metadata or None,
        },
    )


def run_scrape(max_pages: Optional[int], limit: Optional[int], delay: float) -> int:
    scraper = ChipotleJobScraper(delay=delay)
    count = 0
    for job in scraper.scrape(max_pages=max_pages, limit=limit):
        store_listing(job)
        count += 1
    return count


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Chipotle careers manual scraper")
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
        "company": "Chipotle",
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

