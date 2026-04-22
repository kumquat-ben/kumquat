#!/usr/bin/env python3
"""Manual scraper for https://www.capitalonecareers.com.

This script targets the public Radancy/TalentBrew search experience deployed by
Capital One. It reproduces the AJAX calls the site issues when paginating,
normalizes each job listing, and upserts results into the `JobPosting` table so
they surface in Kumquat with the proper `Scraper` association.
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
from typing import Dict, Iterable, Iterator, List, Optional
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
BASE_URL = "https://www.capitalonecareers.com"
SEARCH_PATH = "/search-jobs"
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

SCRAPER_QS = Scraper.objects.filter(
    company="Capital One",
    url=urljoin(BASE_URL, SEARCH_PATH),
).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Capital One scrapers found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Capital One",
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
    date_posted: Optional[str]


@dataclass
class JobListing(JobSummary):
    description_text: Optional[str]
    description_html: Optional[str]
    metadata: Dict[str, object]


class CapitalOneJobScraper:
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
    ) -> Iterator[JobListing]:
        initial_soup = self._fetch_initial_page()
        self._initialize_search_payload(initial_soup)

        if not self._search_payload:
            raise ScraperError("Failed to derive search payload from initial page.")

        total = self._total_pages or 1
        self.logger.info("Detected %s result pages", total)

        processed = 0
        page = 1
        while True:
            soup = initial_soup if page == 1 else self._fetch_results_page(page)

            summaries = list(self._parse_job_summaries(soup))
            if not summaries:
                self.logger.info("No job summaries found on page %s; stopping scrape", page)
                return

            for summary in summaries:
                try:
                    detail = self._fetch_job_detail(summary.detail_url)
                except Exception as exc:  # pragma: no cover
                    self.logger.error("Failed to fetch detail for %s: %s", summary.detail_url, exc)
                    continue

                posted_override = detail.pop("date_posted", None)
                location_override = detail.pop("location_override", None)
                if posted_override:
                    summary.date_posted = posted_override
                if location_override:
                    summary.location = location_override

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
    # HTTP helpers
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

        refined_raw = container.get("data-refined-keywords") or "[]"
        try:
            refined_keywords = json.loads(refined_raw)
        except json.JSONDecodeError:
            refined_keywords = []

        def attr(source: Optional[Tag], name: str, default: Optional[str] = None) -> Optional[str]:
            if not source:
                return default
            value = source.get(name)
            return value if value is not None else default

        payload: Dict[str, object] = {
            "ActiveFacetID": _int_or_none(attr(container, "data-active-facet-id")),
            "Distance": _int_or_none(attr(container, "data-distance")),
            "RadiusUnitType": _int_or_none(attr(filters, "data-radius-unit-type")),
            "RecordsPerPage": _int_or_none(attr(container, "data-records-per-page")) or 15,
            "CurrentPage": 1,
            "TotalPages": _int_or_none(attr(container, "data-total-pages")),
            "TotalResults": _int_or_none(attr(container, "data-total-results")),
            "Keywords": attr(container, "data-keywords", "") or "",
            "Location": attr(container, "data-location", "") or "",
            "Latitude": attr(container, "data-latitude"),
            "Longitude": attr(container, "data-longitude"),
            "ShowRadius": attr(container, "data-show-radius") or "False",
            "FacetTerm": attr(container, "data-facet-term", "") or "",
            "FacetType": _int_or_none(attr(container, "data-facet-type")),
            "SearchResultsModuleName": attr(container, "data-search-results-module-name"),
            "SearchFiltersModuleName": attr(filters, "data-search-filters-module-name"),
            "SortCriteria": _int_or_none(attr(container, "data-sort-criteria")) or 0,
            "SortDirection": _int_or_none(attr(container, "data-sort-direction")) or 0,
            "SearchType": _int_or_none(attr(container, "data-search-type")) or 5,
            "KeywordType": attr(container, "data-keyword-type"),
            "LocationType": attr(container, "data-location-type"),
            "LocationPath": attr(container, "data-location-path"),
            "OrganizationIds": attr(container, "data-organization-ids"),
            "RefinedKeywords": refined_keywords,
            "PostalCode": attr(container, "data-postal-code"),
            "ResultsType": _int_or_none(attr(container, "data-results-type")) or 0,
            "IsPagination": "True",
            "fc": attr(filters, "data-filtered-categories"),
            "fl": attr(filters, "data-filtered-locations"),
            "fcf": attr(filters, "data-filtered-custom-facet"),
            "afc": attr(filters, "data-filtered-advanced-categories"),
            "afl": attr(filters, "data-filtered-advanced-locations"),
            "afcf": attr(filters, "data-filtered-advanced-custom-facet"),
        }

        payload = {k: v for k, v in payload.items() if v not in (None, "") or k in {"Keywords", "Location", "FacetTerm"}}

        self._search_payload = payload
        total_pages = payload.get("TotalPages")
        self._total_pages = int(total_pages) if isinstance(total_pages, int) else None

    def _fetch_results_page(self, page: int) -> BeautifulSoup:
        if not self._search_payload:
            raise ScraperError("Search payload has not been initialized.")

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
            raise ScraperError(f"Unexpected response payload for page {page}: {data!r}")

        if data.get("hasJobs") is False:
            return BeautifulSoup("", "html.parser")

        soup = BeautifulSoup(data.get("results") or "", "html.parser")
        meta = soup.select_one("#search-results")
        if meta:
            updated_total = _int_or_none(meta.get("data-total-pages"))
            if updated_total:
                self._total_pages = updated_total

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
            title = _text_or_none(anchor.select_one("h2")) or ""
            job_id = anchor.get("data-job-id")
            location = _normalize_whitespace(_text_or_none(anchor.select_one(".job-location")))
            date_posted = _normalize_whitespace(_text_or_none(anchor.select_one(".job-date-posted")))

            yield JobSummary(
                title=title,
                detail_url=detail_url,
                job_id=job_id,
                location=location,
                date_posted=date_posted,
            )

    def _fetch_job_detail(self, url: str) -> Dict[str, object]:
        response = self.session.get(url, timeout=45)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        description_node = (
            soup.select_one("section.job-description .ats-description")
            or soup.select_one(".ats-description")
            or soup.select_one("section.job-description")
        )
        description_html = str(description_node) if description_node else None
        description_text = _text_or_none(description_node, separator="\n\n") if description_node else None

        job_section = soup.select_one("section.job-description")
        job_code = None
        posted_label = None
        location_override = None
        job_section_attrs = {}
        if job_section:
            job_code = _normalize_whitespace(_text_or_none(job_section.select_one(".job-id")))
            posted_label = _normalize_whitespace(_text_or_none(job_section.select_one(".job-date")))
            location_node = job_section.select_one(".job-location")
            location_override = _normalize_whitespace(_text_or_none(location_node, separator=" | "))
            job_section_attrs = {
                key: value
                for key, value in job_section.attrs.items()
                if key.startswith("data-")
            }

        apply_url = None
        apply_link = soup.select_one("a.job-apply")
        if apply_link:
            apply_url = apply_link.get("data-apply-url") or apply_link.get("href")

        overview = _extract_overview(soup)
        custom_fields = _extract_custom_fields(soup)
        job_meta = _extract_job_meta(soup)
        json_ld_payload = _extract_json_ld(soup)

        metadata = _compact_metadata(
            (
                ("job_code", job_code),
                ("apply_url", apply_url),
                ("overview", overview or None),
                ("custom_fields", custom_fields or None),
                ("job_meta", job_meta or None),
                ("job_section_attrs", job_section_attrs or None),
                ("json_ld", json_ld_payload),
            )
        )

        result: Dict[str, object] = {
            "description_text": description_text or "",
            "description_html": description_html,
            "metadata": metadata,
        }

        date_override = _extract_date(posted_label)
        if json_ld_payload and isinstance(json_ld_payload, dict):
            ld_date = json_ld_payload.get("datePosted")
            if ld_date:
                date_override = str(ld_date)

        if date_override:
            result["date_posted"] = date_override

        if location_override:
            result["location_override"] = location_override
        elif json_ld_payload and isinstance(json_ld_payload, dict):
            job_location = json_ld_payload.get("jobLocation")
            if isinstance(job_location, dict):
                address = job_location.get("address")
                if isinstance(address, dict):
                    locality = address.get("addressLocality")
                    region = address.get("addressRegion")
                    country = address.get("addressCountry")
                    override = ", ".join([part for part in [locality, region, country] if part])
                    if override:
                        result["location_override"] = override

        return result


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _int_or_none(value: Optional[str]) -> Optional[int]:
    if value in (None, "", "None"):
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _normalize_whitespace(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = " ".join(value.split())
    return text or None


def _text_or_none(node: Optional[Tag], separator: str = " ") -> Optional[str]:
    if not node:
        return None
    text = node.get_text(separator=separator, strip=True)
    return text or None


def _extract_date(label: Optional[str]) -> Optional[str]:
    if not label:
        return None
    cleaned = label.replace("Posted", "").strip()
    if not cleaned:
        return None
    return cleaned


def _extract_overview(soup: BeautifulSoup) -> Dict[str, str]:
    overview: Dict[str, str] = {}
    for item in soup.select(".job-description-overview__item"):
        key = _normalize_whitespace(_text_or_none(item.select_one(".job-description-overview__title")))
        value = _normalize_whitespace(_text_or_none(item.select_one(".job-description-overview__text"), separator=" | "))
        if key and value:
            overview[key] = value
    return overview


def _extract_custom_fields(soup: BeautifulSoup) -> Dict[str, str]:
    fields: Dict[str, str] = {}
    for meta in soup.select("meta[name^='custom_fields.']"):
        name = meta.get("name")
        value = meta.get("content")
        if not name or not value:
            continue
        cleaned = name.split("custom_fields.", 1)[-1]
        fields[cleaned] = value
    return fields


def _extract_job_meta(soup: BeautifulSoup) -> Dict[str, str]:
    meta_fields: Dict[str, str] = {}
    for meta in soup.select("meta[name^='job-'], meta[name^='search-job']"):
        name = meta.get("name")
        value = meta.get("content")
        if not name or not value:
            continue
        meta_fields[name] = value
    return meta_fields


def _extract_json_ld(soup: BeautifulSoup) -> Optional[object]:
    script = soup.find("script", type="application/ld+json")
    if not script or not script.string:
        return None
    try:
        return json.loads(script.string)
    except json.JSONDecodeError:
        return None


def _compact_metadata(pairs: Iterable[tuple[str, object]]) -> Dict[str, object]:
    data: Dict[str, object] = {}
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


def store_listing(listing: JobListing) -> None:
    metadata = dict(listing.metadata or {})
    metadata.setdefault("summary_job_id", listing.job_id)
    metadata.setdefault("source", "capitalonecareers.com")

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
    scraper = CapitalOneJobScraper(delay=delay)
    count = 0
    for job in scraper.scrape(max_pages=max_pages, limit=limit):
        store_listing(job)
        count += 1
    return count


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Capital One careers manual scraper")
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
        "company": "Capital One",
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
