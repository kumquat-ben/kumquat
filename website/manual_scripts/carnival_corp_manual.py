#!/usr/bin/env python3
"""Manual scraper for https://jobs.carnivalcorp.com/search-jobs."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional
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
from django.utils import timezone  # noqa: E402

from scrapers.models import JobPosting, Scraper  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_URL = "https://jobs.carnivalcorp.com"
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
DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 1800), 60)

SCRAPER_QS = Scraper.objects.filter(
    company="Carnival Corporation",
    url=urljoin(BASE_URL, SEARCH_PATH),
).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.getLogger(__name__).warning(
            "Multiple Carnival Corporation scrapers found; using id=%s", SCRAPER.id
        )
else:
    SCRAPER = Scraper.objects.create(
        company="Carnival Corporation",
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
    brand: Optional[str]
    job_function: Optional[str]


@dataclass
class JobListing(JobSummary):
    date_posted: Optional[str]
    description_text: str
    description_html: Optional[str]
    metadata: Dict[str, object]


class CarnivalJobScraper:
    def __init__(
        self,
        *,
        delay: float = 0.2,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.delay = max(delay, 0.0)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)
        self._payload: Optional[Dict[str, object]] = None
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
        if self._payload is None:
            self._initialize_payload()

        total_pages = self._total_pages or 1
        if max_pages is not None:
            total_pages = min(total_pages, max_pages)

        processed = 0
        for page in range(1, total_pages + 1):
            soup = self._fetch_results_page(page)
            summaries = list(self._parse_job_summaries(soup))
            if not summaries:
                self.logger.info("No listings detected on page %s; stopping.", page)
                break

            self.logger.info("Processing %s listings from page %s", len(summaries), page)

            for summary in summaries:
                detail = self._fetch_job_detail(summary.detail_url)
                listing = JobListing(**asdict(summary), **detail)
                yield listing

                processed += 1
                if limit is not None and processed >= limit:
                    self.logger.info("Reached limit=%s; stopping scrape.", limit)
                    return

                if self.delay:
                    time.sleep(self.delay)

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------
    def _initialize_payload(self) -> None:
        response = self.session.get(urljoin(BASE_URL, SEARCH_PATH), timeout=40)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        container = soup.select_one("#search-results")
        filters = soup.select_one("#search-filters")

        if not container:
            raise ScraperError("Unable to locate search results container.")

        payload = _build_payload(container, filters)
        self._payload = payload

        total_pages = payload.get("TotalPages")
        self._total_pages = _to_int(total_pages)
        if not self._total_pages:
            self.logger.warning("Total pages not provided; defaulting to 1.")
            self._total_pages = 1

    def _fetch_results_page(self, page: int) -> BeautifulSoup:
        if self._payload is None:
            raise ScraperError("Scrape payload was not initialized.")

        payload = dict(self._payload)
        payload["CurrentPage"] = page

        response = self.session.post(
            urljoin(BASE_URL, RESULTS_POST_PATH),
            json=payload,
            timeout=45,
        )
        response.raise_for_status()

        data = response.json()
        if not isinstance(data, dict):
            raise ScraperError(f"Unexpected payload for page {page}: {data!r}")

        html = data.get("results") or ""
        soup = BeautifulSoup(html, "html.parser")

        container = soup.select_one("#search-results")
        if container:
            updated_total = _to_int(container.get("data-total-pages"))
            if updated_total:
                self._total_pages = updated_total

        return soup

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------
    def _parse_job_summaries(self, soup: BeautifulSoup) -> Iterable[JobSummary]:
        for item in soup.select("li.sr-results__list-item"):
            anchor = item.select_one("div.details-col > a")
            if not anchor:
                continue

            href = anchor.get("href")
            title = _text_or_none(anchor.select_one("h2")) or ""
            detail_url = urljoin(BASE_URL, (href or "").split("#", 1)[0])
            job_id = anchor.get("data-job-id")
            brand = _text_or_none(item.select_one(".brand-col b"))

            function_text = _text_or_none(item.select_one(".facet-item"))
            if function_text and ":" in function_text:
                _, function_text = function_text.split(":", 1)
            job_function = _normalize_whitespace(function_text)

            location = _normalize_whitespace(_text_or_none(item.select_one(".job-location")))

            yield JobSummary(
                title=title,
                detail_url=detail_url,
                job_id=job_id,
                location=location,
                brand=_normalize_whitespace(brand),
                job_function=job_function,
            )

    def _fetch_job_detail(self, url: str) -> Dict[str, object]:
        response = self.session.get(url, timeout=45)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        description_el = soup.select_one(".job-description") or soup.select_one(".ats-description")

        if description_el:
            description_text = description_el.get_text("\n\n", strip=True)
            description_html = str(description_el)
        else:
            description_text = ""
            description_html = None

        json_ld = _extract_json_ld(soup)
        date_posted = None
        if isinstance(json_ld, dict):
            date_posted = json_ld.get("datePosted")

        meta_apply = soup.find("meta", attrs={"name": "search-job-apply-url"})
        apply_url = meta_apply["content"] if meta_apply and meta_apply.get("content") else None

        metadata = _compact_dict(
            {
                "json_ld": json_ld,
                "apply_url": apply_url,
            }
        )

        return {
            "date_posted": str(date_posted) if date_posted else None,
            "description_text": description_text or "",
            "description_html": description_html,
            "metadata": metadata,
        }


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _build_payload(container: Tag, filters: Optional[Tag]) -> Dict[str, object]:
    try:
        refined_keywords = json.loads(container.get("data-refined-keywords") or "[]")
    except json.JSONDecodeError:
        refined_keywords = []

    payload: Dict[str, object] = {
        "ActiveFacetID": container.get("data-active-facet-id"),
        "Distance": container.get("data-distance"),
        "RadiusUnitType": filters.get("data-radius-unit-type") if filters else None,
        "RecordsPerPage": container.get("data-records-per-page"),
        "CurrentPage": 1,
        "TotalPages": container.get("data-total-pages"),
        "TotalResults": container.get("data-total-results"),
        "Keywords": container.get("data-keywords") or "",
        "Location": container.get("data-location") or "",
        "Latitude": container.get("data-latitude"),
        "Longitude": container.get("data-longitude"),
        "ShowRadius": container.get("data-show-radius") or "False",
        "FacetTerm": container.get("data-facet-term") or "",
        "FacetType": container.get("data-facet-type"),
        "SearchResultsModuleName": container.get("data-search-results-module-name"),
        "SearchFiltersModuleName": filters.get("data-search-filters-module-name") if filters else None,
        "SortCriteria": container.get("data-sort-criteria"),
        "SortDirection": container.get("data-sort-direction"),
        "SearchType": container.get("data-search-type"),
        "KeywordType": container.get("data-keyword-type"),
        "LocationType": container.get("data-location-type"),
        "LocationPath": container.get("data-location-path"),
        "OrganizationIds": container.get("data-organization-ids"),
        "RefinedKeywords": refined_keywords,
        "PostalCode": container.get("data-postal-code"),
        "ResultsType": container.get("data-results-type"),
        "IsPagination": "True",
        "fc": filters.get("data-filtered-categories") if filters else None,
        "fl": filters.get("data-filtered-locations") if filters else None,
        "fcf": filters.get("data-filtered-custom-facet") if filters else None,
        "afc": filters.get("data-filtered-advanced-categories") if filters else None,
        "afl": filters.get("data-filtered-advanced-locations") if filters else None,
        "afcf": filters.get("data-filtered-advanced-custom-facet") if filters else None,
    }

    return {
        key: value
        for key, value in payload.items()
        if value not in (None, "") or key in {"Keywords", "Location", "FacetTerm"}
    }


def _extract_json_ld(soup: BeautifulSoup) -> Optional[object]:
    for script in soup.find_all("script", type="application/ld+json"):
        text = script.string or script.get_text()
        if not text:
            continue
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict) and data.get("@type") == "JobPosting":
            return data
        if isinstance(data, list):
            for entry in data:
                if isinstance(entry, dict) and entry.get("@type") == "JobPosting":
                    return entry
    return None


def _compact_dict(payload: Dict[str, object]) -> Dict[str, object]:
    return {k: v for k, v in payload.items() if v not in (None, "", {}, [])}


def _normalize_whitespace(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    return " ".join(value.split()) or None


def _text_or_none(node: Optional[Tag]) -> Optional[str]:
    if node is None:
        return None
    text = node.get_text(" ", strip=True)
    return text or None


def _to_int(value: Optional[object]) -> Optional[int]:
    if value in (None, "", "None"):
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def persist_listings(scraper: Scraper, listings: Iterable[JobListing]) -> Dict[str, int]:
    created = 0
    updated = 0
    skipped = 0

    for listing in listings:
        title = (listing.title or "").strip()
        link = listing.detail_url
        if not title or not link:
            skipped += 1
            continue

        defaults = {
            "title": title[:255],
            "location": (listing.location or "")[:255],
            "date": (listing.date_posted or "")[:100],
            "description": (listing.description_text or "")[:10000],
            "metadata": _compact_dict(
                {
                    "job_id": listing.job_id,
                    "brand": listing.brand,
                    "job_function": listing.job_function,
                    **(listing.metadata or {}),
                }
            )
            or None,
        }

        obj, created_flag = JobPosting.objects.update_or_create(
            scraper=scraper,
            link=link,
            defaults=defaults,
        )

        if created_flag:
            created += 1
        else:
            updated += 1

    scraper.last_run = timezone.now()
    scraper.save(update_fields=["last_run"])

    return {"created": created, "updated": updated, "skipped": skipped}


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[Iterator[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Carnival Corporation jobs.")
    parser.add_argument("--max-pages", type=int, help="Maximum number of result pages to process.")
    parser.add_argument("--limit", type=int, help="Maximum number of jobs to process.")
    parser.add_argument("--delay", type=float, default=0.2, help="Delay between detail requests.")
    parser.add_argument("--dry-run", action="store_true", help="Scrape without writing to the database.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.",
    )
    return parser.parse_args(argv)


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def main(argv: Optional[Iterator[str]] = None) -> None:
    args = parse_args(argv)
    configure_logging(args.log_level)

    logger = logging.getLogger("carnival_manual")
    logger.info(
        "Starting Carnival Corporation manual scrape (max_pages=%s, limit=%s, dry_run=%s)",
        args.max_pages,
        args.limit,
        args.dry_run,
    )

    scraper = CarnivalJobScraper(delay=args.delay)
    listings = scraper.scrape(max_pages=args.max_pages, limit=args.limit)

    if args.dry_run:
        count = 0
        for listing in listings:
            count += 1
            logger.info("Would persist job: %s - %s", listing.title, listing.detail_url)
        logger.info("Dry run complete; %s listings discovered.", count)
        return

    summary = persist_listings(SCRAPER, listings)
    logger.info("Scrape complete: %s", summary)


if __name__ == "__main__":  # pragma: no cover
    try:
        main()
    except KeyboardInterrupt:
        logging.getLogger("carnival_manual").warning("Scrape interrupted by user.")
