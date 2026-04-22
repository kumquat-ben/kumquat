#!/usr/bin/env python3
"""Manual scraper for https://careers.campbellsoupcompany.com."""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

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
BASE_URL = "https://careers.campbellsoupcompany.com"
START_PATH = "/us/en"
PAGE_PATH_TEMPLATE = "/page/{page}"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.8",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}
DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 3600), 120)

SCRAPER_QS = Scraper.objects.filter(
    company="Campbell Soup Company",
    url=urljoin(BASE_URL, START_PATH),
).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Campbell Soup scrapers found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Campbell Soup Company",
        url=urljoin(BASE_URL, START_PATH),
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
class ScraperError(Exception):
    """Raised when the scraper encounters an unrecoverable issue."""


@dataclass
class JobListing:
    title: str
    link: str
    location: str
    job_id: Optional[str]
    requisition_id: Optional[str]
    categories: Sequence[str]
    employment_types: Sequence[str]
    updated_date: Optional[str]
    description_text: str
    description_html: Optional[str]
    date_posted: Optional[str]
    metadata: Dict[str, Any]


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
def _extract_preload_state(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    script = next((tag for tag in soup.find_all("script") if tag.string and "window.__PRELOAD_STATE__" in tag.string), None)
    if not script or not script.string:
        raise ScraperError("Unable to locate __PRELOAD_STATE__ payload.")

    text = script.string
    marker = "window.__PRELOAD_STATE__ = "
    build_marker = "window.__BUILD__"
    try:
        start = text.index(marker) + len(marker)
        end = text.index(build_marker)
    except ValueError as exc:
        raise ScraperError("Failed to parse __PRELOAD_STATE__ boundaries.") from exc

    payload = text[start:end].strip().rstrip(";")
    try:
        return json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ScraperError("Failed to decode __PRELOAD_STATE__ JSON.") from exc


def _safe_join_unique(values: Iterable[Optional[str]]) -> str:
    seen: List[str] = []
    for value in values:
        if not value:
            continue
        trimmed = value.strip()
        if trimmed and trimmed not in seen:
            seen.append(trimmed)
    return ", ".join(seen)


def _clean_description(text: str) -> str:
    if not text:
        return ""
    soup = BeautifulSoup(text, "html.parser")
    cleaned = soup.get_text("\n", strip=True)
    return cleaned.strip()


def _extract_json_ld(soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        if not script.string:
            continue
        try:
            data = json.loads(script.string)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict):
            if data.get("@type") == "JobPosting":
                return data
        elif isinstance(data, list):
            for entry in data:
                if isinstance(entry, dict) and entry.get("@type") == "JobPosting":
                    return entry
    return None


def _first_location_text(locations: Sequence[Dict[str, Any]], is_remote: bool) -> str:
    options: List[Optional[str]] = []
    for loc in locations:
        options.extend(
            [
                loc.get("locationText"),
                loc.get("locationParsedText"),
                loc.get("cityStateAbbr"),
                loc.get("cityState"),
            ]
        )
    if not options and is_remote:
        options = ["Remote"]
    return _safe_join_unique(options)


def _build_metadata(job_payload: Dict[str, Any], json_ld: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    metadata: Dict[str, Any] = {
        "raw_locations": job_payload.get("locations"),
        "categories": [entry.get("name") for entry in job_payload.get("categories", []) if entry.get("name")],
        "custom_categories": job_payload.get("customCategories"),
        "custom_fields": job_payload.get("customFields"),
        "job_card_extra_fields": job_payload.get("jobCardExtraFields"),
        "employment_status": job_payload.get("employmentStatus"),
        "employment_type_raw": job_payload.get("employmentType"),
        "is_remote": job_payload.get("isRemote"),
        "posting_type": job_payload.get("postingType"),
        "apply_url": job_payload.get("applyURL"),
        "custom_apply_link": job_payload.get("customApplyLink"),
        "source_id": job_payload.get("sourceID"),
        "unique_id": job_payload.get("uniqueID"),
        "company_id": job_payload.get("companyID"),
        "updated_date": job_payload.get("updatedDate"),
    }
    if json_ld:
        metadata["json_ld"] = json_ld
    return metadata


# ---------------------------------------------------------------------------
# Core scraper
# ---------------------------------------------------------------------------
class CampbellSoupJobScraper:
    def __init__(self, *, delay: float = 0.25, session: Optional[requests.Session] = None) -> None:
        self.delay = max(delay, 0.0)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    def _page_url(self, page: int) -> str:
        if page <= 1:
            return urljoin(BASE_URL, START_PATH)
        return urljoin(BASE_URL, PAGE_PATH_TEMPLATE.format(page=page))

    def _page_state(self, page: int) -> Dict[str, Any]:
        url = self._page_url(page)
        self.logger.debug("Fetching listing page %s (%s)", page, url)
        response = self.session.get(url, timeout=45)
        response.raise_for_status()
        state = _extract_preload_state(response.text)
        job_search = state.get("jobSearch") or {}
        jobs = job_search.get("jobs") or []
        total = job_search.get("totalJob") or 0
        return {"jobs": jobs, "total": total, "page_url": url}

    def _fetch_job_detail(self, job_payload: Dict[str, Any]) -> Dict[str, Any]:
        path = job_payload.get("originalURL")
        if not path:
            raise ScraperError(f"Job payload missing originalURL: {job_payload!r}")
        detail_url = urljoin(BASE_URL + "/", path.lstrip("/"))
        self.logger.debug("Fetching detail %s", detail_url)
        response = self.session.get(detail_url, timeout=60)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        json_ld = _extract_json_ld(soup)

        description_html = json_ld.get("description") if json_ld else None
        if not description_html:
            desc_container = soup.select_one(".job-description .description")
            if desc_container:
                description_html = str(desc_container)
        description_text = _clean_description(description_html or "")
        date_posted = json_ld.get("datePosted") if json_ld else None

        return {
            "detail_url": detail_url,
            "description_html": description_html,
            "description_text": description_text,
            "date_posted": date_posted,
            "json_ld": json_ld,
        }

    def iter_jobs(
        self,
        *,
        max_pages: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Iterator[JobListing]:
        processed = 0
        page = 1
        total_pages: Optional[int] = None

        while True:
            if max_pages is not None and page > max_pages:
                self.logger.info("Reached max pages limit (%s); stopping.", max_pages)
                return

            try:
                page_data = self._page_state(page)
            except requests.RequestException as exc:
                raise ScraperError(f"Failed to fetch page {page}: {exc}") from exc

            jobs: List[Dict[str, Any]] = page_data["jobs"]
            if not jobs:
                self.logger.info("No jobs found on page %s; stopping.", page)
                return

            if total_pages is None:
                total_job = int(page_data["total"]) if page_data["total"] else None
                per_page = len(jobs)
                if total_job and per_page:
                    total_pages = math.ceil(total_job / per_page)
                    self.logger.info("Detected %s total jobs across ~%s pages.", total_job, total_pages)

            for job_payload in jobs:
                if limit is not None and processed >= limit:
                    self.logger.info("Reached job limit (%s); stopping.", limit)
                    return

                try:
                    detail = self._fetch_job_detail(job_payload)
                except requests.RequestException as exc:
                    self.logger.error("Failed to fetch detail for %s: %s", job_payload.get("originalURL"), exc)
                    continue
                except ScraperError as exc:
                    self.logger.error("Detail parsing error: %s", exc)
                    continue

                listing = self._build_listing(job_payload, detail)
                yield listing
                processed += 1

                if self.delay:
                    time.sleep(self.delay)

            page += 1
            if total_pages is not None and page > total_pages:
                self.logger.info("Processed all %s pages.", total_pages)
                return

    def _build_listing(self, job_payload: Dict[str, Any], detail: Dict[str, Any]) -> JobListing:
        title = (job_payload.get("title") or "").strip()
        if not title:
            raise ScraperError(f"Encountered job with missing title: {job_payload!r}")

        location = _first_location_text(job_payload.get("locations") or [], bool(job_payload.get("isRemote")))
        metadata = _build_metadata(job_payload, detail.get("json_ld"))

        return JobListing(
            title=title,
            link=detail["detail_url"],
            location=location,
            job_id=job_payload.get("uniqueID") or job_payload.get("reference"),
            requisition_id=job_payload.get("requisitionID"),
            categories=[entry.get("name") for entry in job_payload.get("categories", []) if entry.get("name")],
            employment_types=job_payload.get("employmentType") or [],
            updated_date=job_payload.get("updatedDate"),
            description_text=detail.get("description_text") or "",
            description_html=detail.get("description_html"),
            date_posted=detail.get("date_posted"),
            metadata=metadata,
        )


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------
def store_listing(listing: JobListing) -> None:
    metadata = dict(listing.metadata)
    metadata.setdefault("job_id", listing.job_id)
    metadata.setdefault("requisition_id", listing.requisition_id)
    if listing.categories:
        metadata.setdefault("categories_normalized", list(listing.categories))
    if listing.employment_types:
        metadata.setdefault("employment_types", list(listing.employment_types))
    if listing.description_html:
        metadata.setdefault("description_html", listing.description_html)

    JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.link,
        defaults={
            "title": listing.title[:255],
            "location": (listing.location or "")[:255],
            "date": (listing.date_posted or listing.updated_date or "")[:100],
            "description": listing.description_text[:10000],
            "metadata": metadata,
        },
    )


# ---------------------------------------------------------------------------
# CLI wiring
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Campbell Soup careers manual scraper")
    parser.add_argument("--max-pages", type=int, default=None, help="Maximum number of result pages to process")
    parser.add_argument("--limit", type=int, default=None, help="Stop after processing this many job postings")
    parser.add_argument("--delay", type=float, default=0.25, help="Delay in seconds between job detail requests")
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity",
    )
    return parser.parse_args(argv)


def run_scrape(max_pages: Optional[int], limit: Optional[int], delay: float) -> int:
    scraper = CampbellSoupJobScraper(delay=delay)
    count = 0
    for job in scraper.iter_jobs(max_pages=max_pages, limit=limit):
        store_listing(job)
        count += 1
    return count


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()), format="%(levelname)s:%(name)s:%(message)s")

    start = time.time()
    try:
        processed = run_scrape(args.max_pages, args.limit, args.delay)
    except ScraperError as exc:
        logging.error("Scrape failed: %s", exc)
        return 1
    duration = time.time() - start

    dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)

    summary = {
        "company": SCRAPER.company,
        "url": SCRAPER.url,
        "count": processed,
        "elapsed_seconds": duration,
        "dedupe": dedupe_summary,
    }
    logging.info("Summary: %s", json.dumps(summary))
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
