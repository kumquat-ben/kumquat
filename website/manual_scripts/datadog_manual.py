#!/usr/bin/env python3
"""Manual scraper for Datadog's Data Jobs Monitoring related roles.

This script queries Datadog's public Typesense careers index for roles that are
relevant to the Data Jobs Monitoring product page, enriches each record with
job detail content, and persists the results via the existing Django models.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests
from bs4 import BeautifulSoup

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
# Constants & configuration
# ---------------------------------------------------------------------------
PRODUCT_PAGE_URL = "https://www.datadoghq.com/product/data-jobs-monitoring/"
CAREERS_HOST = "https://careers.datadoghq.com"
TYPESENSE_HOST = "https://dnm1k9zrpctsvjowp-1.a1.typesense.net"
TYPESENSE_COLLECTION = "careers_alias"
TYPESENSE_SEARCH_ENDPOINT = f"{TYPESENSE_HOST}/collections/{TYPESENSE_COLLECTION}/documents/search"
TYPESENSE_API_KEY = "O2QyrgpWb3eKxVCmGVNrORNcSo3pOZJu"
TYPESENSE_QUERY = "data jobs monitoring"
TYPESENSE_QUERY_FIELDS = "title,description,department,team"
DEFAULT_PAGE_SIZE = 20
DEFAULT_DELAY = 0.25
REQUEST_TIMEOUT = (10, 45)
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
)

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 600), 120)
SCRAPER_QS = Scraper.objects.filter(
    company="Datadog",
    url__in={
        "https://www.datadoghq.com/product/data-jobs-monitoring",
        PRODUCT_PAGE_URL,
    },
).order_by("id")

if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning(
            "Multiple Scraper rows matched Datadog page; using id=%s", SCRAPER.id
        )
else:
    SCRAPER = Scraper.objects.create(
        company="Datadog",
        url=PRODUCT_PAGE_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the Datadog manual scraper encounters an unrecoverable issue."""


@dataclass
class JobListing:
    job_id: str
    title: str
    link: str
    location: Optional[str]
    posted_date: Optional[str]
    description_text: str
    description_html: Optional[str]
    metadata: Dict[str, object]


def _html_to_text(value: Optional[str]) -> str:
    if not value:
        return ""
    soup = BeautifulSoup(value, "html.parser")
    return soup.get_text("\n", strip=True)


class DatadogCareersClient:
    def __init__(
        self,
        *,
        query: str = TYPESENSE_QUERY,
        query_fields: str = TYPESENSE_QUERY_FIELDS,
        page_size: int = DEFAULT_PAGE_SIZE,
        delay: float = DEFAULT_DELAY,
    ) -> None:
        self.query = query
        self.query_fields = query_fields
        self.page_size = max(1, page_size)
        self.delay = max(0.0, delay)
        self.logger = logging.getLogger(self.__class__.__name__)

        self.typesense_session = requests.Session()
        self.typesense_session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "application/json",
                "X-TYPESENSE-API-KEY": TYPESENSE_API_KEY,
            }
        )

        self.detail_session = requests.Session()
        self.detail_session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": PRODUCT_PAGE_URL,
            }
        )

    def iter_listings(self, *, limit: Optional[int] = None) -> Iterable[JobListing]:
        retrieved = 0
        page = 1
        while True:
            params = {
                "q": self.query,
                "query_by": self.query_fields,
                "per_page": self.page_size,
                "page": page,
            }
            self.logger.debug("Requesting Typesense page %s", page)
            try:
                response = self.typesense_session.get(
                    TYPESENSE_SEARCH_ENDPOINT, params=params, timeout=REQUEST_TIMEOUT
                )
                response.raise_for_status()
                payload = response.json()
            except (requests.RequestException, ValueError) as exc:
                raise ScraperError(f"Typesense request failed: {exc}") from exc

            hits = payload.get("hits") or []
            if not hits:
                self.logger.info("No additional search results at page %s; stopping.", page)
                break

            for hit in hits:
                document = hit.get("document") or {}
                try:
                    listing = self._build_listing(document)
                except ScraperError as exc:
                    job_ref = document.get("job_id") or document.get("id")
                    self.logger.warning("Skipping job %s: %s", job_ref, exc)
                    continue
                yield listing
                retrieved += 1
                if limit is not None and retrieved >= limit:
                    return

            retrieved_found = payload.get("found")
            if limit is None and isinstance(retrieved_found, int) and retrieved >= retrieved_found:
                break

            page += 1
            if self.delay:
                time.sleep(self.delay)

    def _build_listing(self, document: Dict[str, object]) -> JobListing:
        job_id = str(document.get("job_id") or document.get("internal_job_id") or "").strip()
        title = (document.get("title") or "").strip()
        absolute_url = (document.get("absolute_url") or "").strip()
        rel_url = (document.get("rel_url") or "").strip()

        if not job_id or not title:
            raise ScraperError("Missing required job identifier or title.")

        if absolute_url:
            link = absolute_url
        elif rel_url:
            link = f"{CAREERS_HOST.rstrip('/')}/{rel_url.lstrip('/')}"
        else:
            raise ScraperError("Job detail URL not provided in Typesense payload.")

        posted_date = None
        last_mod = (document.get("last_mod") or "").strip()
        if last_mod:
            iso_value = last_mod.replace("Z", "+00:00")
            try:
                posted_date = datetime.fromisoformat(iso_value).date().isoformat()
            except ValueError:
                posted_date = last_mod

        fallback_description_html = document.get("description") or ""
        fallback_description_text = _html_to_text(fallback_description_html)

        try:
            description_text, description_html = self._fetch_detail_content(link)
        except ScraperError as exc:
            self.logger.warning(
                "Falling back to Typesense description for job %s (%s): %s",
                job_id,
                link,
                exc,
            )
            description_text = fallback_description_text
            description_html = fallback_description_html or None

        metadata = self._build_metadata(document, description_html)

        return JobListing(
            job_id=job_id,
            title=title,
            link=link,
            location=(document.get("location_string") or "").strip() or None,
            posted_date=posted_date,
            description_text=description_text,
            description_html=description_html,
            metadata=metadata,
        )

    def _fetch_detail_content(self, url: str) -> tuple[str, Optional[str]]:
        try:
            response = self.detail_session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to load job detail page: {exc}") from exc

        soup = BeautifulSoup(response.text, "html.parser")
        container = soup.select_one("div.job-description")
        if not container:
            container = soup.select_one("section#jobDescription")
        if not container:
            text_only = soup.get_text("\n", strip=True)
            if not text_only:
                raise ScraperError("Job description container not found.")
            return text_only, None

        description_text = container.get_text("\n", strip=True)
        description_html = container.decode_contents()
        return description_text, description_html or None

    def _build_metadata(
        self, document: Dict[str, object], description_html: Optional[str]
    ) -> Dict[str, object]:
        metadata: Dict[str, object] = {}
        metadata_fields = [
            "job_id",
            "internal_job_id",
            "department",
            "team",
            "language",
            "time_type",
            "last_mod",
            "multi_location",
            "remote",
            "region_APAC",
            "region_Americas",
            "region_EMEA",
        ]
        for field in metadata_fields:
            value = document.get(field)
            if value not in (None, "", [], {}):
                metadata[field] = value

        for field, value in document.items():
            if field.startswith("child_department_") or field.startswith("parent_department_"):
                if value not in (None, "", []):
                    metadata[field] = value
            if field.startswith("location_") and field != "location_string":
                if value not in (None, "", []):
                    metadata[field] = value

        if description_html:
            metadata["description_html"] = description_html

        metadata["source"] = {
            "provider": "typesense",
            "query": self.query,
            "endpoint": TYPESENSE_SEARCH_ENDPOINT,
        }
        metadata["product_page"] = PRODUCT_PAGE_URL
        return metadata


def persist_listing(listing: JobListing) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255] or None,
        "date": (listing.posted_date or "")[:100] or None,
        "description": listing.description_text[:10000],
        "metadata": listing.metadata,
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.link,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Stored Datadog job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Datadog Data Jobs Monitoring manual scraper")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help="Search results per Typesense page (default: %(default)s).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help="Seconds to wait between Typesense requests (default: %(default)s).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and log jobs without writing to the database.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        default="INFO",
        help="Logging verbosity (default: INFO).",
    )
    return parser.parse_args(argv)


def run(args: argparse.Namespace) -> Dict[str, object]:
    client = DatadogCareersClient(page_size=args.page_size, delay=args.delay)
    totals = {
        "fetched": 0,
        "created": 0,
        "updated": 0,
        "skipped": 0,
    }
    stored_links: List[str] = []

    for listing in client.iter_listings(limit=args.limit):
        totals["fetched"] += 1
        if args.dry_run:
            logging.info(
                "Dry-run: would store '%s' (%s) in %s",
                listing.title,
                listing.link,
                listing.location,
            )
            continue
        try:
            created = persist_listing(listing)
        except Exception as exc:
            totals["skipped"] += 1
            logging.error("Failed to persist job '%s': %s", listing.title, exc)
            continue
        stored_links.append(listing.link)
        if created:
            totals["created"] += 1
        else:
            totals["updated"] += 1

    dedupe_summary: Optional[Dict[str, object]] = None
    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)

    result = {
        "company": SCRAPER.company,
        "product_page": PRODUCT_PAGE_URL,
        "query": TYPESENSE_QUERY,
        "totals": totals,
        "stored_links": stored_links,
    }
    if dedupe_summary:
        result["dedupe"] = dedupe_summary
    return result


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    start_time = time.time()
    try:
        outcome = run(args)
    except ScraperError as exc:
        logging.error("Datadog manual scraper failed: %s", exc)
        return 1
    duration = time.time() - start_time
    outcome["elapsed_seconds"] = round(duration, 2)
    print(json.dumps(outcome))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
