#!/usr/bin/env python3
"""Manual scraper for Expand Energy careers (SuccessFactors hosted)."""

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
from typing import Dict, Iterable, Iterator, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag

# ---------------------------------------------------------------------------
# Django bootstrap so the script can write to the shared database
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
# Constants
# ---------------------------------------------------------------------------
BASE_URL = "https://jobs.expandenergy.com"
SEARCH_URL = "https://jobs.expandenergy.com/search/?createNewAlert=false&q=&locationsearch="
LEGACY_SEARCH_URL = urljoin(BASE_URL, "/search/")
CAREERS_PAGE = "https://www.expandenergy.com/careers/"
REQUEST_TIMEOUT = (15, 45)
DEFAULT_HEADERS: Dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": SEARCH_URL,
}

BASE_SEARCH_PARAMS: Dict[str, str] = {
    "createNewAlert": "false",
    "q": "",
    "locationsearch": "",
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 5400), 30)
SCRAPER_CANDIDATE_URLS = (SEARCH_URL, LEGACY_SEARCH_URL)
SCRAPER_QS = Scraper.objects.filter(company="Expand Energy", url__in=SCRAPER_CANDIDATE_URLS).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER.url != SEARCH_URL:
        try:
            SCRAPER.url = SEARCH_URL
            SCRAPER.save(update_fields=["url"])
        except Exception:
            logging.warning("Unable to update Scraper id=%s url to %s", SCRAPER.id, SEARCH_URL, exc_info=True)
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched Expand Energy; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Expand Energy",
        url=SEARCH_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the scrape pipeline encounters an unrecoverable issue."""


@dataclass
class JobSummary:
    job_id: Optional[str]
    title: str
    detail_url: str
    location: Optional[str]
    date_text: Optional[str]
    metadata: Dict[str, object]


@dataclass
class JobListing:
    job_id: Optional[str]
    title: str
    detail_url: str
    location: Optional[str]
    date_text: Optional[str]
    description_text: str
    description_html: Optional[str]
    apply_url: Optional[str]
    metadata: Dict[str, object]


def _clean_text(value: Optional[Tag | str]) -> str:
    if value is None:
        return ""
    if isinstance(value, Tag):
        text = value.get_text(" ", strip=True)
    else:
        text = str(value).strip()
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _strip_label(text: str, label: str) -> str:
    if not text:
        return text
    text = text.strip()
    lower_label = label.lower()
    lower_text = text.lower()
    prefix_with_colon = f"{label}:".lower()
    if lower_text.startswith(prefix_with_colon):
        return text[len(label) + 1 :].strip()
    if lower_text.startswith(lower_label):
        remainder = text[len(label) :].lstrip(": ").strip()
        if remainder:
            return remainder
    return text


class ExpandEnergyJobScraper:
    def __init__(
        self,
        *,
        delay: float = 0.3,
        page_size: int = 25,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.delay = max(0.0, delay)
        self.page_size = max(1, page_size)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.search_url = SEARCH_URL
        self.base_params: Dict[str, str] = dict(BASE_SEARCH_PARAMS)
        self.logger = logging.getLogger(self.__class__.__name__)

    def scrape(
        self,
        *,
        limit: Optional[int] = None,
        max_pages: Optional[int] = None,
    ) -> Iterator[JobListing]:
        fetched = 0
        start_row = 0
        pages = 0
        seen_ids: set[str] = set()
        total_jobs: Optional[int] = None

        while True:
            soup = self._fetch_search_page(start_row)
            if total_jobs is None:
                total_jobs = self._extract_total_jobs(soup)
                if total_jobs is not None:
                    self.logger.info("Discovered %s total jobs on search page.", total_jobs)

            summaries = list(self._parse_job_tiles(soup))
            if not summaries:
                self.logger.info("No job tiles returned at startrow=%s; stopping.", start_row)
                break

            for summary in summaries:
                tracking_key = summary.job_id or summary.detail_url
                if tracking_key in seen_ids:
                    continue
                seen_ids.add(tracking_key)

                detail_payload = self._fetch_job_detail(summary.detail_url)
                merged_metadata = dict(summary.metadata)
                merged_metadata.update(detail_payload["metadata"])

                yield JobListing(
                    job_id=summary.job_id,
                    title=summary.title,
                    detail_url=summary.detail_url,
                    location=detail_payload["location"] or summary.location,
                    date_text=detail_payload["date_text"] or summary.date_text,
                    description_text=detail_payload["description_text"],
                    description_html=detail_payload["description_html"],
                    apply_url=detail_payload["apply_url"],
                    metadata=merged_metadata,
                )
                fetched += 1
                if limit is not None and fetched >= limit:
                    self.logger.info("Reached record limit=%s.", limit)
                    return

            start_row += self.page_size
            pages += 1
            if total_jobs is not None and start_row >= total_jobs:
                self.logger.info("Reached reported total jobs (%s); pagination complete.", total_jobs)
                break
            if max_pages is not None and pages >= max_pages:
                self.logger.info("Reached max_pages=%s; stopping.", max_pages)
                break
            if self.delay:
                time.sleep(self.delay)

    def _fetch_search_page(self, start_row: int) -> BeautifulSoup:
        params: Dict[str, str] = dict(self.base_params)
        params.update(
            {
                "startrow": str(max(0, int(start_row))),
                "sortColumn": "referencedate",
                "sortDirection": "desc",
            }
        )
        response = self.session.get(self.search_url, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")

    def _extract_total_jobs(self, soup: BeautifulSoup) -> Optional[int]:
        label = soup.select_one("#tile-search-results-label")
        if not label:
            return None
        text = label.get_text(" ", strip=True)
        match = re.search(r"of\s+([0-9,]+)\s+Jobs", text, re.IGNORECASE)
        if not match:
            return None
        try:
            return int(match.group(1).replace(",", ""))
        except ValueError:
            return None

    def _parse_job_tiles(self, soup: BeautifulSoup) -> Iterable[JobSummary]:
        container = soup.select_one("ul#job-tile-list")
        if container and container.has_attr("data-per-page"):
            try:
                self.page_size = max(1, int(container["data-per-page"]))
            except (TypeError, ValueError):
                pass

        for item in soup.select("ul#job-tile-list li.job-tile"):
            link = item.select_one("a.jobTitle-link")
            if not link:
                continue

            detail_path = item.get("data-url") or link.get("href") or ""
            detail_url = urljoin(BASE_URL, detail_path)
            job_id = self._extract_job_id(item)
            tile_fields = self._extract_tile_fields(item)
            location = tile_fields.get("Location")
            date_text = tile_fields.get("Date")

            metadata: Dict[str, object] = {
                "source": CAREERS_PAGE,
                "search_url": self.search_url,
                "job_id": job_id,
                "tile_fields": tile_fields,
            }

            yield JobSummary(
                job_id=job_id,
                title=_clean_text(link) or "Untitled Role",
                detail_url=detail_url,
                location=location,
                date_text=date_text,
                metadata=metadata,
            )

    def _extract_job_id(self, item: Tag) -> Optional[str]:
        classes = item.get("class") or []
        for cls in classes:
            match = re.search(r"job-id-([0-9A-Za-z_-]+)", cls)
            if match:
                return match.group(1)
        return None

    def _extract_tile_fields(self, item: Tag) -> Dict[str, str]:
        fields: Dict[str, str] = {}
        for field in item.select("div.section-field"):
            label_el = field.select_one(".section-label")
            if not label_el:
                continue
            label = _clean_text(label_el)
            if not label:
                continue
            value_el: Optional[Tag] = None
            for child in field.children:
                if isinstance(child, Tag) and "section-label" not in (child.get("class") or []):
                    value_el = child
                    break
            if value_el is None:
                value_el = field.select_one(".section-value")
            if value_el is not None:
                value = _clean_text(value_el)
            else:
                value = _strip_label(_clean_text(field), label)
            value = _strip_label(value, label)
            if not value:
                continue
            fields.setdefault(label, value)
        return fields

    def _fetch_job_detail(self, detail_url: str) -> Dict[str, object]:
        response = self.session.get(detail_url, timeout=REQUEST_TIMEOUT)
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise ScraperError(f"Failed to fetch job detail: {detail_url}") from exc

        soup = BeautifulSoup(response.text, "html.parser")
        description_el = soup.select_one(".jobdescription")
        description_html = description_el.decode_contents() if description_el else ""
        description_text = _clean_text(description_el)

        location_el = soup.select_one(".jobLocation")
        job_date_el = soup.select_one(".jobDate")
        apply_el = soup.find("a", string=re.compile(r"apply", re.IGNORECASE))

        apply_url = None
        if apply_el and apply_el.get("href"):
            apply_url = urljoin(BASE_URL, apply_el["href"])

        location_raw = _clean_text(location_el)
        date_raw = _clean_text(job_date_el)
        detail_metadata: Dict[str, object] = {
            "company": _strip_label(_clean_text(soup.select_one(".jobCompany")), "Company"),
            "location_raw": location_raw,
            "date_raw": date_raw,
            "geo_location": _clean_text(soup.select_one(".jobGeoLocation")),
            "nearest_major_market": _strip_label(
                _clean_text(soup.select_one(".jobmarkets")), "Nearest Major Market"
            ),
            "job_segments": _strip_label(_clean_text(soup.select_one(".jobsegments")), "Job Segment"),
        }
        detail_metadata = {key: value for key, value in detail_metadata.items() if value}

        return {
            "description_html": description_html,
            "description_text": description_text,
            "location": _strip_label(location_raw, "Location") if location_el else None,
            "date_text": _strip_label(date_raw, "Date") if job_date_el else None,
            "apply_url": apply_url,
            "metadata": detail_metadata,
        }


def store_listing(listing: JobListing) -> Dict[str, int]:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255],
        "date": (listing.date_text or "")[:100],
        "description": listing.description_text[:10000],
        "metadata": {
            **listing.metadata,
            "description_html": listing.description_html,
            "apply_url": listing.apply_url,
        },
    }

    try:
        _, created = JobPosting.objects.update_or_create(
            scraper=SCRAPER,
            link=listing.detail_url,
            defaults=defaults,
        )
    except IntegrityError as exc:
        raise ScraperError(f"Failed to store job listing at {listing.detail_url}") from exc

    return {"created": 1 if created else 0, "updated": 0 if created else 1}


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Expand Energy careers manual scraper")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of job records to process")
    parser.add_argument("--max-pages", type=int, default=None, help="Maximum number of paginated search pages")
    parser.add_argument("--delay", type=float, default=0.3, help="Delay between paginated requests (seconds)")
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity",
    )
    return parser.parse_args(argv)


def run_scrape(args: argparse.Namespace) -> Dict[str, object]:
    client = ExpandEnergyJobScraper(delay=args.delay)
    processed = 0
    created = 0
    updated = 0

    for job in client.scrape(limit=args.limit, max_pages=args.max_pages):
        result = store_listing(job)
        created += result["created"]
        updated += result["updated"]
        processed += 1

    dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
    return {
        "processed_jobs": processed,
        "created": created,
        "updated": updated,
        "deduplicated": dedupe_summary,
    }


def main(argv: Optional[Iterable[str]] = None) -> int:
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
        "company": "Expand Energy",
        "site": SEARCH_URL,
        "careers_page": CAREERS_PAGE,
        "elapsed_seconds": round(duration, 2),
        **outcome,
    }
    logging.info("Summary: %s", json.dumps(summary))
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
