#!/usr/bin/env python3
"""Manual scraper for AbbVie careers (Attrax platform).

This script paginates through AbbVie's public job listings, enriches each
posting with the structured JSON-LD payload from the job detail page, and
persists the results into the shared ``JobPosting`` table that backs the
``Scraper`` entry for AbbVie.
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
from html import unescape
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional
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

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402
from jobs.text import repair_mojibake  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_URL = "https://careers.abbvie.com"
LISTING_PATH = "/en/jobs"
LISTING_URL = f"{BASE_URL}{LISTING_PATH}"
DEFAULT_PAGE_SIZE = 48
REQUEST_TIMEOUT = 60

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
}

# Ensure a single scraper row is used/created for AbbVie.
SCRAPER_QS = Scraper.objects.filter(company="AbbVie", url=LISTING_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.getLogger(__name__).warning(
            "Multiple Scraper rows matched AbbVie careers; using id=%s", SCRAPER.id
        )
else:
    SCRAPER = Scraper.objects.create(
        company="AbbVie",
        url=LISTING_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=300,
    )


class AbbvieScraperError(Exception):
    """Raised for unrecoverable scraping failures."""


@dataclass
class JobSummary:
    title: str
    detail_url: str
    job_id: Optional[str]
    location: Optional[str]
    teaser: Optional[str]


@dataclass
class JobListing(JobSummary):
    description: str
    date_posted: Optional[str]
    valid_through: Optional[str]
    employment_type: List[str]
    locations: List[str]
    metadata: Dict[str, object]


class AbbvieJobScraper:
    def __init__(
        self,
        *,
        page_size: int = DEFAULT_PAGE_SIZE,
        delay: float = 0.4,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.page_size = max(1, min(page_size, 200))
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def scrape(self, *, limit: Optional[int] = None) -> Iterator[JobListing]:
        seen_links: set[str] = set()
        fetched = 0
        total_pages: Optional[int] = None
        page = 1

        while True:
            html_text = self._fetch_listing_page(page=page)
            if total_pages is None:
                total_pages = self._extract_total_pages(html_text) or 1
                self.logger.debug("Total pages determined: %s", total_pages)

            summaries = list(self._parse_listing_page(html_text))
            if not summaries:
                self.logger.info("No job cards found on page %s; stopping.", page)
                break

            for summary in summaries:
                if summary.detail_url in seen_links:
                    continue
                try:
                    listing = self._enrich_summary(summary)
                except Exception as exc:  # pragma: no cover - network failures
                    self.logger.error("Failed to enrich job %s: %s", summary.detail_url, exc)
                    continue

                seen_links.add(summary.detail_url)
                yield listing
                fetched += 1
                if limit is not None and fetched >= limit:
                    return

            page += 1
            if total_pages is not None and page > total_pages:
                break

            if self.delay:
                time.sleep(self.delay)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _fetch_listing_page(self, *, page: int) -> str:
        params = {"page": page, "size": self.page_size}
        response = self.session.get(
            LISTING_URL,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        if self.delay:
            time.sleep(self.delay)
        return response.text

    def _parse_listing_page(self, html_text: str) -> Iterator[JobSummary]:
        soup = BeautifulSoup(html_text, "html.parser")
        for tile in soup.select("div.attrax-vacancy-tile"):
            title_link = tile.select_one("a.attrax-vacancy-tile__title")
            if not title_link:
                continue

            title = title_link.get_text(strip=True)
            href = title_link.get("href") or ""
            if not href:
                continue
            detail_url = urljoin(BASE_URL, href)

            job_id_value = self._text_or_none(
                tile.select_one(".attrax-vacancy-tile__externalreference-value")
            )
            # Fallback to internal reference when external isn't present.
            if not job_id_value:
                job_id_value = self._text_or_none(
                    tile.select_one(".attrax-vacancy-tile__reference-value")
                )

            location_value = self._text_or_none(
                tile.select_one(".attrax-vacancy-tile__location-freetext .attrax-vacancy-tile__item-value")
            )
            if not location_value:
                location_value = self._text_or_none(
                    tile.select_one(".attrax-vacancy-tile__option-location .attrax-vacancy-tile__item-value")
                )

            teaser = self._text_or_none(tile.select_one(".attrax-vacancy-tile__description-value"))

            yield JobSummary(
                title=title,
                detail_url=detail_url,
                job_id=job_id_value,
                location=location_value,
                teaser=teaser,
            )

    def _enrich_summary(self, summary: JobSummary) -> JobListing:
        html_text = self._fetch_detail_html(summary.detail_url)
        json_ld = self._extract_json_ld(html_text)

        description_html = json_ld.get("description") or ""
        description = self._html_to_text(description_html)

        date_posted = self._coerce_str(json_ld.get("datePosted"))
        valid_through = self._coerce_str(json_ld.get("validThrough"))

        employment_type_raw = json_ld.get("employmentType")
        if isinstance(employment_type_raw, str):
            employment_type = [employment_type_raw.strip()]
        elif isinstance(employment_type_raw, list):
            employment_type = [self._coerce_str(item) for item in employment_type_raw if self._coerce_str(item)]
        else:
            employment_type = []

        locations = self._parse_locations(json_ld) or ([summary.location] if summary.location else [])

        identifier = json_ld.get("identifier")
        identifier_value: Optional[str] = None
        if isinstance(identifier, dict):
            identifier_value = self._coerce_str(identifier.get("value"))

        metadata: Dict[str, object] = {
            "teaser": summary.teaser,
            "employmentType": employment_type,
            "validThrough": valid_through,
            "rawJobId": summary.job_id,
            "identifier": identifier_value,
            "industry": json_ld.get("industry"),
        }

        return JobListing(
            title=summary.title,
            detail_url=summary.detail_url,
            job_id=identifier_value or summary.job_id,
            location=summary.location,
            teaser=summary.teaser,
            description=description,
            date_posted=date_posted,
            valid_through=valid_through,
            employment_type=employment_type,
            locations=locations,
            metadata=metadata,
        )

    def _fetch_detail_html(self, url: str) -> str:
        response = self.session.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        if self.delay:
            time.sleep(self.delay)
        content = response.content
        for encoding in ("utf-8", response.encoding, getattr(response, "apparent_encoding", None)):
            if not encoding:
                continue
            try:
                return content.decode(encoding)
            except (LookupError, UnicodeDecodeError):
                continue
        return response.text

    @staticmethod
    def _extract_total_pages(html_text: str) -> Optional[int]:
        soup = BeautifulSoup(html_text, "html.parser")
        max_page = 1
        pattern = re.compile(r"pagination\((\d+)\)")

        for anchor in soup.select("li.attrax-pagination__last a"):
            href = anchor.get("href") or ""
            match = pattern.search(href)
            if match:
                max_page = max(max_page, int(match.group(1)))

        if max_page == 1:
            for anchor in soup.select("li.attrax-pagination__page-item a"):
                text = anchor.get_text(strip=True)
                if text.isdigit():
                    max_page = max(max_page, int(text))

        return max_page or None

    @staticmethod
    def _extract_json_ld(html_text: str) -> Dict[str, object]:
        soup = BeautifulSoup(html_text, "html.parser")
        script_tag = soup.find("script", attrs={"type": "application/ld+json"})
        if not script_tag or not script_tag.string:
            raise AbbvieScraperError("Job detail JSON-LD payload not found.")

        raw_json = script_tag.string.strip()
        try:
            data = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            raise AbbvieScraperError(f"Failed to parse job JSON-LD: {exc}") from exc

        if not isinstance(data, dict):
            raise AbbvieScraperError("Unexpected JSON-LD structure.")
        return data

    @staticmethod
    def _parse_locations(json_ld: Dict[str, object]) -> List[str]:
        locations: List[str] = []
        job_location = json_ld.get("jobLocation")
        if isinstance(job_location, dict):
            job_location = [job_location]

        if isinstance(job_location, list):
            for location in job_location:
                if not isinstance(location, dict):
                    continue
                address = location.get("address")
                if not isinstance(address, dict):
                    continue
                locality = AbbvieJobScraper._coerce_str(address.get("addressLocality"))
                region = AbbvieJobScraper._coerce_str(address.get("addressRegion"))
                country = AbbvieJobScraper._coerce_str(address.get("addressCountry"))
                parts = [part for part in (locality, region, country) if part]
                if parts:
                    # Remove duplicates while preserving order.
                    seen: set[str] = set()
                    unique_parts = [p for p in parts if not (p in seen or seen.add(p))]
                    locations.append(", ".join(unique_parts))
        return locations

    @staticmethod
    def _text_or_none(element: Optional[BeautifulSoup]) -> Optional[str]:
        if not element:
            return None
        text = element.get_text(separator=" ", strip=True)
        return text or None

    @staticmethod
    def _html_to_text(value: str) -> str:
        if not value:
            return ""
        soup = BeautifulSoup(value, "html.parser")
        for br in soup.find_all("br"):
            br.replace_with("\n")
        text = soup.get_text("\n")
        lines = [unescape(line.strip()) for line in text.splitlines()]
        return repair_mojibake("\n".join(filter(None, lines)))

    @staticmethod
    def _coerce_str(value: object) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            value = value.strip()
            return value or None
        return str(value).strip() or None


def persist_listing(listing: JobListing) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or (listing.locations[0] if listing.locations else ""))[:255] or None,
        "date": (listing.date_posted or listing.valid_through or "")[:100] or None,
        "description": listing.description[:10000],
        "metadata": {
            "jobId": listing.job_id,
            "locations": listing.locations,
            **{k: v for k, v in listing.metadata.items() if v is not None},
        },
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Persisted AbbVie job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape AbbVie careers job listings.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help=f"Number of jobs to request per listing page (default: {DEFAULT_PAGE_SIZE}).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.4,
        help="Seconds to sleep between consecutive HTTP requests.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and display jobs without writing to the database.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s: %(message)s")

    scraper = AbbvieJobScraper(page_size=args.page_size, delay=args.delay)
    totals = {"fetched": 0, "created": 0, "errors": 0}

    for listing in scraper.scrape(limit=args.limit):
        totals["fetched"] += 1
        if args.dry_run:
            print(json.dumps(listing.__dict__, default=str, ensure_ascii=False))
            continue

        try:
            created = persist_listing(listing)
            if created:
                totals["created"] += 1
        except Exception as exc:  # pragma: no cover - defensive persistence guard
            logging.error("Failed to persist job %s: %s", listing.detail_url, exc)
            totals["errors"] += 1

    if not args.dry_run:
        totals["dedupe"] = deduplicate_job_postings(scraper=SCRAPER)

    logging.info(
        "AbbVie scraper finished - fetched=%(fetched)s created=%(created)s errors=%(errors)s",
        totals,
    )
    return 0 if not totals["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
