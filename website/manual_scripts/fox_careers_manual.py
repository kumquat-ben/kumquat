#!/usr/bin/env python3
"""Manual scraper for https://www.foxcareers.com.

This script replicates the in-browser search experience: it walks the paginated
`/Search/JobsList` endpoint, extracts summary metadata, visits each job-detail
page for richer fields (description HTML/text, structured metadata, apply URL),
and stores/updates records via the shared Django `JobPosting` model.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, asdict
from typing import Dict, Iterable, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag
from pathlib import Path

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
BASE_URL = "https://www.foxcareers.com"
SEARCH_URL = urljoin(BASE_URL, "/Search/SearchResults")
LIST_ENDPOINT = urljoin(BASE_URL, "/Search/JobsList")
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.8",
    "Referer": SEARCH_URL,
}
PAGE_SIZE = 10
DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 60)

SCRAPER_QS = Scraper.objects.filter(company="FOX Careers", url=SEARCH_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple FOX Careers scrapers found; using id=%s", SCRAPER.id)
else:  # pragma: no cover - creation path
    SCRAPER = Scraper.objects.create(
        company="FOX Careers",
        url=SEARCH_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the scraper encounters a non-recoverable error."""


@dataclass
class JobSummary:
    title: str
    brand: Optional[str]
    detail_url: str
    job_id: Optional[str]
    location: Optional[str]
    date_posted: Optional[str]


@dataclass
class JobListing(JobSummary):
    apply_url: Optional[str]
    description_text: Optional[str]
    description_html: Optional[str]
    metadata: Dict[str, object]


class FoxCareersScraper:
    def __init__(self, *, delay: float = 0.25, session: Optional[requests.Session] = None) -> None:
        self.delay = max(delay, 0.0)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def scrape(
        self, *, max_pages: Optional[int] = None, limit: Optional[int] = None
    ) -> Iterable[JobListing]:
        page = 0
        yielded = 0

        while True:
            if max_pages is not None and page >= max_pages:
                self.logger.info("Reached max_pages=%s; stopping.", max_pages)
                break

            summaries = self._fetch_jobs_page(page)
            if not summaries:
                self.logger.info("No listings returned for page %s; stopping.", page)
                break

            for summary in summaries:
                detail = self._fetch_job_detail(summary.detail_url)
                metadata = detail["metadata"]
                metadata.setdefault("job_id", summary.job_id)
                metadata.setdefault("brand", summary.brand)
                metadata.setdefault("detail_url", summary.detail_url)

                listing = JobListing(**asdict(summary), **detail)
                yield listing
                yielded += 1

                if limit is not None and yielded >= limit:
                    self.logger.info("Reached limit=%s; stopping.", limit)
                    return

                if self.delay:
                    time.sleep(self.delay)

            if len(summaries) < PAGE_SIZE:
                self.logger.info("Page %s returned %s (<%s) jobs; stopping.", page, len(summaries), PAGE_SIZE)
                break

            page += 1

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _fetch_jobs_page(self, page: int) -> List[JobSummary]:
        params = {
            "page": page,
            "jobFunction": "",
            "brand": "",
            "subBrand": "",
            "brandCategory": "",
            "country": "",
            "location": "",
            "locationType": "",
            "experienceLevel": "",
            "city": "",
            "latitude": 0,
            "longitude": 0,
            "keyword": "",
        }
        self.logger.debug("Fetching FOX Careers jobs page %s", page)
        response = self.session.get(LIST_ENDPOINT, params=params, timeout=45)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        if page == 0:
            total = self._extract_total_count(soup)
            if total is not None:
                self.logger.info("FOX Careers reports %s open jobs.", total)

        summaries: List[JobSummary] = []
        for item in soup.select("div.jobListing"):
            summary = self._parse_job_listing(item)
            if summary:
                summaries.append(summary)
        return summaries

    def _parse_job_listing(self, item: Tag) -> Optional[JobSummary]:
        link_elem = item.select_one("a.searchResultTitle")
        if not link_elem or not link_elem.get("href"):
            return None

        detail_path = link_elem["href"].strip()
        detail_url = urljoin(BASE_URL, detail_path)

        raw_title = link_elem.get_text(" ", strip=True)
        title, job_id = self._split_title_and_id(raw_title, detail_path)

        brand_elem = item.select_one(".searchResultBrand")
        brand = brand_elem.get_text(" ", strip=True) if brand_elem else None

        location = None
        date_posted = None

        detail_blocks = item.select("p.searchResultDetail")
        if detail_blocks:
            location_spans = [
                span.get_text(" ", strip=True)
                for span in detail_blocks[0].find_all("span")
                if span.get_text(strip=True) and span.get_text(strip=True) != ";"
            ]
            location = ", ".join(location_spans) if location_spans else detail_blocks[0].get_text(" ", strip=True)

        for block in detail_blocks:
            text = block.get_text(" ", strip=True)
            if text.lower().startswith("job posting date:"):
                date_posted = text.split(":", 1)[1].strip() or None
                break

        return JobSummary(
            title=title,
            brand=brand,
            detail_url=detail_url,
            job_id=job_id,
            location=location,
            date_posted=date_posted,
        )

    def _fetch_job_detail(self, detail_url: str) -> Dict[str, Optional[object]]:
        self.logger.debug("Fetching job detail %s", detail_url)
        response = self.session.get(detail_url, timeout=45)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        apply_link = soup.select_one("a.applyButton")
        apply_href = apply_link.get("href") if apply_link else None
        apply_url = urljoin(BASE_URL, apply_href) if apply_href else None

        description_block = soup.select_one("#jobDetails .jobDescription") or soup.select_one(".jobDescription")
        description_text = _text_or_none(description_block, separator="\n\n")
        description_html = str(description_block) if description_block else None

        summary_block = soup.select_one("div.jobSummary .summaryText")
        summary_meta: Dict[str, object] = {}
        if summary_block:
            summary_meta.update(self._extract_summary_metadata(summary_block))

        schema_data = self._extract_schema_json(soup)
        if schema_data:
            summary_meta["schema_org"] = schema_data

        if apply_url:
            summary_meta.setdefault("apply_url", apply_url)

        return {
            "apply_url": apply_url,
            "description_text": description_text,
            "description_html": description_html,
            "metadata": summary_meta,
        }

    def _extract_summary_metadata(self, container: Tag) -> Dict[str, str]:
        data: Dict[str, str] = {}
        for p in container.select("p"):
            strong = p.find("strong")
            if not strong:
                continue
            label = strong.get_text(" ", strip=True).rstrip(":").strip()
            strong.extract()
            value_text = p.get_text(" ", strip=True)
            if not value_text:
                spans = [s.get_text(" ", strip=True) for s in p.find_all("span")]
                value_text = ", ".join([s for s in spans if s])
            data[label] = value_text
        return data

    def _extract_schema_json(self, soup: BeautifulSoup) -> Optional[dict]:
        script = soup.find("script", attrs={"type": "application/ld+json"})
        if not script or not script.string:
            return None
        try:
            return json.loads(script.string)
        except json.JSONDecodeError:
            self.logger.warning("Failed to parse schema.org JSON.")
            return None

    @staticmethod
    def _extract_total_count(soup: BeautifulSoup) -> Optional[int]:
        input_elem = soup.select_one("#hiddenJobCount")
        if not input_elem:
            return None
        value = input_elem.get("value")
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _split_title_and_id(title: str, detail_path: str) -> tuple[str, Optional[str]]:
        job_id = None
        if "/JobDetail/" in detail_path:
            parts = detail_path.split("/JobDetail/")[-1].split("/")
            if parts:
                job_id_candidate = parts[0]
                if job_id_candidate:
                    job_id = job_id_candidate

        if "(" in title and title.endswith(")") and job_id:
            possible_id = title.rsplit("(", 1)[-1].strip(" )")
            if possible_id == job_id:
                title = title.rsplit("(", 1)[0].strip()
        return title, job_id


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------
def _text_or_none(element: Optional[Tag], *, separator: str = " ") -> Optional[str]:
    if not element:
        return None
    text = element.get_text(separator=separator, strip=True)
    return text or None


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------
def store_listing(listing: JobListing) -> bool:
    metadata = dict(listing.metadata or {})
    metadata.setdefault("job_id", listing.job_id)
    metadata.setdefault("brand", listing.brand)
    if listing.description_html:
        metadata.setdefault("description_html", listing.description_html)

    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255] or None,
        "date": (listing.date_posted or metadata.get("Job Posting Date") or "")[:100] or None,
        "description": (listing.description_text or "")[:10000],
        "metadata": metadata,
    }

    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults=defaults,
    )
    logging.getLogger("store_listing").debug(
        "Stored FOX job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="FOX Careers manual scraper.")
    parser.add_argument("--max-pages", type=int, default=None, help="Maximum pages to fetch.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to persist.")
    parser.add_argument("--delay", type=float, default=0.25, help="Delay between requests.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print jobs as JSON instead of writing to the database.",
    )
    return parser.parse_args(argv)


def run_scrape(max_pages: Optional[int], limit: Optional[int], delay: float, dry_run: bool) -> Dict[str, object]:
    scraper = FoxCareersScraper(delay=delay)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    try:
        for listing in scraper.scrape(max_pages=max_pages, limit=limit):
            totals["fetched"] += 1
            if dry_run:
                print(json.dumps(asdict(listing), default=str, ensure_ascii=False))
                continue
            try:
                created = store_listing(listing)
                if created:
                    totals["created"] += 1
                else:
                    totals["updated"] += 1
            except Exception as exc:  # pragma: no cover - persistence failure
                logging.error("Failed to store job %s: %s", listing.detail_url, exc)
                totals["errors"] += 1
    except requests.HTTPError as exc:
        logging.error("HTTP error while scraping FOX Careers: %s", exc)
        totals["errors"] += 1
    except requests.RequestException as exc:
        logging.error("Network error while scraping FOX Careers: %s", exc)
        totals["errors"] += 1
    except ScraperError as exc:
        logging.error("FOX Careers scraper stopped: %s", exc)
        totals["errors"] += 1

    if not dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["dedupe"] = dedupe_summary

    return totals


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()), format="%(levelname)s: %(message)s")

    totals = run_scrape(args.max_pages, args.limit, args.delay, args.dry_run)
    logging.info(
        "FOX Careers scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )

    if not args.dry_run and "dedupe" in totals:
        logging.info("Deduplication summary: %s", totals["dedupe"])
    return 0 if not totals["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
