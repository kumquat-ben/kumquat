#!/usr/bin/env python3
"""Manual scraper for Cintas careers (SuccessFactors-hosted).

The script paginates through the public Cintas search results, visits each job
detail page for richer metadata, and persists the postings via Django's ORM.
It is intended for on-demand runs through the manual scripts dashboard.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag

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
# Constants & configuration
# ---------------------------------------------------------------------------
BASE_URL = "https://careers.cintas.com"
SEARCH_URL = f"{BASE_URL}/search/"
SCRAPER_URL = SEARCH_URL
REQUEST_TIMEOUT = 45
DEFAULT_PAGE_SIZE = 25
DEFAULT_DELAY = 0.35
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,"
        "image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": BASE_URL,
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 60)
SCRAPER_QS = Scraper.objects.filter(company="Cintas", url=SCRAPER_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Cintas scraper rows found; using id=%s.", SCRAPER.id)
else:  # pragma: no cover - bootstrap path
    SCRAPER = Scraper.objects.create(
        company="Cintas",
        url=SCRAPER_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )

JOB_ID_PATTERN = re.compile(r"/(\d{6,})/?$")


class ScraperError(Exception):
    """Raised when the scraper encounters a non-recoverable condition."""


def _normalize_whitespace(value: str) -> str:
    return " ".join(value.replace("\xa0", " ").replace("\u202f", " ").split())


def _normalize_description(value: str) -> str:
    value = value.replace("\r", "\n").replace("\xa0", " ").replace("\u202f", " ")
    lines = [line.strip() for line in value.split("\n")]
    cleaned = [line for line in lines if line]
    return "\n".join(cleaned)


def _text_or_none(node: Optional[Tag]) -> Optional[str]:
    if not node:
        return None
    text = _normalize_whitespace(node.get_text(" ", strip=True))
    return text or None


@dataclass
class JobSummary:
    title: str
    detail_url: str
    location: Optional[str]
    posted_date: Optional[str]
    job_id: Optional[str]


@dataclass
class JobListing(JobSummary):
    description_text: str
    description_html: Optional[str]
    metadata: Dict[str, object]


class CintasCareersScraper:
    def __init__(
        self,
        *,
        page_size: int = DEFAULT_PAGE_SIZE,
        delay: float = DEFAULT_DELAY,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.page_size = max(1, page_size)
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    def scrape(
        self,
        *,
        start_row: int = 0,
        max_pages: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Iterable[JobListing]:
        offset = max(0, start_row)
        yielded = 0
        page_index = 0
        announced_total = False

        while True:
            if max_pages is not None and page_index >= max_pages:
                self.logger.info("Reached max_pages=%s; stopping pagination.", max_pages)
                break

            soup = self._fetch_search_page(offset=offset)

            if not announced_total:
                total_jobs = self._extract_total_jobs(soup)
                if total_jobs is not None:
                    self.logger.info("Cintas reports %s open jobs.", total_jobs)
                announced_total = True

            summaries = self._parse_search_rows(soup)
            if not summaries:
                self.logger.info("No job rows returned at startrow=%s; ending scrape.", offset)
                break

            for summary in summaries:
                if limit is not None and yielded >= limit:
                    self.logger.info("Reached limit=%s; stopping.", limit)
                    return

                try:
                    detail = self._fetch_job_detail(summary.detail_url)
                except ScraperError as exc:
                    self.logger.warning("Skipping %s (%s)", summary.detail_url, exc)
                    continue

                metadata = dict(detail.get("metadata") or {})
                if summary.job_id:
                    metadata.setdefault("job_id", summary.job_id)
                detail["metadata"] = metadata

                listing = JobListing(**asdict(summary), **detail)
                yield listing
                yielded += 1

                if limit is not None and yielded >= limit:
                    self.logger.info("Reached limit=%s; stopping.", limit)
                    return

                if self.delay:
                    time.sleep(self.delay)

            if len(summaries) < self.page_size:
                self.logger.info(
                    "Fetched %s jobs (< page_size=%s) at startrow=%s; pagination complete.",
                    len(summaries),
                    self.page_size,
                    offset,
                )
                break

            offset += self.page_size
            page_index += 1

    def _fetch_search_page(self, *, offset: int) -> BeautifulSoup:
        params = {"q": "", "startrow": offset}
        self.logger.debug("Fetching search page startrow=%s", offset)
        try:
            response = self.session.get(SEARCH_URL, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch search page at startrow={offset}: {exc}") from exc
        return BeautifulSoup(response.text, "html.parser")

    def _parse_search_rows(self, soup: BeautifulSoup) -> List[JobSummary]:
        rows = soup.select("tr.data-row")
        summaries: List[JobSummary] = []
        for row in rows:
            summary = self._parse_row(row)
            if summary:
                summaries.append(summary)
        return summaries

    def _parse_row(self, row: Tag) -> Optional[JobSummary]:
        anchor = row.select_one("a.jobTitle-link")
        if not anchor or not anchor.get("href"):
            return None

        title = _text_or_none(anchor)
        detail_path = anchor["href"].strip()
        detail_url = urljoin(BASE_URL, detail_path)

        location = _text_or_none(row.select_one(".jobLocation"))
        posted_date = _text_or_none(row.select_one(".jobDate"))

        job_id: Optional[str] = None
        match = JOB_ID_PATTERN.search(detail_path)
        if match:
            job_id = match.group(1)

        if not title or not detail_url:
            return None

        return JobSummary(
            title=title,
            detail_url=detail_url,
            location=location,
            posted_date=posted_date,
            job_id=job_id,
        )

    def _fetch_job_detail(self, url: str) -> Dict[str, object]:
        self.logger.debug("Fetching job detail %s", url)
        try:
            response = self.session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch job detail {url}: {exc}") from exc

        soup = BeautifulSoup(response.text, "html.parser")
        description_node = (
            soup.select_one("[data-careersite-propertyid='description'] .jobdescription")
            or soup.select_one("[data-careersite-propertyid='description']")
        )

        description_text = ""
        description_html: Optional[str] = None
        if description_node:
            description_html = str(description_node)
            description_text = _normalize_description(description_node.get_text("\n", strip=True))
        else:
            fallback = soup.select_one(".jobdescription, #job") or soup.body
            if fallback:
                description_html = str(fallback)
                description_text = _normalize_description(fallback.get_text("\n", strip=True))

        if not description_text:
            description_text = "Description unavailable."

        properties: Dict[str, str] = {}
        for node in soup.select("[data-careersite-propertyid]"):
            key = (node.get("data-careersite-propertyid") or "").strip()
            if not key or key.lower() == "description":
                continue
            value = _text_or_none(node)
            if value:
                properties[key] = value

        apply_link = soup.select_one("a.apply, a.dialogApplyBtn")
        apply_url: Optional[str] = None
        if apply_link:
            href = (apply_link.get("href") or "").strip()
            if href and not href.lower().startswith("javascript:"):
                apply_url = urljoin(BASE_URL, href)

        metadata: Dict[str, object] = {}
        if properties:
            metadata["detail_properties"] = properties
        if apply_url:
            metadata["apply_url"] = apply_url

        return {
            "description_text": description_text,
            "description_html": description_html,
            "metadata": metadata,
        }

    def _extract_total_jobs(self, soup: BeautifulSoup) -> Optional[int]:
        label = soup.select_one("span.paginationLabel")
        if not label:
            return None
        text = label.get_text(" ", strip=True)
        match = re.search(r"of\s+([\d,]+)", text)
        if not match:
            return None
        try:
            return int(match.group(1).replace(",", ""))
        except ValueError:
            return None


def persist_listing(listing: JobListing) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255] or None,
        "date": (listing.posted_date or "")[:100] or None,
        "description": (listing.description_text or "")[:10000],
        "metadata": listing.metadata or None,
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Stored Cintas job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Cintas careers job listings.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Maximum number of result pages to fetch (default: all).",
    )
    parser.add_argument(
        "--start-row",
        type=int,
        default=0,
        help="Index offset to begin pagination (default: 0).",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help="Override page size used for pagination (default: 25).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help="Seconds to wait between detail requests (default: 0.35).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch jobs but do not write them to the database.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s: %(message)s")

    scraper = CintasCareersScraper(page_size=args.page_size, delay=args.delay)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    for listing in scraper.scrape(
        start_row=args.start_row,
        max_pages=args.max_pages,
        limit=args.limit,
    ):
        totals["fetched"] += 1

        if args.dry_run:
            print(json.dumps(asdict(listing), ensure_ascii=False, default=str))
            continue

        try:
            created = persist_listing(listing)
        except Exception as exc:  # pragma: no cover - defensive persistence path
            logging.error("Failed to persist %s: %s", listing.detail_url, exc)
            totals["errors"] += 1
            continue

        if created:
            totals["created"] += 1
        else:
            totals["updated"] += 1

    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["dedupe"] = dedupe_summary

    logging.info(
        "Cintas scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )
    return 0 if not totals["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
