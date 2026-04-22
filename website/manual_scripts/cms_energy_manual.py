#!/usr/bin/env python3
"""Manual scraper for CMS Energy / Consumers Energy careers (SuccessFactors RMK).

The public careers search hosted at https://careers.consumersenergy.com/search/
exposes paginated HTML tables. This script walks those tables, hydrates each
job-detail page, and persists the postings via the shared Django ORM so that
operations staff can trigger the ingestion on demand.
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
# Bootstrap Django so the script can run standalone from the management UI.
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
# Constants and configuration
# ---------------------------------------------------------------------------
ROOT_CAREERS_URL = "https://www.cmsenergy.com/careers/default.aspx"
BASE_DOMAIN = "https://careers.consumersenergy.com"
SEARCH_URL = f"{BASE_DOMAIN}/search/"
REQUEST_TIMEOUT = 45
DEFAULT_DELAY = 0.35
DEFAULT_PAGE_SIZE = 25
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
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 120)
SCRAPER_QS = Scraper.objects.filter(company="CMS Energy", url=ROOT_CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple CMS Energy scraper rows found; using id=%s.", SCRAPER.id)
else:  # pragma: no cover - bootstrap path
    SCRAPER = Scraper.objects.create(
        company="CMS Energy",
        url=ROOT_CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the CMS Energy careers scrape cannot continue."""


def _clean_whitespace(value: str) -> str:
    return " ".join(value.replace("\xa0", " ").replace("\u202f", " ").split())


def _clean_optional(node: Optional[Tag]) -> Optional[str]:
    if not node:
        return None
    text = node.get_text(" ", strip=True)
    if not text:
        return None
    return _clean_whitespace(text)


def _normalize_description(text: str) -> str:
    lines = [line.strip() for line in text.replace("\r", "\n").split("\n")]
    return "\n".join(line for line in lines if line)


_JOB_ID_PATTERN = re.compile(r"/(\d{6,})/?$")


def _extract_job_id(path: str) -> Optional[str]:
    match = _JOB_ID_PATTERN.search(path or "")
    return match.group(1) if match else None


def _compact_metadata(data: Dict[str, object]) -> Dict[str, object]:
    return {key: value for key, value in data.items() if value not in (None, "", [], {}, ())}


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
    apply_url: Optional[str]
    metadata: Dict[str, object]


class CmsEnergyCareersScraper:
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

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def scrape(
        self,
        *,
        start_row: int = 0,
        max_pages: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Iterable[JobListing]:
        offset = max(0, start_row)
        processed = 0
        page_index = 0
        announced_total = False

        while True:
            if max_pages is not None and page_index >= max_pages:
                self.logger.info("Reached max_pages=%s; stopping pagination.", max_pages)
                break

            soup = self._fetch_search_page(offset=offset)

            if not announced_total:
                total = self._extract_total_jobs(soup)
                if total is not None:
                    self.logger.info("CMS Energy reports %s open jobs.", total)
                announced_total = True

            summaries = self._parse_search_rows(soup)
            if not summaries:
                self.logger.info("No job rows returned at startrow=%s; ending scrape.", offset)
                break

            for summary in summaries:
                if limit is not None and processed >= limit:
                    self.logger.info("Reached limit=%s; stopping.", limit)
                    return

                try:
                    detail = self._fetch_job_detail(summary)
                except ScraperError as exc:
                    self.logger.warning("Skipping %s (%s)", summary.detail_url, exc)
                    continue

                listing = JobListing(**asdict(summary), **detail)
                yield listing
                processed += 1

                if limit is not None and processed >= limit:
                    self.logger.info("Reached limit=%s; stopping.", limit)
                    return

            if len(summaries) < self.page_size:
                self.logger.info(
                    "Received %s jobs (< page_size=%s) at startrow=%s; pagination complete.",
                    len(summaries),
                    self.page_size,
                    offset,
                )
                break

            offset += self.page_size
            page_index += 1
            if self.delay:
                time.sleep(self.delay)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _fetch_search_page(self, *, offset: int) -> BeautifulSoup:
        params = {"q": "", "startrow": offset}
        self.logger.debug("Fetching search page %s", params)
        response = self.session.get(SEARCH_URL, params=params, timeout=REQUEST_TIMEOUT)
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            snippet = response.text[:300].strip()
            raise ScraperError(f"Search request failed: {exc} | {snippet}") from exc
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

        title = _clean_optional(anchor)
        detail_path = anchor["href"].strip()
        detail_url = urljoin(BASE_DOMAIN, detail_path)

        location = _clean_optional(row.select_one("td.colLocation"))
        if not location:
            location = _clean_optional(row.select_one(".jobdetail-phone .jobLocation"))

        posted_date = _clean_optional(row.select_one("td.colDate"))
        if not posted_date:
            posted_date = _clean_optional(row.select_one(".jobdetail-phone .jobDate"))

        job_id = _extract_job_id(detail_path)

        if not title or not detail_url:
            return None

        return JobSummary(
            title=title,
            detail_url=detail_url,
            location=location,
            posted_date=posted_date,
            job_id=job_id,
        )

    def _fetch_job_detail(self, summary: JobSummary) -> Dict[str, object]:
        self.logger.debug("Fetching job detail %s", summary.detail_url)
        response = self.session.get(summary.detail_url, timeout=REQUEST_TIMEOUT)
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            snippet = response.text[:300].strip()
            raise ScraperError(f"Detail request failed: {exc} | {snippet}") from exc

        soup = BeautifulSoup(response.text, "html.parser")

        description_node = soup.select_one(".jobdescription")
        if description_node:
            description_html = str(description_node)
            description_text = _normalize_description(description_node.get_text("\n", strip=True))
        else:
            fallback = soup.select_one(".job") or soup.body
            description_html = str(fallback) if fallback else None
            description_text = _normalize_description(
                fallback.get_text("\n", strip=True) if fallback else ""
            )

        if not description_text:
            description_text = "Description unavailable."

        apply_link = soup.select_one("a.dialogApplyBtn[href], a.apply[href]")
        apply_href = apply_link.get("href").strip() if apply_link and apply_link.get("href") else None
        apply_url = urljoin(BASE_DOMAIN, apply_href) if apply_href else None

        company = _clean_optional(soup.select_one("#job-company span"))
        location_detail = _clean_optional(soup.select_one("#job-location .jobGeoLocation"))
        posted_detail = _clean_optional(soup.select_one("#job-date"))

        meta_payload: Dict[str, str] = {}
        for meta in soup.select("meta[itemprop]"):
            itemprop = (meta.get("itemprop") or "").strip()
            content = (meta.get("content") or "").strip()
            if itemprop and content:
                meta_payload[itemprop] = content

        metadata = _compact_metadata(
            {
                "job_id": summary.job_id,
                "detail_url": summary.detail_url,
                "search_location": summary.location,
                "search_posted_date": summary.posted_date,
                "company_detail": company,
                "location_detail": location_detail,
                "posted_detail": posted_detail,
                "apply_url": apply_url,
                "structured_meta": meta_payload or None,
            }
        )

        return {
            "description_text": description_text,
            "description_html": description_html,
            "apply_url": apply_url,
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
        "location": (listing.location or listing.metadata.get("location_detail") or "")[:255] or None,
        "date": (listing.posted_date or listing.metadata.get("posted_detail") or "")[:100] or None,
        "description": (listing.description_text or "")[:10000],
        "metadata": {
            **listing.metadata,
            "description_html": listing.description_html,
            "apply_url": listing.apply_url,
        },
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Stored CMS Energy job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape CMS Energy / Consumers Energy careers listings."
    )
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Maximum number of result pages to walk (default: all).",
    )
    parser.add_argument(
        "--start-row",
        type=int,
        default=0,
        help="Result offset to begin pagination with (default: 0).",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help="Override SuccessFactors page size (default: 25).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help="Seconds to sleep between listing pages (default: 0.35).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch jobs but do not persist them to the database.",
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

    scraper = CmsEnergyCareersScraper(page_size=args.page_size, delay=args.delay)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    for listing in scraper.scrape(
        start_row=args.start_row,
        max_pages=args.max_pages,
        limit=args.limit,
    ):
        totals["fetched"] += 1
        if args.dry_run:
            print(json.dumps(asdict(listing), default=str, ensure_ascii=False))
            continue
        try:
            created = persist_listing(listing)
            if created:
                totals["created"] += 1
            else:
                totals["updated"] += 1
        except Exception as exc:  # pragma: no cover - persistence fallback
            logging.error("Failed to persist job %s: %s", listing.detail_url, exc)
            totals["errors"] += 1

    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["dedupe"] = dedupe_summary

    logging.info(
        "CMS Energy scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )
    return 0 if not totals["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
