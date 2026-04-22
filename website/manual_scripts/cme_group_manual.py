#!/usr/bin/env python3
"""Manual scraper for CME Group's Workday-hosted job listings."""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional
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

from django.conf import settings  # noqa: E402

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

# ---------------------------------------------------------------------------
# Constants & configuration
# ---------------------------------------------------------------------------
CAREERS_URL = "https://www.cmegroup.com/careers"
WORKDAY_ROOT = "https://cmegroup.wd1.myworkdayjobs.com"
TENANT = "cmegroup"
PORTAL = "cme_careers"
CXS_BASE = f"{WORKDAY_ROOT}/wday/cxs/{TENANT}/{PORTAL}"
JOBS_ENDPOINT = f"{CXS_BASE}/jobs"
DETAIL_ENDPOINT_BASE = CXS_BASE
PUBLIC_JOB_BASE = f"{WORKDAY_ROOT}/{PORTAL}"
SESSION_SEED_URL = f"{WORKDAY_ROOT}/en-US/{PORTAL}"

MAX_PAGE_SIZE = 20
PAGE_SIZE = 20
DEFAULT_DELAY_SECONDS = 0.2
REQUEST_TIMEOUT = (15, 45)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}
HTML_ACCEPT_HEADER = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
JSON_POST_HEADERS = {
    "Content-Type": "application/json",
    "Referer": SESSION_SEED_URL,
    "Origin": WORKDAY_ROOT,
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 1200), 120)
SCRAPER_QS = Scraper.objects.filter(company="CME Group", url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched CME Group; using id=%s.", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="CME Group",
        url=CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the CME Group scraper cannot proceed."""


@dataclass
class JobListing:
    title: str
    link: str
    location: Optional[str]
    date: Optional[str]
    description: str
    metadata: Dict[str, object]


def _clean(value: Optional[str]) -> str:
    return (value or "").strip()


def _html_to_text(html: Optional[str]) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text("\n", strip=True)


def _descriptor(data: Optional[Dict[str, object]]) -> Optional[str]:
    if not isinstance(data, dict):
        return None
    descriptor = data.get("descriptor")
    return descriptor.strip() if isinstance(descriptor, str) else None


class CMEGroupJobScraper:
    def __init__(
        self,
        *,
        page_size: int = PAGE_SIZE,
        delay: float = DEFAULT_DELAY_SECONDS,
        session: Optional[requests.Session] = None,
    ) -> None:
        clamped_size = max(1, min(int(page_size), MAX_PAGE_SIZE))
        if clamped_size != int(page_size):
            logging.getLogger(self.__class__.__name__).debug(
                "Clamping page_size=%s to max supported value=%s",
                page_size,
                MAX_PAGE_SIZE,
            )
        self.page_size = clamped_size
        self.delay = max(0.0, float(delay))
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self._bootstrapped = False
        self.logger = logging.getLogger(self.__class__.__name__)

    def scrape(self, *, limit: Optional[int] = None) -> Iterable[JobListing]:
        self._ensure_session_bootstrap()
        offset = 0
        fetched = 0
        total: Optional[int] = None
        seen_paths: set[str] = set()

        while True:
            payload = self._fetch_page(offset)
            postings = payload.get("jobPostings") or []
            if not postings:
                self.logger.info("No postings returned at offset %s; stopping.", offset)
                break

            if total is None:
                total = self._parse_total(payload.get("total"))
                self.logger.info("Discovered %s total postings.", total)

            for summary in postings:
                external_path = _clean(summary.get("externalPath"))
                if not external_path:
                    continue
                if external_path in seen_paths:
                    continue
                seen_paths.add(external_path)

                try:
                    listing = self._build_listing(summary)
                except ScraperError as exc:
                    self.logger.error("Skipping %s due to error: %s", external_path, exc)
                    continue

                if listing is None:
                    continue

                yield listing
                fetched += 1
                if limit is not None and fetched >= limit:
                    self.logger.info("Reached limit=%s; stopping.", limit)
                    return

            offset += len(postings)
            if total is not None and offset >= total:
                self.logger.info("Reached reported total; pagination complete.")
                break

            if self.delay:
                time.sleep(self.delay)

    def _ensure_session_bootstrap(self, force: bool = False) -> None:
        if self._bootstrapped and not force:
            return

        headers = dict(DEFAULT_HEADERS)
        headers["Accept"] = HTML_ACCEPT_HEADER

        response = self.session.get(SESSION_SEED_URL, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        self._bootstrapped = True

    def _fetch_page(self, offset: int) -> Dict[str, object]:
        payload = {
            "limit": self.page_size,
            "offset": offset,
            "searchText": "",
            "appliedFacets": {},
            "userPreferredLanguage": "en-US",
        }
        response = self.session.post(
            JOBS_ENDPOINT,
            json=payload,
            headers=JSON_POST_HEADERS,
            timeout=REQUEST_TIMEOUT,
        )
        if response.status_code == 400:
            self.logger.debug("Jobs endpoint returned 400; retrying after bootstrap.")
            self._ensure_session_bootstrap(force=True)
            response = self.session.post(
                JOBS_ENDPOINT,
                json=payload,
                headers=JSON_POST_HEADERS,
                timeout=REQUEST_TIMEOUT,
            )
        response.raise_for_status()
        return response.json()

    def _fetch_detail(self, external_path: str) -> Dict[str, object]:
        url_path = external_path.lstrip("/")
        url = f"{DETAIL_ENDPOINT_BASE}/{url_path}"

        headers = {
            "Accept": DEFAULT_HEADERS["Accept"],
            "Referer": SESSION_SEED_URL,
            "Origin": WORKDAY_ROOT,
        }
        response = self.session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        if response.status_code == 400:
            self.logger.debug("Detail endpoint returned 400; retrying after bootstrap.")
            self._ensure_session_bootstrap(force=True)
            response = self.session.get(
                url,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
        response.raise_for_status()
        return response.json()

    def _parse_total(self, value: object) -> Optional[int]:
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def _build_listing(self, summary: Dict[str, object]) -> Optional[JobListing]:
        title = _clean(summary.get("title"))
        external_path = _clean(summary.get("externalPath"))
        if not title or not external_path:
            return None

        try:
            detail_payload = self._fetch_detail(external_path)
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch detail for {external_path}: {exc}") from exc

        info = detail_payload.get("jobPostingInfo") or {}
        description_html = info.get("jobDescription") or ""
        description_text = _html_to_text(description_html)

        public_url = info.get("externalUrl")
        if not public_url:
            public_url = (
                external_path
                if external_path.startswith("http")
                else urljoin(f"{PUBLIC_JOB_BASE}/", external_path.lstrip("/"))
            )

        location = _clean(summary.get("locationsText")) or _clean(info.get("location")) or None
        posted_label = _clean(summary.get("postedOn"))
        start_date = _clean(info.get("startDate"))

        metadata: Dict[str, object] = {
            "jobReqId": _clean(info.get("jobReqId")),
            "jobPostingId": _clean(info.get("jobPostingId")),
            "jobPostingSiteId": _clean(info.get("jobPostingSiteId")),
            "timeType": _clean(info.get("timeType")),
            "postedOn": posted_label or None,
            "startDate": start_date or None,
            "country": _descriptor(info.get("country")),
            "jobRequisitionLocation": _descriptor(info.get("jobRequisitionLocation")),
            "bulletFields": summary.get("bulletFields"),
            "externalPath": external_path,
        }
        clean_metadata = {key: value for key, value in metadata.items() if value not in (None, "", [], {})}

        return JobListing(
            title=title,
            link=public_url,
            location=location,
            date=start_date or posted_label or None,
            description=description_text,
            metadata=clean_metadata,
        )


def persist_listing(listing: JobListing) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255] or None,
        "date": (listing.date or "")[:100] or None,
        "description": listing.description[:10000],
        "metadata": listing.metadata,
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.link,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Persisted CME Group job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape CME Group Workday job listings.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum jobs to process.")
    parser.add_argument(
        "--page-size",
        type=int,
        default=PAGE_SIZE,
        help="Number of records requested per API page (default/max: 20).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY_SECONDS,
        help="Seconds to sleep between Workday API calls (default: 0.2).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print results without persisting.")
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

    scraper = CMEGroupJobScraper(page_size=args.page_size, delay=args.delay)
    totals = {"fetched": 0, "created": 0}

    try:
        for listing in scraper.scrape(limit=args.limit):
            totals["fetched"] += 1
            if args.dry_run:
                print(json.dumps(listing.__dict__, default=str, ensure_ascii=False))
                continue
            created = persist_listing(listing)
            if created:
                totals["created"] += 1
    except (ScraperError, requests.RequestException) as exc:
        logging.error("CME Group scraper failed: %s", exc)
        return 1

    if not args.dry_run and totals["fetched"]:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        logging.info("Deduplication summary: %s", dedupe_summary)

    logging.info(
        "CME Group scraper finished - fetched=%(fetched)s created=%(created)s",
        totals,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
