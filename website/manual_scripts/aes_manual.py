#!/usr/bin/env python3
"""Manual scraper for AES (Workday-hosted) careers listings."""
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
CAREERS_URL = "https://aes.wd1.myworkdayjobs.com/AES_US"
WORKDAY_ROOT = "https://aes.wd1.myworkdayjobs.com"
PORTAL = "AES_US"
CXS_BASE = f"{WORKDAY_ROOT}/wday/cxs/aes/{PORTAL}"
JOBS_ENDPOINT = f"{CXS_BASE}/jobs"
DETAIL_ENDPOINT_BASE = CXS_BASE
SESSION_SEED_URL = f"{WORKDAY_ROOT}/en-US/{PORTAL}"

PAGE_SIZE = 50
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}
HTML_ACCEPT_HEADER = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 600), 30)
SCRAPER_QS = Scraper.objects.filter(company="AES", url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched AES; using id=%s.", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="AES",
        url=CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the AES scraper encounters unrecoverable issues."""


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
    return soup.get_text(" ", strip=True)


class AESJobScraper:
    def __init__(
        self,
        *,
        page_size: int = PAGE_SIZE,
        delay: float = 0.15,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.page_size = max(1, int(page_size))
        self.delay = max(0.0, float(delay))
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self._bootstrapped = False

    def scrape(self, *, limit: Optional[int] = None) -> Iterable[JobListing]:
        self._ensure_session_bootstrap()
        offset = 0
        fetched = 0
        total: Optional[int] = None

        while True:
            payload = self._fetch_page(offset)
            postings = payload.get("jobPostings") or []
            if not postings:
                logging.info("No postings returned at offset %s; stopping.", offset)
                break

            if total is None:
                total = self._parse_total(payload.get("total"))
                logging.info("Discovered total jobs=%s.", total)

            for raw in postings:
                listing = self._build_listing(raw)
                if listing is None:
                    continue
                yield listing
                fetched += 1
                if limit is not None and fetched >= limit:
                    logging.info("Reached limit=%s; stopping.", limit)
                    return

            offset += len(postings)
            if total is not None and offset >= total:
                logging.info("Reached reported total=%s; pagination complete.", total)
                return
            if self.delay:
                time.sleep(self.delay)

    def _ensure_session_bootstrap(self, force: bool = False) -> None:
        if self._bootstrapped and not force:
            return
        headers = DEFAULT_HEADERS.copy()
        headers["Accept"] = HTML_ACCEPT_HEADER
        response = self.session.get(SESSION_SEED_URL, headers=headers, timeout=40)
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
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=40,
        )
        if response.status_code == 400:
            logging.debug("Jobs endpoint returned 400; retrying after bootstrap.")
            self._ensure_session_bootstrap(force=True)
            response = self.session.post(
                JOBS_ENDPOINT,
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"},
                timeout=40,
            )
        response.raise_for_status()
        return response.json()

    def _fetch_detail(self, external_path: str) -> Dict[str, object]:
        path = external_path.lstrip("/")
        url = f"{DETAIL_ENDPOINT_BASE}/{path}"
        response = self.session.get(url, headers={"Accept": DEFAULT_HEADERS["Accept"]}, timeout=40)
        if response.status_code == 400:
            logging.debug("Detail endpoint returned 400; retrying after bootstrap.")
            self._ensure_session_bootstrap(force=True)
            response = self.session.get(url, headers={"Accept": DEFAULT_HEADERS["Accept"]}, timeout=40)
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

        detail_payload = self._fetch_detail(external_path)
        info = detail_payload.get("jobPostingInfo") or {}
        public_url = (
            external_path
            if external_path.startswith("http")
            else urljoin(f"{WORKDAY_ROOT}/{PORTAL}/", external_path.lstrip("/"))
        )

        description_html = info.get("jobDescription") or ""
        description_text = _html_to_text(description_html)
        metadata = {
            "jobReqId": info.get("jobReqId"),
            "remoteType": info.get("remoteType"),
            "timeType": info.get("timeType"),
            "country": (
                info.get("jobRequisitionLocation", {}).get("country", {}).get("descriptor")
                if isinstance(info.get("jobRequisitionLocation"), dict)
                else None
            ),
        }

        return JobListing(
            title=title,
            link=public_url,
            location=_clean(summary.get("locationsText")) or None,
            date=_clean(info.get("startDate") or info.get("postedOn")),
            description=description_text,
            metadata={k: v for k, v in metadata.items() if v},
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
        "Persisted AES job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape AES Workday job listings.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum jobs to process.")
    parser.add_argument(
        "--page-size",
        type=int,
        default=PAGE_SIZE,
        help="Number of records requested per API page (default: 50).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.15,
        help="Seconds to sleep between Workday API calls (default: 0.15).",
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

    scraper = AESJobScraper(page_size=args.page_size, delay=args.delay)
    totals = {"fetched": 0, "created": 0}

    for listing in scraper.scrape(limit=args.limit):
        totals["fetched"] += 1
        if args.dry_run:
            print(json.dumps(listing.__dict__, default=str, ensure_ascii=False))
            continue
        created = persist_listing(listing)
        if created:
            totals["created"] += 1

    if not args.dry_run and totals["fetched"]:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        logging.info("Deduplication summary: %s", dedupe_summary)

    logging.info("AES scraper finished - fetched=%(fetched)s created=%(created)s", totals)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
