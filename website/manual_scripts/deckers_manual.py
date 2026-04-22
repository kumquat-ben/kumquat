#!/usr/bin/env python3
"""Manual scraper for Deckers careers (Workday-powered).

The script pulls job postings from Deckers' Workday API and stores them in the
`JobPosting` table associated with a Deckers scraper entry.
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
CAREERS_URL = "https://deckers.com/careers"
WORKDAY_ROOT = "https://deckers.wd5.myworkdayjobs.com"
TENANT = "deckers"
PORTAL = "Deckers"
CXS_BASE = f"{WORKDAY_ROOT}/wday/cxs/{TENANT}/{PORTAL}"
JOBS_ENDPOINT = f"{CXS_BASE}/jobs"
DETAIL_ENDPOINT_BASE = CXS_BASE
SESSION_SEED_URL = f"{WORKDAY_ROOT}/en-US/{PORTAL}"
PUBLIC_JOB_BASE = f"{WORKDAY_ROOT}/{PORTAL}"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}
HTML_ACCEPT_HEADER = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
CSRF_HEADER_NAME = "X-CALYPSO-CSRF-TOKEN"
CSRF_TOKEN_PATTERN = re.compile(r'token:\s*"([^"]+)"')

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 1200), 60)
SCRAPER_QS = Scraper.objects.filter(company="Deckers", url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched Deckers; using id=%s.", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Deckers",
        url=CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the Deckers scraper cannot continue."""


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


class DeckersJobScraper:
    def __init__(
        self,
        *,
        page_size: int = 20,
        delay: float = 0.1,
        session: Optional[requests.Session] = None,
    ) -> None:
        # Deckers' Workday API rejects page sizes greater than 20 with HTTP 400.
        self.page_size = max(1, min(20, int(page_size)))
        self.delay = max(0.0, float(delay))
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self._bootstrapped = False
        self._csrf_token: Optional[str] = None

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

            for summary in postings:
                listing = self._build_listing(summary)
                if listing is None:
                    continue
                yield listing
                fetched += 1
                if limit is not None and fetched >= limit:
                    logging.info("Reached limit=%s; stopping.", limit)
                    return

            offset += len(postings)
            if total is not None and offset >= total:
                logging.info("Reached reported total jobs=%s; pagination complete.", total)
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
        self._store_csrf_token(response.text)
        self.session.headers["Origin"] = WORKDAY_ROOT
        self.session.headers["Referer"] = SESSION_SEED_URL
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
            timeout=40,
        )
        if response.status_code == 400:
            logging.debug("Jobs endpoint returned 400; retrying after bootstrap.")
            self._ensure_session_bootstrap(force=True)
            response = self.session.post(
                JOBS_ENDPOINT,
                json=payload,
                timeout=40,
            )
        response.raise_for_status()
        return response.json()

    def _fetch_detail(self, external_path: str) -> Dict[str, object]:
        path = external_path.lstrip("/")
        url = f"{DETAIL_ENDPOINT_BASE}/{path}"
        response = self.session.get(url, timeout=40)
        if response.status_code == 400:
            logging.debug("Detail endpoint returned 400; retrying after bootstrap.")
            self._ensure_session_bootstrap(force=True)
            response = self.session.get(url, timeout=40)
        response.raise_for_status()
        return response.json()

    def _store_csrf_token(self, html: str) -> None:
        match = CSRF_TOKEN_PATTERN.search(html)
        if not match:
            logging.warning("Failed to locate CSRF token in Workday bootstrap page.")
            return

        token = match.group(1).strip()
        if not token:
            logging.warning("Empty CSRF token extracted from Workday bootstrap page.")
            return

        self._csrf_token = token
        self.session.headers[CSRF_HEADER_NAME] = token

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
            info.get("externalUrl")
            if info.get("externalUrl")
            else urljoin(f"{PUBLIC_JOB_BASE}/", external_path.lstrip("/"))
        )
        description_html = info.get("jobDescription") or ""
        description_text = _html_to_text(description_html)

        location = (
            _clean(info.get("jobRequisitionLocation", {}).get("descriptor"))
            if isinstance(info.get("jobRequisitionLocation"), dict)
            else None
        ) or _clean(summary.get("locationsText")) or None

        metadata = {
            "jobReqId": info.get("jobReqId"),
            "jobPostingId": info.get("jobPostingId"),
            "timeType": info.get("timeType"),
            "postedOn": info.get("postedOn") or summary.get("postedOn"),
            "startDate": info.get("startDate"),
            "country": (
                info.get("jobRequisitionLocation", {}).get("country", {}).get("descriptor")
                if isinstance(info.get("jobRequisitionLocation"), dict)
                else None
            ),
        }
        metadata = {k: v for k, v in metadata.items() if v}

        return JobListing(
            title=title,
            link=public_url,
            location=location,
            date=_clean(info.get("postedOn") or summary.get("postedOn")) or None,
            description=description_text,
            metadata=metadata,
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
        "Persisted Deckers job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Deckers Workday listings.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum jobs to process.")
    parser.add_argument(
        "--page-size",
        type=int,
        default=20,
        help="Number of records requested per page (max 20, default: 20).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.1,
        help="Seconds to sleep between requests (default: 0.1).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print jobs without persisting.")
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

    scraper = DeckersJobScraper(page_size=args.page_size, delay=args.delay)
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

    logging.info(
        "Deckers scraper finished - fetched=%(fetched)s created=%(created)s",
        totals,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
