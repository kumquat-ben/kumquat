#!/usr/bin/env python3
"""Manual scraper for Constellation Brands' Workday-powered careers site."""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, asdict
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
CAREERS_URL = "https://www.cbrands.com/pages/careers"
WORKDAY_ROOT = "https://cbrands.wd5.myworkdayjobs.com"
TENANT = "cbrands"
PORTAL = "CBI_External_Careers"
CXS_BASE = f"{WORKDAY_ROOT}/wday/cxs/{TENANT}/{PORTAL}"
JOBS_ENDPOINT = f"{CXS_BASE}/jobs"
DETAIL_ENDPOINT_BASE = CXS_BASE
SESSION_SEED_URL = f"{WORKDAY_ROOT}/en-US/{PORTAL}"
MAX_PAGE_SIZE = 20

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

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 600), 30)
SCRAPER_QS = Scraper.objects.filter(company="Constellation Brands", url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Constellation Brands scrapers found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Constellation Brands",
        url=CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the scraper cannot proceed."""


@dataclass
class JobListing:
    title: str
    link: str
    location: Optional[str]
    date: Optional[str]
    description: str
    metadata: Dict[str, object]
    normalized_location: Optional[str] = None


def _strip_or_none(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    stripped = value.strip()
    return stripped or None


def _html_to_text(html: Optional[str]) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text("\n", strip=True)


class ConstellationBrandsWorkdayScraper:
    def __init__(
        self,
        *,
        page_size: int = MAX_PAGE_SIZE,
        delay: float = 0.2,
        session: Optional[requests.Session] = None,
    ) -> None:
        raw_page_size = max(1, int(page_size))
        if raw_page_size > MAX_PAGE_SIZE:
            logging.getLogger(self.__class__.__name__).debug(
                "Requested page_size=%s exceeds Workday limit; capping at %s.",
                raw_page_size,
                MAX_PAGE_SIZE,
            )
        self.page_size = min(raw_page_size, MAX_PAGE_SIZE)
        self.delay = max(0.0, float(delay))
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)
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
                self.logger.info("No postings returned at offset %s; stopping.", offset)
                break

            if total is None:
                total = self._safe_int(payload.get("total"))
                self.logger.info("Reported Workday total listings: %s", total)

            for raw in postings:
                listing = self._build_listing(raw)
                if listing is None:
                    continue
                yield listing
                fetched += 1
                if limit is not None and fetched >= limit:
                    self.logger.info("Reached limit=%s; stopping.", limit)
                    return

            offset += len(postings)
            if total is not None and offset >= total:
                self.logger.info("Reached reported total=%s; pagination complete.", total)
                break
            if self.delay:
                time.sleep(self.delay)

    def _ensure_session_bootstrap(self, force: bool = False) -> None:
        if self._bootstrapped and not force:
            return
        headers = dict(DEFAULT_HEADERS)
        headers["Accept"] = HTML_ACCEPT_HEADER
        response = self.session.get(SESSION_SEED_URL, headers=headers, timeout=40)
        response.raise_for_status()
        self._store_csrf_token(response.text)
        self._bootstrapped = True

    def _fetch_page(self, offset: int) -> Dict[str, object]:
        payload = {
            "limit": self.page_size,
            "offset": offset,
            "searchText": "",
            "appliedFacets": {},
            "userPreferredLanguage": "en-US",
        }
        response = self.session.post(JOBS_ENDPOINT, json=payload, timeout=40)
        if response.status_code == 400:
            self.logger.debug("Jobs endpoint returned 400; retrying with fresh bootstrap.")
            self._ensure_session_bootstrap(force=True)
            response = self.session.post(JOBS_ENDPOINT, json=payload, timeout=40)
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            snippet = response.text[:200].strip()
            raise ScraperError(f"Workday jobs request failed: {exc} :: {snippet}") from exc
        return response.json()

    def _fetch_detail(self, external_path: str) -> Dict[str, object]:
        path = external_path.lstrip("/")
        url = f"{DETAIL_ENDPOINT_BASE}/{path}"
        response = self.session.get(url, timeout=40)
        if response.status_code == 400:
            self.logger.debug("Detail endpoint returned 400; retrying with fresh bootstrap.")
            self._ensure_session_bootstrap(force=True)
            response = self.session.get(url, timeout=40)
        response.raise_for_status()
        return response.json()

    def _build_listing(self, summary: Dict[str, object]) -> Optional[JobListing]:
        title = _strip_or_none(summary.get("title"))
        external_path = _strip_or_none(summary.get("externalPath"))
        if not title or not external_path:
            self.logger.debug("Skipping summary without title/path: %s", summary)
            return None

        try:
            detail_payload = self._fetch_detail(external_path)
        except requests.RequestException as exc:
            self.logger.error("Failed to fetch detail for %s: %s", external_path, exc)
            return None

        info = detail_payload.get("jobPostingInfo") or {}
        description_html = info.get("jobDescription") or ""
        description_text = _html_to_text(description_html)

        external_url = _strip_or_none(info.get("externalUrl"))
        if not external_url:
            external_url = urljoin(f"{WORKDAY_ROOT}/{PORTAL}/", external_path.lstrip("/"))

        location_text = (
            _strip_or_none(info.get("location"))
            or _strip_or_none(summary.get("locationsText"))
        )

        location_descriptor = None
        location_payload = info.get("jobRequisitionLocation")
        if isinstance(location_payload, dict):
            location_descriptor = _strip_or_none(location_payload.get("descriptor"))

        metadata: Dict[str, object] = {
            "jobReqId": info.get("jobReqId"),
            "jobPostingId": info.get("jobPostingId"),
            "jobPostingSiteId": info.get("jobPostingSiteId"),
            "timeType": info.get("timeType"),
            "postedOnText": _strip_or_none(summary.get("postedOn")),
            "startDate": info.get("startDate"),
            "jobPostingEndDate": info.get("jobPostingEndDateAsText"),
            "externalPath": external_path,
            "locationsText": summary.get("locationsText"),
        }
        if location_payload:
            metadata["jobRequisitionLocation"] = location_payload
        if description_html:
            metadata["description_html"] = description_html

        return JobListing(
            title=title,
            link=external_url,
            location=location_text,
            date=_strip_or_none(info.get("startDate")) or _strip_or_none(info.get("postedOn")),
            description=description_text,
            metadata={k: v for k, v in metadata.items() if v},
            normalized_location=location_descriptor,
        )

    def _store_csrf_token(self, html: str) -> None:
        match = CSRF_TOKEN_PATTERN.search(html)
        if not match:
            self.logger.warning("Failed to locate CSRF token in Workday bootstrap page.")
            return
        token = match.group(1).strip()
        if not token:
            self.logger.warning("Workday bootstrap returned empty CSRF token.")
            return
        if self.session.headers.get(CSRF_HEADER_NAME) != token:
            self.logger.debug("Updated Workday CSRF token.")
        self.session.headers[CSRF_HEADER_NAME] = token

    @staticmethod
    def _safe_int(value: Optional[object]) -> Optional[int]:
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None


def persist_listing(listing: JobListing) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255] or None,
        "normalized_location": (listing.normalized_location or "")[:255] or None,
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
        "Persisted Constellation Brands job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Constellation Brands Workday listings.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum jobs to process.")
    parser.add_argument(
        "--page-size",
        type=int,
        default=MAX_PAGE_SIZE,
        help=f"Records per Workday page (max {MAX_PAGE_SIZE}).",
    )
    parser.add_argument(
        "--delay", type=float, default=0.2, help="Seconds to sleep between page requests."
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print jobs without persisting to the database."
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s: %(message)s")

    scraper = ConstellationBrandsWorkdayScraper(page_size=args.page_size, delay=args.delay)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    for listing in scraper.scrape(limit=args.limit):
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
        except Exception as exc:  # pragma: no cover - persistence failure path
            logging.error("Failed to persist %s: %s", listing.link, exc)
            totals["errors"] += 1

    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["dedupe"] = dedupe_summary
        logging.info("Deduplication summary: %s", dedupe_summary)

    logging.info(
        "Constellation Brands scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )
    return 0 if not totals["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
