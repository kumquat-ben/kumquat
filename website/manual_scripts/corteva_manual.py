#!/usr/bin/env python3
"""Manual scraper for Corteva careers powered by Eightfold.ai."""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional
from urllib.parse import urljoin

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
# Constants
# ---------------------------------------------------------------------------
ROOT_LANDING_URL = "https://careers.corteva.com"
BASE_URL = "https://corteva.eightfold.ai"
CAREERS_URL = f"{BASE_URL}/careers"
SEARCH_ENDPOINT = f"{BASE_URL}/api/pcsx/search"
DETAIL_ENDPOINT = f"{BASE_URL}/api/pcsx/position_details"
DEFAULT_DOMAIN = "corteva.com"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": CAREERS_URL,
    "Origin": BASE_URL,
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 600), 60)

SCRAPER_QS = Scraper.objects.filter(company="Corteva", url=ROOT_LANDING_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.getLogger(__name__).warning(
            "Multiple Corteva scrapers found; using id=%s.", SCRAPER.id
        )
else:  # pragma: no cover - creation path
    SCRAPER = Scraper.objects.create(
        company="Corteva",
        url=ROOT_LANDING_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the Corteva scraper cannot proceed."""


@dataclass
class JobSummary:
    position_id: int
    title: str
    detail_url: str
    display_job_id: Optional[str]
    locations: List[str]
    standardized_locations: List[str]
    posted_ts: Optional[int]
    posted_date: Optional[str]
    department: Optional[str]
    work_location_option: Optional[str]
    ats_job_id: Optional[str]
    is_hot: Optional[int]
    location_flexibility: Optional[str]


@dataclass
class JobListing(JobSummary):
    description_text: Optional[str]
    description_html: Optional[str]
    apply_url: Optional[str]
    metadata: Dict[str, object]


def ts_to_datestring(ts: Optional[int]) -> Optional[str]:
    if ts in (None, 0):
        return None
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d")
    except (ValueError, OSError, OverflowError, TypeError):
        return None


def html_to_text(html: Optional[str]) -> Optional[str]:
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = soup.get_text("\n", strip=True)
    return text or None


def locations_to_string(locations: Iterable[str]) -> Optional[str]:
    values = [str(loc).strip() for loc in locations if str(loc).strip()]
    if not values:
        return None
    return "; ".join(dict.fromkeys(values))[:255]


class CortevaCareersClient:
    def __init__(
        self,
        *,
        domain: str = DEFAULT_DOMAIN,
        delay: float = 0.2,
        session: Optional[requests.Session] = None,
        timeout: int = 45,
    ) -> None:
        self.domain = domain
        self.delay = max(0.0, delay)
        self.timeout = max(timeout, 10)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    def iter_positions(self, *, start: int = 0, limit: Optional[int] = None) -> Iterator[JobSummary]:
        fetched = 0
        cursor = max(start, 0)
        total_count: Optional[int] = None

        while True:
            payload = self._fetch_positions_page(cursor)
            positions = payload.get("positions") or []
            if total_count is None:
                total_count = payload.get("count") or 0
                self.logger.info("Corteva API reports %s open jobs.", total_count)

            if not positions:
                self.logger.debug("No positions returned at start=%s; stopping.", cursor)
                break

            for raw in positions:
                summary = self._build_summary(raw)
                yield summary
                fetched += 1
                if limit is not None and fetched >= limit:
                    self.logger.debug("Reached limit=%s; stopping iteration.", limit)
                    return

            cursor += len(positions)
            if total_count is not None and cursor >= total_count:
                self.logger.debug("Reached total_count=%s; stopping.", total_count)
                break

            if self.delay:
                time.sleep(self.delay)

    def fetch_position_detail(self, position_id: int) -> Dict[str, object]:
        params = {"domain": self.domain, "position_id": position_id}
        self.logger.debug("Fetching detail for position_id=%s", position_id)
        response = self.session.get(DETAIL_ENDPOINT, params=params, timeout=self.timeout)
        response.raise_for_status()
        data = response.json()
        if data.get("status") != 200:
            raise ScraperError(f"Detail API returned status {data.get('status')}: {data.get('error')}")
        detail = data.get("data") or {}
        if not detail:
            raise ScraperError(f"Missing detail payload for position_id={position_id}")
        return detail

    def _fetch_positions_page(self, start: int) -> Dict[str, object]:
        params = {"domain": self.domain, "start": start}
        self.logger.debug("Fetching positions page start=%s", start)
        response = self.session.get(SEARCH_ENDPOINT, params=params, timeout=self.timeout)
        response.raise_for_status()
        data = response.json()
        if data.get("status") != 200:
            raise ScraperError(f"Search API returned status {data.get('status')}: {data.get('error')}")
        return data.get("data") or {}

    def _build_summary(self, raw: Dict[str, object]) -> JobSummary:
        position_id = int(raw["id"])
        detail_path = raw.get("positionUrl") or ""
        detail_url = urljoin(BASE_URL, detail_path)
        return JobSummary(
            position_id=position_id,
            title=str(raw.get("name") or "").strip(),
            detail_url=detail_url,
            display_job_id=raw.get("displayJobId"),
            locations=list(raw.get("locations") or []),
            standardized_locations=list(raw.get("standardizedLocations") or []),
            posted_ts=raw.get("postedTs"),
            posted_date=ts_to_datestring(raw.get("postedTs")),
            department=raw.get("department"),
            work_location_option=raw.get("workLocationOption"),
            ats_job_id=raw.get("atsJobId"),
            is_hot=raw.get("isHot"),
            location_flexibility=raw.get("locationFlexibility"),
        )


class CortevaJobScraper:
    def __init__(self, *, client: Optional[CortevaCareersClient] = None) -> None:
        self.client = client or CortevaCareersClient()
        self.logger = logging.getLogger(self.__class__.__name__)

    def scrape(self, *, limit: Optional[int] = None, start_offset: int = 0) -> Iterator[JobListing]:
        for summary in self.client.iter_positions(start=start_offset, limit=limit):
            try:
                detail = self.client.fetch_position_detail(summary.position_id)
            except requests.HTTPError as exc:
                raise ScraperError(f"HTTP error fetching job detail {summary.position_id}: {exc}") from exc
            listing = self._build_listing(summary, detail)
            yield listing

    def _build_listing(self, summary: JobSummary, detail: Dict[str, object]) -> JobListing:
        description_html = detail.get("jobDescription")
        description_text = html_to_text(description_html)
        public_url = detail.get("publicUrl")
        detail_url = public_url or summary.detail_url

        metadata: Dict[str, object] = {
            "display_job_id": summary.display_job_id,
            "department": summary.department,
            "work_location_option": summary.work_location_option,
            "ats_job_id": summary.ats_job_id,
            "posted_ts": summary.posted_ts,
            "creation_ts": detail.get("creationTs"),
            "is_hot": summary.is_hot,
            "location_flexibility": summary.location_flexibility,
            "standardized_locations": summary.standardized_locations,
            "position_extra_details": detail.get("positionExtraDetails"),
            "position_user_actions": detail.get("positionUserActions"),
        }

        return JobListing(
            position_id=summary.position_id,
            title=summary.title,
            detail_url=detail_url,
            display_job_id=summary.display_job_id,
            locations=summary.locations,
            standardized_locations=summary.standardized_locations,
            posted_ts=summary.posted_ts,
            posted_date=summary.posted_date,
            department=summary.department,
            work_location_option=summary.work_location_option,
            ats_job_id=summary.ats_job_id,
            is_hot=summary.is_hot,
            location_flexibility=summary.location_flexibility,
            description_text=description_text,
            description_html=description_html,
            apply_url=public_url,
            metadata=metadata,
        )


def store_listing(listing: JobListing) -> bool:
    metadata = dict(listing.metadata or {})
    metadata.setdefault("position_id", listing.position_id)
    if listing.description_html:
        metadata.setdefault("description_html", listing.description_html)
    if listing.apply_url:
        metadata.setdefault("apply_url", listing.apply_url)

    defaults = {
        "title": listing.title[:255],
        "location": locations_to_string(listing.locations),
        "date": (listing.posted_date or "")[:100] or None,
        "description": (listing.description_text or "")[:10000],
        "metadata": metadata,
    }

    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults=defaults,
    )
    logging.getLogger("store_listing").debug(
        "Stored Corteva job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Corteva careers manual scraper.")
    parser.add_argument("--limit", type=int, help="Maximum number of jobs to fetch.")
    parser.add_argument(
        "--start",
        type=int,
        default=0,
        help="Start offset within the Eightfold search results.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.2,
        help="Delay (seconds) between page fetches.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print job payloads as JSON instead of writing to the database.",
    )
    return parser.parse_args(argv)


def run_scrape(limit: Optional[int], start: int, delay: float, dry_run: bool) -> Dict[str, object]:
    client = CortevaCareersClient(delay=delay)
    scraper = CortevaJobScraper(client=client)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    try:
        for listing in scraper.scrape(limit=limit, start_offset=start):
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
            except Exception as exc:  # pragma: no cover - persistence guardrail
                logging.error("Failed to store Corteva job %s: %s", listing.detail_url, exc)
                totals["errors"] += 1
    except requests.HTTPError as exc:
        logging.error("HTTP error while fetching Corteva jobs: %s", exc)
        totals["errors"] += 1
    except requests.RequestException as exc:
        logging.error("Network error while fetching Corteva jobs: %s", exc)
        totals["errors"] += 1
    except ScraperError as exc:
        logging.error("Corteva scraper stopped: %s", exc)
        totals["errors"] += 1

    if not dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["dedupe"] = dedupe_summary

    return totals


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()), format="%(levelname)s: %(message)s")

    totals = run_scrape(args.limit, args.start, args.delay, args.dry_run)
    logging.info(
        "Corteva scraper finished - fetched=%(fetched)s created=%(created)s updated=%(updated)s errors=%(errors)s",
        totals,
    )

    if not args.dry_run and "dedupe" in totals:
        logging.info("Deduplication summary: %s", totals["dedupe"])

    return 0 if totals["errors"] == 0 else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
