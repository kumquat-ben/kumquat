#!/usr/bin/env python3
"""Manual scraper for https://careers.bms.com/jobs."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Django bootstrap
# ---------------------------------------------------------------------------
CURRENT_FILE = Path(__file__).resolve()
parents = list(CURRENT_FILE.parents)
default_backend_dir = parents[2] if len(parents) > 2 else parents[-1]
BACKEND_DIR = next(
    (candidate for candidate in parents if (candidate / "manage.py").exists()),
    default_backend_dir,
)
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
BASE_URL = "https://careers.bms.com"
JOBS_DOMAIN = "https://jobs.bms.com"
SEARCH_ENDPOINT = f"{JOBS_DOMAIN}/api/pcsx/search"
DETAIL_ENDPOINT = f"{JOBS_DOMAIN}/api/pcsx/position_details"
GROUP_ID = "bms.com"
REFERER_URL = f"{BASE_URL}/jobs"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": REFERER_URL,
    "Connection": "keep-alive",
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 900), 60)
DEFAULT_REQUEST_TIMEOUT = max(getattr(settings, "MANUAL_SCRIPT_REQUEST_TIMEOUT", 30), 10)

SCRAPER_QS = Scraper.objects.filter(
    company="Bristol Myers Squibb",
    url=f"{BASE_URL}/jobs",
).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Bristol Myers Squibb scrapers found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Bristol Myers Squibb",
        url=f"{BASE_URL}/jobs",
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the scraper encounters an unrecoverable issue."""


@dataclass
class JobData:
    position_id: int
    title: str
    display_job_id: Optional[str]
    link: str
    location: Optional[str]
    standardized_locations: List[str]
    posted_date: Optional[str]
    description_text: Optional[str]
    description_html: Optional[str]
    metadata: Dict[str, Any]


def _timestamp_to_date(ts: Optional[int]) -> Optional[str]:
    if ts in (None, "", 0):
        return None
    try:
        dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None
    return dt.strftime("%Y-%m-%d")


def _html_to_text(fragment: Optional[str]) -> Optional[str]:
    if not fragment:
        return None
    soup = BeautifulSoup(fragment, "html.parser")
    text = soup.get_text("\n", strip=True)
    return text or None


def _first_nonempty(values: Optional[List[str]]) -> Optional[str]:
    if not values:
        return None
    for value in values:
        if value:
            return value.strip()
    return None


class BMSJobScraper:
    def __init__(self, *, delay: float = 0.25, session: Optional[requests.Session] = None) -> None:
        self.delay = max(delay, 0.0)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.request_timeout = DEFAULT_REQUEST_TIMEOUT

    def scrape(
        self,
        *,
        query: str = "",
        location: str = "",
        sort_by: Optional[str] = None,
        start_offset: int = 0,
        limit: Optional[int] = None,
    ) -> Iterator[JobData]:
        processed = 0
        cursor = max(start_offset, 0)
        total_count: Optional[int] = None

        while True:
            page_positions, page_meta = self._fetch_page(
                start=cursor,
                query=query,
                location=location,
                sort_by=sort_by,
            )
            if total_count is None:
                total_count = page_meta.get("count") or 0
                self.logger.info("Total positions reported: %s", total_count)

            if not page_positions:
                self.logger.info("No positions returned at offset %s; stopping.", cursor)
                break

            for entry in page_positions:
                try:
                    detail = self._fetch_detail(entry["id"])
                except ScraperError as exc:
                    self.logger.error("Skipping position %s: %s", entry.get("id"), exc)
                    continue

                job = self._build_job(entry, detail, page_meta)
                yield job
                processed += 1

                if limit is not None and processed >= limit:
                    self.logger.info("Limit reached (%s); stopping scrape.", limit)
                    return

                if self.delay:
                    time.sleep(self.delay)

            cursor += len(page_positions)
            if total_count is not None and cursor >= total_count:
                self.logger.info("Reached end of result set (cursor=%s).", cursor)
                break

    def _fetch_page(
        self,
        *,
        start: int,
        query: str,
        location: str,
        sort_by: Optional[str],
    ) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
        params = {
            "domain": GROUP_ID,
            "query": query or "",
            "location": location or "",
            "start": max(start, 0),
        }
        if sort_by:
            params["sort_by"] = sort_by

        try:
            response = self.session.get(SEARCH_ENDPOINT, params=params, timeout=self.request_timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch search page at offset {start}: {exc}") from exc

        try:
            payload = response.json()
        except ValueError as exc:
            raise ScraperError(f"Invalid JSON in search response at offset {start}: {exc}") from exc

        data = payload.get("data") or {}
        positions = data.get("positions") or []
        return positions, data

    def _fetch_detail(self, position_id: int) -> Dict[str, Any]:
        params = {
            "domain": GROUP_ID,
            "position_id": str(position_id),
            "hl": "en",
        }
        try:
            response = self.session.get(DETAIL_ENDPOINT, params=params, timeout=self.request_timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch position details for {position_id}: {exc}") from exc

        try:
            payload = response.json()
        except ValueError as exc:
            raise ScraperError(f"Invalid JSON for position {position_id}: {exc}") from exc

        data = payload.get("data")
        if not isinstance(data, dict):
            raise ScraperError(f"Unexpected detail payload for position {position_id}")
        return data

    def _build_job(
        self,
        search_record: Dict[str, Any],
        detail_record: Dict[str, Any],
        page_meta: Dict[str, Any],
    ) -> JobData:
        title = (detail_record.get("name") or search_record.get("name") or "").strip()
        link = detail_record.get("publicUrl") or self._build_link(search_record.get("positionUrl"))
        standardized_locations = detail_record.get("standardizedLocations") or search_record.get("standardizedLocations") or []
        location = _first_nonempty(detail_record.get("locations") or search_record.get("locations"))
        posted_ts = detail_record.get("postedTs") or search_record.get("postedTs")
        posted_date = _timestamp_to_date(posted_ts)
        description_html = detail_record.get("jobDescription")
        description_text = _html_to_text(description_html)

        detail_meta = dict(detail_record)
        detail_meta.pop("jobDescription", None)

        metadata: Dict[str, Any] = {
            "position_id": search_record.get("id"),
            "display_job_id": detail_record.get("displayJobId") or search_record.get("displayJobId"),
            "ats_job_id": detail_record.get("atsJobId") or search_record.get("atsJobId"),
            "department": detail_record.get("department") or search_record.get("department"),
            "work_location_option": detail_record.get("workLocationOption") or search_record.get("workLocationOption"),
            "location_flexibility": detail_record.get("locationFlexibility") or search_record.get("locationFlexibility"),
            "search_record": search_record,
            "detail_record": detail_meta,
            "search_context": {
                "sortBy": page_meta.get("sortBy"),
                "appliedFilters": page_meta.get("appliedFilters"),
                "resultsMetaData": page_meta.get("resultsMetaData"),
            },
        }

        return JobData(
            position_id=int(search_record["id"]),
            title=title,
            display_job_id=metadata["display_job_id"],
            link=link,
            location=location,
            standardized_locations=standardized_locations,
            posted_date=posted_date,
            description_text=description_text,
            description_html=description_html,
            metadata=metadata,
        )

    @staticmethod
    def _build_link(path: Optional[str]) -> str:
        if not path:
            return f"{BASE_URL}/jobs"
        if path.startswith("http"):
            return path
        if path.startswith("/"):
            return f"{JOBS_DOMAIN}{path}"
        return f"{JOBS_DOMAIN}/{path}"


def store_job(job: JobData) -> None:
    metadata = dict(job.metadata or {})
    if job.description_html:
        metadata.setdefault("description_html", job.description_html)
    if job.standardized_locations:
        metadata.setdefault("standardized_locations", job.standardized_locations)

    JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=job.link,
        defaults={
            "title": (job.title or "")[:255],
            "location": (job.location or "")[:255],
            "date": (job.posted_date or "")[:100],
            "description": (job.description_text or "")[:10000],
            "metadata": metadata or None,
        },
    )


def run_scrape(
    *,
    query: str,
    location: str,
    sort_by: Optional[str],
    start_offset: int,
    limit: Optional[int],
    delay: float,
) -> Dict[str, Any]:
    scraper = BMSJobScraper(delay=delay)
    count = 0
    for job in scraper.scrape(
        query=query,
        location=location,
        sort_by=sort_by,
        start_offset=start_offset,
        limit=limit,
    ):
        store_job(job)
        count += 1

    dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
    return {
        "company": "Bristol Myers Squibb",
        "url": f"{BASE_URL}/jobs",
        "count": count,
        "dedupe": dedupe_summary,
        "query": query or None,
        "location": location or None,
        "sort_by": sort_by or None,
        "start_offset": start_offset,
    }


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bristol Myers Squibb careers manual scraper")
    parser.add_argument("--query", type=str, default="", help="Optional keyword query to filter positions")
    parser.add_argument("--location", type=str, default="", help="Optional location string for search results")
    parser.add_argument("--sort-by", type=str, default=None, help="Sort order parameter accepted by the site (e.g. posted_date_desc)")
    parser.add_argument("--start-offset", type=int, default=0, help="Search offset to begin fetching results from")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of job postings to process")
    parser.add_argument("--delay", type=float, default=0.25, help="Delay (seconds) between detail requests")
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    start_time = time.time()

    try:
        summary = run_scrape(
            query=args.query,
            location=args.location,
            sort_by=args.sort_by,
            start_offset=args.start_offset,
            limit=args.limit,
            delay=args.delay,
        )
    except ScraperError as exc:
        logging.error("Scrape failed: %s", exc)
        return 1
    except Exception as exc:  # pragma: no cover - defensive path
        logging.exception("Unexpected error during scrape: %s", exc)
        return 1

    duration = time.time() - start_time
    summary["elapsed_seconds"] = duration
    logging.info("Summary: %s", json.dumps(summary))
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
