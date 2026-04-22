#!/usr/bin/env python3
"""Manual scraper for FactSet careers (Workday-powered site)."""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# --------------------------------------------------------------------------------------
# Django bootstrap so the script can run standalone (manual scripts dashboard, etc.)
# --------------------------------------------------------------------------------------
BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "website.settings")

import django  # noqa: E402

django.setup()

from django.conf import settings  # noqa: E402

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

# --------------------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------------------
CAREERS_LANDING_URL = "https://www.factset.com/careers"
WORKDAY_HOST = "https://factset.wd108.myworkdayjobs.com"
WORKDAY_SITE = "FactSetCareers"
LIST_ENDPOINT = f"{WORKDAY_HOST}/wday/cxs/factset/{WORKDAY_SITE}/jobs"
PUBLIC_JOB_URL_BASE = f"{WORKDAY_HOST}/{WORKDAY_SITE}/"
REQUEST_TIMEOUT = (10, 30)
WORKDAY_MAX_PAGE_SIZE = 20
DEFAULT_PAGE_SIZE = WORKDAY_MAX_PAGE_SIZE
DEFAULT_DELAY = 0.25
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Referer": CAREERS_LANDING_URL,
}
DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 3600), 120)

SCRAPER_QS = Scraper.objects.filter(company="FactSet", url=CAREERS_LANDING_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple FactSet scraper rows found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="FactSet",
        url=CAREERS_LANDING_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(RuntimeError):
    """Raised when the scraper cannot continue."""


@dataclass
class JobSummary:
    title: str
    external_path: str
    detail_url: str
    posted_on: Optional[str]
    location: Optional[str]
    time_type: Optional[str]
    job_req_id: Optional[str]
    bullet_fields: List[str]


@dataclass
class JobListing(JobSummary):
    description_text: str
    description_html: Optional[str]
    metadata: Dict[str, Any]


# --------------------------------------------------------------------------------------
# Utility helpers
# --------------------------------------------------------------------------------------
def _to_public_url(external_path: str) -> str:
    return urljoin(PUBLIC_JOB_URL_BASE, external_path.lstrip("/"))


def _html_to_text(value: Optional[str]) -> str:
    if not value:
        return ""
    soup = BeautifulSoup(value, "html.parser")
    text = soup.get_text("\n", strip=True)
    lines = [line.strip() for line in text.splitlines()]
    return "\n".join(line for line in lines if line)


def _build_summary(raw: dict) -> JobSummary:
    external_path = raw.get("externalPath") or ""
    detail_url = _to_public_url(external_path)
    bullet_fields_iter: Iterable[str] = raw.get("bulletFields") or []
    bullet_fields = list(bullet_fields_iter)
    return JobSummary(
        title=raw.get("title") or "",
        external_path=external_path,
        detail_url=detail_url,
        posted_on=raw.get("postedOn"),
        location=raw.get("locationsText"),
        time_type=raw.get("timeType"),
        job_req_id=bullet_fields[0] if bullet_fields else None,
        bullet_fields=bullet_fields,
    )


def _build_listing(summary: JobSummary, detail: Dict[str, Any]) -> JobListing:
    info = detail.get("jobPostingInfo") or {}
    description_html = info.get("jobDescription")
    metadata: Dict[str, Any] = {
        "postedOn": info.get("postedOn"),
        "startDate": info.get("startDate"),
        "timeType": info.get("timeType"),
        "jobReqId": info.get("jobReqId"),
        "jobPostingId": info.get("jobPostingId"),
        "jobPostingSiteId": info.get("jobPostingSiteId"),
        "jobRequisitionLocation": (info.get("jobRequisitionLocation") or {}).get("descriptor"),
        "country": (info.get("country") or {}).get("descriptor"),
        "externalUrl": info.get("externalUrl"),
        "canApply": info.get("canApply"),
        "bulletFields": summary.bullet_fields,
        "hiringOrganization": detail.get("hiringOrganization"),
        "similarJobsCount": len(detail.get("similarJobs") or []),
        "workdayExternalPath": summary.external_path,
    }
    return JobListing(
        **summary.__dict__,
        description_text=_html_to_text(description_html),
        description_html=description_html,
        metadata=metadata,
    )


# --------------------------------------------------------------------------------------
# Scraper implementation
# --------------------------------------------------------------------------------------
class FactSetWorkdayScraper:
    def __init__(
        self,
        *,
        page_size: int = DEFAULT_PAGE_SIZE,
        delay: float = DEFAULT_DELAY,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.page_size = max(1, min(page_size, WORKDAY_MAX_PAGE_SIZE))
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    def scrape(
        self,
        *,
        limit: Optional[int] = None,
        start_offset: int = 0,
    ) -> Generator[JobListing, None, None]:
        received = 0
        offset = max(0, start_offset)

        while True:
            page = self._fetch_page(offset)
            postings = page.get("jobPostings") or []
            if not postings:
                self.logger.info("No postings returned at offset %s; stopping.", offset)
                return

            for raw in postings:
                summary = _build_summary(raw)
                detail = self._fetch_detail(summary.external_path)
                yield _build_listing(summary, detail)
                received += 1
                if limit is not None and received >= limit:
                    self.logger.info("Reached limit %s; stopping.", limit)
                    return
                if self.delay:
                    time.sleep(self.delay)

            offset += len(postings)
            total = page.get("total")
            if total is not None and offset >= total:
                self.logger.info("Collected %s postings (total reached); stopping.", offset)
                return
            if not postings:
                return

            if self.delay:
                time.sleep(self.delay)

    def _fetch_page(self, offset: int) -> Dict[str, Any]:
        payload = {
            "appliedFacets": {},
            "limit": self.page_size,
            "offset": offset,
            "searchText": "",
        }
        response = self.session.post(LIST_ENDPOINT, json=payload, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            raise ScraperError(f"Failed to fetch job list (status={response.status_code})")
        return response.json()

    def _fetch_detail(self, external_path: str) -> Dict[str, Any]:
        if not external_path:
            raise ScraperError("Missing external path for job detail request")
        detail_url = f"{WORKDAY_HOST}/wday/cxs/factset/{WORKDAY_SITE}{external_path}"
        response = self.session.get(detail_url, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            raise ScraperError(
                f"Failed to fetch job detail for {external_path} (status={response.status_code})"
            )
        return response.json()


# --------------------------------------------------------------------------------------
# Persistence helpers
# --------------------------------------------------------------------------------------
def store_listing(listing: JobListing) -> None:
    JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults={
            "title": listing.title,
            "location": listing.location or "",
            "date": listing.posted_on or "",
            "description": listing.description_text,
            "metadata": listing.metadata,
        },
    )


# --------------------------------------------------------------------------------------
# Entrypoint
# --------------------------------------------------------------------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="FactSet careers manual scraper")
    parser.add_argument("--limit", type=int, default=None, help="Maximum jobs to import")
    parser.add_argument("--start-offset", type=int, default=0, help="Offset to start scraping from")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Delay between requests")
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
    )
    return parser.parse_args(argv)


def run(args: argparse.Namespace) -> Dict[str, Any]:
    scraper = FactSetWorkdayScraper(page_size=args.page_size, delay=args.delay)
    imported = 0
    for listing in scraper.scrape(limit=args.limit, start_offset=args.start_offset):
        store_listing(listing)
        imported += 1
    dedupe = deduplicate_job_postings(scraper=SCRAPER)
    return {"imported": imported, "dedupe": dedupe}


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    start = time.time()
    try:
        result = run(args)
    except ScraperError as exc:
        logging.exception("Scraper failure: %s", exc)
        return 1
    duration = time.time() - start
    summary = {
        "company": "FactSet",
        "careers_url": CAREERS_LANDING_URL,
        "imported": result["imported"],
        "elapsed_seconds": duration,
        "dedupe": result["dedupe"],
    }
    logging.info("Summary: %s", json.dumps(summary))
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
