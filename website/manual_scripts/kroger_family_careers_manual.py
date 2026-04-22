#!/usr/bin/env python3
"""Manual scraper for https://www.krogerfamilycareers.com/.

Fetches paginated job summaries from the Oracle HCM Candidate Experience API,
hydrates each posting with the detail payload, and stores the resulting
records through the Django ORM. Intended for one-off/manual runs via the
operations dashboard.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Set
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
from django.db import IntegrityError  # noqa: E402

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_SITE_URL = "https://www.krogerfamilycareers.com/en/sites/CX_2001"
JOB_DETAIL_PATH = "/job/{job_id}"
API_ROOT = "https://eluq.fa.us2.oraclecloud.com/hcmRestApi/resources/latest"
LIST_ENDPOINT = f"{API_ROOT}/recruitingCEJobRequisitions"
DETAIL_ENDPOINT = f"{API_ROOT}/recruitingCEJobRequisitionDetails"
SITE_NUMBER = "CX_2001"
FACETS = "LOCATIONS;WORK_LOCATIONS;WORKPLACE_TYPES;TITLES;CATEGORIES;ORGANIZATIONS;POSTING_DATES;FLEX_FIELDS"
DEFAULT_PAGE_SIZE = 100
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": BASE_SITE_URL,
    "Origin": "https://www.krogerfamilycareers.com",
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 30)
SCRAPER_QS = Scraper.objects.filter(company="Kroger", url=BASE_SITE_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched; using the earliest (id=%s).", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Kroger",
        url=BASE_SITE_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the scrape pipeline cannot proceed."""


@dataclass
class JobListing:
    job_id: str
    title: str
    link: str
    location: Optional[str]
    posted_date: Optional[str]
    description_text: str
    description_html: Optional[str]
    metadata: Dict[str, object]


def _html_to_text(value: Optional[str]) -> str:
    if not value:
        return ""
    soup = BeautifulSoup(value, "html.parser")
    text = soup.get_text("\n", strip=True)
    return text.strip()


def _combine_sections(sections: Iterable[Optional[str]]) -> str:
    cleaned: List[str] = []
    for section in sections:
        if section and section.strip():
            cleaned.append(section.strip())
    return "\n\n".join(cleaned)


class KrogerCareersClient:
    def __init__(self, page_size: int = DEFAULT_PAGE_SIZE, delay: float = 0.0, session: Optional[requests.Session] = None) -> None:
        self.page_size = max(1, min(int(page_size), 200))
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

    def _build_finder(self, offset: int) -> str:
        params = [
            f"siteNumber={SITE_NUMBER}",
            f"facetsList={FACETS}",
            f"offset={offset}",
            f"limit={self.page_size}",
        ]
        return f"findReqs;{','.join(params)}"

    def _fetch_list_page(self, offset: int) -> Dict[str, object]:
        finder = self._build_finder(offset)
        try:
            resp = self.session.get(
                LIST_ENDPOINT,
                params={
                    "onlyData": "true",
                    "expand": "requisitionList",
                    "finder": finder,
                },
                timeout=(15, 45),
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch listing page at offset {offset}: {exc}") from exc

        payload = resp.json()
        items = payload.get("items")
        if not items:
            return {}
        first = items[0] or {}
        return first

    def _fetch_detail(self, job_id: str) -> Dict[str, object]:
        try:
            resp = self.session.get(
                DETAIL_ENDPOINT,
                params={
                    "onlyData": "true",
                    "q": f"Id={job_id}",
                },
                timeout=(15, 45),
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch detail for job {job_id}: {exc}") from exc

        payload = resp.json()
        items = payload.get("items") or []
        if not items:
            logging.warning("Detail payload empty for job %s", job_id)
            return {}
        return items[0]

    def iter_listings(
        self,
        *,
        max_pages: Optional[int] = None,
        max_results: Optional[int] = None,
    ) -> Iterator[JobListing]:
        offset = 0
        page = 0
        seen_ids: Set[str] = set()
        total_jobs: Optional[int] = None
        yielded = 0

        while True:
            page_data = self._fetch_list_page(offset)
            summaries = page_data.get("requisitionList") or []
            if not summaries:
                break

            if total_jobs is None:
                total_jobs = int(page_data.get("TotalJobsCount") or len(summaries))

            for summary in summaries:
                job_id = str(summary.get("Id") or "").strip()
                if not job_id:
                    continue
                if job_id in seen_ids:
                    continue
                seen_ids.add(job_id)
                detail = self._fetch_detail(job_id)
                listing = self._build_listing(summary, detail)
                yield listing
                yielded += 1
                if max_results and yielded >= max_results:
                    return

            offset += len(summaries)
            page += 1
            if total_jobs is not None and offset >= total_jobs:
                break
            if max_pages and page >= max_pages:
                break
            if self.delay:
                time.sleep(self.delay)

    def _build_listing(self, summary: Dict[str, object], detail: Dict[str, object]) -> JobListing:
        job_id = str(summary.get("Id") or detail.get("Id") or "").strip()
        title = str(summary.get("Title") or detail.get("Title") or "").strip()
        location = str(detail.get("PrimaryLocation") or summary.get("PrimaryLocation") or "").strip() or None
        posted_date = str(summary.get("PostedDate") or detail.get("ExternalPostedStartDate") or "").strip() or None

        sections = [
            detail.get("ExternalDescriptionStr"),
            detail.get("ExternalResponsibilitiesStr"),
            detail.get("ExternalQualificationsStr"),
            detail.get("CorporateDescriptionStr"),
        ]
        combined_html = _combine_sections(sections) or None
        description_text = _html_to_text(combined_html) if combined_html else ""

        metadata: Dict[str, object] = {
            "job_id": job_id or None,
            "requisition_id": detail.get("RequisitionId"),
            "category": detail.get("Category"),
            "department": detail.get("Department"),
            "organization": detail.get("Organization"),
            "business_unit": detail.get("BusinessUnit"),
            "job_schedule": detail.get("JobSchedule") or summary.get("JobSchedule"),
            "job_shift": detail.get("JobShift") or summary.get("JobShift"),
            "workplace_type": detail.get("WorkplaceType") or summary.get("WorkplaceType"),
            "worker_type": detail.get("WorkerType") or summary.get("WorkerType"),
            "job_type": detail.get("JobType") or summary.get("JobType"),
            "study_level": detail.get("StudyLevel") or summary.get("StudyLevel"),
            "geography_id": detail.get("GeographyId") or summary.get("GeographyId"),
            "hot_job": detail.get("HotJobFlag") if detail.get("HotJobFlag") is not None else summary.get("HotJobFlag"),
            "be_first_to_apply": summary.get("BeFirstToApplyFlag") if summary.get("BeFirstToApplyFlag") is not None else detail.get("BeFirstToApplyFlag"),
            "raw_sections": {
                "external_description": detail.get("ExternalDescriptionStr"),
                "external_responsibilities": detail.get("ExternalResponsibilitiesStr"),
                "external_qualifications": detail.get("ExternalQualificationsStr"),
                "corporate_description": detail.get("CorporateDescriptionStr"),
            },
        }

        link = urljoin(BASE_SITE_URL, JOB_DETAIL_PATH.format(job_id=job_id))

        return JobListing(
            job_id=job_id,
            title=title,
            link=link,
            location=location,
            posted_date=posted_date,
            description_text=description_text,
            description_html=combined_html,
            metadata=metadata,
        )


# ---------------------------------------------------------------------------
# Persistence layer
# ---------------------------------------------------------------------------
def store_listing(listing: JobListing) -> None:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255],
        "date": (listing.posted_date or "")[:100],
        "description": listing.description_text[:10000],
        "metadata": {
            **listing.metadata,
            "description_html": listing.description_html,
        },
    }
    try:
        JobPosting.objects.update_or_create(
            scraper=SCRAPER,
            link=listing.link,
            defaults=defaults,
        )
    except IntegrityError as exc:
        raise ScraperError(f"Failed to store job {listing.job_id}: {exc}") from exc


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kroger Family Careers manual scraper")
    parser.add_argument("--max-pages", type=int, default=None, help="Maximum number of listing pages to fetch")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of job records to process")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE, help="Page size for API pagination (default 100)")
    parser.add_argument("--delay", type=float, default=0.0, help="Delay in seconds between page fetches")
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity",
    )
    return parser.parse_args(argv)


def run_scrape(args: argparse.Namespace) -> Dict[str, object]:
    client = KrogerCareersClient(page_size=args.page_size, delay=args.delay)
    count = 0
    for listing in client.iter_listings(max_pages=args.max_pages, max_results=args.limit):
        store_listing(listing)
        count += 1
    dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
    return {
        "processed_jobs": count,
        "deduplicated": dedupe_summary,
    }


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    start = time.time()
    try:
        outcome = run_scrape(args)
    except ScraperError as exc:
        logging.error("Scrape failed: %s", exc)
        return 1
    duration = time.time() - start
    summary = {
        "company": "Kroger",
        "site": BASE_SITE_URL,
        "elapsed_seconds": round(duration, 2),
        **outcome,
    }
    logging.info("Summary: %s", json.dumps(summary))
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
