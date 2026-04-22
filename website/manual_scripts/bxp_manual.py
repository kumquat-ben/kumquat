#!/usr/bin/env python3
"""Manual scraper for the BXP careers site hosted on Oracle Cloud."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Set
from urllib.parse import urljoin

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
SITE_NUMBER = "CX_5001"
BASE_UI_URL = "https://edxn.fa.us2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_5001"
BASE_API_URL = "https://edxn.fa.us2.oraclecloud.com/hcmRestApi/resources/latest"
SEARCH_ENDPOINT = f"{BASE_API_URL}/recruitingCEJobRequisitions"
DETAIL_ENDPOINT = f"{BASE_API_URL}/recruitingCEJobRequisitionDetails"
PAGE_SIZE = 25
REQUEST_TIMEOUT = (10, 40)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Referer": BASE_UI_URL,
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 900), 60)

SCRAPER_QS = Scraper.objects.filter(
    company="BXP",
    url=BASE_UI_URL,
).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple BXP scrapers found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="BXP",
        url=BASE_UI_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(RuntimeError):
    """Raised when the scraper encounters an unrecoverable error."""


@dataclass
class JobSummary:
    job_id: str
    title: str
    url: str
    posted_date: Optional[str]
    location: Optional[str]
    short_description: Optional[str]
    workplace_type: Optional[str]
    metadata: Dict[str, object]


@dataclass
class JobDetail:
    description_html: str
    description_text: str
    metadata: Dict[str, object]


@dataclass
class JobRecord:
    job_id: str
    title: str
    url: str
    posted_date: Optional[str]
    location: Optional[str]
    description_html: str
    description_text: str
    metadata: Dict[str, object]


class CandidateExperienceClient:
    """Light-weight client for Oracle Cloud Candidate Experience endpoints."""

    def __init__(
        self,
        *,
        site_number: str = SITE_NUMBER,
        session: Optional[requests.Session] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.site_number = site_number
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    def iter_job_summaries(self, *, limit: Optional[int] = None) -> Iterator[JobSummary]:
        seen_ids: Set[str] = set()
        offset = 0

        while True:
            jobs, total_count = self._fetch_jobs_page(offset=offset)
            if not jobs:
                self.logger.debug("No jobs returned at offset=%s; stopping pagination.", offset)
                break

            new_jobs = 0
            for job in jobs:
                if job.job_id in seen_ids:
                    continue
                seen_ids.add(job.job_id)
                new_jobs += 1
                yield job
                if limit is not None and len(seen_ids) >= limit:
                    self.logger.debug("Reached limit=%s; stopping job iteration.", limit)
                    return

            if new_jobs == 0:
                self.logger.debug(
                    "No unseen jobs after offset=%s (duplicates encountered); stopping.",
                    offset,
                )
                break

            offset += PAGE_SIZE
            if offset >= total_count:
                self.logger.debug(
                    "Pagination complete (offset=%s >= total_count=%s).", offset, total_count
                )
                break

    def fetch_job_detail(self, job_id: str) -> JobDetail:
        params = {
            "finder": f'ById;Id="{job_id}",siteNumber={self.site_number}',
            "expand": "all",
            "onlyData": "true",
        }
        data = self._get(DETAIL_ENDPOINT, params=params)
        items = data.get("items") or []
        if not items:
            raise ScraperError(f"No detail payload received for job {job_id!r}.")
        detail = items[0]

        description_html = (detail.get("ExternalDescriptionStr") or "").strip()
        description_text = ""
        if description_html:
            soup = BeautifulSoup(description_html, "html.parser")
            description_text = soup.get_text("\n", strip=True)

        metadata: Dict[str, object] = {
            "requisition_id": detail.get("RequisitionId"),
            "job_schedule": detail.get("JobSchedule"),
            "job_family_id": detail.get("JobFamilyId"),
            "job_function": detail.get("JobFunction"),
            "workplace_type": detail.get("WorkplaceType"),
            "workplace_type_code": detail.get("WorkplaceTypeCode"),
            "posting_start": detail.get("ExternalPostedStartDate"),
            "posting_end": detail.get("ExternalPostedEndDate"),
            "primary_location": detail.get("PrimaryLocation"),
            "primary_location_country": detail.get("PrimaryLocationCountry"),
            "secondary_locations": detail.get("secondaryLocations") or [],
            "work_locations": detail.get("workLocation") or [],
            "other_work_locations": detail.get("otherWorkLocations") or [],
            "primary_location_coordinates": detail.get("primaryLocationCoordinates") or [],
            "skills": detail.get("skills") or [],
        }

        return JobDetail(
            description_html=description_html,
            description_text=description_text,
            metadata=metadata,
        )

    def _fetch_jobs_page(self, *, offset: int) -> tuple[List[JobSummary], int]:
        params = {
            "finder": f"findReqs;siteNumber={self.site_number}",
            "onlyData": "true",
            "expand": (
                "requisitionList.workLocation,"
                "requisitionList.otherWorkLocations,"
                "requisitionList.secondaryLocations,"
                "requisitionList.requisitionFlexFields"
            ),
            "offset": offset,
            "limit": PAGE_SIZE,
        }
        data = self._get(SEARCH_ENDPOINT, params=params)

        items = data.get("items") or []
        if not items:
            return [], 0

        search_record = items[0]
        total_count = int(search_record.get("TotalJobsCount") or 0)
        requisitions = search_record.get("requisitionList") or []

        summaries: List[JobSummary] = []
        for entry in requisitions:
            job_id = (entry.get("Id") or "").strip()
            title = (entry.get("Title") or "").strip()
            if not job_id or not title:
                continue

            url = urljoin(BASE_UI_URL + "/", f"job/{job_id}")
            summary_metadata: Dict[str, object] = {
                "posted_date": entry.get("PostedDate"),
                "posting_end_date": entry.get("PostingEndDate"),
                "primary_location": entry.get("PrimaryLocation"),
                "primary_location_country": entry.get("PrimaryLocationCountry"),
                "distance": entry.get("Distance"),
                "hot_job_flag": entry.get("HotJobFlag"),
                "trending_flag": entry.get("TrendingFlag"),
                "be_first_to_apply": entry.get("BeFirstToApplyFlag"),
                "geography_id": entry.get("GeographyId"),
                "job_family": entry.get("JobFamily"),
                "job_function": entry.get("JobFunction"),
                "worker_type": entry.get("WorkerType"),
                "contract_type": entry.get("ContractType"),
                "job_schedule": entry.get("JobSchedule"),
                "job_shift": entry.get("JobShift"),
                "workplace_type": entry.get("WorkplaceType"),
                "workplace_type_code": entry.get("WorkplaceTypeCode"),
                "secondary_locations": entry.get("secondaryLocations") or [],
                "other_work_locations": entry.get("otherWorkLocations") or [],
            }

            summaries.append(
                JobSummary(
                    job_id=job_id,
                    title=title,
                    url=url,
                    posted_date=(entry.get("PostedDate") or "").strip() or None,
                    location=(entry.get("PrimaryLocation") or "").strip() or None,
                    short_description=(entry.get("ShortDescriptionStr") or "").strip() or None,
                    workplace_type=(entry.get("WorkplaceType") or "").strip() or None,
                    metadata=summary_metadata,
                )
            )

        return summaries, total_count or len(summaries)

    def _get(self, url: str, *, params: Dict[str, object]) -> Dict[str, object]:
        response = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            message = (
                f"HTTP error {response.status_code} for {response.url}: "
                f"{response.text[:2000]}"
            )
            raise ScraperError(message) from exc
        return response.json()


def store_job(record: JobRecord) -> bool:
    defaults = {
        "title": record.title[:255],
        "location": (record.location or "")[:255] or None,
        "date": (record.posted_date or "")[:100] or None,
        "description": record.description_text[:10000],
        "metadata": {
            **record.metadata,
            "job_id": record.job_id,
            "description_html": record.description_html,
        },
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=record.url,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Persisted BXP job %s (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def run_scraper(*, limit: Optional[int], logger: logging.Logger) -> Dict[str, int]:
    summary = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}
    client = CandidateExperienceClient(logger=logger)

    for job_summary in client.iter_job_summaries(limit=limit):
        summary["fetched"] += 1
        try:
            detail = client.fetch_job_detail(job_summary.job_id)
            combined_metadata = {
                **job_summary.metadata,
                **detail.metadata,
                "short_description": job_summary.short_description,
            }
            record = JobRecord(
                job_id=job_summary.job_id,
                title=job_summary.title,
                url=job_summary.url,
                posted_date=job_summary.posted_date,
                location=job_summary.location,
                description_html=detail.description_html,
                description_text=detail.description_text,
                metadata=combined_metadata,
            )
            created = store_job(record)
            if created:
                summary["created"] += 1
            else:
                summary["updated"] += 1
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("Failed to process job %s: %s", job_summary.job_id, exc)
            summary["errors"] += 1

    return summary


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="BXP careers manual scraper")
    parser.add_argument("--limit", type=int, default=None, help="Process at most N jobs.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity (default: INFO)",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    logger = logging.getLogger("bxp.manual")

    try:
        totals = run_scraper(limit=args.limit, logger=logger)
    except ScraperError as exc:
        logger.error("Scraper failed: %s", exc)
        return 1

    dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
    totals["dedupe"] = dedupe_summary

    logger.info("Summary: %s", totals)
    print(totals)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
