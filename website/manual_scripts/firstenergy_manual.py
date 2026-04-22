#!/usr/bin/env python3
"""Manual scraper for FirstEnergy careers (Oracle Cloud Candidate Experience)."""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional
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
# Constants
# ---------------------------------------------------------------------------
CAREERS_LANDING_URL = "https://www.firstenergycorp.com/careers.html"
CAREERS_BASE_URL = "https://careers.firstenergycorp.com"
ORACLE_BASE_URL = "https://fa-etjd-saasfaprod1.fa.ocs.oraclecloud.com"
SITE_NUMBER = "3000"
SITE_CODE = "FirstEnergyCareers"
JOB_LIST_ENDPOINT = f"{ORACLE_BASE_URL}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
JOB_DETAIL_ENDPOINT = f"{ORACLE_BASE_URL}/hcmRestApi/resources/latest/recruitingCEJobRequisitionDetails"
JOB_DETAIL_URL_TEMPLATE = (
    f"{CAREERS_BASE_URL}/#en/sites/{SITE_CODE}/job/{{job_id}}"
)
REQUEST_TIMEOUT = (15, 45)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": CAREERS_BASE_URL,
    "Referer": f"{CAREERS_BASE_URL}/#en/sites/{SITE_CODE}/search",
}
DEFAULT_PAGE_SIZE = 50
DEFAULT_DELAY = 0.25
DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 1800), 120)

SCRAPER_QS = Scraper.objects.filter(company="FirstEnergy", url=CAREERS_LANDING_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple FirstEnergy scrapers found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="FirstEnergy",
        url=CAREERS_LANDING_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the scraper encounters a critical error."""


@dataclass
class JobSummary:
    job_id: str
    title: str
    location: Optional[str]
    posted_date: Optional[str]
    workplace_type: Optional[str]
    detail_url: str
    metadata: Dict[str, object]


@dataclass
class JobListing(JobSummary):
    description_text: str
    description_html: Optional[str]
    metadata: Dict[str, object]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _clean_html(value: Optional[str]) -> str:
    if not value:
        return ""
    soup = BeautifulSoup(value, "html.parser")
    text = soup.get_text("\n", strip=True)
    return text.strip()


def _dedupe_preserve_order(items: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    ordered: List[str] = []
    for item in items:
        if not item:
            continue
        if item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def _compose_location(detail: dict, fallback: Optional[str]) -> Optional[str]:
    primary = (detail.get("PrimaryLocation") or "").strip()
    secondary = [
        (entry.get("Name") or "").strip()
        for entry in detail.get("secondaryLocations") or []
        if entry.get("Name")
    ]
    other = [
        (entry.get("Name") or "").strip()
        for entry in detail.get("otherWorkLocations") or []
        if entry.get("Name")
    ]
    work_locations = [
        (entry.get("LocationName") or entry.get("Name") or "").strip()
        for entry in detail.get("workLocation") or []
        if entry.get("LocationName") or entry.get("Name")
    ]
    combined = _dedupe_preserve_order([primary, *secondary, *other, *work_locations, fallback or ""])
    combined = [part for part in combined if part]
    return ", ".join(combined) if combined else None


def _build_listing_metadata(summary: JobSummary, detail: dict) -> Dict[str, object]:
    metadata = dict(summary.metadata)
    detail_fields = [
        "Category",
        "RequisitionType",
        "RequisitionId",
        "ExternalPostedStartDate",
        "ExternalPostedEndDate",
        "NumberOfOpenings",
        "HiringManager",
        "Organization",
        "BusinessUnit",
        "Department",
        "LegalEmployer",
        "JobSchedule",
        "JobShift",
        "JobType",
        "WorkerType",
        "JobFunction",
        "JobFamilyId",
        "WorkplaceType",
        "WorkplaceTypeCode",
        "HotJobFlag",
        "TrendingFlag",
        "BeFirstToApplyFlag",
        "primaryLocationCoordinates",
    ]
    for field in detail_fields:
        value = detail.get(field)
        if value not in (None, "", []):
            metadata[field] = value

    if detail.get("secondaryLocations"):
        metadata["secondary_locations_detail"] = [
            entry.get("Name") for entry in detail["secondaryLocations"] if entry.get("Name")
        ]
    if detail.get("otherWorkLocations"):
        metadata["other_work_locations_detail"] = [
            entry.get("Name") for entry in detail["otherWorkLocations"] if entry.get("Name")
        ]
    if detail.get("workLocation"):
        metadata["work_location_detail"] = [
            entry.get("LocationName") or entry.get("Name")
            for entry in detail["workLocation"]
            if entry.get("LocationName") or entry.get("Name")
        ]
    return metadata


# ---------------------------------------------------------------------------
# Scraper implementation
# ---------------------------------------------------------------------------
class FirstEnergyCareersClient:
    def __init__(
        self,
        *,
        page_size: int = DEFAULT_PAGE_SIZE,
        delay: float = DEFAULT_DELAY,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.page_size = max(1, min(int(page_size), 200))
        self.delay = max(delay, 0.0)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    def iter_listings(
        self,
        *,
        offset: int = 0,
        limit: Optional[int] = None,
    ) -> Iterator[JobListing]:
        cursor = max(0, offset)
        fetched = 0
        total_jobs: Optional[int] = None

        while True:
            page_data = self._fetch_list_page(cursor)
            summaries = page_data.get("requisitionList") or []
            if total_jobs is None:
                total_jobs = int(page_data.get("TotalJobsCount") or len(summaries))
                self.logger.info("FirstEnergy careers reports %s open jobs.", total_jobs)

            if not summaries:
                self.logger.debug("No requisitions returned at offset=%s; stopping.", cursor)
                break

            for raw_summary in summaries:
                try:
                    summary = self._build_summary(raw_summary)
                    detail = self._fetch_job_detail(summary.job_id)
                    description_html = detail.get("ExternalDescriptionStr") or ""
                    description_sections = [
                        detail.get("ExternalDescriptionStr"),
                        detail.get("ExternalQualificationsStr"),
                        detail.get("ExternalResponsibilitiesStr"),
                        detail.get("CorporateDescriptionStr"),
                        detail.get("OrganizationDescriptionStr"),
                    ]
                    description_text = _clean_html("\n\n".join(filter(None, description_sections)))
                    location = _compose_location(detail, summary.location)
                    metadata = _build_listing_metadata(summary, detail)

                    listing = JobListing(
                        job_id=summary.job_id,
                        title=summary.title,
                        location=location,
                        posted_date=summary.posted_date,
                        workplace_type=summary.workplace_type,
                        detail_url=summary.detail_url,
                        metadata=metadata,
                        description_text=description_text,
                        description_html=detail.get("ExternalDescriptionStr"),
                    )
                    yield listing
                    fetched += 1
                except Exception as exc:
                    self.logger.error("Failed to process job summary %s: %s", raw_summary.get("Id"), exc)
                    continue

                if limit is not None and fetched >= limit:
                    self.logger.info("Reached limit=%s; stopping.", limit)
                    return

                if self.delay:
                    time.sleep(self.delay)

            cursor += len(summaries)
            if total_jobs is not None and cursor >= total_jobs:
                self.logger.debug("Reached cursor=%s >= total_jobs=%s; stopping.", cursor, total_jobs)
                break

    def _fetch_list_page(self, offset: int) -> Dict[str, object]:
        finder = f"findReqs;siteNumber={SITE_NUMBER},offset={offset},limit={self.page_size}"
        params = {
            "onlyData": "true",
            "expand": "requisitionList",
            "finder": finder,
        }
        self.logger.debug("Fetching requisition list at offset=%s", offset)
        response = self.session.get(JOB_LIST_ENDPOINT, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        payload = response.json()
        items = payload.get("items") or []
        if not items:
            return {}
        return items[0] or {}

    def _fetch_job_detail(self, job_id: str) -> Dict[str, object]:
        params = {
            "onlyData": "true",
            "expand": "all",
            "finder": f'ById;Id="{job_id}",siteNumber={SITE_NUMBER}',
        }
        self.logger.debug("Fetching job detail for %s", job_id)
        response = self.session.get(JOB_DETAIL_ENDPOINT, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        payload = response.json()
        items = payload.get("items") or []
        if not items:
            raise ScraperError(f"Detail payload empty for job_id={job_id}")
        return items[0]

    def _build_summary(self, data: Dict[str, object]) -> JobSummary:
        job_id = str(data.get("Id") or "").strip()
        if not job_id:
            raise ScraperError("Missing job Id in summary payload.")

        title = (data.get("Title") or "").strip()
        if not title:
            raise ScraperError(f"Missing title for job {job_id}")

        location = (data.get("PrimaryLocation") or "").strip() or None
        posted_date = (data.get("PostedDate") or "").strip() or None
        workplace_type = (data.get("WorkplaceType") or "").strip() or None
        detail_url = JOB_DETAIL_URL_TEMPLATE.format(job_id=job_id)

        metadata = {
            "job_id": job_id,
            "primary_location_country": data.get("PrimaryLocationCountry"),
            "hot_job": data.get("HotJobFlag"),
            "workplace_type_code": data.get("WorkplaceTypeCode"),
            "short_description": data.get("ShortDescriptionStr"),
        }
        if data.get("ExternalQualificationsStr"):
            metadata["summary_external_qualifications"] = data["ExternalQualificationsStr"]
        if data.get("ExternalResponsibilitiesStr"):
            metadata["summary_external_responsibilities"] = data["ExternalResponsibilitiesStr"]

        return JobSummary(
            job_id=job_id,
            title=title,
            location=location,
            posted_date=posted_date,
            workplace_type=workplace_type,
            detail_url=detail_url,
            metadata=metadata,
        )


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------
def store_listing(listing: JobListing) -> bool:
    metadata = dict(listing.metadata or {})
    if listing.description_html:
        metadata.setdefault("description_html", listing.description_html)

    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255] or None,
        "date": (listing.posted_date or "")[:100] or None,
        "description": listing.description_text[:10000],
        "metadata": metadata,
    }

    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults=defaults,
    )
    logging.getLogger("store_listing").debug(
        "Stored FirstEnergy job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="FirstEnergy careers manual scraper.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument("--offset", type=int, default=0, help="Starting offset for pagination.")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE, help="Jobs to request per page.")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Delay between requests in seconds.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print job payloads as JSON instead of persisting them.",
    )
    return parser.parse_args(argv)


def run_scrape(
    *,
    limit: Optional[int],
    offset: int,
    page_size: int,
    delay: float,
    dry_run: bool,
) -> Dict[str, object]:
    client = FirstEnergyCareersClient(page_size=page_size, delay=delay)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    try:
        for listing in client.iter_listings(offset=offset, limit=limit):
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
            except Exception as exc:  # pragma: no cover - persistence failure
                logging.error("Failed to store FirstEnergy job %s: %s", listing.detail_url, exc)
                totals["errors"] += 1
    except requests.HTTPError as exc:
        logging.error("HTTP error while scraping FirstEnergy careers: %s", exc)
        totals["errors"] += 1
    except requests.RequestException as exc:
        logging.error("Network error while scraping FirstEnergy careers: %s", exc)
        totals["errors"] += 1
    except ScraperError as exc:
        logging.error("FirstEnergy scraper stopped: %s", exc)
        totals["errors"] += 1

    if not dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["dedupe"] = dedupe_summary

    return totals


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()), format="%(levelname)s: %(message)s")

    totals = run_scrape(
        limit=args.limit,
        offset=args.offset,
        page_size=args.page_size,
        delay=args.delay,
        dry_run=args.dry_run,
    )
    logging.info(
        "FirstEnergy scraper finished - fetched=%(fetched)s created=%(created)s updated=%(updated)s errors=%(errors)s",
        totals,
    )
    if not args.dry_run and "dedupe" in totals:
        logging.info("Deduplication summary: %s", totals["dedupe"])
    return 0 if not totals["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
