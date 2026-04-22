#!/usr/bin/env python3
"""Manual scraper for CooperCompanies' Oracle Cloud careers site."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

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
CAREERS_LANDING_URL = "https://www.coopercos.com/careers/"
ORACLE_BASE_URL = "https://hcjy.fa.us2.oraclecloud.com"
SITE_CODE = "CX_1"
SITE_NUMBER = "CX_1"
JOB_LIST_ENDPOINT = f"{ORACLE_BASE_URL}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
JOB_DETAIL_ENDPOINT = (
    f"{ORACLE_BASE_URL}/hcmRestApi/resources/latest/recruitingCEJobRequisitionDetails"
)
JOB_DETAIL_URL_TEMPLATE = (
    f"{ORACLE_BASE_URL}/hcmUI/CandidateExperience/en/sites/{SITE_CODE}/job/{{job_id}}"
)
REQUEST_TIMEOUT = (15, 45)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": ORACLE_BASE_URL,
    "Referer": f"{ORACLE_BASE_URL}/hcmUI/CandidateExperience/en/sites/{SITE_CODE}/search",
}
DEFAULT_PAGE_SIZE = 50
DEFAULT_DELAY = 0.25
DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 1800), 120)

SCRAPER_QS = Scraper.objects.filter(
    company="CooperCompanies", url=CAREERS_LANDING_URL
).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning(
            "Multiple CooperCompanies scraper rows found; using id=%s", SCRAPER.id
        )
else:
    SCRAPER = Scraper.objects.create(
        company="CooperCompanies",
        url=CAREERS_LANDING_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the CooperCompanies scraper encounters a fatal error."""


@dataclass
class JobSummary:
    job_id: str
    title: str
    detail_url: str
    posted_date: Optional[str]
    location: Optional[str]
    metadata: Dict[str, object]


@dataclass
class JobListing:
    job_id: str
    title: str
    detail_url: str
    posted_date: Optional[str]
    location: Optional[str]
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
    return soup.get_text("\n", strip=True)


def _dedupe_preserve_order(items: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    ordered: List[str] = []
    for item in items:
        if not item:
            continue
        norm = item.strip()
        if not norm or norm in seen:
            continue
        seen.add(norm)
        ordered.append(norm)
    return ordered


def _compose_location(raw: dict) -> Optional[str]:
    primary = raw.get("PrimaryLocation")
    secondary = [entry.get("Name") for entry in raw.get("secondaryLocations") or []]
    other = [entry.get("Name") for entry in raw.get("otherWorkLocations") or []]
    work_locations = [
        entry.get("LocationName") or entry.get("Name")
        for entry in raw.get("workLocation") or []
    ]
    combined = _dedupe_preserve_order([primary, *secondary, *other, *work_locations])
    return ", ".join(combined) if combined else None


def _build_listing_metadata(summary: JobSummary, detail: dict) -> Dict[str, object]:
    metadata = dict(summary.metadata)
    detail_meta_keys = [
        "RequisitionId",
        "ExternalPostedStartDate",
        "ExternalPostedEndDate",
        "JobFamilyId",
        "GeographyId",
        "GeographyNodeId",
        "JobFunction",
        "JobFunctionCode",
        "WorkerType",
        "JobType",
        "StudyLevel",
        "JobSchedule",
        "JobShift",
        "WorkplaceType",
        "WorkplaceTypeCode",
        "NumberOfOpenings",
        "HiringManager",
        "Organization",
        "Department",
        "BusinessUnit",
        "LegalEmployer",
        "HotJobFlag",
        "TrendingFlag",
        "BeFirstToApplyFlag",
    ]
    for key in detail_meta_keys:
        value = detail.get(key)
        if value not in (None, "", []):
            metadata[key] = value

    if detail.get("secondaryLocations"):
        metadata["secondary_locations"] = [
            entry.get("Name")
            for entry in detail["secondaryLocations"]
            if entry.get("Name")
        ]
    if detail.get("otherWorkLocations"):
        metadata["other_work_locations"] = [
            entry.get("Name")
            for entry in detail["otherWorkLocations"]
            if entry.get("Name")
        ]
    if detail.get("workLocation"):
        metadata["work_location_detail"] = detail["workLocation"]
    if detail.get("requisitionFlexFields"):
        metadata["requisition_flex_fields"] = detail["requisitionFlexFields"]
    if detail.get("primaryLocationCoordinates"):
        metadata["primary_location_coordinates"] = detail["primaryLocationCoordinates"]

    metadata.setdefault("job_id", summary.job_id)
    metadata.setdefault("detail_url", summary.detail_url)
    if summary.location:
        metadata.setdefault("location_display", summary.location)
    return metadata


def _merge_description(detail: dict) -> tuple[str, Optional[str]]:
    """Assemble description HTML/text from Oracle content fragments."""
    html_sections: List[str] = []
    for label, content in (
        ("Description", detail.get("ExternalDescriptionStr")),
        ("Responsibilities", detail.get("ExternalResponsibilitiesStr")),
        ("Qualifications", detail.get("ExternalQualificationsStr")),
        ("Corporate", detail.get("CorporateDescriptionStr")),
        ("Organization", detail.get("OrganizationDescriptionStr")),
    ):
        if not content:
            continue
        if label != "Description":
            html_sections.append(f"<h3>{label}</h3>")
        html_sections.append(content)

    if not html_sections:
        return "", None

    description_html = "\n\n".join(html_sections)
    description_text = _clean_html(description_html)
    return description_text, description_html


# ---------------------------------------------------------------------------
# Scraper implementation
# ---------------------------------------------------------------------------
class CooperCompaniesCareersScraper:
    def __init__(
        self,
        *,
        page_size: int = DEFAULT_PAGE_SIZE,
        delay: float = DEFAULT_DELAY,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.page_size = max(1, min(page_size, 200))
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    def scrape(
        self,
        *,
        limit: Optional[int] = None,
        start_offset: int = 0,
    ) -> Iterable[JobListing]:
        offset = max(0, start_offset)
        fetched = 0

        while True:
            summaries, total_jobs = self._fetch_batch(limit=self.page_size, offset=offset)
            if not summaries:
                self.logger.info("No job summaries returned at offset=%s; stopping.", offset)
                break

            for summary in summaries:
                try:
                    detail = self._fetch_job_detail(summary.job_id)
                except (requests.RequestException, ScraperError) as exc:
                    self.logger.error(
                        "Failed to fetch detail for job %s: %s", summary.job_id, exc
                    )
                    continue

                listing = self._build_listing(summary, detail)
                yield listing
                fetched += 1

                if limit is not None and fetched >= limit:
                    self.logger.info("Reached limit=%s; stopping.", limit)
                    return

                if self.delay:
                    time.sleep(self.delay)

            offset += len(summaries)
            if total_jobs is not None and offset >= total_jobs:
                self.logger.info(
                    "Reached API reported total (%s >= %s); stopping.",
                    offset,
                    total_jobs,
                )
                break

    def _fetch_batch(
        self, *, limit: int, offset: int
    ) -> tuple[List[JobSummary], Optional[int]]:
        params = {
            "onlyData": "true",
            "expand": (
                "requisitionList.workLocation,"
                "requisitionList.otherWorkLocations,"
                "requisitionList.secondaryLocations,"
                "flexFieldsFacet.values,"
                "requisitionList.requisitionFlexFields"
            ),
            "finder": f"findReqs;siteNumber={SITE_NUMBER},limit={limit},offset={offset}",
        }
        response = self.session.get(JOB_LIST_ENDPOINT, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        payload = response.json()

        items = payload.get("items") or []
        if not items:
            return [], payload.get("count")

        search_blob = items[0]
        total_jobs = search_blob.get("TotalJobsCount")
        requisitions = search_blob.get("requisitionList") or []
        summaries: List[JobSummary] = []

        for raw in requisitions:
            summary = self._parse_summary(raw)
            if summary:
                summaries.append(summary)

        return summaries, total_jobs

    def _parse_summary(self, raw: dict) -> Optional[JobSummary]:
        job_id = str(raw.get("Id") or "").strip()
        title = (raw.get("Title") or "").strip()
        if not job_id or not title:
            return None

        detail_url = JOB_DETAIL_URL_TEMPLATE.format(job_id=job_id)
        posted_date = (raw.get("PostedDate") or "").strip() or None
        location = _compose_location(raw)
        metadata: Dict[str, object] = {
            "primary_location": raw.get("PrimaryLocation"),
            "primary_location_country": raw.get("PrimaryLocationCountry"),
            "job_family": raw.get("JobFamily"),
            "job_function": raw.get("JobFunction"),
            "worker_type": raw.get("WorkerType"),
            "workplace_type": raw.get("WorkplaceType"),
            "short_description": raw.get("ShortDescriptionStr"),
            "hot_job_flag": raw.get("HotJobFlag"),
            "be_first_to_apply_flag": raw.get("BeFirstToApplyFlag"),
            "relevancy": raw.get("Relevancy"),
            "geography_id": raw.get("GeographyId"),
            "trending_flag": raw.get("TrendingFlag"),
        }
        if raw.get("workLocation"):
            metadata["work_location_raw"] = raw["workLocation"]
        if raw.get("otherWorkLocations"):
            metadata["other_work_locations_raw"] = raw["otherWorkLocations"]
        if raw.get("secondaryLocations"):
            metadata["secondary_locations_raw"] = raw["secondaryLocations"]
        if raw.get("flexFields"):
            metadata["flex_fields"] = raw["flexFields"]

        return JobSummary(
            job_id=job_id,
            title=title,
            detail_url=detail_url,
            posted_date=posted_date,
            location=location,
            metadata=metadata,
        )

    def _fetch_job_detail(self, job_id: str) -> dict:
        params = {
            "onlyData": "true",
            "expand": "all",
            "finder": f'ById;Id="{job_id}",siteNumber={SITE_NUMBER}',
        }
        response = self.session.get(JOB_DETAIL_ENDPOINT, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        payload = response.json()
        items = payload.get("items") or []
        if not items:
            raise ScraperError(f"Job detail payload empty for job_id={job_id}")
        return items[0]

    def _build_listing(self, summary: JobSummary, detail: dict) -> JobListing:
        description_text, description_html = _merge_description(detail)
        metadata = _build_listing_metadata(summary, detail)
        if description_html:
            metadata.setdefault("description_html", description_html)

        return JobListing(
            job_id=summary.job_id,
            title=summary.title,
            detail_url=summary.detail_url,
            posted_date=summary.posted_date,
            location=summary.location,
            description_text=description_text,
            description_html=description_html,
            metadata=metadata,
        )


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------
def store_listing(listing: JobListing) -> bool:
    metadata = dict(listing.metadata or {})
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255],
        "date": (listing.posted_date or "")[:100],
        "description": listing.description_text[:10000],
        "metadata": metadata,
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults=defaults,
    )
    return created


# ---------------------------------------------------------------------------
# CLI orchestration
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CooperCompanies careers manual scraper")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process")
    parser.add_argument("--offset", type=int, default=0, help="Starting offset into the job list")
    parser.add_argument(
        "--page-size", type=int, default=DEFAULT_PAGE_SIZE, help="API page size (max 200)"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help="Delay in seconds between job detail requests",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity",
    )
    return parser.parse_args(argv)


def run_scrape(args: argparse.Namespace) -> Dict[str, object]:
    scraper = CooperCompaniesCareersScraper(page_size=args.page_size, delay=args.delay)
    processed = 0
    created = 0

    for listing in scraper.scrape(limit=args.limit, start_offset=args.offset):
        if store_listing(listing):
            created += 1
        processed += 1

    dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
    return {
        "processed_jobs": processed,
        "created_jobs": created,
        "deduplicated": dedupe_summary,
    }


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    start = time.time()
    try:
        outcome = run_scrape(args)
    except (requests.RequestException, ScraperError) as exc:
        logging.error("Scrape failed: %s", exc)
        return 1
    duration = time.time() - start
    summary = {
        "company": "CooperCompanies",
        "site": CAREERS_LANDING_URL,
        "elapsed_seconds": round(duration, 2),
        **outcome,
    }
    logging.info("Summary: %s", json.dumps(summary))
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

