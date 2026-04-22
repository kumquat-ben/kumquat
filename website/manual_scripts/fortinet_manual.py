#!/usr/bin/env python3
"""Manual scraper for Fortinet careers (Oracle Cloud Candidate Experience)."""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, asdict
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
CAREERS_LANDING_URL = "https://www.fortinet.com/corporate/careers"
ORACLE_BASE_URL = "https://edel.fa.us2.oraclecloud.com"
SITE_NUMBER = "2001"
SITE_CODE = "CX_2001"
JOB_LIST_ENDPOINT = f"{ORACLE_BASE_URL}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
JOB_DETAIL_ENDPOINT = f"{ORACLE_BASE_URL}/hcmRestApi/resources/latest/recruitingCEJobRequisitionDetails"
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

SCRAPER_QS = Scraper.objects.filter(company="Fortinet", url=CAREERS_LANDING_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Fortinet scraper rows found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Fortinet",
        url=CAREERS_LANDING_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the scraper cannot continue."""


@dataclass
class JobSummary:
    job_id: str
    title: str
    detail_url: str
    posted_date: Optional[str]
    location: Optional[str]
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
    return soup.get_text("\n", strip=True)


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


def _compose_location(raw: dict) -> Optional[str]:
    primary = (raw.get("PrimaryLocation") or "").strip()
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
        "ShortDescriptionStr",
        "ExternalQualificationsStr",
        "ExternalResponsibilitiesStr",
        "CorporateDescriptionStr",
        "OrganizationDescriptionStr",
        "WorkplaceType",
        "WorkplaceTypeCode",
        "NumberOfOpenings",
        "HiringManager",
        "Organization",
        "Department",
        "BusinessUnit",
        "LegalEmployer",
        "JobFunction",
        "JobType",
        "WorkerType",
    ]
    for key in detail_meta_keys:
        value = detail.get(key)
        if value not in (None, "", []):
            metadata[key] = value
    if detail.get("secondaryLocations"):
        metadata["secondary_locations_detail"] = [
            entry.get("Name") for entry in detail["secondaryLocations"] if entry.get("Name")
        ]
    if detail.get("otherWorkLocations"):
        metadata["other_work_locations_detail"] = [
            entry.get("Name") for entry in detail["otherWorkLocations"] if entry.get("Name")
        ]
    primary_coords = detail.get("primaryLocationCoordinates")
    if primary_coords:
        metadata["primary_location_coordinates"] = primary_coords
    return metadata


# ---------------------------------------------------------------------------
# Scraper implementation
# ---------------------------------------------------------------------------
class FortinetCareersScraper:
    def __init__(
        self,
        *,
        page_size: int = DEFAULT_PAGE_SIZE,
        delay: float = DEFAULT_DELAY,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.page_size = max(1, page_size)
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
        total_jobs: Optional[int] = None

        while True:
            listings, total_jobs = self._fetch_batch(limit=self.page_size, offset=offset)
            if not listings:
                self.logger.info("No additional Fortinet jobs returned at offset %s; stopping.", offset)
                break

            self.logger.debug(
                "Fetched %s Fortinet summaries (offset=%s, total=%s)",
                len(listings),
                offset,
                total_jobs,
            )

            for summary in listings:
                detail = self._fetch_job_detail(summary.job_id)
                html_sections: List[str] = []
                for label, content in (
                    ("description", detail.get("ExternalDescriptionStr")),
                    ("responsibilities", detail.get("ExternalResponsibilitiesStr")),
                    ("qualifications", detail.get("ExternalQualificationsStr")),
                ):
                    if content:
                        if label != "description":
                            html_sections.append(f"<h3>{label.title()}</h3>")
                        html_sections.append(content)
                description_html = "\n\n".join(html_sections) if html_sections else None
                description_text = _clean_html(description_html) if description_html else ""

                metadata = _build_listing_metadata(summary, detail)
                metadata.setdefault("job_id", summary.job_id)
                metadata.setdefault("detail_url", summary.detail_url)
                metadata.setdefault("primary_location_raw", detail.get("PrimaryLocation"))
                if summary.location:
                    metadata.setdefault("location_display", summary.location)

                listing = JobListing(
                    job_id=summary.job_id,
                    title=summary.title,
                    detail_url=summary.detail_url,
                    posted_date=summary.posted_date,
                    location=summary.location,
                    description_text=description_text,
                    description_html=description_html,
                    metadata=metadata,
                )

                yield listing
                fetched += 1

                if limit is not None and fetched >= limit:
                    self.logger.info("Reached limit=%s; stopping.", limit)
                    return

                if self.delay:
                    time.sleep(self.delay)

            offset += len(listings)
            if total_jobs is not None and offset >= total_jobs:
                self.logger.info(
                    "Reached Fortinet reported total (%s >= %s); stopping.",
                    offset,
                    total_jobs,
                )
                break

    def _fetch_batch(self, *, limit: int, offset: int) -> tuple[List[JobSummary], Optional[int]]:
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
            "primary_location_country": raw.get("PrimaryLocationCountry"),
            "workplace_type": raw.get("WorkplaceType"),
            "workplace_type_code": raw.get("WorkplaceTypeCode"),
            "job_family": raw.get("JobFamily"),
            "job_function": raw.get("JobFunction"),
            "worker_type": raw.get("WorkerType"),
            "short_description": raw.get("ShortDescriptionStr"),
            "requisition_flex_fields": raw.get("requisitionFlexFields"),
            "other_work_locations": [
                entry.get("Name") for entry in raw.get("otherWorkLocations") or [] if entry.get("Name")
            ],
            "secondary_locations": [
                entry.get("Name") for entry in raw.get("secondaryLocations") or [] if entry.get("Name")
            ],
        }

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
        "Stored Fortinet job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fortinet careers manual scraper.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument("--offset", type=int, default=0, help="Starting offset for pagination.")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE, help="Jobs to request per page.")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Delay between job detail requests.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print jobs as JSON instead of persisting to the database.",
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
    scraper = FortinetCareersScraper(page_size=page_size, delay=delay)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    try:
        for listing in scraper.scrape(limit=limit, start_offset=offset):
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
                logging.error("Failed to store Fortinet job %s: %s", listing.detail_url, exc)
                totals["errors"] += 1
    except requests.HTTPError as exc:
        logging.error("HTTP error while scraping Fortinet careers: %s", exc)
        totals["errors"] += 1
    except requests.RequestException as exc:
        logging.error("Network error while scraping Fortinet careers: %s", exc)
        totals["errors"] += 1
    except ScraperError as exc:
        logging.error("Fortinet scraper stopped: %s", exc)
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
        "Fortinet scraper finished - fetched=%(fetched)s created=%(created)s updated=%(updated)s errors=%(errors)s",
        totals,
    )
    if not args.dry_run and "dedupe" in totals:
        logging.info("Deduplication summary: %s", totals["dedupe"])
    return 0 if not totals["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

