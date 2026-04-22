#!/usr/bin/env python3
"""Manual scraper for Dollar Tree's Phenom-powered careers site.

This script walks the search results pagination, hydrates each job with its
detail payload, and persists postings via the shared Django ORM models.
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
from typing import Dict, Iterator, Optional, Set

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Django bootstrap so the script can run standalone
# ---------------------------------------------------------------------------
BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "website.settings")

import django  # noqa: E402
from django.conf import settings  # noqa: E402
from django.db import IntegrityError  # noqa: E402

django.setup()

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

# ---------------------------------------------------------------------------
# Constants & configuration
# ---------------------------------------------------------------------------
ROOT_LANDING_URL = "https://www.dollartree.com/careers"
BASE_DOMAIN = "https://careers.dollartree.com"
SEARCH_PATH = "/us/en/search-results"
JOB_DETAIL_PATH = "/us/en/job/{job_seq}"
REQUEST_TIMEOUT = (15, 45)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": ROOT_LANDING_URL,
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 30)
SCRAPER_QS = Scraper.objects.filter(company="Dollar Tree", url=ROOT_LANDING_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Dollar Tree scraper entries found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Dollar Tree",
        url=ROOT_LANDING_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the scrape pipeline cannot proceed."""


@dataclass
class JobListing:
    job_seq: str
    title: str
    link: str
    location: Optional[str]
    posted_date: Optional[str]
    apply_url: Optional[str]
    description_text: str
    description_html: Optional[str]
    metadata: Dict[str, object]


def _html_to_text(value: Optional[str]) -> str:
    if not value:
        return ""
    soup = BeautifulSoup(value, "html.parser")
    return soup.get_text("\n", strip=True).strip()


class DollarTreeCareersClient:
    def __init__(self, *, delay: float = 0.0, session: Optional[requests.Session] = None) -> None:
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    def iter_listings(
        self,
        *,
        max_pages: Optional[int] = None,
        max_results: Optional[int] = None,
    ) -> Iterator[JobListing]:
        offset = 0
        page = 0
        yielded = 0
        seen_sequences: Set[str] = set()
        total_hits: Optional[int] = None

        while True:
            listing_payload = self._fetch_listing_page(offset)
            jobs = listing_payload.get("jobs") or []
            if not jobs:
                self.logger.info("No jobs returned at offset %s; stopping.", offset)
                break

            if total_hits is None:
                total_hits = listing_payload.get("totalHits")
                self.logger.info(
                    "Discovered %s total jobs (page size %s).",
                    total_hits,
                    len(jobs),
                )

            for job in jobs:
                job_seq = (job.get("jobSeqNo") or "").strip()
                if not job_seq or job_seq in seen_sequences:
                    continue
                seen_sequences.add(job_seq)

                try:
                    detail_payload = self._fetch_job_detail(job_seq)
                    listing = self._build_listing(job, detail_payload)
                except ScraperError as exc:
                    self.logger.error("Skipping job %s due to error: %s", job_seq, exc)
                    continue

                yield listing
                yielded += 1

                if max_results is not None and yielded >= max_results:
                    self.logger.info("Reached result limit %s; stopping.", max_results)
                    return

            offset += len(jobs)
            page += 1

            if total_hits is not None and offset >= total_hits:
                self.logger.info("Reached reported total hits; stopping pagination.")
                break

            if max_pages is not None and page >= max_pages:
                self.logger.info("Reached page limit %s; stopping.", max_pages)
                break

            if self.delay:
                time.sleep(self.delay)

    def _fetch_listing_page(self, offset: int) -> Dict[str, object]:
        params = {"from": offset} if offset else None
        try:
            response = self.session.get(
                f"{BASE_DOMAIN}{SEARCH_PATH}",
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch listings at offset {offset}: {exc}") from exc

        ddo = self._extract_ddo(response.text)
        refine = ddo.get("eagerLoadRefineSearch") or {}
        data = refine.get("data") or {}

        return {
            "jobs": data.get("jobs") or [],
            "totalHits": refine.get("totalHits") or refine.get("hits"),
        }

    def _fetch_job_detail(self, job_seq: str) -> Dict[str, object]:
        detail_url = f"{BASE_DOMAIN}{JOB_DETAIL_PATH.format(job_seq=job_seq)}"
        try:
            response = self.session.get(detail_url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch detail for {job_seq}: {exc}") from exc

        ddo = self._extract_ddo(response.text)
        job_detail = ((ddo.get("jobDetail") or {}).get("data") or {}).get("job") or {}
        if not job_detail:
            raise ScraperError(f"Detail payload missing for {job_seq}")
        return job_detail

    def _extract_ddo(self, html: str) -> Dict[str, object]:
        marker = "phApp.ddo = "
        start = html.find(marker)
        if start == -1:
            raise ScraperError("Unable to locate phApp.ddo payload.")
        start += len(marker)
        while start < len(html) and html[start].isspace():
            start += 1
        if start >= len(html) or html[start] != "{":
            raise ScraperError("Unexpected phApp.ddo structure.")

        depth = 0
        end = start
        for idx in range(start, len(html)):
            char = html[idx]
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    end = idx
                    break
        else:
            raise ScraperError("Unable to parse phApp.ddo payload (unterminated object).")

        payload = html[start : end + 1].replace("\n", " ")
        try:
            return json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ScraperError(f"Failed to decode phApp.ddo payload: {exc}") from exc

    def _build_listing(self, summary: Dict[str, object], detail: Dict[str, object]) -> JobListing:
        job_seq = (summary.get("jobSeqNo") or detail.get("jobSeqNo") or "").strip()
        title = (summary.get("title") or detail.get("title") or "").strip()
        location = (
            summary.get("cityStateCountry")
            or summary.get("location")
            or detail.get("cityStateCountry")
            or detail.get("location")
        )
        posted_date = (summary.get("postedDate") or detail.get("postedDate") or "").strip() or None
        apply_url = summary.get("applyUrl") or detail.get("applyUrl")

        structure = detail.get("structureData") or {}
        description_html = (
            structure.get("description")
            or detail.get("description")
            or detail.get("ml_Description")
        )
        description_text = _html_to_text(description_html)

        metadata: Dict[str, object] = {
            "job_seq_no": job_seq or None,
            "job_id": summary.get("jobId") or detail.get("jobId"),
            "req_id": summary.get("reqId") or detail.get("reqId"),
            "ref_num": summary.get("refNum") or detail.get("refNum"),
            "job_visibility": summary.get("jobVisibility") or detail.get("jobVisibility"),
            "job_type": summary.get("type") or detail.get("type"),
            "category": summary.get("category") or detail.get("category"),
            "site_type": summary.get("siteType") or detail.get("siteType"),
            "city": summary.get("city") or detail.get("city"),
            "state": summary.get("state") or detail.get("state"),
            "country": summary.get("country") or detail.get("country"),
            "multi_location": summary.get("multi_location") or detail.get("multi_location"),
            "multi_category": summary.get("multi_category") or detail.get("multi_category"),
            "posted_date": posted_date,
            "date_created": summary.get("dateCreated") or detail.get("dateCreated"),
            "description_teaser": summary.get("descriptionTeaser") or detail.get("descriptionTeaser"),
            "apply_url": apply_url,
            "external_apply": summary.get("externalApply") or detail.get("externalApply"),
            "ml_skills": summary.get("ml_skills") or detail.get("ml_skills"),
            "store_number": summary.get("storeNumber") or detail.get("storeNumber"),
            "store_number_from_location": detail.get("storeNumberFromLocation"),
            "salary_range": detail.get("salaryRange"),
            "job_family": detail.get("jobFamily"),
            "job_family_group": detail.get("jobFamilyGroup"),
            "job_requisition_id": detail.get("jobRequisitionId"),
            "location_display": detail.get("locationDisplay"),
            "location_type": detail.get("locationType"),
            "management_level": detail.get("managementLevel"),
            "structure_data": {
                "employmentType": structure.get("employmentType"),
                "jobLocation": structure.get("jobLocation"),
                "identifier": structure.get("identifier"),
                "description": description_html,
            },
        }

        detail_link = f"{BASE_DOMAIN}{JOB_DETAIL_PATH.format(job_seq=job_seq)}"

        return JobListing(
            job_seq=job_seq,
            title=title,
            link=detail_link,
            location=location or None,
            posted_date=posted_date,
            apply_url=apply_url,
            description_text=description_text,
            description_html=description_html,
            metadata={key: value for key, value in metadata.items() if value},
        )


def store_listing(listing: JobListing) -> None:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255],
        "date": (listing.posted_date or "")[:100],
        "description": listing.description_text[:10000],
        "metadata": {
            **listing.metadata,
            "description_html": listing.description_html,
            "apply_url": listing.apply_url,
        },
    }
    try:
        JobPosting.objects.update_or_create(
            scraper=SCRAPER,
            link=listing.link,
            defaults=defaults,
        )
    except IntegrityError as exc:
        raise ScraperError(f"Failed to store job {listing.job_seq}: {exc}") from exc


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dollar Tree careers manual scraper")
    parser.add_argument("--max-pages", type=int, default=None, help="Maximum number of result pages to fetch")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of job records to process")
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
    client = DollarTreeCareersClient(delay=args.delay)
    processed = 0
    for listing in client.iter_listings(max_pages=args.max_pages, max_results=args.limit):
        store_listing(listing)
        processed += 1
    dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
    return {"processed_jobs": processed, "deduplicated": dedupe_summary}


def main(argv: Optional[list[str]] = None) -> int:
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
        "company": "Dollar Tree",
        "site": ROOT_LANDING_URL,
        "elapsed_seconds": round(duration, 2),
        **outcome,
    }
    logging.info("Summary: %s", json.dumps(summary))
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
