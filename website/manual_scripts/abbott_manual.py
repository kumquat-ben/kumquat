#!/usr/bin/env python3
"""Manual scraper for Abbott's careers search results.

This script harvests job listings from the Abbott / Phenom careers site,
hydrates each summary with the job-detail payload, and stores the results
through the Django ORM. It is intended for manual/on-demand runs via the
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
from typing import Dict, Iterator, List, Optional, Set

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
# Constants & configuration
# ---------------------------------------------------------------------------
ROOT_LANDING_URL = "https://www.abbott.com/careers/search-jobs.html"
BASE_DOMAIN = "https://www.jobs.abbott"
SEARCH_PATH = "/us/en/search-results"
JOB_DETAIL_PATH = "/us/en/job/{job_seq}"
SEARCH_URL = f"{BASE_DOMAIN}{SEARCH_PATH}"
DEFAULT_PAGE_SIZE = 10
REQUEST_TIMEOUT = (15, 45)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": ROOT_LANDING_URL,
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 30)
SCRAPER_QS = Scraper.objects.filter(company="Abbott", url=ROOT_LANDING_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched; using the earliest (id=%s).", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Abbott",
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


class AbbottCareersClient:
    def __init__(self, delay: float = 0.0, session: Optional[requests.Session] = None) -> None:
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.page_size = DEFAULT_PAGE_SIZE

    # ------------------------------------------------------------------ #
    # Public API                                                         #
    # ------------------------------------------------------------------ #
    def iter_listings(
        self,
        *,
        max_pages: Optional[int] = None,
        max_results: Optional[int] = None,
    ) -> Iterator[JobListing]:
        offset = 0
        page = 0
        yielded = 0
        seen_seqs: Set[str] = set()
        total_hits: Optional[int] = None

        while True:
            listing_payload = self._fetch_listing_page(offset)
            jobs = listing_payload.get("jobs") or []
            if not jobs:
                logging.info("No jobs returned at offset %s; stopping pagination.", offset)
                break

            if total_hits is None:
                total_hits = listing_payload.get("totalHits")
                logging.info("Discovered %s total jobs (page size %s).", total_hits, len(jobs))

            for job in jobs:
                job_seq = (job.get("jobSeqNo") or "").strip()
                if not job_seq:
                    continue
                if job_seq in seen_seqs:
                    continue
                seen_seqs.add(job_seq)

                detail_payload = self._fetch_job_detail(job_seq)
                listing = self._build_listing(job, detail_payload)
                yield listing
                yielded += 1

                if max_results and yielded >= max_results:
                    logging.info("Reached job limit %s; exiting.", max_results)
                    return

            offset += len(jobs)
            page += 1
            if total_hits is not None and offset >= total_hits:
                logging.info("Reached total_hits=%s; pagination complete.", total_hits)
                break
            if max_pages and page >= max_pages:
                logging.info("Reached max_pages=%s; pagination stopped.", max_pages)
                break
            if self.delay:
                time.sleep(self.delay)

    # ------------------------------------------------------------------ #
    # Internals                                                          #
    # ------------------------------------------------------------------ #
    def _fetch_listing_page(self, offset: int) -> Dict[str, object]:
        params = {"from": offset} if offset else None
        try:
            resp = self.session.get(SEARCH_URL, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch listing page at offset {offset}: {exc}") from exc

        ddo = self._extract_ddo(resp.text)
        refine = ddo.get("eagerLoadRefineSearch") or {}
        data = refine.get("data") or {}
        jobs = data.get("jobs") or []
        total_hits = refine.get("totalHits")
        hits = refine.get("hits")

        if refine.get("status") and refine.get("status") != "success":
            logging.warning(
                "Listing payload reported non-success status (%s) at offset %s.",
                refine.get("status"),
                offset,
            )

        return {"jobs": jobs, "totalHits": total_hits or hits}

    def _fetch_job_detail(self, job_seq: str) -> Dict[str, object]:
        detail_url = f"{BASE_DOMAIN}{JOB_DETAIL_PATH.format(job_seq=job_seq)}"
        try:
            resp = self.session.get(detail_url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch job detail for {job_seq}: {exc}") from exc

        ddo = self._extract_ddo(resp.text)
        job_detail = ((ddo.get("jobDetail") or {}).get("data") or {}).get("job") or {}
        if not job_detail:
            logging.warning("Detail payload missing for job %s", job_seq)
        return job_detail

    def _extract_ddo(self, html: str) -> Dict[str, object]:
        marker = "phApp.ddo = "
        start = html.find(marker)
        if start == -1:
            raise ScraperError("Unable to locate phApp.ddo payload in response.")
        start += len(marker)
        while start < len(html) and html[start].isspace():
            start += 1
        if start >= len(html) or html[start] != "{":
            raise ScraperError("Unexpected phApp.ddo payload structure.")

        depth = 0
        end = start
        for idx in range(start, len(html)):
            ch = html[idx]
            if ch == "{":
                depth += 1
            elif ch == "}":
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
        location = (summary.get("cityStateCountry") or summary.get("location") or detail.get("location") or "").strip()
        location = location or None
        posted_date = (summary.get("postedDate") or detail.get("postedDate") or "").strip() or None
        apply_url = summary.get("applyUrl") or detail.get("applyUrl")

        structure = detail.get("structureData") or {}
        description_html = (
            structure.get("description")
            or detail.get("description")
            or detail.get("ml_Description")
        )
        description_text = _html_to_text(description_html) if description_html else ""

        metadata: Dict[str, object] = {
            "job_seq_no": job_seq or None,
            "job_id": summary.get("jobId") or detail.get("jobId"),
            "req_id": summary.get("reqId") or detail.get("reqId"),
            "job_code": detail.get("jobCode"),
            "category": summary.get("category") or detail.get("category"),
            "sub_category": summary.get("subCategory") or detail.get("subCategory"),
            "type": summary.get("type") or detail.get("type"),
            "city": summary.get("city") or detail.get("city"),
            "state": summary.get("state") or detail.get("state"),
            "country": summary.get("country") or detail.get("country"),
            "city_state_country": summary.get("cityStateCountry") or detail.get("cityStateCountry"),
            "posted_date": posted_date,
            "date_created": summary.get("dateCreated") or detail.get("dateCreated"),
            "description_teaser": summary.get("descriptionTeaser") or detail.get("descriptionTeaser"),
            "apply_url": apply_url,
            "external_apply": summary.get("externalApply") or detail.get("externalApply"),
            "multi_location": summary.get("multi_location"),
            "multi_location_array": summary.get("multi_location_array"),
            "ml_skills": summary.get("ml_skills") or detail.get("ml_skills"),
            "division": detail.get("division"),
            "company_name": detail.get("companyName"),
            "salary_range": detail.get("salaryRange"),
            "travel": detail.get("travel"),
            "job_profile": detail.get("jobProfile"),
            "additional_locations": detail.get("additionalJobPostingLocations"),
            "boiler_plate_text": detail.get("boilerPlateText"),
            "raw_structure": {
                "description": structure.get("description"),
                "employmentType": structure.get("employmentType"),
                "jobLocation": structure.get("jobLocation"),
                "identifier": structure.get("identifier"),
            },
            "description_html": description_html,
        }

        detail_link = f"{BASE_DOMAIN}{JOB_DETAIL_PATH.format(job_seq=job_seq)}"

        return JobListing(
            job_seq=job_seq,
            title=title,
            link=detail_link,
            location=location,
            posted_date=posted_date,
            apply_url=apply_url,
            description_text=description_text,
            description_html=description_html,
            metadata=metadata,
        )


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------
def store_listing(listing: JobListing) -> None:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255],
        "date": (listing.posted_date or "")[:100],
        "description": listing.description_text[:10000],
        "metadata": listing.metadata,
    }
    try:
        JobPosting.objects.update_or_create(
            scraper=SCRAPER,
            link=listing.link,
            defaults=defaults,
        )
    except IntegrityError as exc:
        raise ScraperError(f"Failed to store job {listing.job_seq}: {exc}") from exc


# ---------------------------------------------------------------------------
# CLI orchestration
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Abbott careers manual scraper")
    parser.add_argument("--max-pages", type=int, default=None, help="Maximum number of listing pages to fetch")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of job records to process")
    parser.add_argument("--delay", type=float, default=0.0, help="Delay in seconds between paginated requests")
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity",
    )
    return parser.parse_args(argv)


def run_scrape(args: argparse.Namespace) -> Dict[str, object]:
    client = AbbottCareersClient(delay=args.delay)
    processed = 0
    for listing in client.iter_listings(max_pages=args.max_pages, max_results=args.limit):
        store_listing(listing)
        processed += 1
    dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
    return {"processed_jobs": processed, "deduplicated": dedupe_summary}


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
        "company": "Abbott",
        "site": ROOT_LANDING_URL,
        "elapsed_seconds": round(duration, 2),
        **outcome,
    }
    logging.info("Summary: %s", json.dumps(summary))
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
