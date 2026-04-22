#!/usr/bin/env python3
"""Manual scraper for https://www.thecloroxcompany.com/careers.

The Clorox Company exposes a public JSON job-search API that powers the
consumer careers page. This script pulls the job summaries, hydrates each row
with the detailed payload, and persists the results via the Django ORM so the
operations dashboard can refresh Clorox postings on demand.
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
from typing import Dict, Iterator, List, Optional

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
from django.db import IntegrityError  # noqa: E402

django.setup()

from django.conf import settings  # noqa: E402

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402


# ---------------------------------------------------------------------------
# Constants & configuration
# ---------------------------------------------------------------------------
ROOT_LANDING_URL = "https://www.thecloroxcompany.com/careers"
BASE_API_URL = "https://api.clorox.com/job-search/v1"
JOB_QUERY_URL = f"{BASE_API_URL}/job-query"
JOB_DETAIL_URL = f"{BASE_API_URL}/job-detail"
REQUEST_TIMEOUT = (15, 45)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.thecloroxcompany.com",
    "Referer": ROOT_LANDING_URL,
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 30)
SCRAPER_QS = Scraper.objects.filter(company="The Clorox Company", url=ROOT_LANDING_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched; using the earliest (id=%s).", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="The Clorox Company",
        url=ROOT_LANDING_URL,
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
    locations: List[str]
    location_text: Optional[str]
    posted_date: Optional[str]
    category: Optional[str]
    job_type: Optional[str]
    apply_url: Optional[str]
    description_text: str
    description_html: Optional[str]
    metadata: Dict[str, object]


def _html_to_text(value: Optional[str]) -> str:
    if not value:
        return ""
    soup = BeautifulSoup(value, "html.parser")
    return soup.get_text("\n", strip=True).strip()


def _normalize_locations(raw: object) -> List[str]:
    values: List[str] = []
    if isinstance(raw, (list, tuple)):
        source = list(raw)
    elif isinstance(raw, str):
        source = [raw]
    else:
        source = []

    seen = set()
    for item in source:
        text = (item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        values.append(text)
    return values


class CloroxCareersClient:
    def __init__(self, delay: float = 0.0, session: Optional[requests.Session] = None) -> None:
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    def iter_listings(self, *, limit: Optional[int] = None) -> Iterator[JobListing]:
        summaries = self._fetch_job_summaries()
        yielded = 0
        for summary in summaries:
            job_id = str(summary.get("Job_Requisition_Reference") or "").strip()
            if not job_id:
                self.logger.debug("Skipping summary without requisition reference: %s", summary)
                continue

            detail = self._fetch_job_detail(job_id)
            listing = self._build_listing(summary, detail)
            yield listing
            yielded += 1

            if limit is not None and yielded >= limit:
                self.logger.info("Reached limit %s; stopping iteration.", limit)
                break

            if self.delay:
                time.sleep(self.delay)

    # ------------------------------------------------------------------ #
    # HTTP helpers                                                       #
    # ------------------------------------------------------------------ #
    def _fetch_job_summaries(self) -> List[Dict[str, object]]:
        try:
            response = self.session.get(JOB_QUERY_URL, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch job summaries: {exc}") from exc

        try:
            payload = response.json()
        except ValueError as exc:
            raise ScraperError(f"Unable to decode job summaries payload: {exc}") from exc

        if not isinstance(payload, list):
            raise ScraperError("Unexpected job summaries payload; expected a list.")

        return payload

    def _fetch_job_detail(self, job_id: str) -> Dict[str, object]:
        params = {"job_unique_identifier": job_id}
        try:
            response = self.session.get(JOB_DETAIL_URL, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch job detail for {job_id}: {exc}") from exc

        try:
            payload = response.json()
        except ValueError as exc:
            raise ScraperError(f"Unable to decode job detail payload for {job_id}: {exc}") from exc

        if not isinstance(payload, dict):
            raise ScraperError(f"Unexpected job detail payload for {job_id}; expected an object.")

        return payload

    # ------------------------------------------------------------------ #
    # Transformation helpers                                             #
    # ------------------------------------------------------------------ #
    def _build_listing(self, summary: Dict[str, object], detail: Dict[str, object]) -> JobListing:
        job_id = str(detail.get("Job_Requisition_Reference") or summary.get("Job_Requisition_Reference") or "").strip()
        title = (
            str(detail.get("Job_Posting_Title") or summary.get("Job_Posting_Title") or "").strip()
        )

        locations = _normalize_locations(detail.get("Job_Location") or summary.get("Job_Location"))
        location_text = "; ".join(locations) if locations else None
        posted_date = str(
            detail.get("Recruiting_Start_Date") or summary.get("Recruiting_Start_Date") or ""
        ).strip() or None
        category = (detail.get("Job_Category") or summary.get("Job_Category") or None) or None
        job_type = (detail.get("Job_type") or summary.get("Job_type") or None) or None
        apply_url = (
            str(detail.get("External_Apply_URL") or detail.get("External_Job_Path") or "").strip()
            or None
        )
        link = (
            str(detail.get("External_Job_Path") or detail.get("External_Apply_URL") or "").strip()
            or f"{ROOT_LANDING_URL}#job-{job_id}"
        )

        description_html = detail.get("Job_Description") or None
        description_text = _html_to_text(description_html)

        metadata: Dict[str, object] = {
            "job_id": job_id or None,
            "category": category,
            "job_type": job_type,
            "locations": locations,
            "posted_date": posted_date,
            "apply_url": apply_url,
            "summary_payload": summary,
            "detail_payload": detail,
            "source": {
                "query_url": JOB_QUERY_URL,
                "detail_url": JOB_DETAIL_URL,
            },
        }

        return JobListing(
            job_id=job_id,
            title=title,
            link=link,
            locations=locations,
            location_text=location_text,
            posted_date=posted_date,
            category=category,
            job_type=job_type,
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
        "location": (listing.location_text or "")[:255],
        "date": (listing.posted_date or "")[:100],
        "description": listing.description_text[:10000],
        "metadata": {**listing.metadata, "description_html": listing.description_html},
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
# CLI orchestration
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="The Clorox Company careers manual scraper")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of job records to process")
    parser.add_argument("--delay", type=float, default=0.0, help="Delay in seconds between detail requests")
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity",
    )
    return parser.parse_args(argv)


def run_scrape(args: argparse.Namespace) -> Dict[str, object]:
    client = CloroxCareersClient(delay=args.delay)
    processed = 0
    for listing in client.iter_listings(limit=args.limit):
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
        "company": "The Clorox Company",
        "site": ROOT_LANDING_URL,
        "elapsed_seconds": round(duration, 2),
        **outcome,
    }
    logging.info("Summary: %s", json.dumps(summary))
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
