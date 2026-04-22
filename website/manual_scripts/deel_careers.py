#!/usr/bin/env python3
"""Custom scraper for https://www.deel.com/careers (Ashby-powered job board)."""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Generator, Iterable, List, Optional

import requests

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

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CAREERS_URL = "https://www.deel.com/careers"
JOB_BOARD_URL = "https://jobs.ashbyhq.com/Deel"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 30)

SCRAPER_QS = Scraper.objects.filter(company="Deel", url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Deel scrapers found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Deel",
        url=CAREERS_URL,
        code="custom-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the scraper cannot continue."""


@dataclass
class JobSummary:
    posting_id: str
    title: str
    detail_url: str
    department: Optional[str]
    team: Optional[str]
    location: Optional[str]
    workplace_type: Optional[str]
    employment_type: Optional[str]
    published_date: Optional[str]
    job_id: Optional[str]
    requisition_id: Optional[str]
    secondary_locations: List[str]
    is_remote: Optional[bool]


@dataclass
class JobListing(JobSummary):
    description_text: Optional[str]
    description_html: Optional[str]
    metadata: Dict[str, object]


class DeelJobScraper:
    def __init__(self, delay: float = 0.2, session: Optional[requests.Session] = None) -> None:
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    def scrape(self, limit: Optional[int] = None) -> Generator[JobListing, None, None]:
        summaries = list(self._fetch_job_summaries())
        self.logger.info("Discovered %s job postings", len(summaries))

        yielded = 0
        for summary in summaries:
            detail = self._fetch_job_detail(summary.detail_url)
            listing = JobListing(**asdict(summary), **detail)
            yield listing
            yielded += 1
            if limit is not None and yielded >= limit:
                self.logger.info("Reached limit %s; stopping scrape", limit)
                return
            if self.delay:
                time.sleep(self.delay)

    # ------------------------------------------------------------------
    # Fetch + parse helpers
    # ------------------------------------------------------------------
    def _fetch_job_summaries(self) -> Iterable[JobSummary]:
        response = self.session.get(JOB_BOARD_URL, timeout=40)
        response.raise_for_status()
        app_data = _extract_app_data(response.text)
        job_board = app_data.get("jobBoard") or {}
        job_postings = job_board.get("jobPostings") or []

        if not isinstance(job_postings, list) or not job_postings:
            raise ScraperError("No job postings found in Ashby job board payload.")

        for posting in job_postings:
            if not posting.get("isListed", True):
                continue
            posting_id = posting.get("id")
            if not posting_id:
                continue
            secondary_locations = [
                loc.get("locationName")
                for loc in (posting.get("secondaryLocations") or [])
                if isinstance(loc, dict) and loc.get("locationName")
            ]
            yield JobSummary(
                posting_id=posting_id,
                title=posting.get("title") or "",
                detail_url=f"{JOB_BOARD_URL}/{posting_id}",
                department=posting.get("departmentName"),
                team=posting.get("teamName"),
                location=posting.get("locationName"),
                workplace_type=posting.get("workplaceType"),
                employment_type=posting.get("employmentType"),
                published_date=posting.get("publishedDate"),
                job_id=posting.get("jobId"),
                requisition_id=posting.get("jobRequisitionId"),
                secondary_locations=secondary_locations,
                is_remote=posting.get("workplaceType") == "Remote",
            )

    def _fetch_job_detail(self, url: str) -> Dict[str, Optional[object]]:
        response = self.session.get(url, timeout=40)
        response.raise_for_status()
        app_data = _extract_app_data(response.text)
        posting = app_data.get("posting") or {}

        description_text = posting.get("descriptionPlainText")
        description_html = posting.get("descriptionHtml")

        metadata = {
            "job_posting_id": posting.get("id"),
            "job_id": posting.get("jobId"),
            "job_requisition_id": posting.get("jobRequisitionId"),
            "department": posting.get("departmentName"),
            "team": posting.get("teamName"),
            "location": posting.get("locationName"),
            "secondary_locations": posting.get("secondaryLocationNames") or [],
            "workplace_type": posting.get("workplaceType"),
            "employment_type": posting.get("employmentType"),
            "published_date": posting.get("publishedDate"),
            "application_deadline": posting.get("applicationDeadline"),
            "is_remote": posting.get("isRemote"),
            "short_description": posting.get("shortDescription"),
            "compensation_summary": posting.get("scrapeableCompensationSalarySummary"),
            "updated_at": posting.get("updatedAt"),
        }

        return {
            "description_text": description_text,
            "description_html": description_html,
            "metadata": metadata,
        }


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _extract_app_data(html: str) -> Dict[str, object]:
    marker = "window.__appData = "
    idx = html.find(marker)
    if idx == -1:
        raise ScraperError("Ashby app data marker not found in HTML response.")

    start = idx + len(marker)
    depth = 0
    in_string = False
    escape = False
    end = None

    for i, ch in enumerate(html[start:], start=start):
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break

    if end is None:
        raise ScraperError("Failed to parse Ashby app data JSON payload.")

    payload = html[start:end]
    try:
        return json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ScraperError("Invalid Ashby app data JSON payload.") from exc


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def store_listing(listing: JobListing) -> None:
    JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults={
            "title": (listing.title or "")[:255],
            "location": (listing.location or "")[:255],
            "date": (listing.published_date or "")[:100],
            "description": (listing.description_text or listing.description_html or "")[:10000],
            "metadata": listing.metadata,
        },
    )


def run_scrape(limit: Optional[int], delay: float) -> int:
    scraper = DeelJobScraper(delay=delay)
    count = 0
    for job in scraper.scrape(limit=limit):
        store_listing(job)
        count += 1
    return count


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deel careers scraper")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--delay", type=float, default=0.2)
    parser.add_argument("--log-level", type=str, default="INFO", choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"])
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    start = time.time()
    try:
        count = run_scrape(args.limit, args.delay)
    except ScraperError as exc:
        logging.error("Scrape failed: %s", exc)
        return 1
    dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
    duration = time.time() - start
    summary = {
        "company": "Deel",
        "url": CAREERS_URL,
        "count": count,
        "elapsed_seconds": duration,
        "dedupe": dedupe_summary,
    }
    logging.info("Summary: %s", json.dumps(summary))
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
