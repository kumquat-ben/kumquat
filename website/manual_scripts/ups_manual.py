#!/usr/bin/env python3
"""Manual scraper for UPS careers (jobs-ups.com).

This script mimics other manual scrapers in the repo: it walks the public
search-results pages, extracts the embedded `phApp.ddo` JSON payload that
contains job listings, and stores the jobs using Django models. It also
grabs key metadata (locations, categories, pay info) straight from the JSON.
"""
from __future__ import annotations

import argparse
import html
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, asdict
from typing import Dict, Iterable, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Django setup
# ---------------------------------------------------------------------------
BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "website.settings")

import django  # noqa: E402

django.setup()

from django.conf import settings  # noqa: E402

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_URL = "https://www.jobs-ups.com"
SEARCH_PATH = "/us/en/search-results"
DEFAULT_PAGE_SIZE = 10  # UPS site paginates in batches of 10 via ?from=<offset>
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.8",
    "Referer": urljoin(BASE_URL, SEARCH_PATH),
}

SCRAPER_QS = Scraper.objects.filter(company="UPS", url=urljoin(BASE_URL, SEARCH_PATH)).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple UPS scraper entries found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="UPS",
        url=urljoin(BASE_URL, SEARCH_PATH),
        code="manual-script",
        interval_hours=24,
        timeout_seconds=max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 60),
    )


class ScraperError(Exception):
    """Raised when the scraper cannot recover from an error."""


@dataclass
class JobSummary:
    job_id: str
    title: str
    detail_url: str
    location: Optional[str]
    city: Optional[str]
    state: Optional[str]
    country: Optional[str]
    employment_type: Optional[str]
    category: Optional[str]
    posted_date: Optional[str]
    apply_url: Optional[str]
    description: Optional[str]
    metadata: Dict[str, object]


class UPSJobScraper:
    def __init__(self, *, delay: float = 0.25, session: Optional[requests.Session] = None) -> None:
        self.delay = max(delay, 0.0)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    def scrape(self, *, limit: Optional[int] = None) -> Iterable[JobSummary]:
        offset = 0
        yielded = 0

        while True:
            page_jobs = self._fetch_jobs_page(offset)
            if not page_jobs:
                self.logger.info("No jobs returned at offset %s; stopping.", offset)
                break

            for job in page_jobs:
                yield job
                yielded += 1
                if limit is not None and yielded >= limit:
                    self.logger.info("Reached limit=%s; stopping scrape.", limit)
                    return

            if len(page_jobs) < DEFAULT_PAGE_SIZE:
                self.logger.info("Last page returned %s jobs (< %s); stopping.", len(page_jobs), DEFAULT_PAGE_SIZE)
                break

            offset += DEFAULT_PAGE_SIZE
            if self.delay:
                time.sleep(self.delay)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _fetch_jobs_page(self, offset: int) -> List[JobSummary]:
        params = {"from": offset, "s": 1} if offset else None
        self.logger.debug("Fetching UPS search results offset=%s", offset)
        response = self.session.get(urljoin(BASE_URL, SEARCH_PATH), params=params, timeout=45)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        json_payload = self._extract_ddo_json(soup)
        if json_payload is None:
            raise ScraperError("Failed to locate embedded phApp.ddo JSON payload.")

        jobs_data = json_payload.get("eagerLoadRefineSearch", {}).get("data", {}).get("jobs", []) or []
        summaries: List[JobSummary] = []
        for raw in jobs_data:
            summary = self._build_summary(raw)
            summaries.append(summary)
        self.logger.debug("Extracted %s job summaries at offset=%s", len(summaries), offset)
        return summaries

    def _extract_ddo_json(self, soup: BeautifulSoup) -> Optional[dict]:
        script_tags = soup.find_all("script")
        target_text = None
        for script in script_tags:
            if not script.string:
                continue
            unescaped = html.unescape(script.string)
            if "phApp.ddo" in unescaped:
                target_text = unescaped
                break

        if not target_text:
            return None

        try:
            start = target_text.index("phApp.ddo = ") + len("phApp.ddo = ")
            end = target_text.index("; phApp.experimentData")
        except ValueError:
            return None

        payload = target_text[start:end]
        try:
            return json.loads(payload)
        except json.JSONDecodeError as exc:
            self.logger.error("Failed to decode phApp.ddo JSON: %s", exc)
            return None

    def _build_summary(self, job: Dict[str, object]) -> JobSummary:
        job_id = str(job.get("jobId") or job.get("reqId") or "")
        title = (job.get("title") or "").strip()
        apply_url = (job.get("applyUrl") or "") or None
        detail_url = apply_url[:-5] if apply_url and apply_url.endswith("/apply") else (apply_url or "")

        location_text = job.get("cityStateCountry") or job.get("location") or None
        posted_date = self._normalize_date(job.get("postedDate"))
        description = job.get("descriptionTeaser") or ""

        metadata = {
            "jobSeqNo": job.get("jobSeqNo"),
            "cityStateCountry": job.get("cityStateCountry"),
            "city": job.get("city"),
            "state": job.get("state"),
            "country": job.get("country"),
            "employmentType": job.get("employmentType"),
            "category": job.get("category"),
            "type": job.get("type"),
            "location": job.get("location"),
            "address": job.get("address"),
            "latitude": job.get("latitude"),
            "longitude": job.get("longitude"),
            "badge": job.get("badge"),
            "ml_skills": job.get("ml_skills"),
            "raw": job,
        }

        return JobSummary(
            job_id=job_id,
            title=title,
            detail_url=detail_url or "",
            location=location_text,
            city=job.get("city"),
            state=job.get("state"),
            country=job.get("country"),
            employment_type=job.get("employmentType"),
            category=job.get("category"),
            posted_date=posted_date,
            apply_url=apply_url,
            description=description,
            metadata=metadata,
        )

    @staticmethod
    def _normalize_date(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        # Format looks like 2025-10-16T00:00:00.000+0000
        # Keep only the date part
        return value.split("T", 1)[0]


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------
def persist_listing(listing: JobSummary) -> bool:
    job_url = listing.detail_url or f"{BASE_URL}/us/en/job/{listing.job_id}"
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255] or None,
        "date": (listing.posted_date or "")[:100] or None,
        "description": (listing.description or "")[:10000],
        "metadata": listing.metadata,
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=job_url,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Persisted UPS job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape UPS job listings.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument(
        "--delay",
        type=float,
        default=0.25,
        help="Delay between page requests in seconds (default: 0.25).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print jobs as JSON without writing to the database.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s: %(message)s")

    scraper = UPSJobScraper(delay=args.delay)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    try:
        for job in scraper.scrape(limit=args.limit):
            totals["fetched"] += 1
            if args.dry_run:
                print(json.dumps(asdict(job), default=str, ensure_ascii=False))
                continue
            try:
                created = persist_listing(job)
                if created:
                    totals["created"] += 1
                else:
                    totals["updated"] += 1
            except Exception as exc:  # pragma: no cover - persistence failure
                logging.error("Failed to persist job %s: %s", job.job_id, exc)
                totals["errors"] += 1
    except ScraperError as exc:
        logging.error("UPS scraper stopped due to error: %s", exc)
        totals["errors"] += 1

    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["dedupe"] = dedupe_summary

    logging.info(
        "UPS scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )
    return 0 if not totals["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
