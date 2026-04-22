#!/usr/bin/env python3
"""Manual scraper for Extra Space Storage careers (Workday-powered)."""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Generator, Iterable, Optional
from urllib.parse import urljoin

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
CAREERS_URL = "https://careers.extraspace.com"
WORKDAY_ROOT = "https://extraspace.wd5.myworkdayjobs.com"
TENANT = "extraspace"
PORTAL = "ESS_External"
SESSION_SEED_URL = f"{WORKDAY_ROOT}/en-US/{PORTAL}"
CXS_BASE = f"{WORKDAY_ROOT}/wday/cxs/{TENANT}/{PORTAL}"
JOBS_ENDPOINT = f"{CXS_BASE}/jobs"
JOB_DETAIL_BASE = CXS_BASE
JOB_PAGE_BASE = f"{WORKDAY_ROOT}/{PORTAL}"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": JOB_PAGE_BASE,
}

REQUEST_TIMEOUT = (15, 45)
MAX_PAGE_SIZE = 20
DEFAULT_PAGE_SIZE = MAX_PAGE_SIZE
DEFAULT_DELAY = 0.25
DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 1800), 120)

SCRAPER_QS = Scraper.objects.filter(company="Extra Space Storage", url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Extra Space Storage scraper rows found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Extra Space Storage",
        url=CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the Extra Space Storage scraper cannot proceed."""


@dataclass
class JobSummary:
    job_id: Optional[str]
    title: str
    external_path: str
    detail_url: str
    location_text: Optional[str]
    posted_on: Optional[str]


@dataclass
class JobListing(JobSummary):
    description: str
    date_posted: Optional[str]
    metadata: Dict[str, object]


def _html_to_text(value: Optional[str]) -> str:
    if not value:
        return ""
    soup = BeautifulSoup(value, "html.parser")
    return soup.get_text("\n", strip=True).strip()


def _clean_metadata(source: Dict[str, object]) -> Dict[str, object]:
    return {key: value for key, value in source.items() if value not in (None, "", [], {})}


class ExtraSpaceWorkdayScraper:
    def __init__(
        self,
        *,
        page_size: int = DEFAULT_PAGE_SIZE,
        delay: float = DEFAULT_DELAY,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.page_size = max(1, min(page_size, MAX_PAGE_SIZE))
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)
        self._bootstrapped = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def scrape(self, *, limit: Optional[int] = None) -> Generator[JobListing, None, None]:
        fetched = 0
        for summary in self._iter_summaries(limit=limit):
            try:
                listing = self._build_listing(summary)
            except Exception as exc:  # pragma: no cover - defensive logging
                self.logger.error("Failed to build listing for %s: %s", summary.detail_url, exc)
                continue
            yield listing
            fetched += 1
            if limit is not None and fetched >= limit:
                return
            if self.delay:
                time.sleep(self.delay)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _iter_summaries(self, *, limit: Optional[int]) -> Iterable[JobSummary]:
        offset = 0
        retrieved = 0
        total: Optional[int] = None

        while True:
            payload = {
                "limit": self.page_size,
                "offset": offset,
                "searchText": "",
                "appliedFacets": {},
                "userPreferredLanguage": "en-US",
            }
            self.logger.debug("Requesting Workday jobs offset=%s", offset)
            response = self.session.post(JOBS_ENDPOINT, json=payload, timeout=REQUEST_TIMEOUT[1])
            if response.status_code == 400 and not self._bootstrapped:
                self.logger.info("Retrying Workday jobs request after session bootstrap.")
                self._ensure_session_bootstrap(force=True)
                response = self.session.post(
                    JOBS_ENDPOINT, json=payload, timeout=REQUEST_TIMEOUT[1]
                )
            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                snippet = response.text[:200].strip()
                self.logger.error(
                    "Workday jobs request failed (%s): %s", response.status_code, snippet
                )
                raise ScraperError(f"Workday jobs request failed: {exc}") from exc

            data = response.json()
            postings = data.get("jobPostings") or []
            if not postings:
                self.logger.info("No job postings returned at offset %s; stopping.", offset)
                return

            if total is None:
                try:
                    total = int(data.get("total") or 0)
                except (TypeError, ValueError):
                    total = None

            for raw in postings:
                external_path = (raw.get("externalPath") or "").strip()
                if not external_path:
                    self.logger.debug("Skipping job without externalPath: %s", raw)
                    continue
                detail_url = (
                    external_path
                    if external_path.startswith("http")
                    else urljoin(f"{JOB_PAGE_BASE.rstrip('/')}/", external_path.lstrip("/"))
                )
                summary = JobSummary(
                    job_id=(raw.get("bulletFields") or [None])[0],
                    title=(raw.get("title") or "").strip(),
                    external_path=external_path,
                    detail_url=detail_url,
                    location_text=(raw.get("locationsText") or "").strip() or None,
                    posted_on=(raw.get("postedOn") or "").strip() or None,
                )
                if not summary.title or not summary.detail_url:
                    self.logger.debug("Skipping incomplete job summary: %s", raw)
                    continue
                yield summary
                retrieved += 1
                if limit is not None and retrieved >= limit:
                    return

            offset += len(postings)
            if total is not None and offset >= total:
                self.logger.info("Reached reported total (%s); pagination complete.", total)
                return
            if self.delay:
                time.sleep(self.delay)

    def _build_listing(self, summary: JobSummary) -> JobListing:
        detail_payload = self._fetch_job_detail(summary.external_path)
        info = detail_payload.get("jobPostingInfo") or {}
        description_html = info.get("jobDescription") or ""
        description_text = _html_to_text(description_html)
        metadata = _clean_metadata(
            {
                "job_req_id": info.get("jobReqId") or summary.job_id,
                "job_posting_id": info.get("jobPostingId"),
                "job_posting_site_id": info.get("jobPostingSiteId"),
                "time_type": info.get("timeType"),
                "start_date": info.get("startDate"),
                "posted_on": info.get("postedOn") or summary.posted_on,
                "workday_location": info.get("location"),
                "job_requisition_location": (
                    (info.get("jobRequisitionLocation") or {}).get("descriptor")
                ),
                "job_requisition_country": (
                    (info.get("jobRequisitionLocation") or {})
                    .get("country", {})
                    .get("descriptor")
                ),
                "country": (info.get("country") or {}).get("descriptor"),
                "apply_url": info.get("externalUrl") or summary.detail_url,
                "questionnaire_id": info.get("questionnaireId"),
                "include_resume_parsing": info.get("includeResumeParsing"),
            }
        )
        listing_data = summary.__dict__.copy()
        listing_data.update(
            {
                "description": description_text,
                "date_posted": info.get("startDate") or summary.posted_on,
                "metadata": metadata,
            }
        )
        return JobListing(**listing_data)

    def _fetch_job_detail(self, external_path: str) -> Dict[str, object]:
        url = urljoin(f"{JOB_DETAIL_BASE.rstrip('/')}/", external_path.lstrip("/"))
        self.logger.debug("Fetching job detail JSON %s", url)
        response = self.session.get(url, timeout=REQUEST_TIMEOUT[1])
        if response.status_code == 404:
            raise ScraperError(f"Job detail endpoint returned 404 for {external_path}")
        response.raise_for_status()
        return response.json()

    def _ensure_session_bootstrap(self, force: bool = False) -> None:
        if self._bootstrapped and not force:
            return
        self.logger.debug("Bootstrapping Workday session via %s", SESSION_SEED_URL)
        response = self.session.get(SESSION_SEED_URL, timeout=REQUEST_TIMEOUT[1])
        response.raise_for_status()
        self._bootstrapped = True


def persist_listing(listing: JobListing) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location_text or "")[:255] or None,
        "date": (listing.date_posted or listing.posted_on or "")[:100] or None,
        "description": listing.description[:10000],
        "metadata": listing.metadata,
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Persisted Extra Space Storage job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Extra Space Storage careers job listings.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help="Jobs to request per Workday page (max 20).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help="Seconds to sleep between Workday requests.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch jobs without writing to the database.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s: %(message)s")

    scraper = ExtraSpaceWorkdayScraper(page_size=args.page_size, delay=args.delay)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    for listing in scraper.scrape(limit=args.limit):
        totals["fetched"] += 1
        if args.dry_run:
            print(json.dumps(listing.__dict__, default=str, ensure_ascii=False))
            continue

        try:
            created = persist_listing(listing)
            if created:
                totals["created"] += 1
            else:
                totals["updated"] += 1
        except Exception as exc:  # pragma: no cover - persistence safeguards
            logging.error("Failed to persist job %s: %s", listing.detail_url, exc)
            totals["errors"] += 1

    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["dedupe"] = dedupe_summary

    logging.info(
        "Extra Space Storage scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )
    return 0 if not totals["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
