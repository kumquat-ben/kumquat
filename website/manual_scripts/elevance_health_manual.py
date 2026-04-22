#!/usr/bin/env python3
"""Manual scraper for Elevance Health careers (Workday-powered).

This script paginates through the public Workday job listings API backing
https://careers.elevancehealth.com, enriches each summary with detail data,
and stores the resulting payloads via the Django ORM.
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
# Constants & configuration
# ---------------------------------------------------------------------------
CAREERS_URL = "https://careers.elevancehealth.com"
WORKDAY_ROOT = "https://elevancehealth.wd1.myworkdayjobs.com"
TENANT = "elevancehealth"
PORTAL = "ANT"
CXS_BASE = f"{WORKDAY_ROOT}/wday/cxs/{TENANT}/{PORTAL}"
JOBS_ENDPOINT = f"{CXS_BASE}/jobs"
JOB_DETAIL_BASE = f"{WORKDAY_ROOT}/{PORTAL}"
SESSION_SEED_URL = f"{WORKDAY_ROOT}/en-US/{PORTAL}"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 30)

SCRAPER_QS = Scraper.objects.filter(company="Elevance Health", url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Elevance Health scrapers found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Elevance Health",
        url=CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the scraping pipeline encounters an unrecoverable error."""


@dataclass
class JobSummary:
    job_id: Optional[str]
    title: str
    detail_path: str
    detail_url: str
    location_text: Optional[str]
    posted_on: Optional[str]


@dataclass
class JobListing(JobSummary):
    description_text: str
    description_html: Optional[str]
    apply_url: Optional[str]
    date_posted: Optional[str]
    metadata: Dict[str, object]


class ElevanceHealthJobScraper:
    def __init__(
        self,
        *,
        page_size: int = 20,
        delay: float = 0.25,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.page_size = max(1, page_size)
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
                listing = self._enrich_summary(summary)
            except Exception as exc:  # pragma: no cover - defensive logging
                self.logger.error("Failed to enrich %s: %s", summary.detail_url, exc)
                continue
            yield listing
            fetched += 1
            if limit is not None and fetched >= limit:
                return

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _iter_summaries(self, *, limit: Optional[int]) -> Iterable[JobSummary]:
        offset = 0
        retrieved = 0
        total: Optional[int] = None

        self._ensure_session_bootstrap()

        while True:
            payload = {
                "limit": self.page_size,
                "offset": offset,
                "searchText": "",
                "appliedFacets": {},
                "userPreferredLanguage": "en-US",
            }
            self.logger.debug("Requesting jobs offset=%s", offset)
            response = self.session.post(JOBS_ENDPOINT, json=payload, timeout=40)
            if response.status_code == 400 and not self._bootstrapped:
                self.logger.info("Workday jobs request returned 400; retrying after bootstrap")
                self._ensure_session_bootstrap(force=True)
                response = self.session.post(JOBS_ENDPOINT, json=payload, timeout=40)

            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                snippet = response.text[:200].strip()
                raise ScraperError(f"Workday jobs request failed: {exc} :: {snippet}") from exc

            data = response.json()
            job_postings = data.get("jobPostings") or []
            if not job_postings:
                self.logger.info("No job postings returned at offset %s; stopping.", offset)
                return

            if total is None:
                total = _safe_int(data.get("total"))

            for raw in job_postings:
                detail_path = (raw.get("externalPath") or "").strip()
                if not detail_path:
                    continue

                detail_url = urljoin(f"{JOB_DETAIL_BASE.rstrip('/')}/", detail_path.lstrip("/"))
                title = (raw.get("title") or "").strip()
                if not title:
                    self.logger.debug("Skipping job with missing title: %s", raw)
                    continue

                job_id = None
                bullet_fields = raw.get("bulletFields") or []
                if bullet_fields:
                    job_id = (bullet_fields[0] or "").strip() or None

                summary = JobSummary(
                    job_id=job_id,
                    title=title,
                    detail_path=detail_path,
                    detail_url=detail_url,
                    location_text=_strip_or_none(raw.get("locationsText")),
                    posted_on=_strip_or_none(raw.get("postedOn")),
                )
                yield summary
                retrieved += 1
                if limit is not None and retrieved >= limit:
                    return

            offset += self.page_size
            if total is not None and offset >= total:
                self.logger.info("Reached reported Workday total (%s); stopping.", total)
                return

            if self.delay:
                time.sleep(self.delay)

    def _enrich_summary(self, summary: JobSummary) -> JobListing:
        detail_json = self._fetch_detail_json(summary.detail_path)
        posting_info = detail_json.get("jobPostingInfo") or {}

        description_html = (posting_info.get("jobDescription") or "").strip() or None
        description_text = _html_to_text(description_html) if description_html else ""
        if not description_text:
            description_text = summary.title

        date_posted = (
            _strip_or_none(posting_info.get("startDate"))
            or _strip_or_none(posting_info.get("postedOn"))
            or summary.posted_on
        )

        primary_location = (
            _strip_or_none(posting_info.get("location")) or summary.location_text or ""
        )

        metadata_posting = dict(posting_info)
        if "jobDescription" in metadata_posting:
            metadata_posting["jobDescription"] = None
        metadata_posting.setdefault("additionalLocations", detail_json.get("jobPostingInfo", {}).get("additionalLocations", []))

        location_value = primary_location or summary.location_text

        metadata = {
            "job_id": summary.job_id,
            "detail_path": summary.detail_path,
            "posted_on_text": summary.posted_on,
            "locations_text": summary.location_text,
            "job_posting_info": metadata_posting,
            "hiringOrganization": detail_json.get("hiringOrganization"),
            "primary_location": location_value,
        }
        if detail_json.get("similarJobs"):
            metadata["similarJobs"] = detail_json["similarJobs"]

        summary_data = summary.__dict__.copy()
        summary_data["location_text"] = location_value

        return JobListing(
            **summary_data,
            description_text=description_text,
            description_html=description_html,
            apply_url=_strip_or_none(posting_info.get("externalUrl")),
            date_posted=date_posted,
            metadata=metadata,
        )

    def _fetch_detail_json(self, detail_path: str) -> Dict[str, object]:
        if not detail_path:
            raise ScraperError("Missing Workday detail path")
        api_url = urljoin(f"{CXS_BASE.rstrip('/')}/", detail_path.lstrip("/"))
        response = self.session.get(api_url, timeout=40)
        if response.status_code == 400 and not self._bootstrapped:
            self.logger.info("Workday detail request returned 400; retry after bootstrap")
            self._ensure_session_bootstrap(force=True)
            response = self.session.get(api_url, timeout=40)
        response.raise_for_status()
        try:
            data = response.json()
        except ValueError as exc:
            raise ScraperError(f"Failed to parse detail JSON at {api_url}") from exc
        if not isinstance(data, dict):
            raise ScraperError("Unexpected detail response structure")
        return data

    def _ensure_session_bootstrap(self, force: bool = False) -> None:
        if self._bootstrapped and not force:
            return
        self.logger.debug("Bootstrapping session with %s", SESSION_SEED_URL)
        resp = self.session.get(SESSION_SEED_URL, timeout=40)
        resp.raise_for_status()
        self._bootstrapped = True


def persist_listing(listing: JobListing) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location_text or "")[:255] or None,
        "date": (listing.date_posted or "")[:100] or None,
        "description": listing.description_text[:10000],
        "metadata": {
            **listing.metadata,
            "description_html": listing.description_html,
            "apply_url": listing.apply_url,
        },
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Persisted Elevance Health job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape Elevance Health Workday-backed job listings."
    )
    parser.add_argument("--limit", type=int, default=None, help="Maximum jobs to process.")
    parser.add_argument(
        "--page-size", type=int, default=20, help="Number of jobs requested per Workday page."
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.25,
        help="Seconds to sleep between Workday pagination requests.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Display job payloads without writing to the database.",
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

    scraper = ElevanceHealthJobScraper(page_size=args.page_size, delay=args.delay)
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
        except Exception as exc:  # pragma: no cover - persistence failure path
            logging.error("Failed to persist job %s: %s", listing.detail_url, exc)
            totals["errors"] += 1

    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["dedupe"] = dedupe_summary

    logging.info(
        "Elevance Health scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )
    print(json.dumps(totals))
    return 0 if not totals["errors"] else 1


def _strip_or_none(value: Optional[object]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _safe_int(value: Optional[object]) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _html_to_text(value: Optional[str]) -> str:
    if not value:
        return ""
    soup = BeautifulSoup(value, "html.parser")
    text = soup.get_text("\n", strip=True)
    return text.replace("\u202f", " ").replace("\xa0", " ").strip()


if __name__ == "__main__":
    raise SystemExit(main())
