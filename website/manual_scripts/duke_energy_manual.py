#!/usr/bin/env python3
"""Manual scraper for Duke Energy careers (Workday-powered).

This script calls the public Workday JSON endpoints backing
https://www.duke-energy.com/our-company/careers (tenant ``dukeenergy``).
It paginates job summaries, enriches each job via the detail API, and
persists the results into ``scrapers.JobPosting``.
"""
from __future__ import annotations

import argparse
import html
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
CAREERS_URL = "https://www.duke-energy.com/our-company/careers"
WORKDAY_ROOT = "https://dukeenergy.wd1.myworkdayjobs.com"
TENANT = "dukeenergy"
PORTAL = "Search"
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

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 900), 60)
SCRAPER_QS = Scraper.objects.filter(company="Duke Energy", url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched Duke Energy; using id=%s.", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Duke Energy",
        url=CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised for unrecoverable errors while scraping Duke Energy careers."""


@dataclass
class JobSummary:
    job_id: Optional[str]
    title: str
    detail_path: str
    detail_url: str
    location_text: Optional[str]
    posted_on: Optional[str]
    remote_type: Optional[str]


@dataclass
class JobListing(JobSummary):
    description: str
    date_posted: Optional[str]
    metadata: Dict[str, object]


class DukeEnergyJobScraper:
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
            except ScraperError as exc:
                self.logger.error("Failed to enrich job %s: %s", summary.detail_url, exc)
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
                self.logger.info("Jobs endpoint returned 400; retrying after rebootstrap.")
                self._ensure_session_bootstrap(force=True)
                response = self.session.post(JOBS_ENDPOINT, json=payload, timeout=40)
            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                snippet = response.text[:200].strip()
                raise ScraperError(f"Workday jobs request failed ({response.status_code}): {snippet}") from exc

            data = response.json()
            job_postings = data.get("jobPostings") or []
            if not job_postings:
                self.logger.info("No job postings returned at offset %s; stopping.", offset)
                return

            if total is None:
                try:
                    total = int(data.get("total") or 0)
                except (TypeError, ValueError):
                    total = None

            for raw in job_postings:
                detail_path = raw.get("externalPath") or ""
                if not detail_path:
                    self.logger.debug("Skipping posting without externalPath: %s", raw)
                    continue
                detail_url = (
                    detail_path
                    if detail_path.startswith("http")
                    else urljoin(f"{JOB_DETAIL_BASE.rstrip('/')}/", detail_path.lstrip("/"))
                )
                summary = JobSummary(
                    job_id=(raw.get("bulletFields") or [None])[0],
                    title=self._clean_text(raw.get("title")) or "",
                    detail_path=detail_path,
                    detail_url=detail_url,
                    location_text=self._clean_text(raw.get("locationsText")),
                    posted_on=self._clean_text(raw.get("postedOn")),
                    remote_type=self._clean_text(raw.get("remoteType")),
                )
                if not summary.title or not summary.detail_url:
                    self.logger.debug("Skipping invalid job summary payload: %s", raw)
                    continue
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
        info: Dict[str, object] = detail_json.get("jobPostingInfo") or {}

        description_html = info.get("jobDescription") if isinstance(info, dict) else ""
        description_text = self._normalize_description(description_html) or "Description unavailable."

        location_candidate = None
        if isinstance(info, dict):
            location_candidate = self._clean_text(info.get("location"))
        location_text = location_candidate or summary.location_text

        posted_on_detail = self._clean_text(info.get("postedOn")) if isinstance(info, dict) else None
        date_posted = posted_on_detail or summary.posted_on

        metadata = self._build_metadata(summary=summary, info=info, detail=detail_json)

        base = summary.__dict__.copy()
        base["location_text"] = location_text

        return JobListing(
            **base,
            description=description_text,
            date_posted=date_posted,
            metadata=metadata,
        )

    def _fetch_detail_json(self, detail_path: str) -> Dict[str, object]:
        detail_url = urljoin(f"{CXS_BASE.rstrip('/')}/", detail_path.lstrip("/"))
        response = self.session.get(detail_url, timeout=40)
        if response.status_code == 404:
            raise ScraperError(f"Job detail endpoint returned 404 for path {detail_path!r}")
        if response.status_code == 400 and not self._bootstrapped:
            self.logger.info("Detail endpoint returned 400; attempting rebootstrap.")
            self._ensure_session_bootstrap(force=True)
            response = self.session.get(detail_url, timeout=40)
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            snippet = response.text[:200].strip()
            raise ScraperError(
                f"Workday detail request failed ({response.status_code}): {snippet}"
            ) from exc

        try:
            return response.json()
        except json.JSONDecodeError as exc:
            raise ScraperError(f"Failed to parse job detail JSON for {detail_path!r}: {exc}") from exc

    def _build_metadata(
        self,
        *,
        summary: JobSummary,
        info: Dict[str, object],
        detail: Dict[str, object],
    ) -> Dict[str, object]:
        if not isinstance(info, dict):
            info = {}
        metadata: Dict[str, object] = {
            "job_id": summary.job_id,
            "job_req_id": info.get("jobReqId"),
            "job_posting_id": info.get("jobPostingId"),
            "job_posting_site_id": info.get("jobPostingSiteId"),
            "detail_path": summary.detail_path,
            "remote_type": summary.remote_type or info.get("remoteType"),
            "additional_locations": info.get("additionalLocations"),
            "time_type": info.get("timeType"),
            "time_left_to_apply": info.get("timeLeftToApply"),
            "job_posting_end": info.get("jobPostingEndDateAsText"),
            "external_url": info.get("externalUrl"),
            "start_date": info.get("startDate"),
            "end_date": info.get("endDate"),
            "posted_on_text": summary.posted_on,
            "posted_on_detail": info.get("postedOn"),
            "location_summary": summary.location_text,
            "location_detail": info.get("location"),
            "user_authenticated": detail.get("userAuthenticated"),
            "hiring_organization": detail.get("hiringOrganization"),
        }
        return {key: value for key, value in metadata.items() if value not in (None, "", [])}

    @staticmethod
    def _normalize_description(raw_html: Optional[str]) -> str:
        if not raw_html:
            return ""
        unescaped = html.unescape(str(raw_html))
        soup = BeautifulSoup(unescaped, "html.parser")
        text = soup.get_text("\n", strip=True)
        lines = [line.strip() for line in text.splitlines()]
        cleaned = "\n".join(line for line in lines if line)
        return cleaned

    @staticmethod
    def _clean_text(value: Optional[object]) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        text = (
            text.replace("\r", "\n")
            .replace("\xa0", " ")
            .replace("\u202f", " ")
            .replace("\u200b", "")
        )
        return text or None

    def _ensure_session_bootstrap(self, force: bool = False) -> None:
        if self._bootstrapped and not force:
            return
        self.logger.debug("Bootstrapping Workday session via %s", SESSION_SEED_URL)
        resp = self.session.get(SESSION_SEED_URL, timeout=40)
        resp.raise_for_status()
        self._bootstrapped = True


def persist_listing(listing: JobListing) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location_text or "")[:255] or None,
        "date": (listing.date_posted or "")[:100] or None,
        "description": listing.description[:10000],
        "metadata": listing.metadata,
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Persisted Duke Energy job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Duke Energy Workday careers listings.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument(
        "--page-size",
        type=int,
        default=20,
        help="Number of jobs to request per Workday API page.",
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
        help="Fetch and display jobs without writing to the database.",
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

    scraper = DukeEnergyJobScraper(page_size=args.page_size, delay=args.delay)
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
        except Exception as exc:  # pragma: no cover - persistence failure fallback
            logging.error("Failed to persist job %s: %s", listing.detail_url, exc)
            totals["errors"] += 1

    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["dedupe"] = dedupe_summary

    logging.info(
        "Duke Energy scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )
    return 0 if not totals["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
