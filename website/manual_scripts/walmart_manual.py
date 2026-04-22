#!/usr/bin/env python3
"""Manual scraper for Walmart Careers (Workday-powered).

This script paginates through the public Workday CXS job listings API used by
https://careers.walmart.com, visits each job detail page to extract metadata
from its JSON-LD payload, and writes/updates `JobPosting` rows associated with
the "Walmart" scraper entry.
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
from urllib.parse import urljoin
from typing import Dict, Generator, Iterable, Optional

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Django setup (aligns with existing manual script conventions)
# ---------------------------------------------------------------------------
BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "website.settings")

import django  # noqa: E402

django.setup()

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
RESULTS_URL = (
    "https://careers.walmart.com/results?q=&page=1&sort=rank"
    "&expand=department,brand,type,rate&jobCareerArea=all"
)
WORKDAY_ROOT = "https://walmart.wd5.myworkdayjobs.com"
PORTAL = "WalmartExternal"
CXS_BASE = f"{WORKDAY_ROOT}/wday/cxs/walmart/{PORTAL}"
JOBS_ENDPOINT = f"{CXS_BASE}/jobs"
JOB_DETAIL_BASE = f"{WORKDAY_ROOT}/en-US/{PORTAL}"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

SCRAPER_QS = Scraper.objects.filter(company="Walmart", url=RESULTS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched; using id=%s.", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Walmart",
        url=RESULTS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=300,
    )


class ScraperError(Exception):
    """Raised when the scraper encounters an unrecoverable error."""


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
    description: str
    date_posted: Optional[str]
    metadata: Dict[str, object]


class WalmartJobScraper:
    def __init__(self, *, page_size: int = 20, delay: float = 0.25, session: Optional[requests.Session] = None) -> None:
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
            response = requests.post(
                JOBS_ENDPOINT,
                json=payload,
                headers=self.session.headers,
                timeout=40,
            )
            if response.status_code == 400 and not self._bootstrapped:
                self.logger.info("Workday API returned 400; retrying after session bootstrap.")
                self._ensure_session_bootstrap(force=True)
                response = requests.post(
                    JOBS_ENDPOINT,
                    json=payload,
                    headers=self.session.headers,
                    timeout=40,
                )
            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                snippet = response.text[:200].strip()
                self.logger.error(
                    "Workday jobs request failed (%s): %s", response.status_code, snippet
                )
                self.logger.debug("Request headers: %s", dict(response.request.headers))
                self.logger.debug("Request body: %s", response.request.body)
                raise ScraperError(f"Workday jobs request failed: {exc}") from exc
            data = response.json()

            job_postings = data.get("jobPostings") or []
            if not job_postings:
                self.logger.info("No job postings found at offset %s; stopping.", offset)
                return

            total = total or data.get("total")

            for raw in job_postings:
                detail_path = raw.get("externalPath") or ""
                detail_url = f"{JOB_DETAIL_BASE.rstrip('/')}{detail_path}"
                summary = JobSummary(
                    job_id=(raw.get("bulletFields") or [None])[0],
                    title=(raw.get("title") or "").strip(),
                    detail_path=detail_path,
                    detail_url=detail_url,
                    location_text=(raw.get("locationsText") or "").strip() or None,
                    posted_on=(raw.get("postedOn") or "").strip() or None,
                )
                yield summary
                retrieved += 1
                if limit is not None and retrieved >= limit:
                    return

            offset += self.page_size
            if total is not None and offset >= int(total):
                self.logger.info("Reached total count (%s); stopping pagination.", total)
                return

            if self.delay:
                time.sleep(self.delay)

    def _enrich_summary(self, summary: JobSummary) -> JobListing:
        detail_html = self._fetch_detail_html(summary.detail_url)
        json_ld = self._extract_json_ld(detail_html)

        description_text = ""
        if isinstance(json_ld, dict):
            description_text = html.unescape((json_ld.get("description") or "").strip())
        else:
            self.logger.debug("Unexpected JSON-LD structure for %s", summary.detail_url)

        if not description_text:
            description_text = "Description unavailable."
        else:
            description_text = (
                description_text.replace("\u202f", " ").replace("\xa0", " ").strip()
            )

        date_posted = None
        if isinstance(json_ld, dict):
            raw_date = (json_ld.get("datePosted") or "").strip()
            date_posted = raw_date or summary.posted_on

        metadata: Dict[str, object] = {
            "job_id": summary.job_id,
            "posted_on_text": summary.posted_on,
            "locations_text": summary.location_text,
        }
        if isinstance(json_ld, dict):
            metadata["json_ld"] = json_ld

        return JobListing(
            **summary.__dict__,
            description=description_text,
            date_posted=date_posted,
            metadata=metadata,
        )

    def _fetch_detail_html(self, url: str) -> str:
        headers = dict(self.session.headers)
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        resp = self.session.get(url, headers=headers, timeout=40)
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "")
        if "application/json" in content_type:
            try:
                data = resp.json()
            except ValueError:
                return resp.text
            redirect_path = data.get("url")
            if redirect_path:
                redirect_url = (
                    redirect_path if redirect_path.startswith("http") else urljoin(WORKDAY_ROOT, redirect_path)
                )
                return self._fetch_detail_html(redirect_url)
        return resp.text

    @staticmethod
    def _extract_json_ld(html_text: str) -> Dict[str, object]:
        soup = BeautifulSoup(html_text, "html.parser")
        script_tag = soup.find("script", attrs={"type": "application/ld+json"})
        if not script_tag:
            raise ScraperError("Job detail JSON-LD payload not found.")
        raw_json = script_tag.string or script_tag.get_text()
        if not raw_json:
            raise ScraperError("Job detail JSON-LD payload not found.")
        try:
            data = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            raise ScraperError(f"Failed to parse JSON-LD: {exc}") from exc
        return data if isinstance(data, dict) else {"raw": data}

    def _ensure_session_bootstrap(self, force: bool = False) -> None:
        if self._bootstrapped and not force:
            return
        landing_url = f"{JOB_DETAIL_BASE}/"
        self.logger.debug("Bootstrapping Workday session via %s", landing_url)
        resp = self.session.get(landing_url, timeout=40)
        resp.raise_for_status()
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
        "Persisted job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Walmart careers listings.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument(
        "--page-size", type=int, default=20, help="Number of jobs to request per Workday API page."
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

    scraper = WalmartJobScraper(page_size=args.page_size, delay=args.delay)
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
        except Exception as exc:  # pragma: no cover - persistence error path
            logging.error("Failed to persist job %s: %s", listing.detail_url, exc)
            totals["errors"] += 1

    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["dedupe"] = dedupe_summary

    logging.info(
        "Walmart scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )
    return 0 if not totals["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
