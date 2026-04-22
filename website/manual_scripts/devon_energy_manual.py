#!/usr/bin/env python3
"""Manual scraper for Devon Energy careers (Workday-powered)."""
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
from typing import Dict, Iterable, Iterator, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "website.settings")

import django  # noqa: E402

django.setup()

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

SCRAPER_URL = "https://www.devonenergy.com/careers"
JOB_SEARCH_URL = "https://careers.devonenergy.com/jobs"
WORKDAY_HOST = "https://devonenergy.wd5.myworkdayjobs.com"
PORTAL = "Careers"
JOB_DETAIL_ROOT = f"{WORKDAY_HOST}/en-US/{PORTAL}"
JOBS_ENDPOINT = f"{WORKDAY_HOST}/wday/cxs/devonenergy/{PORTAL}/jobs"
REQUEST_TIMEOUT = (10, 30)
DEFAULT_PAGE_SIZE = 20
DEFAULT_DELAY = 0.25
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json",
    "Referer": JOB_SEARCH_URL,
}

SCRAPER_QS = Scraper.objects.filter(company="Devon Energy", url=SCRAPER_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched Devon Energy; using id=%s.", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Devon Energy",
        url=SCRAPER_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=600,
    )


class ScraperError(Exception):
    """Raised when the Devon Energy scrape encounters an unrecoverable error."""


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


class DevonEnergyJobScraper:
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
        self._bootstrapped = False

    def scrape(self, *, limit: Optional[int] = None) -> Iterator[JobListing]:
        processed = 0
        for summary in self._iter_summaries(limit=limit):
            try:
                listing = self._hydrate_summary(summary)
            except Exception as exc:  # pragma: no cover - defensive logging
                self.logger.error("Failed to hydrate job %s: %s", summary.detail_url, exc)
                continue
            yield listing
            processed += 1
            if limit is not None and processed >= limit:
                return

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
            response = self.session.post(JOBS_ENDPOINT, json=payload, timeout=REQUEST_TIMEOUT)

            if response.status_code in {400, 403}:
                self.logger.info("Workday API returned %s; retrying after bootstrap.", response.status_code)
                self._ensure_session_bootstrap(force=True)
                response = self.session.post(JOBS_ENDPOINT, json=payload, timeout=REQUEST_TIMEOUT)

            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                snippet = response.text[:200].strip()
                self.logger.error("Jobs request failed (%s): %s", response.status_code, snippet)
                raise ScraperError(f"Jobs request failed: {exc}") from exc

            data = response.json()
            postings = data.get("jobPostings") or []
            if not postings:
                self.logger.info("No job postings returned at offset %s; stopping.", offset)
                return

            total = total or data.get("total")

            for raw in postings:
                detail_path = (raw.get("externalPath") or "").strip()
                if not detail_path:
                    continue
                detail_url = urljoin(f"{JOB_DETAIL_ROOT.rstrip('/')}/", detail_path.lstrip("/"))
                summary = JobSummary(
                    job_id=(raw.get("bulletFields") or [None])[0],
                    title=(raw.get("title") or "").strip(),
                    detail_path=detail_path,
                    detail_url=detail_url,
                    location_text=(raw.get("locationsText") or "").strip() or None,
                    posted_on=(raw.get("postedOn") or "").strip() or None,
                )
                if not summary.title:
                    continue
                yield summary
                retrieved += 1
                if limit is not None and retrieved >= limit:
                    return

            offset += self.page_size
            if total is not None:
                try:
                    total_int = int(total)
                except (TypeError, ValueError):
                    total_int = None
                if total_int is not None and offset >= total_int:
                    self.logger.info("Reached total advertised count (%s); stopping.", total_int)
                    return

            if self.delay:
                time.sleep(self.delay)

    def _hydrate_summary(self, summary: JobSummary) -> JobListing:
        detail_html = self._fetch_detail_html(summary.detail_url)
        json_ld = self._extract_json_ld(detail_html)

        raw_description = ""
        date_posted = summary.posted_on
        if isinstance(json_ld, dict):
            raw_description = (json_ld.get("description") or "").strip()
            date_posted = (json_ld.get("datePosted") or "").strip() or date_posted

        description = self._normalize_description(raw_description)
        metadata: Dict[str, object] = {
            "job_id": summary.job_id,
            "posted_on_text": summary.posted_on,
            "locations_text": summary.location_text,
            "detail_path": summary.detail_path,
        }
        if isinstance(json_ld, dict) and json_ld:
            json_ld_copy = dict(json_ld)
            if isinstance(json_ld_copy.get("description"), str):
                json_ld_copy["description"] = description
            metadata["json_ld"] = json_ld_copy

        return JobListing(
            **summary.__dict__,
            description=description,
            date_posted=date_posted or None,
            metadata=metadata,
        )

    def _fetch_detail_html(self, url: str) -> str:
        headers = dict(self.session.headers)
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        response = self.session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        content_type = response.headers.get("Content-Type", "")
        if "application/json" in content_type:
            try:
                data = response.json()
            except ValueError:
                return response.text
            redirect_path = data.get("url")
            if redirect_path:
                redirect_url = (
                    redirect_path if redirect_path.startswith("http") else urljoin(WORKDAY_HOST, redirect_path)
                )
                return self._fetch_detail_html(redirect_url)
        return response.text

    @staticmethod
    def _extract_json_ld(html_text: str) -> Dict[str, object]:
        soup = BeautifulSoup(html_text, "html.parser")
        script_tag = soup.find("script", attrs={"type": "application/ld+json"})
        if not script_tag or not script_tag.string:
            raise ScraperError("Job detail JSON-LD payload not found.")
        try:
            data = json.loads(script_tag.string)
        except json.JSONDecodeError as exc:
            raise ScraperError(f"Failed to parse JSON-LD: {exc}") from exc
        return data if isinstance(data, dict) else {"raw": data}

    @staticmethod
    def _normalize_description(text: str) -> str:
        if not text:
            return "Description unavailable."
        clean = html.unescape(text)
        clean = clean.replace("\r\n", "\n").replace("\r", "\n")
        clean = clean.replace("\u202f", " ").replace("\xa0", " ")
        try:
            clean = clean.encode("latin1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass
        return clean.strip() or "Description unavailable."

    def _ensure_session_bootstrap(self, *, force: bool = False) -> None:
        if self._bootstrapped and not force:
            return
        response = self.session.get(JOB_SEARCH_URL, timeout=REQUEST_TIMEOUT)
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
        "Persisted job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Devon Energy careers listings.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE, help="Number of jobs to request per page.")
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help="Seconds to sleep between Workday pagination requests.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Fetch listings without writing to the database.")
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

    scraper = DevonEnergyJobScraper(page_size=args.page_size, delay=args.delay)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    for listing in scraper.scrape(limit=args.limit):
        totals["fetched"] += 1
        if args.dry_run:
            print(json.dumps(listing.__dict__, default=str, ensure_ascii=False))
            continue

        try:
            created = persist_listing(listing)
        except Exception as exc:  # pragma: no cover - persistence error handling
            logging.error("Failed to persist job %s: %s", listing.detail_url, exc)
            totals["errors"] += 1
            continue

        if created:
            totals["created"] += 1
        else:
            totals["updated"] += 1

    if not args.dry_run:
        totals["dedupe"] = deduplicate_job_postings(scraper=SCRAPER)

    logging.info(
        "Devon Energy scraper finished - fetched=%(fetched)s created=%(created)s updated=%(updated)s errors=%(errors)s",
        totals,
    )
    if totals["errors"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
