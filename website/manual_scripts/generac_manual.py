#!/usr/bin/env python3
"""Manual scraper for Generac's Workday-powered careers portal."""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Generator, Iterable, Optional
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
CAREERS_URL = "https://www.generac.com/about/careers/"
WORKDAY_ROOT = "https://generac.wd5.myworkdayjobs.com"
PORTAL = "External"
CXS_BASE = f"{WORKDAY_ROOT}/wday/cxs/generac/{PORTAL}"
JOBS_ENDPOINT = f"{CXS_BASE}/jobs"
SESSION_SEED_URL = f"{WORKDAY_ROOT}/en-US/{PORTAL}"
JOB_PUBLIC_BASE = f"{WORKDAY_ROOT}/{PORTAL}"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": WORKDAY_ROOT,
    "Referer": WORKDAY_ROOT,
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 900), 60)
SCRAPER_QS = Scraper.objects.filter(company="Generac", url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Generac scraper rows detected; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Generac",
        url=CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised for recoverable errors while scraping Generac Workday."""


@dataclass
class JobSummary:
    title: str
    detail_path: str
    detail_url: str
    location_text: Optional[str]
    posted_on: Optional[str]
    job_id: Optional[str]


@dataclass
class JobListing(JobSummary):
    description_text: Optional[str]
    description_html: Optional[str]
    start_date: Optional[str]
    time_type: Optional[str]
    remote_type: Optional[str]
    hiring_organization: Optional[str]
    metadata: Dict[str, object]


class GeneracWorkdayScraper:
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

    def scrape(self, *, limit: Optional[int] = None) -> Generator[JobListing, None, None]:
        processed = 0
        for summary in self._iter_summaries(limit=limit):
            try:
                detail = self._fetch_detail(summary.detail_path)
            except Exception as exc:  # pragma: no cover - defensive logging
                self.logger.error("Failed to enrich job %s: %s", summary.detail_path, exc)
                continue
            listing = JobListing(**asdict(summary), **detail)
            yield listing
            processed += 1
            if limit is not None and processed >= limit:
                break
            if self.delay:
                time.sleep(self.delay)

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
            response = self.session.post(JOBS_ENDPOINT, json=payload, timeout=40)
            if response.status_code == 400 and not self._bootstrapped:
                self.logger.info("Workday API returned 400; re-bootstrapping session.")
                self._ensure_session_bootstrap(force=True)
                response = self.session.post(JOBS_ENDPOINT, json=payload, timeout=40)

            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                snippet = response.text[:200].strip()
                self.logger.error("Jobs request failed (%s): %s", response.status_code, snippet)
                raise ScraperError(f"Workday jobs request failed: {exc}") from exc

            data = response.json()
            postings = data.get("jobPostings") or []
            if total is None:
                try:
                    total = int(data.get("total") or 0)
                    self.logger.info("Generac Workday total jobs reported: %s", total)
                except (TypeError, ValueError):
                    total = None

            if not postings:
                self.logger.info("No postings returned at offset %s; stopping pagination.", offset)
                break

            for raw in postings:
                detail_path = raw.get("externalPath") or ""
                if not detail_path:
                    continue
                if limit is not None and retrieved >= limit:
                    return

                detail_url = (
                    detail_path
                    if detail_path.startswith("http")
                    else f"{JOB_PUBLIC_BASE.rstrip('/')}{detail_path}"
                )
                job_id = (raw.get("bulletFields") or [None])[0]
                summary = JobSummary(
                    title=(raw.get("title") or "").strip(),
                    detail_path=detail_path,
                    detail_url=detail_url,
                    location_text=(raw.get("locationsText") or "").strip() or None,
                    posted_on=(raw.get("postedOn") or "").strip() or None,
                    job_id=job_id.strip() if isinstance(job_id, str) else None,
                )
                if not summary.title:
                    continue

                yield summary
                retrieved += 1

            if total is not None and offset + self.page_size >= total:
                break
            offset += self.page_size

    def _fetch_detail(self, detail_path: str) -> Dict[str, object]:
        detail_url = f"{CXS_BASE}{detail_path}"
        response = self.session.get(detail_url, timeout=40)
        response.raise_for_status()
        data = response.json().get("jobPostingInfo") or {}

        description_html = data.get("jobDescription") or ""
        description_text = BeautifulSoup(description_html, "html.parser").get_text(
            "\n", strip=True
        )

        hiring_org = ""
        org = data.get("hiringOrganization")
        if isinstance(org, dict):
            hiring_org = org.get("name") or ""

        metadata = {
            "jobPostingId": data.get("jobPostingId"),
            "jobReqId": data.get("jobReqId"),
            "postedOn": data.get("postedOn"),
            "canApply": data.get("canApply"),
            "includeResumeParsing": data.get("includeResumeParsing"),
            "externalUrl": data.get("externalUrl"),
            "remoteType": data.get("remoteType"),
            "timeType": data.get("timeType"),
            "country": data.get("country"),
            "logo": data.get("logoImage"),
        }

        return {
            "description_text": description_text or None,
            "description_html": description_html or None,
            "start_date": data.get("startDate"),
            "time_type": data.get("timeType"),
            "remote_type": data.get("remoteType"),
            "hiring_organization": hiring_org or None,
            "metadata": metadata,
        }

    def _ensure_session_bootstrap(self, force: bool = False) -> None:
        if self._bootstrapped and not force:
            return
        self.logger.debug("Bootstrapping Workday session via %s", SESSION_SEED_URL)
        response = self.session.get(SESSION_SEED_URL, timeout=40)
        response.raise_for_status()
        self._bootstrapped = True


def persist_listing(listing: JobListing) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location_text or "")[:255] or None,
        "date": (listing.start_date or listing.posted_on or "")[:100] or None,
        "description": (listing.description_text or listing.description_html or "")[:10000],
        "metadata": {
            **listing.metadata,
            "job_id": listing.job_id,
            "detail_path": listing.detail_path,
            "detail_url": listing.detail_url,
            "hiringOrganization": listing.hiring_organization,
        },
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Persisted Generac job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Generac Workday careers portal.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum jobs to process.")
    parser.add_argument(
        "--page-size", type=int, default=20, help="Number of jobs per Workday request."
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.25,
        help="Seconds to sleep between requests (default: 0.25).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print job payloads without modifying the database.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s: %(message)s")

    scraper = GeneracWorkdayScraper(page_size=args.page_size, delay=args.delay)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    for listing in scraper.scrape(limit=args.limit):
        totals["fetched"] += 1
        if args.dry_run:
            print(json.dumps(asdict(listing), ensure_ascii=False, default=str))
            continue

        try:
            created = persist_listing(listing)
            if created:
                totals["created"] += 1
            else:
                totals["updated"] += 1
        except Exception as exc:  # pragma: no cover - persistence guardrail
            logging.error("Failed to persist job %s: %s", listing.detail_url, exc)
            totals["errors"] += 1

    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["dedupe"] = dedupe_summary

    logging.info(
        "Generac scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )
    return 0 if not totals["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
