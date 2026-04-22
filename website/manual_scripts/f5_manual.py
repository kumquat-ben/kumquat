#!/usr/bin/env python3
"""Manual scraper for F5's Workday-powered careers portal."""
from __future__ import annotations

import argparse
import contextlib
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
CAREERS_URL = "https://www.f5.com/company/careers"
WORKDAY_ROOT = "https://ffive.wd5.myworkdayjobs.com"
TENANT = "ffive"
PORTAL = "f5jobs"
CXS_BASE = f"{WORKDAY_ROOT}/wday/cxs/{TENANT}/{PORTAL}"
JOBS_ENDPOINT = f"{CXS_BASE}/jobs"
SESSION_SEED_URL = f"{WORKDAY_ROOT}/en-US/{PORTAL}"
JOB_PUBLIC_BASE = f"{WORKDAY_ROOT}/{PORTAL}"

DEFAULT_PAGE_SIZE = 20
DEFAULT_DELAY = 0.25
REQUEST_TIMEOUT = 40
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": WORKDAY_ROOT,
    "Referer": WORKDAY_ROOT,
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 1200), 120)

SCRAPER_QS = Scraper.objects.filter(company="F5", url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple F5 scraper rows detected; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="F5",
        url=CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the Workday scraper encounters a blocking condition."""


@dataclass
class JobSummary:
    title: str
    detail_path: str
    detail_url: str
    location_text: Optional[str]
    posted_on: Optional[str]
    job_id: Optional[str]
    remote_type: Optional[str]


@dataclass
class JobListing(JobSummary):
    description_text: Optional[str]
    description_html: Optional[str]
    start_date: Optional[str]
    time_type: Optional[str]
    hiring_organization: Optional[str]
    metadata: Dict[str, object]


def _dedupe_preserve_order(values: Iterable[Optional[str]]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if not value:
            continue
        value = value.strip()
        if not value:
            continue
        if value not in seen:
            seen.add(value)
            ordered.append(value)
    return ordered


def _compose_location(info: dict, fallback: Optional[str]) -> Optional[str]:
    parts = _dedupe_preserve_order(
        [
            info.get("location"),
            info.get("locationsText"),
            (info.get("jobRequisitionLocation") or {}).get("descriptor"),
            (info.get("primaryLocation") or {}).get("descriptor") if isinstance(info.get("primaryLocation"), dict) else None,
            fallback,
        ]
    )
    country = info.get("jobRequisitionLocation", {}).get("country")
    if isinstance(country, dict):
        descriptor = country.get("descriptor")
        if descriptor and descriptor not in parts:
            parts.append(descriptor.strip())
    return ", ".join(parts) if parts else None


class F5WorkdayScraper:
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

    def scrape(
        self,
        *,
        limit: Optional[int],
        start_offset: int,
    ) -> Generator[JobListing, None, None]:
        processed = 0
        for summary in self._iter_summaries(limit=limit, offset=start_offset):
            try:
                detail = self._fetch_detail(summary)
            except Exception as exc:  # pragma: no cover - defensive guardrail
                self.logger.error("Failed to fetch detail for %s: %s", summary.detail_path, exc)
                continue

            listing = JobListing(**asdict(summary), **detail)
            yield listing
            processed += 1
            if limit is not None and processed >= limit:
                break
            if self.delay:
                time.sleep(self.delay)

    def _iter_summaries(self, *, limit: Optional[int], offset: int) -> Iterable[JobSummary]:
        current_offset = max(0, offset)
        retrieved = 0
        total: Optional[int] = None

        self._ensure_session_bootstrap()

        while True:
            payload = {
                "limit": self.page_size,
                "offset": current_offset,
                "searchText": "",
                "appliedFacets": {},
                "userPreferredLanguage": "en-US",
            }

            response = self.session.post(JOBS_ENDPOINT, json=payload, timeout=REQUEST_TIMEOUT)
            if response.status_code == 400 and not self._bootstrapped:
                self.logger.info("Workday returned 400; retrying after re-seeding session.")
                self._ensure_session_bootstrap(force=True)
                response = self.session.post(JOBS_ENDPOINT, json=payload, timeout=REQUEST_TIMEOUT)

            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                snippet = response.text[:200].strip()
                raise ScraperError(f"F5 Workday jobs request failed: {exc} | {snippet}") from exc

            data = response.json()
            postings = data.get("jobPostings") or []
            if total is None:
                with contextlib.suppress(TypeError, ValueError):
                    total = int(data.get("total") or 0)
                    self.logger.info("F5 Workday reports %s total jobs", total)

            if not postings:
                self.logger.info("Pagination exhausted at offset %s.", current_offset)
                break

            for raw in postings:
                detail_path = raw.get("externalPath") or ""
                title = (raw.get("title") or "").strip()
                if not title or not detail_path:
                    continue

                detail_url = (
                    detail_path
                    if detail_path.startswith("http")
                    else f"{JOB_PUBLIC_BASE.rstrip('/')}{detail_path}"
                )
                bullet_fields = raw.get("bulletFields") or []
                job_identifier: Optional[str] = None
                for entry in reversed(bullet_fields):
                    if isinstance(entry, str) and entry.strip():
                        job_identifier = entry.strip()
                        break

                summary = JobSummary(
                    title=title,
                    detail_path=detail_path,
                    detail_url=detail_url,
                    location_text=(raw.get("locationsText") or raw.get("location") or "").strip() or None,
                    posted_on=(raw.get("postedOn") or "").strip() or None,
                    job_id=job_identifier,
                    remote_type=(raw.get("remoteType") or "").strip() or None,
                )

                yield summary
                retrieved += 1
                if limit is not None and retrieved >= limit:
                    return

            if total is not None and (current_offset + self.page_size) >= total:
                break
            current_offset += self.page_size

    def _fetch_detail(self, summary: JobSummary) -> Dict[str, object]:
        detail_url = f"{CXS_BASE}{summary.detail_path}"
        response = self.session.get(detail_url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        payload = response.json()
        info = payload.get("jobPostingInfo") or {}

        description_html = info.get("jobDescription") or ""
        description_text = BeautifulSoup(description_html, "html.parser").get_text("\n", strip=True)

        hiring_org = None
        org_info = info.get("hiringOrganization")
        if isinstance(org_info, dict):
            hiring_org = (org_info.get("name") or "").strip() or None

        location = _compose_location(info, summary.location_text)

        metadata: Dict[str, object] = {
            "jobPostingId": info.get("jobPostingId"),
            "jobReqId": info.get("jobReqId"),
            "jobPostingSiteId": info.get("jobPostingSiteId"),
            "canApply": info.get("canApply"),
            "includeResumeParsing": info.get("includeResumeParsing"),
            "remoteType": info.get("remoteType"),
            "timeType": info.get("timeType"),
            "country": info.get("country"),
            "jobRequisitionLocation": info.get("jobRequisitionLocation"),
            "externalUrl": info.get("externalUrl"),
            "sections": payload.get("sections"),
        }

        metadata = {k: v for k, v in metadata.items() if v not in (None, "", [], {})}
        if description_html:
            metadata.setdefault("description_html", description_html)

        return {
            "description_text": description_text or None,
            "description_html": description_html or None,
            "start_date": info.get("startDate"),
            "time_type": info.get("timeType"),
            "remote_type": (info.get("remoteType") or summary.remote_type),
            "hiring_organization": hiring_org,
            "metadata": metadata,
            "location_text": location or summary.location_text,
        }

    def _ensure_session_bootstrap(self, force: bool = False) -> None:
        if self._bootstrapped and not force:
            return
        response = self.session.get(SESSION_SEED_URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        self._bootstrapped = True


def persist_listing(listing: JobListing) -> bool:
    metadata = dict(listing.metadata or {})
    metadata.update(
        {
            "job_id": listing.job_id,
            "detail_path": listing.detail_path,
            "remoteType": listing.remote_type,
            "timeType": listing.time_type,
            "hiringOrganization": listing.hiring_organization,
        }
    )

    defaults = {
        "title": listing.title[:255],
        "location": (listing.location_text or "")[:255] or None,
        "date": (listing.start_date or listing.posted_on or "")[:100] or None,
        "description": (listing.description_text or listing.description_html or "")[:10000],
        "metadata": metadata,
    }

    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Persisted F5 job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape F5 Workday careers portal.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument("--offset", type=int, default=0, help="Starting offset for pagination.")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE, help="Jobs per Workday request.")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Seconds to sleep between detail requests.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print jobs as JSON without persisting to the database.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging verbosity.",
    )
    return parser.parse_args(argv)


def run_scraper(
    *,
    limit: Optional[int],
    offset: int,
    page_size: int,
    delay: float,
    dry_run: bool,
) -> Dict[str, object]:
    scraper = F5WorkdayScraper(page_size=page_size, delay=delay)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    try:
        for listing in scraper.scrape(limit=limit, start_offset=offset):
            totals["fetched"] += 1
            if dry_run:
                print(json.dumps(asdict(listing), ensure_ascii=False, default=str))
                continue

            try:
                created = persist_listing(listing)
                if created:
                    totals["created"] += 1
                else:
                    totals["updated"] += 1
            except Exception as exc:  # pragma: no cover - persistence safeguard
                logging.error("Failed to persist F5 job %s: %s", listing.detail_url, exc)
                totals["errors"] += 1
    except ScraperError as exc:
        logging.error("F5 scraper stopped due to an API error: %s", exc)
        totals["errors"] += 1
    except requests.HTTPError as exc:
        logging.error("HTTP error while scraping F5 careers: %s", exc)
        totals["errors"] += 1
    except requests.RequestException as exc:
        logging.error("Network error while scraping F5 careers: %s", exc)
        totals["errors"] += 1

    if not dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["dedupe"] = dedupe_summary

    return totals


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()), format="%(levelname)s: %(message)s")

    totals = run_scraper(
        limit=args.limit,
        offset=args.offset,
        page_size=args.page_size,
        delay=args.delay,
        dry_run=args.dry_run,
    )
    logging.info(
        "F5 scraper finished - fetched=%(fetched)s created=%(created)s updated=%(updated)s errors=%(errors)s",
        totals,
    )
    if not args.dry_run and "dedupe" in totals:
        logging.info("Deduplication summary: %s", totals["dedupe"])
    return 0 if not totals["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
