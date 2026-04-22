#!/usr/bin/env python3
"""Manual scraper for Broadcom's Workday-powered careers site.

The public careers portal at https://www.broadcom.com/company/careers/
ultimately redirects candidates to the Workday tenant hosted at
https://broadcom.wd1.myworkdayjobs.com/External_Career. This script talks
directly to the Workday JSON API, normalizes each posting, and persists the
results into ``scrapers.JobPosting`` so they become visible inside Kumquat.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Django bootstrap
# ---------------------------------------------------------------------------
CURRENT_FILE = Path(__file__).resolve()
parents = list(CURRENT_FILE.parents)
default_backend_dir = parents[2] if len(parents) > 2 else parents[-1]
BACKEND_DIR = next(
    (candidate for candidate in parents if (candidate / "manage.py").exists()),
    default_backend_dir,
)
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
CAREERS_URL = "https://www.broadcom.com/company/careers/"
WORKDAY_ROOT = "https://broadcom.wd1.myworkdayjobs.com"
TENANT = "broadcom"
PORTAL = "External_Career"
CXS_BASE = f"{WORKDAY_ROOT}/wday/cxs/{TENANT}/{PORTAL}"
JOBS_ENDPOINT = f"{CXS_BASE}/jobs"
SESSION_SEED_URL = f"{WORKDAY_ROOT}/en-US/{PORTAL}"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": WORKDAY_ROOT,
    "Referer": SESSION_SEED_URL,
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 900), 300)

SCRAPER_QS = Scraper.objects.filter(company="Broadcom", url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched Broadcom; using id=%s.", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Broadcom",
        url=CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(RuntimeError):
    """Raised when the scraper encounters a non-recoverable error."""


@dataclass
class JobSummary:
    job_id: Optional[str]
    title: str
    detail_path: str
    detail_url: str
    location_text: Optional[str]
    posted_on: Optional[str]
    time_type: Optional[str]


@dataclass
class JobListing(JobSummary):
    description: str
    metadata: Dict[str, object]


class BroadcomJobScraper:
    """Client wrapper for Broadcom's Workday job listings."""

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
    def scrape(self, *, limit: Optional[int] = None) -> Iterator[JobListing]:
        fetched = 0
        for summary in self._iter_summaries(limit=limit):
            try:
                listing = self._enrich_summary(summary)
            except Exception as exc:  # pragma: no cover - defensive guardrail
                self.logger.error("Failed to enrich job %s: %s", summary.detail_url, exc)
                continue

            yield listing
            fetched += 1
            if limit is not None and fetched >= limit:
                break

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _iter_summaries(self, *, limit: Optional[int]) -> Iterator[JobSummary]:
        self._ensure_session_bootstrap()

        offset = 0
        retrieved = 0
        total: Optional[int] = None
        retry_bootstrap = False

        while True:
            payload = {
                "limit": self.page_size,
                "offset": offset,
                "searchText": "",
                "appliedFacets": {},
                "userPreferredLanguage": "en-US",
            }

            self.logger.debug("Requesting Workday jobs offset=%s", offset)
            response = self.session.post(JOBS_ENDPOINT, json=payload, timeout=40)

            if response.status_code in (400, 401, 422) and not retry_bootstrap:
                self.logger.info(
                    "Workday returned status %s; retrying after session bootstrap.",
                    response.status_code,
                )
                self._ensure_session_bootstrap(force=True)
                retry_bootstrap = True
                response = self.session.post(JOBS_ENDPOINT, json=payload, timeout=40)

            retry_bootstrap = False

            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                snippet = response.text[:200].strip()
                raise ScraperError(
                    f"Workday jobs request failed ({response.status_code}): {snippet}"
                ) from exc

            data = response.json()
            job_postings = data.get("jobPostings") or []
            if not job_postings:
                self.logger.info("No job postings found at offset %s; stopping.", offset)
                return

            if total is None:
                try:
                    total = int(data.get("total") or 0)
                except (TypeError, ValueError):
                    total = None

            for raw in job_postings:
                detail_path = _clean_text(raw.get("externalPath")) or ""
                detail_url = f"{WORKDAY_ROOT}/{PORTAL}{detail_path}" if detail_path else CAREERS_URL

                summary = JobSummary(
                    job_id=_first_non_empty(raw.get("bulletFields")),
                    title=_clean_text(raw.get("title")) or "Untitled Job",
                    detail_path=detail_path,
                    detail_url=detail_url,
                    location_text=_clean_text(raw.get("locationsText")),
                    posted_on=_clean_text(raw.get("postedOn")),
                    time_type=_clean_text(raw.get("timeType")),
                )

                if not summary.detail_path:
                    self.logger.debug("Skipping job with missing detail path: %s", raw)
                    continue

                yield summary
                retrieved += 1
                if limit is not None and retrieved >= limit:
                    return

            offset += self.page_size
            if total is not None and offset >= total:
                self.logger.info("Reached Workday reported total (%s); stopping.", total)
                return

            if self.delay:
                time.sleep(self.delay)

    def _enrich_summary(self, summary: JobSummary) -> JobListing:
        payload = self._fetch_detail_payload(summary.detail_path)
        job_info = payload.get("jobPostingInfo") or {}

        description_html = job_info.get("jobDescription") or ""
        description_text = _normalize_html(description_html) or "Description unavailable."

        primary_location = _clean_text(job_info.get("location")) or summary.location_text
        start_date = _clean_text(job_info.get("startDate"))
        posted_on = start_date or summary.posted_on

        metadata: Dict[str, object] = {
            "job_id": summary.job_id,
            "job_req_id": job_info.get("jobReqId"),
            "job_posting_id": job_info.get("jobPostingId"),
            "job_posting_site_id": job_info.get("jobPostingSiteId"),
            "time_type": summary.time_type or job_info.get("timeType"),
            "posted_on_text": summary.posted_on,
            "start_date": job_info.get("startDate"),
            "workday_location": job_info.get("jobRequisitionLocation"),
            "country": job_info.get("country"),
            "external_url": job_info.get("externalUrl"),
            "hiring_organization": payload.get("hiringOrganization"),
            "similar_jobs": payload.get("similarJobs"),
        }
        if description_html:
            metadata["description_html"] = description_html

        listing_dict = asdict(summary)
        listing_dict.update(
            {
                "description": description_text,
                "metadata": metadata,
                "location_text": primary_location,
                "posted_on": posted_on,
                "time_type": summary.time_type or job_info.get("timeType"),
            }
        )
        return JobListing(**listing_dict)

    def _fetch_detail_payload(self, detail_path: str) -> Dict[str, object]:
        detail_url = f"{CXS_BASE}{detail_path}"
        response = self.session.get(detail_url, timeout=40)

        if response.status_code == 404:
            raise ScraperError(f"Job detail returned 404 for {detail_path}")

        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            snippet = response.text[:200].strip()
            raise ScraperError(
                f"Job detail request failed ({response.status_code}): {snippet}"
            ) from exc

        try:
            return response.json()
        except ValueError as exc:
            raise ScraperError("Job detail payload was not valid JSON.") from exc

    def _ensure_session_bootstrap(self, *, force: bool = False) -> None:
        if self._bootstrapped and not force:
            return

        headers = dict(self.session.headers)
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"

        try:
            response = self.session.get(SESSION_SEED_URL, headers=headers, timeout=40)
            if response.status_code >= 400:
                self.logger.warning(
                    "Session bootstrap returned status %s; continuing anyway.",
                    response.status_code,
                )
        except requests.RequestException as exc:  # pragma: no cover - network safeguard
            self.logger.warning("Failed to bootstrap Workday session: %s", exc)
        finally:
            self._bootstrapped = True


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
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


def _normalize_html(html_text: str) -> str:
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "html.parser")
    text = soup.get_text("\n", strip=True)
    lines = [line.strip() for line in text.splitlines()]
    return "\n".join(line for line in lines if line)


def _first_non_empty(items: Optional[Iterable[object]]) -> Optional[str]:
    if not items:
        return None
    for item in items:
        cleaned = _clean_text(item)
        if cleaned:
            return cleaned
    return None


def persist_listing(listing: JobListing) -> bool:
    metadata = dict(listing.metadata or {})
    metadata.setdefault("time_type", listing.time_type)

    defaults = {
        "title": listing.title[:255],
        "location": (listing.location_text or "")[:255] or None,
        "date": (listing.posted_on or "")[:100] or None,
        "description": listing.description[:10000],
        "metadata": metadata,
    }

    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults=defaults,
    )

    logging.getLogger("persist").debug(
        "Persisted Broadcom job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape Broadcom's Workday careers job listings."
    )
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
        help="Seconds to sleep between pagination requests (default: 0.25).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print jobs instead of storing them.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(levelname)s: %(message)s",
    )

    scraper = BroadcomJobScraper(page_size=args.page_size, delay=args.delay)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    for listing in scraper.scrape(limit=args.limit):
        totals["fetched"] += 1
        if args.dry_run:
            print(json.dumps(asdict(listing), default=str, ensure_ascii=False))
            continue

        try:
            created = persist_listing(listing)
            if created:
                totals["created"] += 1
            else:
                totals["updated"] += 1
        except Exception as exc:  # pragma: no cover - persistence safeguard
            logging.error("Failed to persist job %s: %s", listing.detail_url, exc)
            totals["errors"] += 1

    if not args.dry_run:
        totals["dedupe"] = deduplicate_job_postings(scraper=SCRAPER)

    logging.info(
        "Broadcom scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )

    return 0 if totals["errors"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
