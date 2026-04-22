#!/usr/bin/env python3
"""Manual scraper for Cadence (Workday-powered careers portal).

This script targets the public careers portal exposed at
https://cadence.wd1.myworkdayjobs.com/External_Careers, iterates through the
Workday JSON APIs, and upserts the resulting postings into the JobPosting
table so they appear in Kumquat alongside automated scrapers.
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
from typing import Any, Dict, Iterable, Iterator, List, Optional
from urllib.parse import urljoin

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
CAREERS_LANDING_URL = "https://www.cadence.com/en_US/home/company/careers.html"
WORKDAY_ROOT = "https://cadence.wd1.myworkdayjobs.com"
TENANT = "cadence"
PORTAL = "External_Careers"
CXS_BASE = f"{WORKDAY_ROOT}/wday/cxs/{TENANT}/{PORTAL}"
JOB_DETAIL_BASE = f"{WORKDAY_ROOT}/en-US/{PORTAL}"
SESSION_SEED_URL = f"{WORKDAY_ROOT}/{PORTAL}"
JOBS_ENDPOINT = f"{CXS_BASE}/jobs"
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
    "Referer": SESSION_SEED_URL,
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 900), 60)

SCRAPER_QS = Scraper.objects.filter(company="Cadence", url=CAREERS_LANDING_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched Cadence; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Cadence",
        url=CAREERS_LANDING_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(RuntimeError):
    """Raised when a non-recoverable scraping error occurs."""


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
    description_html: str
    posted_date: Optional[str]
    country: Optional[str]
    time_type: Optional[str]
    metadata: Dict[str, Any]


class CadenceJobScraper:
    """Client for interacting with Cadence's Workday API."""

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
        processed = 0
        for summary in self._iter_summaries(limit=limit):
            try:
                listing = self._enrich_summary(summary)
            except ScraperError as exc:
                self.logger.error("Failed to enrich job %s: %s", summary.detail_url, exc)
                continue
            except requests.RequestException as exc:
                self.logger.error("HTTP error while enriching %s: %s", summary.detail_url, exc)
                continue

            yield listing
            processed += 1
            if limit is not None and processed >= limit:
                break

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _iter_summaries(self, *, limit: Optional[int]) -> Iterator[JobSummary]:
        self._ensure_session_bootstrap()

        offset = 0
        total: Optional[int] = None
        seen = 0
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
            response = self.session.post(JOBS_ENDPOINT, json=payload, timeout=REQUEST_TIMEOUT)

            if response.status_code in (401, 403, 422) and not retry_bootstrap:
                self.logger.info(
                    "Workday returned status %s; attempting session re-bootstrap.",
                    response.status_code,
                )
                self._ensure_session_bootstrap(force=True)
                retry_bootstrap = True
                response = self.session.post(JOBS_ENDPOINT, json=payload, timeout=REQUEST_TIMEOUT)

            retry_bootstrap = False

            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                snippet = response.text[:200].strip()
                raise ScraperError(
                    f"Jobs endpoint request failed ({response.status_code}): {snippet}"
                ) from exc

            data = response.json()
            job_postings: List[Dict[str, Any]] = data.get("jobPostings") or []

            if not job_postings:
                self.logger.info("No job postings returned at offset %s; stopping iteration.", offset)
                return

            if total is None:
                total = _to_int(data.get("total"))
                if total:
                    self.logger.info("Workday reports %s total postings.", total)

            for raw in job_postings:
                detail_path = _clean_text(raw.get("externalPath"))
                title = _clean_text(raw.get("title"))

                if not title or not detail_path:
                    self.logger.debug("Skipping malformed posting payload: %s", raw)
                    continue

                detail_url = self._build_detail_url(detail_path)
                summary = JobSummary(
                    job_id=_first_non_empty(raw.get("bulletFields")),
                    title=title,
                    detail_path=detail_path,
                    detail_url=detail_url,
                    location_text=_clean_text(raw.get("locationsText")),
                    posted_on=_clean_text(raw.get("postedOn")),
                )
                yield summary
                seen += 1
                if limit is not None and seen >= limit:
                    self.logger.debug("Limit reached while iterating summaries.")
                    return

            offset += self.page_size
            if total is not None and offset >= total:
                self.logger.info("Reached reported Workday total (%s); stopping iteration.", total)
                return

            if self.delay:
                time.sleep(self.delay)

    def _enrich_summary(self, summary: JobSummary) -> JobListing:
        detail_payload = self._fetch_detail_json(summary.detail_path)
        info = detail_payload.get("jobPostingInfo") or {}

        description_html = info.get("jobDescription") or ""
        description_text = _html_to_text(description_html)
        if not description_text:
            description_text = "Description unavailable."

        location_override = _clean_text(
            (info.get("jobRequisitionLocation") or {}).get("descriptor")
        ) or _clean_text(info.get("location"))
        posted_date = _clean_text(info.get("startDate")) or _clean_text(info.get("postedOn")) or summary.posted_on
        time_type = _clean_text(info.get("timeType"))
        country = _clean_text((info.get("country") or {}).get("descriptor"))

        metadata: Dict[str, Any] = {
            "job_req_id": info.get("jobReqId"),
            "job_posting_id": info.get("jobPostingId"),
            "job_posting_site": info.get("jobPostingSiteId"),
            "job_posting_url": info.get("externalUrl"),
            "location_text_original": summary.location_text,
            "posted_on_text": summary.posted_on,
            "start_date": info.get("startDate"),
            "time_type": time_type,
            "country_id": (info.get("country") or {}).get("id"),
            "job_requisition_location": info.get("jobRequisitionLocation"),
            "job_description_html": description_html or None,
            "similar_jobs": detail_payload.get("similarJobs"),
        }
        metadata = {key: value for key, value in metadata.items() if value not in (None, "", [])}

        return JobListing(
            job_id=summary.job_id,
            title=summary.title,
            detail_path=summary.detail_path,
            detail_url=summary.detail_url,
            location_text=location_override or summary.location_text,
            posted_on=summary.posted_on,
            description_text=description_text,
            description_html=description_html,
            posted_date=posted_date,
            country=country,
            time_type=time_type,
            metadata=metadata,
        )

    # ------------------------------------------------------------------
    # Networking helpers
    # ------------------------------------------------------------------
    def _fetch_detail_json(self, detail_path: str) -> Dict[str, Any]:
        if not detail_path:
            raise ScraperError("Missing Workday detail path.")

        url = urljoin(CXS_BASE.rstrip("/") + "/", detail_path.lstrip("/"))
        self.logger.debug("Fetching job detail JSON: %s", url)

        response = self.session.get(url, timeout=REQUEST_TIMEOUT)

        if response.status_code in (401, 403, 422):
            self.logger.info(
                "Workday detail returned status %s; retrying with fresh session.",
                response.status_code,
            )
            self._ensure_session_bootstrap(force=True)
            response = self.session.get(url, timeout=REQUEST_TIMEOUT)

        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            snippet = response.text[:200].strip()
            raise ScraperError(
                f"Detail request failed ({response.status_code}): {snippet}"
            ) from exc

        try:
            return response.json()
        except ValueError as exc:
            raise ScraperError("Failed to parse Workday detail JSON.") from exc

    def _build_detail_url(self, detail_path: str) -> str:
        if detail_path.startswith("http"):
            return detail_path
        return urljoin(JOB_DETAIL_BASE.rstrip("/") + "/", detail_path.lstrip("/"))

    def _ensure_session_bootstrap(self, *, force: bool = False) -> None:
        if self._bootstrapped and not force:
            return

        headers = dict(self.session.headers)
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"

        try:
            response = self.session.get(SESSION_SEED_URL, headers=headers, timeout=REQUEST_TIMEOUT)
            if response.status_code >= 400:
                self.logger.warning(
                    "Session bootstrap returned status %s; continuing anyway.",
                    response.status_code,
                )
        except requests.RequestException as exc:
            self.logger.warning("Failed to bootstrap Workday session: %s", exc)
        finally:
            self._bootstrapped = True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _clean_text(value: Optional[Any]) -> Optional[str]:
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


def _html_to_text(html_fragment: str) -> str:
    if not html_fragment:
        return ""
    soup = BeautifulSoup(html_fragment, "html.parser")
    text = soup.get_text("\n", strip=True)
    lines = [line.strip() for line in text.splitlines()]
    return "\n".join(line for line in lines if line)


def _first_non_empty(items: Optional[Iterable[Any]]) -> Optional[str]:
    if not items:
        return None
    for item in items:
        cleaned = _clean_text(item)
        if cleaned:
            return cleaned
    return None


def _to_int(value: Optional[Any]) -> Optional[int]:
    if value in (None, "", "None"):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def persist_listing(listing: JobListing) -> bool:
    description = listing.description_text or "Description unavailable."
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location_text or "")[:255] or None,
        "date": (listing.posted_date or "")[:100] or None,
        "description": description[:10000],
        "metadata": listing.metadata,
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Persisted Cadence job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape Cadence Workday careers job listings."
    )
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument(
        "--page-size",
        type=int,
        default=20,
        help="Number of jobs to request per Workday API page (default: 20).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.25,
        help="Seconds to sleep between pagination requests (default: 0.25).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print jobs instead of persisting them.")
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

    scraper = CadenceJobScraper(page_size=args.page_size, delay=args.delay)
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
        (
            "Cadence scraper finished - fetched=%(fetched)s created=%(created)s "
            "updated=%(updated)s errors=%(errors)s"
        ),
        totals,
    )

    return 0 if totals["errors"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
