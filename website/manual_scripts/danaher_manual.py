#!/usr/bin/env python3
"""Manual scraper for Danaher's Phenom-powered careers site."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional, Set

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
CAREERS_LANDING_URL = "https://jobs.danaher.com/global/en"
SEARCH_RESULTS_URL = "https://jobs.danaher.com/global/en/search-results"
JOB_DETAIL_URL_TEMPLATE = "https://jobs.danaher.com/global/en/job/{job_seq}"
REQUEST_TIMEOUT = (15, 45)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": CAREERS_LANDING_URL,
}
DEFAULT_DELAY = 0.25
DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 1800), 120)

SCRAPER_QS = Scraper.objects.filter(company="Danaher", url=CAREERS_LANDING_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.getLogger(__name__).warning(
            "Multiple Scraper rows matched Danaher careers; using id=%s", SCRAPER.id
        )
else:
    SCRAPER = Scraper.objects.create(
        company="Danaher",
        url=CAREERS_LANDING_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


# ---------------------------------------------------------------------------
# Helpers and data containers
# ---------------------------------------------------------------------------
class ScraperError(RuntimeError):
    """Raised when the scraper pipeline cannot continue."""


def _html_to_text(value: Optional[str]) -> str:
    if not value:
        return ""
    soup = BeautifulSoup(value, "html.parser")
    return soup.get_text("\n", strip=True).strip()


def _clean_metadata(source: Dict[str, object]) -> Dict[str, object]:
    cleaned: Dict[str, object] = {}
    for key, value in source.items():
        if value is None:
            continue
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                continue
            cleaned[key] = stripped
            continue
        if isinstance(value, (list, dict)) and not value:
            continue
        cleaned[key] = value
    return cleaned


@dataclass
class JobListing:
    job_seq: str
    title: str
    link: str
    location: Optional[str]
    posted_date: Optional[str]
    description_text: str
    description_html: Optional[str]
    apply_url: Optional[str]
    metadata: Dict[str, object]


# ---------------------------------------------------------------------------
# HTTP client
# ---------------------------------------------------------------------------
class DanaherCareersClient:
    def __init__(
        self,
        *,
        delay: float = DEFAULT_DELAY,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def iter_listings(
        self,
        *,
        limit: Optional[int] = None,
        max_pages: Optional[int] = None,
        start_offset: int = 0,
    ) -> Iterator[JobListing]:
        offset = max(0, start_offset)
        page_index = 0
        yielded = 0
        seen_sequences: Set[str] = set()
        total_hits: Optional[int] = None

        while True:
            payload = self._fetch_listing_page(offset)
            jobs = payload["jobs"]

            if not jobs:
                if page_index == 0:
                    self.logger.warning(
                        "No job listings discovered at offset %s; page structure may have changed.",
                        offset,
                    )
                else:
                    self.logger.info("Pagination exhausted at offset %s.", offset)
                break

            total_hits = payload["total_hits"] if payload["total_hits"] is not None else total_hits
            page_size = len(jobs)

            self.logger.debug(
                "Processing page %s (offset=%s, page_size=%s, total_hits=%s)",
                page_index + 1,
                offset,
                page_size,
                total_hits,
            )

            for job in jobs:
                job_seq = (job.get("jobSeqNo") or "").strip()
                if not job_seq:
                    self.logger.debug("Skipping job without jobSeqNo: %s", job)
                    continue
                if job_seq in seen_sequences:
                    self.logger.debug("Skipping duplicate jobSeqNo %s", job_seq)
                    continue
                seen_sequences.add(job_seq)

                try:
                    detail_payload, detail_url, detail_html = self._fetch_job_detail(job_seq)
                    listing = self._build_listing(job, detail_payload, detail_url, detail_html)
                except ScraperError as exc:
                    self.logger.error("Skipping job %s due to error: %s", job_seq, exc)
                    continue

                yield listing
                yielded += 1

                if limit is not None and yielded >= limit:
                    self.logger.info("Reached job limit %s; stopping.", limit)
                    return

            offset += page_size
            page_index += 1

            if total_hits is not None and offset >= total_hits:
                self.logger.info("Reached reported total hits %s; stopping.", total_hits)
                break

            if max_pages is not None and page_index >= max_pages:
                self.logger.info("Reached page cap %s; stopping.", max_pages)
                break

            if self.delay:
                time.sleep(self.delay)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _fetch_listing_page(self, offset: int) -> Dict[str, object]:
        params = {"from": offset} if offset else None
        try:
            response = self.session.get(
                SEARCH_RESULTS_URL,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch search results at offset {offset}: {exc}") from exc

        ddo = self._extract_ddo(response.text)
        refine = ddo.get("eagerLoadRefineSearch") or {}
        data = refine.get("data") or {}

        jobs = data.get("jobs") or []
        total_hits = refine.get("totalHits")

        return {"jobs": jobs, "total_hits": total_hits}

    def _fetch_job_detail(self, job_seq: str) -> tuple[Dict[str, object], str, str]:
        detail_url = JOB_DETAIL_URL_TEMPLATE.format(job_seq=job_seq)
        try:
            response = self.session.get(detail_url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch job detail for {job_seq}: {exc}") from exc

        html = response.text
        ddo = self._extract_ddo(html)
        job_detail = ((ddo.get("jobDetail") or {}).get("data") or {}).get("job") or {}
        if not job_detail:
            raise ScraperError(f"No job detail payload returned for {job_seq}")

        final_url = response.url or detail_url
        return job_detail, final_url, html

    def _build_listing(
        self,
        summary: Dict[str, object],
        detail: Dict[str, object],
        detail_url: str,
        detail_html: str,
    ) -> JobListing:
        job_seq = (detail.get("jobSeqNo") or summary.get("jobSeqNo") or "").strip()
        if not job_seq:
            raise ScraperError("Detail payload missing jobSeqNo.")

        title = (detail.get("title") or summary.get("title") or "").strip()
        if not title:
            raise ScraperError(f"Job {job_seq} missing title.")

        location = (
            detail.get("cityStateCountry")
            or summary.get("cityStateCountry")
            or detail.get("location")
            or summary.get("location")
        )
        posted_date = (
            detail.get("postedDate")
            or summary.get("postedDate")
            or detail.get("dateCreated")
            or summary.get("dateCreated")
        )

        description_html = (
            detail.get("structureData", {}).get("description")
            or detail.get("description")
            or detail.get("ml_Description")
            or detail.get("jobDescription")
        )
        description_text = _html_to_text(description_html)
        if not description_text:
            description_text = "Description unavailable."

        apply_url = detail.get("applyUrl") or summary.get("applyUrl")

        metadata = _clean_metadata(
            {
                "job_seq_no": job_seq,
                "job_id": detail.get("jobId") or summary.get("jobId"),
                "req_id": detail.get("reqId") or summary.get("reqId"),
                "category": detail.get("category") or summary.get("category"),
                "job_family": detail.get("jobFamily"),
                "job_family_group": detail.get("jobFamilyGroup"),
                "time_type": detail.get("timeType"),
                "worker_type": detail.get("workerType"),
                "industry": detail.get("industry") or summary.get("industry"),
                "company_name": detail.get("companyName"),
                "opco": detail.get("opco") or summary.get("opco"),
                "ats": detail.get("ats"),
                "apply_url": apply_url,
                "city": detail.get("city") or summary.get("city"),
                "state": detail.get("state") or summary.get("state"),
                "country": detail.get("country") or summary.get("country"),
                "postal_code": detail.get("postalCode"),
                "latitude": detail.get("latitude") or summary.get("latitude"),
                "longitude": detail.get("longitude") or summary.get("longitude"),
                "city_state_country": detail.get("cityStateCountry") or summary.get("cityStateCountry"),
                "multi_location": detail.get("multi_location") or summary.get("multi_location"),
                "multi_location_array": detail.get("multi_location_array")
                or summary.get("multi_location_array"),
                "multi_category": detail.get("multi_category") or summary.get("multi_category"),
                "multi_category_array": detail.get("multi_category_array")
                or summary.get("multi_category_array"),
                "map_query_location": detail.get("mapQueryLocation"),
                "job_visibility": detail.get("jobVisibility") or summary.get("jobVisibility"),
                "posting_status": detail.get("postingStatus"),
                "posted_date": posted_date,
                "date_created": detail.get("dateCreated") or summary.get("dateCreated"),
                "job_updated": detail.get("jobUpdated"),
                "job_requisition_id": detail.get("jobRequisitionId"),
                "remote": detail.get("remote"),
                "remote_type": detail.get("remoteTypeValue"),
                "structure_data": detail.get("structureData"),
                "description_html": description_html,
                "detail_url": detail_url,
                "detail_locale": detail.get("locale") or summary.get("locale"),
            }
        )

        return JobListing(
            job_seq=job_seq,
            title=title,
            link=detail_url,
            location=location or None,
            posted_date=posted_date,
            description_text=description_text,
            description_html=description_html,
            apply_url=apply_url,
            metadata=metadata,
        )

    @staticmethod
    def _extract_ddo(html: str) -> Dict[str, object]:
        marker = "phApp.ddo = "
        start = html.find(marker)
        if start == -1:
            raise ScraperError("Unable to locate phApp.ddo payload.")

        start += len(marker)
        while start < len(html) and html[start].isspace():
            start += 1
        if start >= len(html) or html[start] != "{":
            raise ScraperError("Unexpected phApp.ddo JSON structure.")

        depth = 0
        end = start
        for idx in range(start, len(html)):
            char = html[idx]
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    end = idx + 1
                    break
        else:
            raise ScraperError("Unterminated phApp.ddo payload.")

        raw_payload = html[start:end].replace("\n", " ")
        try:
            return json.loads(raw_payload)
        except json.JSONDecodeError as exc:
            raise ScraperError(f"Failed to decode phApp.ddo payload: {exc}") from exc


# ---------------------------------------------------------------------------
# Persistence and CLI
# ---------------------------------------------------------------------------
def persist_listing(listing: JobListing) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255] or None,
        "date": (listing.posted_date or "")[:100] or None,
        "description": listing.description_text[:10000],
        "metadata": {
            **listing.metadata,
            "apply_url": listing.apply_url,
            "description_html": listing.description_html,
        },
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.link,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Stored job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Danaher careers manual scraper.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Maximum number of result pages to fetch.",
    )
    parser.add_argument(
        "--start-offset",
        type=int,
        default=0,
        help="Starting offset (number of jobs to skip) for pagination.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help="Seconds to sleep between paginated requests.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and display jobs without persisting them.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO).",
    )
    return parser.parse_args(argv)


def run(args: argparse.Namespace) -> Dict[str, int]:
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}
    client = DanaherCareersClient(delay=args.delay)

    for listing in client.iter_listings(
        limit=args.limit,
        max_pages=args.max_pages,
        start_offset=args.start_offset,
    ):
        totals["fetched"] += 1

        if args.dry_run:
            print(json.dumps(asdict(listing), default=str, ensure_ascii=False))
            continue

        try:
            created = persist_listing(listing)
        except Exception as exc:  # pragma: no cover - persistence error path
            logging.error("Failed to persist job %s: %s", listing.link, exc)
            totals["errors"] += 1
            continue

        if created:
            totals["created"] += 1
        else:
            totals["updated"] += 1

    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["deduplicated"] = dedupe_summary

    return totals


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(levelname)s: %(message)s",
    )
    start = time.time()

    try:
        totals = run(args)
    except ScraperError as exc:
        logging.error("Danaher careers scrape failed: %s", exc)
        return 1

    duration = time.time() - start
    summary = {
        "company": "Danaher",
        "site": CAREERS_LANDING_URL,
        "elapsed_seconds": round(duration, 2),
        **totals,
    }
    logging.info("Scrape summary: %s", json.dumps(summary))
    if args.dry_run:
        print(json.dumps(summary))
    return 0 if not totals.get("errors") else 1


if __name__ == "__main__":
    raise SystemExit(main())
