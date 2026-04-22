#!/usr/bin/env python3
"""Manual scraper for the CVS Health careers site (Phenom platform)."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, Tuple

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
from django.db import IntegrityError  # noqa: E402

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

# ---------------------------------------------------------------------------
# Constants & configuration
# ---------------------------------------------------------------------------
CAREERS_ROOT = "https://jobs.cvshealth.com"
SEARCH_PATH = "/us/en/search-results"
JOB_DETAIL_PATH = "/us/en/job/{job_seq}"
CAREERS_SEARCH_URL = f"{CAREERS_ROOT}{SEARCH_PATH}"

REQUEST_TIMEOUT: Tuple[int, int] = (10, 40)
DEFAULT_LISTING_DELAY = 0.25
DEFAULT_DETAIL_DELAY = 0.05

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": CAREERS_ROOT,
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 1200), 30)

SCRAPER_QS = Scraper.objects.filter(company="CVS Health", url=CAREERS_SEARCH_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple CVS Health scraper rows found; using id=%s.", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="CVS Health",
        url=CAREERS_SEARCH_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the CVS Health scraper encounters an unrecoverable error."""


@dataclass
class CVSHealthJob:
    job_seq: str
    title: str
    link: str
    location: Optional[str]
    posted_date: Optional[str]
    apply_url: Optional[str]
    description_text: str
    description_html: Optional[str]
    metadata: Dict[str, Any]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _strip(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    trimmed = value.strip()
    return trimmed or None


def _clean_text(html_fragment: Optional[str]) -> str:
    if not html_fragment:
        return ""
    soup = BeautifulSoup(html_fragment, "html.parser")
    return soup.get_text("\n", strip=True).strip()


def _compact_metadata(items: Iterable[Tuple[str, Any]]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in items:
        if value is None:
            continue
        if isinstance(value, str):
            trimmed = value.strip()
            if not trimmed:
                continue
            result[key] = trimmed
            continue
        if isinstance(value, (list, dict)) and not value:
            continue
        result[key] = value
    return result


def _extract_ddo(html_text: str) -> Dict[str, Any]:
    marker = "phApp.ddo = "
    start = html_text.find(marker)
    if start == -1:
        raise ScraperError("Unable to locate phApp.ddo payload.")
    start += len(marker)

    while start < len(html_text) and html_text[start].isspace():
        start += 1

    if start >= len(html_text) or html_text[start] != "{":
        raise ScraperError("Unexpected phApp.ddo payload structure.")

    depth = 0
    end = start
    for idx in range(start, len(html_text)):
        char = html_text[idx]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                end = idx
                break
    else:
        raise ScraperError("Unable to parse phApp.ddo payload (unterminated object).")

    payload = html_text[start : end + 1]
    try:
        return json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ScraperError(f"Failed to decode phApp.ddo payload: {exc}") from exc


class CVSHealthCareersClient:
    def __init__(
        self,
        *,
        session: Optional[requests.Session] = None,
        listing_delay: float = DEFAULT_LISTING_DELAY,
        detail_delay: float = DEFAULT_DETAIL_DELAY,
    ) -> None:
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.listing_delay = max(0.0, listing_delay)
        self.detail_delay = max(0.0, detail_delay)
        self.logger = logging.getLogger(self.__class__.__name__)

    def iter_jobs(
        self,
        *,
        max_pages: Optional[int] = None,
        max_results: Optional[int] = None,
    ) -> Iterator[CVSHealthJob]:
        offset = 0
        page = 0
        yielded = 0
        seen_sequences: Set[str] = set()

        while True:
            jobs, total_hits = self._fetch_listing_page(offset=offset)
            if not jobs:
                self.logger.info("No jobs returned at offset %s; stopping pagination.", offset)
                break

            self.logger.debug(
                "Fetched %s jobs at offset %s (total hits: %s).", len(jobs), offset, total_hits
            )

            for job_summary in jobs:
                job_seq = _strip(job_summary.get("jobSeqNo"))
                title = _strip(job_summary.get("title"))
                if not job_seq or not title:
                    continue
                if job_seq in seen_sequences:
                    continue
                seen_sequences.add(job_seq)

                try:
                    detail_payload = self._fetch_job_detail(job_seq)
                    listing = self._build_job(job_summary, detail_payload)
                except ScraperError as exc:
                    self.logger.warning("Skipping job %s due to error: %s", job_seq, exc)
                    continue

                yield listing
                yielded += 1

                if max_results and yielded >= max_results:
                    self.logger.info("Reached max_results=%s; stopping.", max_results)
                    return

                if self.detail_delay:
                    time.sleep(self.detail_delay)

            offset += len(jobs)
            page += 1

            if max_pages and page >= max_pages:
                self.logger.info("Reached max_pages=%s; stopping pagination.", max_pages)
                break

            if self.listing_delay:
                time.sleep(self.listing_delay)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _fetch_listing_page(self, *, offset: int) -> Tuple[List[Dict[str, Any]], Optional[int]]:
        params = {"from": offset} if offset else None
        try:
            response = self.session.get(CAREERS_SEARCH_URL, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch listing page at offset {offset}: {exc}") from exc

        ddo = _extract_ddo(response.text)
        refine = ddo.get("eagerLoadRefineSearch") or {}
        data = refine.get("data") or {}
        jobs = data.get("jobs") or []
        total_hits = refine.get("totalHits") or refine.get("hits")
        return jobs, total_hits

    def _fetch_job_detail(self, job_seq: str) -> Dict[str, Any]:
        detail_url = f"{CAREERS_ROOT}{JOB_DETAIL_PATH.format(job_seq=job_seq)}"
        try:
            response = self.session.get(detail_url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch job detail {job_seq}: {exc}") from exc

        ddo = _extract_ddo(response.text)
        job_detail = ((ddo.get("jobDetail") or {}).get("data") or {}).get("job")
        if not isinstance(job_detail, dict):
            raise ScraperError(f"Unexpected job detail payload for {job_seq}.")
        return job_detail

    def _build_job(self, summary: Dict[str, Any], detail: Dict[str, Any]) -> CVSHealthJob:
        job_seq = _strip(summary.get("jobSeqNo") or detail.get("jobSeqNo")) or ""
        title = _strip(summary.get("title") or detail.get("title")) or ""
        if not job_seq or not title:
            raise ScraperError("Missing job sequence or title.")

        location = (
            _strip(summary.get("cityStateCountry"))
            or _strip(summary.get("location"))
            or _strip(detail.get("cityStateCountry"))
            or _strip(detail.get("location"))
        )
        posted_date = _strip(summary.get("postedDate") or detail.get("jobUpdatedDate"))
        apply_url = _strip(summary.get("applyUrl") or detail.get("applyUrl"))

        structure = detail.get("structureData") or {}
        description_html = (
            structure.get("description")
            or detail.get("jobDescription")
            or detail.get("description")
            or ""
        )
        description_text = _clean_text(description_html) or _strip(description_html) or ""

        structure_snapshot = {
            key: value
            for key, value in (
                ("employmentType", structure.get("employmentType")),
                ("identifier", structure.get("identifier")),
                ("jobLocation", structure.get("jobLocation")),
            )
            if value
        }

        metadata = _compact_metadata(
            (
                ("job_seq_no", job_seq),
                ("req_id", summary.get("reqId") or detail.get("reqId")),
                ("job_id", summary.get("jobId") or detail.get("jobId")),
                ("job_code", detail.get("jobCode")),
                ("job_unique_identifier", detail.get("jobUniqueIdentifier")),
                ("category", summary.get("category") or detail.get("category_raw")),
                ("sub_category", summary.get("subCategory") or detail.get("subCategory")),
                ("type", summary.get("type") or detail.get("type")),
                ("remote", summary.get("remote") or detail.get("remote")),
                ("city", summary.get("city") or detail.get("city")),
                ("state", summary.get("state") or detail.get("state")),
                ("country", summary.get("country") or detail.get("country")),
                ("posted_date", posted_date),
                ("date_created", summary.get("dateCreated") or detail.get("dateCreated")),
                ("job_updated_date", detail.get("jobUpdatedDate")),
                ("description_teaser", summary.get("descriptionTeaser") or detail.get("descriptionTeaser")),
                ("apply_url", apply_url),
                ("external_apply", summary.get("externalApply") or detail.get("externalApply")),
                ("job_visibility", summary.get("jobVisibility") or detail.get("jobVisibility")),
                ("multi_location", summary.get("multi_location") or detail.get("multi_location")),
                ("multi_location_array", summary.get("multi_location_array") or detail.get("multi_location_array")),
                ("latitude", summary.get("latitude") or detail.get("latitude")),
                ("longitude", summary.get("longitude") or detail.get("longitude")),
                ("ai_summary", detail.get("ai_summary")),
                ("division", detail.get("division")),
                ("company_name", detail.get("companyName")),
                ("job_profile", detail.get("jobProfile")),
                ("job_family_group", detail.get("jobFamilyGroup")),
                ("ml_skills", summary.get("ml_skills") or detail.get("ml_skills")),
                ("structure_snapshot", structure_snapshot),
            )
        )

        detail_link = f"{CAREERS_ROOT}{JOB_DETAIL_PATH.format(job_seq=job_seq)}"

        return CVSHealthJob(
            job_seq=job_seq,
            title=title,
            link=detail_link,
            location=location,
            posted_date=posted_date,
            apply_url=apply_url,
            description_text=description_text,
            description_html=description_html if description_html else None,
            metadata=metadata,
        )


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------
def store_job(listing: CVSHealthJob) -> None:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255],
        "date": (listing.posted_date or "")[:100],
        "description": listing.description_text[:10000],
        "metadata": listing.metadata,
    }
    try:
        JobPosting.objects.update_or_create(
            scraper=SCRAPER,
            link=listing.link,
            defaults=defaults,
        )
    except IntegrityError as exc:
        raise ScraperError(f"Failed to store job {listing.job_seq}: {exc}") from exc


# ---------------------------------------------------------------------------
# CLI orchestration
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CVS Health careers manual scraper")
    parser.add_argument("--max-pages", type=int, default=None, help="Maximum number of listing pages to process")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to persist")
    parser.add_argument(
        "--listing-delay",
        type=float,
        default=DEFAULT_LISTING_DELAY,
        help="Delay (seconds) between successive listing page requests",
    )
    parser.add_argument(
        "--detail-delay",
        type=float,
        default=DEFAULT_DETAIL_DELAY,
        help="Delay (seconds) between detail page requests",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity",
    )
    return parser.parse_args(argv)


def run_scrape(args: argparse.Namespace) -> Dict[str, Any]:
    client = CVSHealthCareersClient(
        listing_delay=args.listing_delay,
        detail_delay=args.detail_delay,
    )
    processed = 0
    for listing in client.iter_jobs(max_pages=args.max_pages, max_results=args.limit):
        store_job(listing)
        processed += 1
    dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
    return {"processed_jobs": processed, "deduplicated": dedupe_summary}


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    start = time.time()
    try:
        outcome = run_scrape(args)
    except ScraperError as exc:
        logging.error("Scrape failed: %s", exc)
        return 1
    duration = time.time() - start
    summary = {
        "company": "CVS Health",
        "site": CAREERS_SEARCH_URL,
        "elapsed_seconds": round(duration, 2),
        **outcome,
    }
    logging.info("Summary: %s", json.dumps(summary))
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
