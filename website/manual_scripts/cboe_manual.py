#!/usr/bin/env python3
"""Manual scraper for the Cboe Global Markets careers site (Phenom platform)."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Set
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# -----------------------------------------------------------------------------
# Django bootstrap
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# Constants & configuration
# -----------------------------------------------------------------------------
BASE_DOMAIN = "https://careers.cboe.com"
SEARCH_PATH = "/us/en/search-results"
DETAIL_PATH = "/us/en/job/{job_seq}"
ROOT_URL = urljoin(BASE_DOMAIN, SEARCH_PATH.lstrip("/"))
REQUEST_TIMEOUT = (10, 30)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": urljoin(BASE_DOMAIN, "/us/en"),
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 1800), 60)
SCRAPER_QS = Scraper.objects.filter(company="Cboe Global Markets", url=ROOT_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning(
            "Multiple scraper rows matched for Cboe Global Markets; using the earliest (id=%s).",
            SCRAPER.id,
        )
else:
    SCRAPER = Scraper.objects.create(
        company="Cboe Global Markets",
        url=ROOT_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the scraping pipeline encounters an unrecoverable issue."""


@dataclass
class JobRecord:
    title: str
    link: str
    location: Optional[str]
    date: Optional[str]
    description_text: str
    description_html: Optional[str]
    metadata: Dict[str, object]


def _extract_ddo(html: str) -> Dict[str, object]:
    match = re.search(r"phApp\.ddo\s*=\s*(\{.*?\});\s*phApp", html, re.DOTALL)
    if not match:
        raise ScraperError("Unable to locate phApp.ddo payload in response.")
    payload = match.group(1)
    try:
        return json.loads(payload)
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise ScraperError(f"Failed to decode phApp.ddo payload: {exc}") from exc


def _clean_text(fragment: Optional[str]) -> str:
    if not fragment:
        return ""
    soup = BeautifulSoup(fragment, "html.parser")
    return soup.get_text("\n", strip=True).strip()


def _compact_metadata(pairs: Iterable[tuple[str, object]]) -> Dict[str, object]:
    data: Dict[str, object] = {}
    for key, value in pairs:
        if value in (None, "", [], {}):
            continue
        data[key] = value
    return data


class CboeCareersClient:
    def __init__(
        self,
        *,
        session: Optional[requests.Session] = None,
        page_delay: float = 0.15,
        detail_delay: float = 0.1,
    ) -> None:
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.page_delay = max(0.0, page_delay)
        self.detail_delay = max(0.0, detail_delay)

    def iter_job_records(
        self,
        *,
        max_pages: Optional[int] = None,
        max_results: Optional[int] = None,
    ) -> Iterator[JobRecord]:
        offset = 0
        page = 0
        seen_seqs: Set[str] = set()
        total_hits: Optional[int] = None

        while True:
            listing_payload = self._fetch_listing_page(offset)
            jobs: List[Dict[str, object]] = list(listing_payload.get("jobs") or [])
            if not jobs:
                break

            if total_hits is None:
                total_hits = listing_payload.get("total_hits")
                logging.info(
                    "Discovered %s total jobs (page size %s).",
                    total_hits if total_hits is not None else len(jobs),
                    len(jobs),
                )

            for summary in jobs:
                job_seq = (summary.get("jobSeqNo") or "").strip()
                if not job_seq:
                    continue
                if job_seq in seen_seqs:
                    continue
                seen_seqs.add(job_seq)

                detail = self._fetch_job_detail(job_seq)
                record = self._build_job_record(summary, detail)
                yield record

                if self.detail_delay:
                    time.sleep(self.detail_delay)

                if max_results and len(seen_seqs) >= max_results:
                    logging.info("Reached max-results limit (%s); stopping.", max_results)
                    return

            offset += len(jobs)
            page += 1
            if max_pages and page >= max_pages:
                logging.info("Reached max-pages limit (%s); stopping pagination.", max_pages)
                break
            if total_hits is not None and offset >= total_hits:
                logging.info("Processed all available jobs (%s).", total_hits)
                break
            if self.page_delay:
                time.sleep(self.page_delay)

    # ------------------------------------------------------------------ #
    # HTTP helpers                                                       #
    # ------------------------------------------------------------------ #
    def _fetch_listing_page(self, offset: int) -> Dict[str, object]:
        params = {"from": offset} if offset else None
        try:
            resp = self.session.get(ROOT_URL, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch listing page at offset {offset}: {exc}") from exc

        ddo = _extract_ddo(resp.text)
        refine = ddo.get("eagerLoadRefineSearch") or {}
        data = refine.get("data") or {}
        return {
            "jobs": data.get("jobs") or [],
            "total_hits": refine.get("totalHits") or refine.get("hits"),
        }

    def _fetch_job_detail(self, job_seq: str) -> Dict[str, object]:
        detail_url = urljoin(BASE_DOMAIN, DETAIL_PATH.format(job_seq=job_seq).lstrip("/"))
        try:
            resp = self.session.get(detail_url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch job detail for {job_seq}: {exc}") from exc

        ddo = _extract_ddo(resp.text)
        job_detail = ((ddo.get("jobDetail") or {}).get("data") or {}).get("job") or {}
        if not job_detail:
            logging.warning("No job detail payload returned for job_seq=%s.", job_seq)
        return job_detail

    # ------------------------------------------------------------------ #
    # Record construction                                                #
    # ------------------------------------------------------------------ #
    def _build_job_record(self, summary: Dict[str, object], detail: Dict[str, object]) -> JobRecord:
        job_seq = (summary.get("jobSeqNo") or detail.get("jobSeqNo") or "").strip()
        link = urljoin(BASE_DOMAIN, DETAIL_PATH.format(job_seq=job_seq).lstrip("/")) if job_seq else ROOT_URL

        description_html = (
            detail.get("description")
            or (detail.get("structureData") or {}).get("description")
            or detail.get("ml_Description")
            or ""
        )
        description_text = _clean_text(description_html)

        location_label = (
            summary.get("cityStateCountry")
            or summary.get("location")
            or detail.get("cityStateCountry")
            or detail.get("location")
        )
        posted_date = summary.get("postedDate") or detail.get("postedDate")

        metadata = _compact_metadata(
            (
                ("job_id", summary.get("jobId") or detail.get("jobId")),
                ("req_id", summary.get("reqId") or detail.get("reqId")),
                ("job_seq_no", job_seq),
                ("job_type", summary.get("type") or detail.get("type")),
                ("category", summary.get("category") or detail.get("category")),
                ("city_state_country", summary.get("cityStateCountry") or detail.get("cityStateCountry")),
                ("address", summary.get("address") or detail.get("address")),
                ("apply_url", summary.get("applyUrl") or detail.get("applyUrl")),
                ("external_apply", summary.get("externalApply") or detail.get("externalApply")),
                ("ml_skills", summary.get("ml_skills") or detail.get("ml_skills")),
                ("job_visibility", summary.get("jobVisibility") or detail.get("jobVisibility")),
                ("date_created", summary.get("dateCreated") or detail.get("dateCreated")),
                ("posted_date", posted_date),
                ("latitude", summary.get("latitude") or detail.get("latitude")),
                ("longitude", summary.get("longitude") or detail.get("longitude")),
                ("multi_location", summary.get("multi_location") or detail.get("multi_location")),
                ("multi_location_array", summary.get("multi_location_array") or detail.get("multi_location_array")),
                ("structure_data", (detail.get("structureData") or {}) or None),
                ("description_teaser", summary.get("descriptionTeaser") or detail.get("descriptionTeaser")),
            )
        )

        if description_html:
            metadata["description_html"] = description_html

        return JobRecord(
            title=(summary.get("title") or detail.get("title") or "").strip(),
            link=link,
            location=(location_label or "").strip() or None,
            date=str(posted_date) if posted_date else None,
            description_text=description_text,
            description_html=description_html if description_html else None,
            metadata=metadata,
        )


def store_job(record: JobRecord) -> None:
    metadata = dict(record.metadata or {})
    JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=record.link,
        defaults={
            "title": record.title[:255],
            "location": (record.location or "")[:255],
            "date": (record.date or "")[:100],
            "description": record.description_text[:10000],
            "metadata": metadata,
        },
    )


def run_scrape(
    *,
    max_pages: Optional[int],
    max_results: Optional[int],
    page_delay: float,
    detail_delay: float,
) -> int:
    client = CboeCareersClient(page_delay=page_delay, detail_delay=detail_delay)
    processed = 0
    for record in client.iter_job_records(max_pages=max_pages, max_results=max_results):
        store_job(record)
        processed += 1
    return processed


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cboe Global Markets careers manual scraper")
    parser.add_argument("--max-pages", type=int, default=None, help="Maximum result pages to process")
    parser.add_argument("--max-results", type=int, default=None, help="Maximum job records to process")
    parser.add_argument("--page-delay", type=float, default=0.15, help="Delay between listing page fetches (seconds)")
    parser.add_argument("--detail-delay", type=float, default=0.1, help="Delay between job detail fetches (seconds)")
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))
    start_time = time.time()

    try:
        processed = run_scrape(
            max_pages=args.max_pages,
            max_results=args.max_results,
            page_delay=args.page_delay,
            detail_delay=args.detail_delay,
        )
    except ScraperError as exc:
        logging.error("Scrape failed: %s", exc)
        return 1

    dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
    duration = time.time() - start_time
    summary = {
        "company": "Cboe Global Markets",
        "url": ROOT_URL,
        "processed": processed,
        "elapsed_seconds": duration,
        "dedupe": dedupe_summary,
    }
    logging.info("Summary: %s", json.dumps(summary))
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

