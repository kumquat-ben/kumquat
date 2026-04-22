#!/usr/bin/env python3
"""Manual scraper for DaVita careers (Phenom + Workday).

This script walks the public search results hosted on https://careers.davita.com,
hydrates each job with its corresponding Workday detail page, and persists the
listings through the shared ``JobPosting`` model for on-demand/manual ingestion.
"""
from __future__ import annotations

import argparse
import html
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional, Set
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
# Constants & configuration
# ---------------------------------------------------------------------------
BASE_DOMAIN = "https://careers.davita.com"
SEARCH_PATH = "/search-results"
SEARCH_URL = f"{BASE_DOMAIN}{SEARCH_PATH}"
DEFAULT_PAGE_SIZE = 10
REQUEST_TIMEOUT = (10, 45)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": SEARCH_URL,
}

WORKDAY_PORTAL = "DKC_External"
WORKDAY_ROOT = "https://davita.wd1.myworkdayjobs.com"
WORKDAY_DETAIL_BASE = f"{WORKDAY_ROOT}/{WORKDAY_PORTAL}"

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 60)
SCRAPER_QS = Scraper.objects.filter(company="DaVita", url=SEARCH_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.getLogger(__name__).warning(
            "Multiple Scraper rows matched DaVita; using id=%s.", SCRAPER.id
        )
else:
    SCRAPER = Scraper.objects.create(
        company="DaVita",
        url=SEARCH_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
class ScraperError(Exception):
    """Raised when the scraper encounters an unrecoverable error."""


@dataclass
class JobSummary:
    job_seq: str
    req_id: Optional[str]
    title: str
    detail_url: str
    apply_url: Optional[str]
    location_text: Optional[str]
    city: Optional[str]
    state: Optional[str]
    country: Optional[str]
    posted_date: Optional[str]
    description_teaser: Optional[str]
    metadata: Dict[str, object] = field(default_factory=dict)


@dataclass
class JobListing(JobSummary):
    description: str = ""
    date_posted: Optional[str] = None
    employment_type: Optional[str] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _clean(value: Optional[str]) -> str:
    return (value or "").strip()


def _strip_html(value: Optional[str]) -> str:
    if not value:
        return ""
    soup = BeautifulSoup(value, "html.parser")
    return soup.get_text("\n", strip=True).strip()


class DaVitaJobScraper:
    def __init__(self, *, delay: float = 0.25, session: Optional[requests.Session] = None) -> None:
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
        start_offset: int = 0,
        limit: Optional[int] = None,
        max_pages: Optional[int] = None,
    ) -> Iterator[JobListing]:
        if start_offset < 0:
            raise ValueError("start_offset must be >= 0.")

        offset = start_offset
        yielded = 0
        pages = 0
        seen_sequences: Set[str] = set()
        total_hits: Optional[int] = None

        while True:
            jobs, total_hits = self._fetch_search_page(offset)
            if not jobs:
                self.logger.info("No jobs returned at offset=%s; stopping pagination.", offset)
                break

            for job in jobs:
                try:
                    summary = self._build_summary(job)
                except ScraperError as exc:
                    self.logger.debug("Skipping job due to summary error: %s", exc)
                    continue

                if summary.job_seq in seen_sequences:
                    self.logger.debug("Duplicate job_seq %s detected; skipping.", summary.job_seq)
                    continue
                seen_sequences.add(summary.job_seq)

                try:
                    listing = self._hydrate_listing(summary)
                except ScraperError as exc:
                    self.logger.warning("Failed to hydrate job %s: %s", summary.detail_url, exc)
                    continue

                yield listing
                yielded += 1
                if limit is not None and yielded >= limit:
                    self.logger.info("Reached limit=%s; stopping scrape.", limit)
                    return

            offset += len(jobs)
            pages += 1
            if max_pages is not None and pages >= max_pages:
                self.logger.info("Reached max_pages=%s; stopping.", max_pages)
                break

            if total_hits is not None and offset >= total_hits:
                self.logger.info("Reached totalHits=%s; pagination complete.", total_hits)
                break

            if self.delay:
                time.sleep(self.delay)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _fetch_search_page(self, offset: int) -> tuple[list[dict], Optional[int]]:
        params = {"from": offset} if offset else None
        self.logger.debug("Fetching search-results offset=%s", offset)
        resp = self.session.get(SEARCH_URL, params=params, timeout=REQUEST_TIMEOUT)

        if resp.status_code >= 500:
            raise ScraperError(f"Search results request failed with status {resp.status_code}.")

        payload = self._extract_ddo_json(resp.text)
        refine = payload.get("eagerLoadRefineSearch") or {}
        data = refine.get("data") or {}
        jobs = data.get("jobs") or []
        total_hits = refine.get("totalHits")
        return jobs, int(total_hits) if total_hits else None

    def _extract_ddo_json(self, html_text: str) -> dict:
        soup = BeautifulSoup(html_text, "html.parser")
        for script in soup.find_all("script"):
            raw_text = script.string or script.get_text()
            if not raw_text:
                continue
            decoded = html.unescape(raw_text)
            marker = "phApp.ddo = "
            if marker not in decoded:
                continue
            start = decoded.find(marker) + len(marker)
            end_token = "; phApp.experimentData"
            if end_token in decoded:
                end = decoded.find(end_token, start)
            else:
                # Fallback: end of script tag
                end = len(decoded)
            payload = decoded[start:end].strip()
            if not payload:
                continue
            try:
                return json.loads(payload)
            except json.JSONDecodeError as exc:
                raise ScraperError(f"Failed to decode phApp.ddo JSON: {exc}") from exc

        raise ScraperError("Unable to locate phApp.ddo payload in search results page.")

    def _build_summary(self, job: Dict[str, object]) -> JobSummary:
        job_seq = _clean(job.get("jobSeqNo"))
        req_id = _clean(job.get("reqId") or job.get("jobId"))
        title = _clean(job.get("title"))
        apply_url = _clean(job.get("applyUrl"))

        if not job_seq or not title:
            raise ScraperError("Job payload missing jobSeqNo or title.")

        detail_url = apply_url.rstrip("/")
        if detail_url.endswith("/apply"):
            detail_url = detail_url[: -len("/apply")]
        if not detail_url and apply_url:
            detail_url = apply_url
        if not detail_url:
            detail_url = urljoin(WORKDAY_DETAIL_BASE + "/", req_id) if req_id else ""

        location_text = (
            _clean(job.get("cityStateCountry"))
            or _clean(job.get("location"))
            or _clean(job.get("cityState"))
            or None
        )

        metadata = {
            "job_seq_no": job_seq,
            "req_id": req_id,
            "job_id": _clean(job.get("jobId")),
            "category": job.get("category"),
            "subCategory": job.get("subCategory"),
            "type": job.get("type"),
            "site_type": job.get("siteType"),
            "visibility": job.get("jobVisibility"),
            "ml_skills": job.get("ml_skills"),
            "multi_location": job.get("multi_location"),
            "multi_location_array": job.get("multi_location_array"),
            "address": job.get("address"),
            "latitude": job.get("latitude"),
            "longitude": job.get("longitude"),
            "industry": job.get("industry"),
            "raw": job,
        }

        return JobSummary(
            job_seq=job_seq,
            req_id=req_id or None,
            title=title,
            detail_url=detail_url,
            apply_url=apply_url or None,
            location_text=location_text,
            city=_clean(job.get("city")) or None,
            state=_clean(job.get("state")) or None,
            country=_clean(job.get("country")) or None,
            posted_date=_clean(job.get("postedDate")) or None,
            description_teaser=_clean(job.get("descriptionTeaser")) or None,
            metadata={k: v for k, v in metadata.items() if v},
        )

    def _hydrate_listing(self, summary: JobSummary) -> JobListing:
        description = summary.description_teaser or ""
        employment_type = None
        date_posted = summary.posted_date
        json_ld_metadata: Dict[str, object] = {}

        if summary.detail_url:
            try:
                detail_html = self._fetch_detail_html(summary.detail_url)
                json_ld_payload = self._parse_json_ld(detail_html)
            except ScraperError as exc:
                self.logger.debug("Detail page parse failed for %s: %s", summary.detail_url, exc)
            else:
                raw_description = (json_ld_payload.get("description") or "").strip()
                text_description = _strip_html(html.unescape(raw_description))
                if text_description:
                    description = text_description

                employment_type = _clean(json_ld_payload.get("employmentType") or summary.metadata.get("type"))
                date_posted = _clean(json_ld_payload.get("datePosted")) or date_posted
                json_ld_metadata = json_ld_payload

        metadata = dict(summary.metadata)
        if json_ld_metadata:
            metadata["json_ld"] = json_ld_metadata

        payload = dict(summary.__dict__)
        payload.update(
            description=description or "",
            employment_type=employment_type or None,
            date_posted=date_posted or None,
            metadata=metadata,
        )
        return JobListing(**payload)

    def _fetch_detail_html(self, url: str) -> str:
        headers = dict(self.session.headers)
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        self.logger.debug("Fetching job detail: %s", url)
        resp = self.session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        if resp.status_code >= 400:
            raise ScraperError(f"Detail page request failed ({resp.status_code}) for {url}.")
        return resp.text

    def _parse_json_ld(self, html_text: str) -> Dict[str, object]:
        soup = BeautifulSoup(html_text, "html.parser")
        script_tag = soup.find("script", attrs={"type": "application/ld+json"})
        if not script_tag:
            raise ScraperError("JSON-LD script not found in detail page.")

        raw_json = script_tag.string or script_tag.get_text()
        if not raw_json:
            raise ScraperError("JSON-LD payload empty in detail page.")

        try:
            data = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            raise ScraperError(f"Failed to parse JSON-LD payload: {exc}") from exc

        return data if isinstance(data, dict) else {"raw": data}


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------
def persist_listing(listing: JobListing) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location_text or "")[:255] or None,
        "date": (listing.date_posted or listing.posted_date or "")[:100] or None,
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


# ---------------------------------------------------------------------------
# CLI wiring
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape careers.davita.com and persist job postings.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument("--start-offset", type=int, default=0, help="Starting offset (default: 0).")
    parser.add_argument("--max-pages", type=int, default=None, help="Maximum number of result pages to fetch.")
    parser.add_argument("--delay", type=float, default=0.25, help="Delay (seconds) between page fetches.")
    parser.add_argument("--dry-run", action="store_true", help="Print listings instead of writing to the database.")
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

    scraper = DaVitaJobScraper(delay=args.delay)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    for listing in scraper.iter_listings(
        start_offset=args.start_offset,
        limit=args.limit,
        max_pages=args.max_pages,
    ):
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
        except Exception as exc:  # pragma: no cover - persistence failure path
            logging.error("Failed to persist job %s: %s", listing.detail_url, exc)
            totals["errors"] += 1

    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        logging.info("Deduplicated job postings: %s", dedupe_summary)

    logging.info(
        "DaVita scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
