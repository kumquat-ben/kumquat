#!/usr/bin/env python3
"""Custom scraper for https://www.rippling.com/careers (Rippling ATS)."""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Django setup
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
CAREERS_URL = "https://www.rippling.com/careers"
OPEN_ROLES_URL = "https://www.rippling.com/careers/open-roles"
COMPANY_NAME = "Rippling"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

REQUEST_TIMEOUT = (10, 40)
DEFAULT_DELAY = 0.2
DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 30)

SCRAPER_QS = Scraper.objects.filter(company=COMPANY_NAME, url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Rippling scrapers found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company=COMPANY_NAME,
        url=CAREERS_URL,
        code="custom-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the scraper cannot continue."""


@dataclass
class JobSummary:
    posting_id: str
    title: str
    detail_url: str
    department: Optional[str]
    location: Optional[str]
    locations: List[str]
    workplace_types: List[str]
    language: Optional[str]
    is_remote: Optional[bool]


@dataclass
class JobListing(JobSummary):
    description_text: Optional[str]
    description_html: Optional[str]
    metadata: Dict[str, object]


class RipplingJobScraper:
    def __init__(self, delay: float = DEFAULT_DELAY, session: Optional[requests.Session] = None) -> None:
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    def scrape(self, limit: Optional[int] = None) -> Generator[JobListing, None, None]:
        summaries = list(self._fetch_job_summaries())
        self.logger.info("Discovered %s job postings", len(summaries))

        yielded = 0
        for summary in summaries:
            detail = self._fetch_job_detail(summary.detail_url)
            listing = JobListing(**asdict(summary), **detail)
            yield listing
            yielded += 1
            if limit is not None and yielded >= limit:
                self.logger.info("Reached limit %s; stopping scrape", limit)
                return
            if self.delay:
                time.sleep(self.delay)

    # ------------------------------------------------------------------
    # Fetch + parse helpers
    # ------------------------------------------------------------------
    def _fetch_job_summaries(self) -> Iterable[JobSummary]:
        response = self.session.get(OPEN_ROLES_URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        next_data = _extract_next_data(response.text)
        jobs_payload = next_data.get("props", {}).get("pageProps", {}).get("jobs") or {}
        items = jobs_payload.get("items") or []

        if not isinstance(items, list) or not items:
            raise ScraperError("No job listings found on Rippling open roles page.")

        for item in items:
            if not isinstance(item, dict):
                continue
            posting_id = item.get("id")
            title = (item.get("name") or "").strip()
            detail_url = item.get("url") or ""
            if not (posting_id and title and detail_url):
                continue

            raw_locations = item.get("locations") or []
            location_names, workplace_types = _extract_location_names(raw_locations)
            is_remote = any(wt.upper() == "REMOTE" for wt in workplace_types) if workplace_types else None
            department = (item.get("department") or {}).get("name")

            yield JobSummary(
                posting_id=str(posting_id),
                title=title,
                detail_url=detail_url,
                department=department,
                location=location_names[0] if location_names else None,
                locations=location_names,
                workplace_types=workplace_types,
                language=item.get("language"),
                is_remote=is_remote,
            )

    def _fetch_job_detail(self, url: str) -> Dict[str, Optional[object]]:
        response = self.session.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        next_data = _extract_next_data(response.text)
        api_data = next_data.get("props", {}).get("pageProps", {}).get("apiData") or {}
        job_post = api_data.get("jobPost") or {}

        description_html = _combine_description_html(job_post.get("description"))
        description_text = _clean_text(description_html)
        detail_locations = _normalize_locations(job_post.get("workLocations"))

        metadata = _build_metadata(
            api_data=api_data,
            job_post=job_post,
            detail_locations=detail_locations,
        )

        return {
            "description_text": description_text,
            "description_html": description_html,
            "metadata": metadata,
        }


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _extract_next_data(html: str) -> Dict[str, Any]:
    import re

    match = re.search(
        r'<script[^>]*id="__NEXT_DATA__"[^>]*>(?P<payload>.*?)</script>',
        html,
        re.DOTALL,
    )
    if not match:
        raise ScraperError("Next.js payload marker not found in HTML response.")
    payload = match.group("payload")
    try:
        return json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ScraperError("Invalid Next.js payload JSON.") from exc


def _clean_text(text: Optional[str]) -> str:
    if not text:
        return ""
    soup = BeautifulSoup(text, "html.parser")
    extracted = soup.get_text("\n", strip=True)
    lines = [line.strip() for line in extracted.splitlines()]
    return "\n".join(line for line in lines if line)


def _combine_description_html(description: Optional[object]) -> str:
    if not description:
        return ""
    if isinstance(description, str):
        return description
    if isinstance(description, dict):
        parts = [value for value in description.values() if value]
        return "\n".join(parts)
    return ""


def _normalize_locations(value: Optional[object]) -> List[str]:
    locations: List[str] = []
    if isinstance(value, list):
        for entry in value:
            if isinstance(entry, str):
                if entry.strip():
                    locations.append(entry.strip())
            elif isinstance(entry, dict):
                name = (entry.get("name") or "").strip()
                if name:
                    locations.append(name)
    elif isinstance(value, str) and value.strip():
        locations.append(value.strip())
    return locations


def _extract_location_names(raw_locations: Iterable[object]) -> Tuple[List[str], List[str]]:
    names: List[str] = []
    workplace_types: List[str] = []
    for entry in raw_locations or []:
        if isinstance(entry, dict):
            name = (entry.get("name") or "").strip()
            if name:
                names.append(name)
            workplace = entry.get("workplaceType")
            if workplace:
                workplace_types.append(str(workplace))
        elif isinstance(entry, str):
            entry = entry.strip()
            if entry:
                names.append(entry)
    return names, workplace_types


def _build_metadata(
    *,
    api_data: Dict[str, Any],
    job_post: Dict[str, Any],
    detail_locations: List[str],
) -> Dict[str, Any]:
    employment_type = job_post.get("employmentType") or {}
    department = job_post.get("department") or {}
    description = job_post.get("description")

    metadata: Dict[str, Any] = {
        "rippling_job_uuid": job_post.get("uuid"),
        "job_board_url": job_post.get("url"),
        "company_name": job_post.get("companyName"),
        "employment_type": employment_type.get("id") or employment_type.get("label"),
        "employment_type_label": employment_type.get("label"),
        "department": department.get("name"),
        "department_tree": department.get("department_tree"),
        "work_locations": detail_locations,
        "created_on": job_post.get("createdOn"),
        "active_job_application": job_post.get("activeJobApplication"),
        "job_board": api_data.get("jobBoard"),
        "pay_range_details": api_data.get("payRangeDetails"),
    }
    if description:
        metadata["description_sections"] = description
    return {key: value for key, value in metadata.items() if value not in (None, "", [], {})}


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def store_listing(listing: JobListing) -> None:
    metadata = dict(listing.metadata)
    metadata.setdefault("summary_locations", listing.locations)
    metadata.setdefault("summary_workplace_types", listing.workplace_types)
    metadata.setdefault("summary_language", listing.language)
    metadata.setdefault("summary_department", listing.department)
    metadata.setdefault("summary_is_remote", listing.is_remote)
    primary_location = listing.location
    if not primary_location:
        work_locations = metadata.get("work_locations") or []
        if isinstance(work_locations, list) and work_locations:
            primary_location = str(work_locations[0])
    JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults={
            "title": (listing.title or "")[:255],
            "location": (primary_location or "")[:255],
            "date": (listing.metadata.get("created_on") or "")[:100],
            "description": (listing.description_text or listing.description_html or "")[:10000],
            "metadata": metadata,
        },
    )


def run_scrape(limit: Optional[int], delay: float) -> int:
    scraper = RipplingJobScraper(delay=delay)
    count = 0
    for job in scraper.scrape(limit=limit):
        store_listing(job)
        count += 1
    return count


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rippling careers scraper")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY)
    parser.add_argument("--log-level", type=str, default="INFO", choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"])
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    start = time.time()
    try:
        count = run_scrape(args.limit, args.delay)
    except ScraperError as exc:
        logging.error("Scrape failed: %s", exc)
        return 1
    dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
    duration = time.time() - start
    summary = {
        "company": COMPANY_NAME,
        "url": CAREERS_URL,
        "count": count,
        "elapsed_seconds": duration,
        "dedupe": dedupe_summary,
    }
    logging.info("Summary: %s", json.dumps(summary))
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
