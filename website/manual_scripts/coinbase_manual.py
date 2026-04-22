#!/usr/bin/env python3
"""Manual scraper for Coinbase careers (Greenhouse-powered).

This script pulls the public Coinbase job catalog from the Greenhouse API,
transforms each posting into the shared ``JobPosting`` model, and stores the
results so operations can trigger it ad hoc from the manual scripts dashboard.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional

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
CAREERS_URL = "https://www.coinbase.com/careers"
GREENHOUSE_API_URL = "https://api.greenhouse.io/v1/boards/coinbase/jobs"
COMPANY_NAME = "Coinbase"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}
REQUEST_TIMEOUT = (10, 30)
GREENHOUSE_MAX_PAGE_SIZE = 500
DEFAULT_PAGE_SIZE = GREENHOUSE_MAX_PAGE_SIZE
DEFAULT_DELAY = 0.25
DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 1800), 120)

SCRAPER_QS = Scraper.objects.filter(company=COMPANY_NAME, url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Coinbase scraper rows found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company=COMPANY_NAME,
        url=CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
@dataclass
class CoinbaseJob:
    job_id: int
    title: str
    link: str
    location: Optional[str]
    posted_at: Optional[str]
    description: str
    metadata: Dict[str, Any]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _clean_text(text: Optional[str]) -> str:
    if not text:
        return ""
    soup = BeautifulSoup(text, "html.parser")
    extracted = soup.get_text("\n", strip=True)
    lines = [line.strip() for line in extracted.splitlines()]
    return "\n".join(line for line in lines if line)


def _simplify_metadata(entries: Optional[Iterable[Dict[str, Any]]]) -> Dict[str, Any]:
    simplified: Dict[str, Any] = {}
    for entry in entries or []:
        name = entry.get("name")
        value = entry.get("value")
        if not name:
            continue
        simplified[name] = value
    return simplified


def _build_metadata(raw: Dict[str, Any]) -> Dict[str, Any]:
    departments = [dept.get("name") for dept in raw.get("departments") or [] if dept.get("name")]
    offices = [office.get("name") for office in raw.get("offices") or [] if office.get("name")]
    metadata: Dict[str, Any] = {
        "greenhouse_job_id": raw.get("id"),
        "internal_job_id": raw.get("internal_job_id"),
        "requisition_id": raw.get("requisition_id"),
        "company_name": raw.get("company_name"),
        "education_requirement": raw.get("education"),
        "employment_type": raw.get("employment"),
        "updated_at": raw.get("updated_at"),
        "first_published": raw.get("first_published"),
        "departments": departments,
        "offices": offices,
        "location_struct": raw.get("location"),
        "data_compliance": raw.get("data_compliance"),
        "custom_fields": _simplify_metadata(raw.get("metadata")),
    }
    html_content = raw.get("content")
    if html_content:
        metadata["description_html"] = html_content
    clean_metadata = {key: value for key, value in metadata.items() if value not in (None, "", [], {})}
    return clean_metadata


def _to_listing(raw: Dict[str, Any]) -> Optional[CoinbaseJob]:
    job_id = raw.get("id")
    title = (raw.get("title") or "").strip()
    link = raw.get("absolute_url") or ""
    if not (job_id and title and link):
        return None
    location_name = (raw.get("location") or {}).get("name") or ""
    posted_at = raw.get("first_published") or raw.get("updated_at")
    description_text = _clean_text(raw.get("content"))
    metadata = _build_metadata(raw)
    return CoinbaseJob(
        job_id=int(job_id),
        title=title,
        link=link,
        location=location_name.strip() or None,
        posted_at=posted_at,
        description=description_text,
        metadata=metadata,
    )


# ---------------------------------------------------------------------------
# Scraper implementation
# ---------------------------------------------------------------------------
class CoinbaseGreenhouseScraper:
    def __init__(
        self,
        *,
        session: Optional[requests.Session] = None,
        per_page: int = DEFAULT_PAGE_SIZE,
        delay: float = DEFAULT_DELAY,
    ) -> None:
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.per_page = max(1, min(per_page, GREENHOUSE_MAX_PAGE_SIZE))
        self.delay = max(0.0, delay)
        self.logger = logging.getLogger(self.__class__.__name__)

    def scrape(self, *, limit: Optional[int] = None) -> Iterator[CoinbaseJob]:
        fetched = 0
        page = 1

        while True:
            params = {
                "content": "true",
                "page": page,
                "per_page": self.per_page,
            }
            response = self.session.get(
                GREENHOUSE_API_URL,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            payload = response.json()
            jobs = payload.get("jobs") or []
            if not jobs:
                self.logger.debug("No jobs returned at page=%s; stopping.", page)
                break

            for raw_job in jobs:
                listing = _to_listing(raw_job)
                if not listing:
                    self.logger.debug("Skipping job with insufficient data: %s", raw_job)
                    continue
                yield listing
                fetched += 1
                if limit is not None and fetched >= limit:
                    self.logger.info("Limit reached (%s); stopping.", limit)
                    return

            if len(jobs) < self.per_page:
                break

            page += 1
            if self.delay:
                time.sleep(self.delay)


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------
def persist_listing(listing: CoinbaseJob) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255],
        "date": listing.posted_at or "",
        "description": listing.description[:10000],
        "metadata": listing.metadata,
    }
    _, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.link,
        defaults=defaults,
    )
    return created


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Coinbase careers listings.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument(
        "--per-page",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help="Number of jobs to request per Greenhouse page (max 500).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help="Seconds to sleep between API requests.",
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


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s: %(message)s")

    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    scraper = CoinbaseGreenhouseScraper(session=session, per_page=args.per_page, delay=args.delay)

    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    for listing in scraper.scrape(limit=args.limit):
        totals["fetched"] += 1
        if args.dry_run:
            print(
                json.dumps(
                    {
                        "job_id": listing.job_id,
                        "title": listing.title,
                        "link": listing.link,
                        "location": listing.location,
                        "posted_at": listing.posted_at,
                    },
                    ensure_ascii=False,
                )
            )
            continue

        try:
            created = persist_listing(listing)
        except Exception as exc:  # pragma: no cover - persistence failure path
            logging.error("Failed to persist job %s: %s", listing.link, exc)
            totals["errors"] += 1
            continue

        if created:
            totals["created"] += 1
        else:
            totals["updated"] += 1

    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        logging.info("Deduplication summary: %s", json.dumps(dedupe_summary))

    logging.info(
        "Coinbase scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )
    return 0 if not totals["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
