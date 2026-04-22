#!/usr/bin/env python3
"""Manual scraper for Crown Castle careers (Ultipro job board)."""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional
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

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_URL = "https://recruiting2.ultipro.com"
TENANT_ALIAS = "CRO1010CCUSA"
JOB_BOARD_ID = "74c30440-80fa-4099-8981-2e10b7193d27"
LISTING_URL = f"{BASE_URL}/{TENANT_ALIAS}/JobBoard/{JOB_BOARD_ID}/"
LOAD_SEARCH_URL = f"{LISTING_URL}JobBoardView/LoadSearchResults"
DETAIL_URL_TEMPLATE = f"{LISTING_URL}OpportunityDetail?opportunityId={{job_id}}"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}
REQUEST_TIMEOUT = (10, 45)
DEFAULT_PAGE_SIZE = 20
DEFAULT_DELAY = 0.4

SCRAPER_QS = Scraper.objects.filter(company="Crown Castle", url=LISTING_URL.rstrip("/")).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.getLogger(__name__).warning(
            "Multiple Crown Castle scraper rows found; using id=%s", SCRAPER.id
        )
else:
    SCRAPER = Scraper.objects.create(
        company="Crown Castle",
        url=LISTING_URL.rstrip("/"),
        code="manual-script",
        interval_hours=24,
        timeout_seconds=900,
    )


class CrownCastleScraperError(Exception):
    """Raised when the Crown Castle scraper encounters an unrecoverable error."""


@dataclass
class JobSummary:
    job_id: str
    title: str
    requisition_number: Optional[str]
    posted_date: Optional[str]
    locations: List[Dict[str, object]]
    detail_url: str
    featured: bool
    full_time: Optional[bool]
    raw_payload: Dict[str, object]


@dataclass
class JobListing(JobSummary):
    description: str
    metadata: Dict[str, object] = field(default_factory=dict)

    @property
    def location_label(self) -> Optional[str]:
        return compose_location(self.locations)


def compose_location(locations: Iterable[Dict[str, object]]) -> Optional[str]:
    labels: List[str] = []
    for entry in locations or []:
        address = entry.get("Address") or {}
        city = (address.get("City") or "").strip()
        state = ""
        state_data = address.get("State") or {}
        if isinstance(state_data, dict):
            state = (state_data.get("Code") or state_data.get("Name") or "").strip()
        country = ""
        country_data = address.get("Country") or {}
        if isinstance(country_data, dict):
            country = (country_data.get("Name") or country_data.get("Code") or "").strip()

        parts = [part for part in [city, state] if part]
        if parts:
            label = ", ".join(parts)
            if country and country not in label:
                label = f"{label}, {country}"
            labels.append(label)
            continue

        localized = (entry.get("LocalizedDescription") or "").strip()
        if localized:
            labels.append(localized)
            continue

        name = (entry.get("DisplayName") or "").strip()
        if name:
            labels.append(name)

    if labels:
        seen: set[str] = set()
        deduped: List[str] = []
        for label in labels:
            if label not in seen:
                seen.add(label)
                deduped.append(label)
        return " | ".join(deduped)
    return None


def html_to_text(value: Optional[str]) -> str:
    if not value:
        return ""
    soup = BeautifulSoup(value, "html.parser")
    text = soup.get_text("\n", strip=True)
    lines = [line.strip() for line in text.splitlines()]
    cleaned = "\n".join(filter(None, lines))
    return cleaned.strip()


class CrownCastleClient:
    def __init__(self, *, page_size: int = DEFAULT_PAGE_SIZE, delay: float = DEFAULT_DELAY) -> None:
        self.page_size = max(1, min(page_size, 50))
        self.delay = max(0.0, delay)
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    def iter_job_summaries(self, *, limit: Optional[int] = None) -> Iterator[JobSummary]:
        skip = 0
        fetched = 0
        total_count: Optional[int] = None

        while True:
            payload = {
                "opportunitySearch": {
                    "Top": self.page_size,
                    "Skip": skip,
                    "QueryString": "",
                    "SortType": 0,
                    "MatchCriteria": 0,
                    "LocationLatitude": None,
                    "LocationLongitude": None,
                    "LocationId": None,
                }
            }
            response = self.session.post(
                LOAD_SEARCH_URL,
                headers={"Content-Type": "application/json"},
                data=json.dumps(payload),
                timeout=REQUEST_TIMEOUT,
            )
            if response.status_code != 200:
                raise CrownCastleScraperError(
                    f"Search request failed (status {response.status_code}): {response.text[:200]}"
                )

            data = response.json()
            opportunities = data.get("opportunities") or []
            if total_count is None:
                total_count = int(data.get("totalCount") or 0)
                self.logger.debug("Total opportunities reported: %s", total_count)

            if not opportunities:
                self.logger.debug("No opportunities returned at skip=%s; stopping.", skip)
                break

            for item in opportunities:
                job_id = item.get("Id")
                title = (item.get("Title") or "").strip()
                if not job_id or not title:
                    continue
                summary = JobSummary(
                    job_id=job_id,
                    title=title,
                    requisition_number=item.get("RequisitionNumber"),
                    posted_date=item.get("PostedDate"),
                    locations=item.get("Locations") or [],
                    detail_url=DETAIL_URL_TEMPLATE.format(job_id=job_id),
                    featured=bool(item.get("Featured")),
                    full_time=item.get("FullTime"),
                    raw_payload=item,
                )
                yield summary
                fetched += 1
                if limit is not None and fetched >= limit:
                    return

            skip += self.page_size
            if total_count is not None and skip >= total_count:
                break
            if self.delay:
                time.sleep(self.delay)

    def enrich_summary(self, summary: JobSummary) -> JobListing:
        response = self.session.get(summary.detail_url, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            raise CrownCastleScraperError(
                f"Failed to fetch detail page {summary.detail_url} (status {response.status_code})"
            )

        detail_data = extract_opportunity_detail(response.text)
        description_html = detail_data.get("Description")
        description = html_to_text(description_html) or summary.raw_payload.get("BriefDescription") or ""

        metadata = {
            "summary": summary.raw_payload,
            "detail": detail_data,
        }

        return JobListing(
            job_id=summary.job_id,
            title=summary.title,
            requisition_number=summary.requisition_number,
            posted_date=summary.posted_date or detail_data.get("PostedDate"),
            locations=detail_data.get("Locations") or summary.locations,
            detail_url=summary.detail_url,
            featured=summary.featured,
            full_time=detail_data.get("FullTime", summary.full_time),
            raw_payload=summary.raw_payload,
            description=description,
            metadata=metadata,
        )


DETAIL_REGEX = re.compile(
    r"var\s+opportunity\s*=\s*new\s+US\.Opportunity\.CandidateOpportunityDetail\((\{.*?\})\);",
    re.DOTALL,
)


def extract_opportunity_detail(html: str) -> Dict[str, object]:
    match = DETAIL_REGEX.search(html)
    if not match:
        raise CrownCastleScraperError("Failed to locate opportunity detail JSON in detail page.")

    payload = match.group(1)
    try:
        return json.loads(payload)
    except json.JSONDecodeError as exc:
        snippet = payload[:2000]
        raise CrownCastleScraperError(f"Unable to decode opportunity detail JSON: {exc}\nSnippet: {snippet}") from exc


def persist_listing(listing: JobListing) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location_label or "")[:255] or None,
        "date": (listing.posted_date or "")[:100] or None,
        "description": listing.description[:10000],
        "metadata": {
            "job_id": listing.job_id,
            "requisition_number": listing.requisition_number,
            "full_time": listing.full_time,
            "featured": listing.featured,
            "locations": listing.locations,
            "raw": listing.metadata,
        },
    }

    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Persisted Crown Castle job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Crown Castle job listings and persist them.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to fetch.")
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help=f"Number of jobs per request (default: {DEFAULT_PAGE_SIZE}).",
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
        help="Print jobs as JSON instead of writing to the database.",
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

    if args.limit is not None and args.limit <= 0:
        logging.error("limit must be a positive integer when provided.")
        return 2

    if args.page_size <= 0:
        logging.error("page-size must be a positive integer.")
        return 2

    client = CrownCastleClient(page_size=args.page_size, delay=args.delay)
    totals = {"fetched": 0, "created": 0, "errors": 0}

    try:
        for summary in client.iter_job_summaries(limit=args.limit):
            try:
                listing = client.enrich_summary(summary)
            except Exception as exc:  # pragma: no cover - network/HTML parsing guard
                logging.error("Failed to enrich job %s: %s", summary.job_id, exc)
                totals["errors"] += 1
                continue

            totals["fetched"] += 1

            if args.dry_run:
                print(json.dumps(listing.__dict__, default=str, ensure_ascii=False))
                continue

            try:
                created = persist_listing(listing)
                if created:
                    totals["created"] += 1
            except Exception as exc:  # pragma: no cover - DB guard
                logging.error("Failed to persist job %s: %s", listing.job_id, exc)
                totals["errors"] += 1
    except CrownCastleScraperError as exc:
        logging.error("Scraper halted due to unrecoverable error: %s", exc)
        return 1

    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["dedupe"] = dedupe_summary
        logging.info("Deduplication summary: %s", dedupe_summary)

    logging.info(
        "Crown Castle scraper finished - fetched=%(fetched)s created=%(created)s errors=%(errors)s",
        totals,
    )
    return 0 if not totals["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
