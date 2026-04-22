#!/usr/bin/env python3
"""Manual scraper for Edwards Lifesciences careers search (Algolia + Workday).

This script pulls the open roles published on https://www.edwards.com/careers/jobsearch
by querying the public Algolia index the site relies on, hydrates each record with
the corresponding Workday job detail payload, and persists the consolidated result
into the shared ``JobPosting`` table (via the ``Scraper`` row for Edwards Lifesciences).
"""
from __future__ import annotations

import argparse
import html
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional
from urllib.parse import urlparse

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
# Constants and configuration
# ---------------------------------------------------------------------------
CAREERS_URL = "https://www.edwards.com/careers/jobsearch"
COMPANY_NAME = "Edwards Lifesciences"

ALGOLIA_APP_ID = "LTJQZME6D2"
ALGOLIA_SEARCH_KEY = "581faeb7ca3c507195b2bca68b07303d"
ALGOLIA_INDEX = "EdwardsCareersJobs"
ALGOLIA_ENDPOINT = f"https://{ALGOLIA_APP_ID.lower()}-dsn.algolia.net/1/indexes/{ALGOLIA_INDEX}/query"

WORKDAY_HOST = "https://edwards.wd5.myworkdayjobs.com"
WORKDAY_PORTAL = "EdwardsCareers"

DEFAULT_TIMEOUT = (10, 40)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

SCRAPER_QS = Scraper.objects.filter(company=COMPANY_NAME, url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.getLogger(__name__).warning(
            "Multiple Scraper rows matched %s careers; using id=%s", COMPANY_NAME, SCRAPER.id
        )
else:
    SCRAPER = Scraper.objects.create(
        company=COMPANY_NAME,
        url=CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=300,
    )


class EdwardsScraperError(Exception):
    """Raised when the Edwards careers scraping pipeline cannot proceed."""


@dataclass
class AlgoliaJobHit:
    job_id: str
    title: str
    apply_url: str
    primary_location: Optional[str]
    location: Optional[str]
    country: Optional[str]
    category: Optional[str]
    business_unit: Optional[str]
    time_type: Optional[str]
    employee_type: Optional[str]
    remote: Optional[str]
    start_date: Optional[str]
    all_locations: List[str]
    raw: Dict[str, object]


@dataclass
class JobListing:
    job_id: str
    title: str
    link: str
    apply_url: str
    location: Optional[str]
    date_posted: Optional[str]
    description: str
    metadata: Dict[str, object]


def _clean_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def _html_to_text(html_fragment: Optional[str]) -> str:
    if not html_fragment:
        return ""
    soup = BeautifulSoup(html_fragment, "html.parser")
    text = soup.get_text("\n", strip=True)
    return html.unescape(text).strip()


def _without_apply_suffix(url: str) -> str:
    cleaned = url.rstrip("/")
    if cleaned.endswith("/apply"):
        cleaned = cleaned[: -len("/apply")]
    return cleaned


def _workday_api_url(apply_url: str) -> str:
    cleaned = _without_apply_suffix(apply_url)
    parsed = urlparse(cleaned)
    if not parsed.scheme or not parsed.netloc:
        raise EdwardsScraperError(f"Invalid apply URL encountered: {apply_url}")

    path = parsed.path
    portal_prefix = f"/{WORKDAY_PORTAL}"
    if portal_prefix not in path:
        raise EdwardsScraperError(f"Apply URL missing expected portal segment: {apply_url}")

    suffix = path.split(portal_prefix, 1)[1]
    if not suffix:
        raise EdwardsScraperError(f"Unable to derive Workday path from apply URL: {apply_url}")

    return f"{parsed.scheme}://{parsed.netloc}/wday/cxs/edwards/{WORKDAY_PORTAL}{suffix}"


class EdwardsJobScraper:
    def __init__(
        self,
        *,
        hits_per_page: int = 100,
        delay: float = 0.2,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.hits_per_page = max(1, min(hits_per_page, 1000))
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)
        self._algolia_headers = {
            **DEFAULT_HEADERS,
            "X-Algolia-API-Key": ALGOLIA_SEARCH_KEY,
            "X-Algolia-Application-Id": ALGOLIA_APP_ID,
            "Content-Type": "application/json",
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def scrape(self, *, limit: Optional[int] = None) -> Iterator[JobListing]:
        seen_ids: set[str] = set()
        fetched = 0

        for hit in self._iter_algolia_hits():
            if limit is not None and fetched >= limit:
                break

            if hit.job_id in seen_ids:
                continue
            seen_ids.add(hit.job_id)

            try:
                listing = self._hydrate_hit(hit)
            except Exception as exc:  # pragma: no cover - defensive logging of live failures
                self.logger.error("Failed to hydrate Edwards job %s: %s", hit.job_id, exc)
                continue

            yield listing
            fetched += 1

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _iter_algolia_hits(self) -> Iterator[AlgoliaJobHit]:
        page = 0
        total_pages: Optional[int] = None

        while True:
            params = f"query=&hitsPerPage={self.hits_per_page}&page={page}"
            payload = {"params": params}
            self.logger.debug("Querying Algolia page=%s size=%s", page, self.hits_per_page)

            response = self.session.post(
                ALGOLIA_ENDPOINT,
                headers=self._algolia_headers,
                json=payload,
                timeout=DEFAULT_TIMEOUT,
            )
            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                snippet = response.text[:200]
                raise EdwardsScraperError(f"Algolia request failed ({response.status_code}): {snippet}") from exc

            data = response.json()
            hits = data.get("hits") or []

            if total_pages is None:
                try:
                    total_pages = int(data.get("nbPages"))
                except (TypeError, ValueError):
                    total_pages = None
                try:
                    total_hits = int(data.get("nbHits"))
                except (TypeError, ValueError):
                    total_hits = None
                self.logger.info(
                    "Discovered Edwards Algolia index size: %s hits across %s pages",
                    total_hits,
                    total_pages,
                )

            if not hits:
                self.logger.info("Algolia returned no hits at page %s; stopping pagination.", page)
                return

            for raw in hits:
                job_id = str(raw.get("id") or raw.get("objectID") or "").strip()
                title = _clean_text(raw.get("JobPostingTitle") or "")
                apply_url = _clean_text(raw.get("ApplyUrl") or "")
                if not job_id or not title or not apply_url:
                    self.logger.debug("Skipping incomplete Algolia record: %s", raw)
                    continue

                hit = AlgoliaJobHit(
                    job_id=job_id,
                    title=title,
                    apply_url=apply_url,
                    primary_location=_clean_text(raw.get("PrimaryJobPostingLocation")),
                    location=_clean_text(raw.get("Location")),
                    country=_clean_text(raw.get("Country")),
                    category=_clean_text(raw.get("Category")),
                    business_unit=_clean_text(raw.get("BusinessUnit")),
                    time_type=_clean_text(raw.get("TimeType")),
                    employee_type=_clean_text(raw.get("EmployeeType")),
                    remote=_clean_text(raw.get("Remote")),
                    start_date=str(raw.get("startDate")) if raw.get("startDate") is not None else None,
                    all_locations=[loc for loc in (raw.get("allLocations") or []) if isinstance(loc, str)],
                    raw=raw,
                )
                yield hit

            page += 1
            if total_pages is not None and page >= total_pages:
                self.logger.info("Reached final Algolia page (%s); stopping.", total_pages)
                return

            if self.delay:
                time.sleep(self.delay)

    def _hydrate_hit(self, hit: AlgoliaJobHit) -> JobListing:
        api_url = _workday_api_url(hit.apply_url)
        detail = self._fetch_workday_detail(api_url)
        info = detail.get("jobPostingInfo") or {}

        description_html = info.get("jobDescription")
        description_text = _html_to_text(description_html) or "Description unavailable."

        detail_url = info.get("externalUrl") or _without_apply_suffix(hit.apply_url)

        location = (
            _clean_text(info.get("location"))
            or hit.primary_location
            or hit.location
        )

        metadata: Dict[str, object] = {
            "apply_url": hit.apply_url,
            "job_id": hit.job_id,
            "algolia": {
                "time_type": hit.time_type,
                "employee_type": hit.employee_type,
                "business_unit": hit.business_unit,
                "category": hit.category,
                "remote": hit.remote,
                "country": hit.country,
                "location": hit.location,
                "primary_location": hit.primary_location,
                "all_locations": hit.all_locations,
                "start_date_epoch_ms": hit.start_date,
                "object_id": hit.raw.get("objectID"),
            },
            "workday": {
                key: info.get(key)
                for key in (
                    "id",
                    "jobReqId",
                    "jobPostingId",
                    "jobPostingSiteId",
                    "postedOn",
                    "startDate",
                    "timeType",
                )
            },
            "job_requisition_location": info.get("jobRequisitionLocation"),
            "workday_country": info.get("country"),
        }

        return JobListing(
            job_id=hit.job_id,
            title=hit.title,
            link=detail_url,
            apply_url=hit.apply_url,
            location=location,
            date_posted=_clean_text(info.get("startDate")) or _clean_text(info.get("postedOn")),
            description=description_text,
            metadata=metadata,
        )

    def _fetch_workday_detail(self, url: str) -> Dict[str, object]:
        self.logger.debug("Fetching Workday detail: %s", url)
        response = self.session.get(url, timeout=DEFAULT_TIMEOUT)
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            snippet = response.text[:200]
            raise EdwardsScraperError(
                f"Workday job detail request failed ({response.status_code}): {snippet}"
            ) from exc

        data = response.json()
        if not isinstance(data, dict) or "jobPostingInfo" not in data:
            raise EdwardsScraperError("Unexpected Workday payload structure.")
        return data


def persist_listing(listing: JobListing) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255] or None,
        "date": (listing.date_posted or "")[:100] or None,
        "description": listing.description[:10000],
        "metadata": listing.metadata,
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.link,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Persisted Edwards job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Edwards Lifesciences job listings.")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of jobs to process (default: unlimited).",
    )
    parser.add_argument(
        "--hits-per-page",
        type=int,
        default=100,
        help="Number of Algolia hits to request per page (max 1000).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.2,
        help="Seconds to wait between Algolia pages to avoid rate limits.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and display jobs without writing to the database.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s: %(message)s")

    scraper = EdwardsJobScraper(hits_per_page=args.hits_per_page, delay=args.delay)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    for listing in scraper.scrape(limit=args.limit):
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
        except Exception as exc:  # pragma: no cover - persistence error path
            logging.error("Failed to persist job %s: %s", listing.link, exc)
            totals["errors"] += 1

    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["dedupe"] = dedupe_summary

    logging.info(
        "Edwards scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )
    return 0 if not totals["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

