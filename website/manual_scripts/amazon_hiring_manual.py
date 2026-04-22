#!/usr/bin/env python3
"""Manual scraper for Amazon Jobs search API job listings.

This script talks to the public search.json endpoint that powers
https://www.amazon.jobs/en/search and stores job postings in the shared
JobPosting table. It mirrors the conventions used by the other manual
scripts so operations staff can schedule or run it ad-hoc.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Dict, Generator, Iterable, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Django setup
# ---------------------------------------------------------------------------
BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "website.settings")

import django  # noqa: E402

django.setup()

from django.utils import timezone  # noqa: E402

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_URL = "https://www.amazon.jobs"
SEARCH_URL = f"{BASE_URL}/en/search"
SEARCH_API_URL = f"{SEARCH_URL}.json"
JSON_ACCEPT_HEADER = "application/json, text/plain, */*"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/127.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT_SECONDS = 60
MAX_PAGE_SIZE = 100

SCRAPER_PRIMARY_URL = SEARCH_URL
SCRAPER_LEGACY_URL = "https://hiring.amazon.com/app#/jobSearch"
SCRAPER_URL_CHOICES = [SCRAPER_PRIMARY_URL, SCRAPER_LEGACY_URL]

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
_SCRAPER: Optional[Scraper] = None


@dataclass
class JobListing:
    job_id: str
    title: str
    detail_url: str
    location_text: Optional[str]
    posted_on: Optional[str]
    description: str
    metadata: Dict[str, object]


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------
def html_to_text(html: Optional[str]) -> Optional[str]:
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = soup.get_text("\n", strip=True)
    return text.strip() or None


def parse_param_list(param_pairs: Iterable[str]) -> Dict[str, str]:
    params: Dict[str, str] = {}
    for raw in param_pairs:
        if "=" not in raw:
            raise ValueError(f"Parameter must be in KEY=VALUE format (got '{raw}')")
        key, value = raw.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            raise ValueError(f"Parameter key may not be empty (got '{raw}')")
        params[key] = value
    return params


def combine_description(job: Dict[str, object]) -> str:
    segments: List[str] = []

    primary = html_to_text(job.get("description"))
    if primary:
        segments.append(primary)

    basic = html_to_text(job.get("basic_qualifications"))
    if basic:
        segments.append("Basic qualifications:\n" + basic)

    preferred = html_to_text(job.get("preferred_qualifications"))
    if preferred:
        segments.append("Preferred qualifications:\n" + preferred)

    short_desc = html_to_text(job.get("description_short"))
    if short_desc and short_desc not in segments:
        segments.append(short_desc)

    description = "\n\n".join(filter(None, segments))
    fallback = html_to_text(job.get("description_short")) or job.get("description") or ""
    return description.strip() or str(fallback).strip()


# ---------------------------------------------------------------------------
# ORM helpers
# ---------------------------------------------------------------------------
def get_scraper() -> Scraper:
    global _SCRAPER
    if _SCRAPER:
        return _SCRAPER

    qs = Scraper.objects.filter(company="Amazon", url__in=SCRAPER_URL_CHOICES).order_by("id")
    if qs.exists():
        scraper = qs.first()
        if scraper.url != SCRAPER_PRIMARY_URL:
            scraper.url = SCRAPER_PRIMARY_URL
            scraper.save(update_fields=["url"])
    else:
        scraper = Scraper.objects.create(
            company="Amazon",
            url=SCRAPER_PRIMARY_URL,
            code="manual-script",
            interval_hours=24,
            timeout_seconds=300,
        )

    if qs.count() > 1:
        logging.getLogger(__name__).warning(
            "Multiple Scraper rows matched Amazon hiring; using id=%s.", scraper.id
        )

    _SCRAPER = scraper
    return scraper


# ---------------------------------------------------------------------------
# Amazon Jobs client
# ---------------------------------------------------------------------------
class AmazonJobsClient:
    def __init__(
        self,
        *,
        user_agent: str = DEFAULT_USER_AGENT,
        session: Optional[requests.Session] = None,
        extra_params: Optional[Dict[str, str]] = None,
    ) -> None:
        self.session = session or requests.Session()
        self.session.headers.clear()
        self.session.headers.update(
            {
                "Accept": JSON_ACCEPT_HEADER,
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Referer": SEARCH_URL,
                "Origin": BASE_URL,
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "User-Agent": user_agent,
            }
        )
        self.extra_params = extra_params or {}

    def _fetch_page(self, *, offset: int, limit: int) -> Dict[str, object]:
        params = dict(self.extra_params)
        params["offset"] = max(0, offset)
        params["result_limit"] = max(1, min(limit, MAX_PAGE_SIZE))

        response = self.session.get(SEARCH_API_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        if response.status_code != 200:
            logging.error(
                "Amazon Jobs request failed (%s): %s",
                response.status_code,
                response.text[:500],
            )
            raise RuntimeError(f"Amazon Jobs request failed ({response.status_code}); see log for details.")

        try:
            data: Dict[str, object] = response.json()
        except ValueError as exc:
            logging.error("Amazon Jobs response was not valid JSON: %s", response.text[:500])
            raise RuntimeError("Amazon Jobs response was not valid JSON.") from exc

        if data.get("error"):
            logging.error("Amazon Jobs returned an error payload: %s", data["error"])
            raise RuntimeError(f"Amazon Jobs reported an error: {data['error']}")

        return data

    def iter_jobs(
        self,
        *,
        page_size: int,
        limit: Optional[int] = None,
        offset: int = 0,
        delay: float = 0.0,
    ) -> Generator[Dict[str, object], None, None]:
        fetched = 0
        current_offset = max(0, offset)

        while True:
            if limit is not None:
                remaining = limit - fetched
                if remaining <= 0:
                    return
                page_limit = min(page_size, remaining)
            else:
                page_limit = page_size

            page_limit = max(1, min(page_limit, MAX_PAGE_SIZE))

            data = self._fetch_page(offset=current_offset, limit=page_limit)
            jobs: List[Dict[str, object]] = data.get("jobs") or []
            hits = int(data.get("hits") or 0)

            logging.debug(
                "Fetched %s jobs at offset %s (hits=%s, page_limit=%s)",
                len(jobs),
                current_offset,
                hits,
                page_limit,
            )

            if not jobs:
                return

            for job in jobs:
                yield job
                fetched += 1
                if limit is not None and fetched >= limit:
                    return

            current_offset += len(jobs)

            if hits and current_offset >= hits:
                return

            if len(jobs) < page_limit:
                return

            if delay:
                time.sleep(delay)


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------
def combine_team_metadata(team: object) -> Optional[Dict[str, object]]:
    if isinstance(team, dict):
        return {k: v for k, v in team.items() if v is not None}
    return None


def build_listing(job: Dict[str, object]) -> JobListing:
    job_id = job.get("id")
    if not job_id:
        raise ValueError("Job payload missing 'id'.")

    detail_path = job.get("job_path") or ""
    detail_url = urljoin(BASE_URL, detail_path) if detail_path else job.get("url_next_step") or SEARCH_URL

    location_text = (job.get("normalized_location") or job.get("location") or "").strip() or None
    description = combine_description(job)

    metadata: Dict[str, object] = {
        "jobId": job_id,
        "id_icims": job.get("id_icims"),
        "job_path": job.get("job_path"),
        "company_name": job.get("company_name"),
        "job_category": job.get("job_category"),
        "business_category": job.get("business_category"),
        "job_family": job.get("job_family"),
        "job_schedule_type": job.get("job_schedule_type"),
        "country_code": job.get("country_code"),
        "city": job.get("city"),
        "state": job.get("state"),
        "normalized_location": job.get("normalized_location"),
        "location": job.get("location"),
        "optional_search_labels": job.get("optional_search_labels"),
        "primary_search_label": job.get("primary_search_label"),
        "source_system": job.get("source_system"),
        "url_next_step": job.get("url_next_step"),
        "description_short": html_to_text(job.get("description_short")),
        "basic_qualifications": html_to_text(job.get("basic_qualifications")),
        "preferred_qualifications": html_to_text(job.get("preferred_qualifications")),
        "locations": job.get("locations"),
        "is_intern": job.get("is_intern"),
        "is_manager": job.get("is_manager"),
        "team": combine_team_metadata(job.get("team")),
        "posted_date": job.get("posted_date"),
        "updated_time": job.get("updated_time"),
        "job_posting_search_request": job.get("job_posting_search_request"),
        "fetched_at": timezone.now().isoformat(),
    }

    posted_on = job.get("posted_date")

    return JobListing(
        job_id=str(job_id),
        title=(job.get("title") or "Amazon Job").strip(),
        detail_url=detail_url,
        location_text=location_text,
        posted_on=posted_on,
        description=description,
        metadata=metadata,
    )


def persist_listing(listing: JobListing) -> bool:
    scraper = get_scraper()
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location_text or "")[:255] or None,
        "date": (listing.posted_on or "")[:100] or None,
        "description": listing.description[:10000],
        "metadata": listing.metadata,
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=scraper,
        link=listing.detail_url,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Persisted job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


# ---------------------------------------------------------------------------
# CLI + orchestration
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Amazon Jobs listings via search.json.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument(
        "--page-size",
        type=int,
        default=MAX_PAGE_SIZE,
        help=f"Number of jobs per API page (1-{MAX_PAGE_SIZE}).",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Initial offset within the Amazon Jobs result set.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Seconds to sleep between API calls.",
    )
    parser.add_argument(
        "--category",
        help="Category filter as seen on amazon.jobs (e.g. 'Software Development').",
    )
    parser.add_argument(
        "--business-category",
        dest="business_category",
        help="Business category filter (slug).",
    )
    parser.add_argument("--keywords", help="Keyword search terms.")
    parser.add_argument("--team", help="Team filter slug.")
    parser.add_argument(
        "--loc-query",
        dest="loc_query",
        help="Location query string, e.g. 'Seattle, WA'.",
    )
    parser.add_argument(
        "--country-code",
        dest="country_code",
        help="Filter results to a two-letter country code (e.g. 'US').",
    )
    parser.add_argument(
        "--param",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Additional querystring parameter forwarded directly to search.json (repeatable).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and display jobs without writing to the database.",
    )
    return parser.parse_args(argv)


def build_query_params(args: argparse.Namespace) -> Dict[str, str]:
    params = parse_param_list(args.param)

    if args.category and "category" not in params:
        params["category"] = args.category
    if args.business_category and "business_category" not in params:
        params["business_category"] = args.business_category
    if args.keywords and "keywords" not in params:
        params["keywords"] = args.keywords
    if args.team and "team" not in params:
        params["team"] = args.team
    if args.loc_query and "loc_query" not in params:
        params["loc_query"] = args.loc_query
    if args.country_code and "country_code" not in params:
        params["country_code"] = args.country_code

    return params


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s: %(message)s")

    if args.page_size <= 0:
        logging.error("page-size must be a positive integer.")
        return 2

    if args.limit is not None and args.limit <= 0:
        logging.error("limit must be a positive integer when provided.")
        return 2

    if args.offset < 0:
        logging.error("offset may not be negative.")
        return 2

    try:
        query_params = build_query_params(args)
    except ValueError as exc:
        logging.error("Invalid --param value: %s", exc)
        return 2

    client = AmazonJobsClient(extra_params=query_params)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    for job in client.iter_jobs(
        page_size=args.page_size,
        limit=args.limit,
        offset=args.offset,
        delay=args.delay,
    ):
        totals["fetched"] += 1

        try:
            listing = build_listing(job)
        except Exception as exc:
            logging.error("Failed to build listing for job payload: %s", exc)
            totals["errors"] += 1
            continue

        if args.dry_run:
            print(json.dumps(listing.__dict__, default=str, ensure_ascii=False))
            continue

        try:
            created = persist_listing(listing)
            if created:
                totals["created"] += 1
            else:
                totals["updated"] += 1
        except Exception as exc:
            logging.error("Failed to persist job %s: %s", listing.job_id, exc)
            totals["errors"] += 1

    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=get_scraper())
        totals["dedupe"] = dedupe_summary

    logging.info(
        "Amazon jobs scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )
    return 0 if not totals["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

