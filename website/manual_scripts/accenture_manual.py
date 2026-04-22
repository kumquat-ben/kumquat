#!/usr/bin/env python3
"""Manual scraper for Accenture's public careers search.

The Accenture careers site exposes an Elastic-style JSON API that powers the
search results experience on https://www.accenture.com/us-en/careers/jobsearch.
This script pages through that API, normalises each listing, and persists it via
the Django ORM so that operations staff can run it on-demand.
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
from typing import Dict, Iterable, List, Optional

import requests

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
CAREERS_URL = "https://www.accenture.com/us-en/careers/jobsearch"
API_URL = "https://www.accenture.com/api/accenture/elastic/findjobs"
COUNTRY_SITE = "us-en"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 600), 30)
SCRAPER_QS = Scraper.objects.filter(company="Accenture", url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched Accenture; using id=%s.", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Accenture",
        url=CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the scrape pipeline cannot proceed."""


@dataclass
class JobListing:
    title: str
    link: str
    location: Optional[str]
    date: Optional[str]
    description: str
    metadata: Dict[str, object]


def _clean(value: Optional[str]) -> str:
    return (value or "").strip()


class AccentureJobScraper:
    def __init__(
        self,
        *,
        page_size: int = 40,
        delay: float = 0.2,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.page_size = max(1, int(page_size))
        self.delay = max(0.0, float(delay))
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

    # ------------------------------------------------------------------ #
    # Public API                                                         #
    # ------------------------------------------------------------------ #
    def scrape(self, *, limit: Optional[int] = None) -> Iterable[JobListing]:
        offset = 0
        fetched = 0
        total_hits: Optional[int] = None

        while True:
            payload = self._fetch_page(offset)
            items = payload.get("data") or []
            if not items:
                logging.info("No jobs returned at offset %s; stopping.", offset)
                break

            if total_hits is None:
                total_hits = self._parse_total_hits(payload.get("totalHits"))
                logging.info("Discovered totalHits=%s (page size %s).", total_hits, len(items))

            for raw in items:
                listing = self._build_listing(raw)
                if listing is None:
                    continue
                yield listing
                fetched += 1
                if limit is not None and fetched >= limit:
                    logging.info("Reached limit=%s; stopping scrape.", limit)
                    return

            offset += len(items)
            if total_hits is not None and offset >= total_hits:
                logging.info("Reached totalHits=%s; pagination complete.", total_hits)
                return
            if len(items) < self.page_size:
                logging.info("Page contained %s results (< page_size); stopping.", len(items))
                return
            if self.delay:
                time.sleep(self.delay)

    # ------------------------------------------------------------------ #
    # Helpers                                                            #
    # ------------------------------------------------------------------ #
    def _fetch_page(self, offset: int) -> Dict[str, object]:
        form = {
            "startIndex": str(offset),
            "maxResultSize": str(self.page_size),
            "jobKeyword": "",
            "jobCountry": "USA",
            "jobLanguage": "en",
            "countrySite": COUNTRY_SITE,
            "sortBy": "0",
            "searchType": "vectorSearch",
            "enableQueryBoost": "true",
            "minScore": "0.0",
            "getFeedbackJudgmentEnabled": "true",
            "useCleanEmbedding": "true",
            "score": "true",
        }
        try:
            response = self.session.post(API_URL, data=form, headers={"CSRF-Token": ""}, timeout=40)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch jobs offset={offset}: {exc}") from exc
        return response.json()

    def _parse_total_hits(self, raw: object) -> Optional[int]:
        if isinstance(raw, dict):
            raw = raw.get("total")
        try:
            return int(raw) if raw is not None else None
        except (TypeError, ValueError):
            return None

    def _build_listing(self, raw: Dict[str, object]) -> Optional[JobListing]:
        title = _clean(raw.get("title"))
        detail_template = _clean(raw.get("jobDetailUrl"))
        if "{0}" in detail_template:
            link = detail_template.replace("{0}", COUNTRY_SITE)
        else:
            link = detail_template
        if not title or not link:
            return None

        locations = raw.get("location")
        if isinstance(locations, list):
            location_text = "; ".join(_clean(loc) for loc in locations if _clean(loc))
        else:
            location_text = _clean(locations if isinstance(locations, str) else None)

        description = _clean(raw.get("jobDescriptionClean")) or _clean(raw.get("jobDescription"))
        metadata = {
            "requisitionId": raw.get("requisitionId"),
            "careerLevel": raw.get("careerLevel"),
            "jobTypeDescription": raw.get("jobTypeDescription"),
            "regionName": raw.get("regionName"),
            "workdaySkill": raw.get("workdaySkill"),
            "mustHaveSkills": raw.get("mustHaveSkills"),
            "goodToHaveSkills": raw.get("goodToHaveSkills"),
            "jobFamilyGroup": raw.get("jobFamilyGroup"),
        }

        return JobListing(
            title=title,
            link=link,
            location=location_text or None,
            date=_clean(raw.get("updateDate")),
            description=description,
            metadata={k: v for k, v in metadata.items() if v},
        )


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------
def persist_listing(listing: JobListing) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255] or None,
        "date": (listing.date or "")[:100] or None,
        "description": listing.description[:10000],
        "metadata": listing.metadata,
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.link,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Persisted Accenture job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


# ---------------------------------------------------------------------------
# CLI / entry point
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Accenture job listings.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument(
        "--page-size",
        type=int,
        default=40,
        help="Number of records to request per API page (default: 40).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.2,
        help="Seconds to sleep between page fetches (default: 0.2).",
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
        help="Logging level (default: INFO).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s: %(message)s")

    scraper = AccentureJobScraper(page_size=args.page_size, delay=args.delay)
    totals = {"fetched": 0, "created": 0}

    for listing in scraper.scrape(limit=args.limit):
        totals["fetched"] += 1
        if args.dry_run:
            print(json.dumps(listing.__dict__, default=str, ensure_ascii=False))
            continue
        created = persist_listing(listing)
        if created:
            totals["created"] += 1

    if not args.dry_run and totals["fetched"]:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        logging.info("Deduplication summary: %s", dedupe_summary)

    logging.info("Accenture scraper finished - fetched=%(fetched)s created=%(created)s", totals)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
