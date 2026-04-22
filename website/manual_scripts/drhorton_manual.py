#!/usr/bin/env python3
"""Manual scraper for D.R. Horton's Taleo-powered careers site."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from urllib.parse import unquote

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
CAREERS_URL = "https://www.drhorton.com/careers"
TALEO_BASE = "https://drhorton.taleo.net"
PORTAL_ID = "101430233"
SEARCH_ENDPOINT = f"{TALEO_BASE}/careersection/rest/jobboard/searchjobs?lang=en&portal={PORTAL_ID}"
JOB_DETAIL_BASE = f"{TALEO_BASE}/careersection/2/jobdetail.ftl"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "tz": "America/Chicago",
    "tzname": "Central Time",
}
DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 900), 120)

SCRAPER_QS = Scraper.objects.filter(company="D.R. Horton", url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched D.R. Horton; using id=%s.", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="D.R. Horton",
        url=CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )

# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------
class ScraperError(Exception):
    """Raised when scraping encounters a non-recoverable error."""


@dataclass
class JobSummary:
    job_id: str
    contest_no: str
    title: str
    detail_url: str
    location: Optional[str]
    date_posted: Optional[str]
    columns: List[str]


@dataclass
class JobListing(JobSummary):
    description_text: Optional[str]
    description_html: Optional[str]
    metadata: Dict[str, object]


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
def _decode_percent_payload(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    cleaned = value.replace("!*! ", "!*!").replace("!*!%", "!%")
    cleaned = cleaned.replace("!*!", "")
    cleaned = cleaned.lstrip("!")
    decoded = unquote(cleaned)
    return decoded.strip() or None


def _html_to_text(html: Optional[str]) -> Optional[str]:
    if not html:
        return None
    text = BeautifulSoup(html, "html.parser").get_text("\n", strip=True)
    return text or None


def _first(values: Optional[List[str]]) -> Optional[str]:
    if not values:
        return None
    for value in values:
        if value:
            return value
    return None


# ---------------------------------------------------------------------------
# Scraper implementation
# ---------------------------------------------------------------------------
class DrHortonJobScraper:
    def __init__(
        self,
        *,
        delay: float = 0.25,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.delay = max(delay, 0.0)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    def scrape(
        self,
        *,
        max_pages: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Iterable[JobListing]:
        fetched = 0
        page_no = 1

        while True:
            if max_pages is not None and page_no > max_pages:
                self.logger.info("Reached max_pages=%s; stopping.", max_pages)
                break

            data = self._fetch_jobs_page(page_no)
            requisitions = data.get("requisitionList") or []
            if not requisitions:
                self.logger.info("No requisitions returned for page %s; stopping.", page_no)
                break

            for raw in requisitions:
                summary = self._parse_summary(raw)
                if summary is None:
                    continue

                detail = self._fetch_job_detail(summary.contest_no, summary.detail_url)
                listing = JobListing(**asdict(summary), **detail)
                yield listing

                fetched += 1
                if limit is not None and fetched >= limit:
                    self.logger.info("Reached limit=%s; stopping.", limit)
                    return

                if self.delay:
                    time.sleep(self.delay)

            paging = data.get("pagingData") or {}
            total_count = paging.get("totalCount")
            page_size = paging.get("pageSize") or len(requisitions)

            if total_count and page_size:
                total_pages = (int(total_count) + int(page_size) - 1) // int(page_size)
                if page_no >= total_pages:
                    break

            page_no += 1

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _fetch_jobs_page(self, page_no: int) -> dict:
        payload = {"pageNo": max(page_no, 1)}
        self.logger.debug("Fetching Dr Horton jobs page %s", page_no)
        response = self.session.post(SEARCH_ENDPOINT, json=payload, timeout=45)
        response.raise_for_status()
        return response.json()

    def _parse_summary(self, raw: dict) -> Optional[JobSummary]:
        columns = raw.get("column") or []
        if not columns:
            return None

        title = (columns[0] or "").strip()
        location = self._parse_location(columns[1] if len(columns) > 1 else None)
        date_posted = (columns[2] or "").strip() if len(columns) > 2 else None

        job_id = str(raw.get("jobId") or "").strip()
        contest_no = str(raw.get("contestNo") or "").strip()
        if not contest_no:
            self.logger.debug("Skipping summary without contest number: %s", raw)
            return None

        detail_url = f"{JOB_DETAIL_BASE}?job={contest_no}&lang=en"
        return JobSummary(
            job_id=job_id,
            contest_no=contest_no,
            title=title or contest_no,
            detail_url=detail_url,
            location=location,
            date_posted=date_posted or None,
            columns=list(columns),
        )

    def _parse_location(self, raw_location: Optional[str]) -> Optional[str]:
        if not raw_location:
            return None
        raw_location = raw_location.strip()
        try:
            parsed = json.loads(raw_location)
            if isinstance(parsed, list):
                cleaned = [loc for loc in parsed if loc]
                if cleaned:
                    return ", ".join(cleaned)
        except json.JSONDecodeError:
            pass
        return raw_location or None

    def _fetch_job_detail(self, contest_no: str, detail_url: str) -> Dict[str, object]:
        self.logger.debug("Fetching job detail %s", detail_url)
        response = self.session.get(detail_url, timeout=45)
        response.raise_for_status()
        return self._parse_job_detail(response.text, contest_no, detail_url)

    def _parse_job_detail(self, html: str, contest_no: str, detail_url: str) -> Dict[str, object]:
        soup = BeautifulSoup(html, "html.parser")
        script_text = None
        for script in soup.find_all("script"):
            text = script.string or script.get_text()
            if not text:
                continue
            if "api.fillList('requisitionDescriptionInterface', 'descRequisition'" in text:
                script_text = text
                break

        if not script_text:
            raise ScraperError(f"Unable to locate Taleo payload for contest {contest_no}")

        values = self._extract_fill_list_fields(script_text)
        description_html = _decode_percent_payload(_first(values.get("reqlistitem.description")))
        qualification_html = _decode_percent_payload(_first(values.get("reqlistitem.qualification")))

        sections: Dict[str, Optional[str]] = {
            "description_html": description_html,
            "qualification_html": qualification_html,
        }

        combined_parts = []
        if description_html:
            combined_parts.append(description_html)
        if qualification_html:
            combined_parts.append("<h3>Qualifications</h3>" + qualification_html)
        combined_html = "\n\n".join(combined_parts) if combined_parts else None

        description_text_parts = [
            _html_to_text(description_html),
            _html_to_text(qualification_html),
        ]
        description_text = "\n\n".join([part for part in description_text_parts if part]) or None

        metadata: Dict[str, object] = {
            "job_id": _first(values.get("reqlistitem.no")) or _first(values.get("reqlistitem.contestnumber")),
            "contest_number": contest_no,
            "job_field": _first(values.get("reqlistitem.jobfield")),
            "primary_location": _first(values.get("reqlistitem.primarylocation")),
            "other_locations": _first(values.get("reqlistitem.otherlocations")),
            "organization": _first(values.get("reqlistitem.organization")),
            "schedule": _first(values.get("reqlistitem.jobschedule")),
            "posting_datetime": _first(values.get("reqlistitem.postingdate")),
            "referral_bonus": _first(values.get("reqlistitem.referralbonus")),
            "raw_sections": {k: v for k, v in sections.items() if v},
            "raw_columns": values.get("reqlistitem.no"),
        }

        metadata = {key: value for key, value in metadata.items() if value}

        return {
            "description_text": description_text,
            "description_html": combined_html,
            "metadata": metadata,
        }

    def _extract_fill_list_fields(self, script_text: str) -> Dict[str, List[str]]:
        hlid_match = re.search(r"_hlid:\s*\[(.*?)\]", script_text, re.S)
        values_match = re.search(
            r"api\.fillList\('requisitionDescriptionInterface',\s*'descRequisition',\s*\[(.*?)\]\);",
            script_text,
            re.S,
        )
        if not hlid_match or not values_match:
            raise ScraperError("Unable to parse Taleo fillList payload.")

        labels = re.findall(r"'([^']*)'", hlid_match.group(1))
        values = re.findall(r"'([^']*)'", values_match.group(1))

        if len(labels) != len(values):
            raise ScraperError("Taleo payload field/value length mismatch.")

        field_map: Dict[str, List[str]] = {}
        for label, value in zip(labels, values):
            field_map.setdefault(label, []).append(value)
        return field_map


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------
def store_listing(listing: JobListing) -> bool:
    metadata = dict(listing.metadata or {})
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or listing.metadata.get("primary_location") or "")[:255] or None,
        "date": (listing.date_posted or listing.metadata.get("posting_datetime") or "")[:100] or None,
        "description": (listing.description_text or listing.description_html or "")[:10000],
        "metadata": metadata,
    }

    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults=defaults,
    )
    logging.getLogger("store_listing").debug(
        "Stored job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


# ---------------------------------------------------------------------------
# CLI helpers
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="D.R. Horton manual scraper.")
    parser.add_argument("--max-pages", type=int, default=None, help="Maximum pagination pages to fetch.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to persist.")
    parser.add_argument("--delay", type=float, default=0.25, help="Delay (seconds) between detail requests.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print scraped jobs as JSON instead of storing them.",
    )
    return parser.parse_args(argv)


def run_scrape(
    *,
    max_pages: Optional[int],
    limit: Optional[int],
    delay: float,
    dry_run: bool,
) -> Dict[str, object]:
    scraper = DrHortonJobScraper(delay=delay)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    try:
        for listing in scraper.scrape(max_pages=max_pages, limit=limit):
            totals["fetched"] += 1

            if dry_run:
                print(json.dumps(asdict(listing), default=str, ensure_ascii=False))
                continue

            try:
                created = store_listing(listing)
                if created:
                    totals["created"] += 1
                else:
                    totals["updated"] += 1
            except Exception as exc:  # pragma: no cover - persistence safeguard
                logging.exception("Failed to store job %s: %s", listing.detail_url, exc)
                totals["errors"] += 1
    except requests.HTTPError as exc:
        logging.error("HTTP error while scraping D.R. Horton: %s", exc)
        totals["errors"] += 1
    except requests.RequestException as exc:
        logging.error("Network error while scraping D.R. Horton: %s", exc)
        totals["errors"] += 1
    except ScraperError as exc:
        logging.error("Extractor error: %s", exc)
        totals["errors"] += 1

    if not dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["dedupe"] = dedupe_summary

    return totals


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()), format="%(levelname)s: %(message)s")

    totals = run_scrape(
        max_pages=args.max_pages,
        limit=args.limit,
        delay=args.delay,
        dry_run=args.dry_run,
    )
    logging.info(
        "D.R. Horton scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )
    if not args.dry_run and "dedupe" in totals:
        logging.info("Deduplication summary: %s", totals["dedupe"])
    return 0 if not totals["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
