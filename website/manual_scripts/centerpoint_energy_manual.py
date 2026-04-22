#!/usr/bin/env python3
"""Manual scraper for https://careers.centerpointenergy.com."""

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
from typing import Dict, Iterable, Iterator, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag

# ---------------------------------------------------------------------------
# Django bootstrap
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_URL = "https://careers.centerpointenergy.com"
SEARCH_PATH = "/search/"
DEFAULT_SEARCH_PARAMS = {"q": "", "locationsearch": ""}
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.8",
    "Referer": BASE_URL,
}
DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 3600), 60)

SCRAPER_QS = Scraper.objects.filter(
    company="CenterPoint Energy",
    url=urljoin(BASE_URL, SEARCH_PATH),
).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple CenterPoint Energy scrapers found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="CenterPoint Energy",
        url=urljoin(BASE_URL, SEARCH_PATH),
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )

JOB_ID_PATTERN = re.compile(r"/(?P<job_id>\d+)/?$")


class ScraperError(Exception):
    """Raised when the scraper encounters an unrecoverable issue."""


@dataclass
class JobSummary:
    title: str
    detail_url: str
    job_id: Optional[str]
    location: Optional[str]
    date_posted: Optional[str]
    department: Optional[str]
    facility: Optional[str]


@dataclass
class JobListing(JobSummary):
    description_text: str
    description_html: Optional[str]
    metadata: Dict[str, object]


def _normalize_whitespace(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = re.sub(r"\s+", " ", value)
    cleaned = cleaned.strip()
    return cleaned or None


def _text_or_none(node: Optional[Tag], *, separator: str = " ") -> Optional[str]:
    if node is None:
        return None
    text = node.get_text(separator, strip=True)
    return _normalize_whitespace(text)


def _compact_metadata(pairs: Iterable[tuple[str, object]]) -> Dict[str, object]:
    data: Dict[str, object] = {}
    for key, value in pairs:
        if value is None:
            continue
        if isinstance(value, str):
            trimmed = value.strip()
            if not trimmed:
                continue
            data[key] = trimmed
            continue
        if isinstance(value, (list, dict, tuple, set)) and not value:
            continue
        data[key] = value
    return data


def _job_id_from_url(url: str) -> Optional[str]:
    path = urlparse(url).path
    match = JOB_ID_PATTERN.search(path)
    if match:
        return match.group("job_id")
    return None


class CenterPointEnergyJobScraper:
    def __init__(self, *, delay: float = 0.25, session: Optional[requests.Session] = None) -> None:
        self.delay = max(delay, 0.0)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    def scrape(
        self,
        *,
        max_pages: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Iterator[JobListing]:
        seen_links: set[str] = set()
        startrow = 0
        page_index = 0
        processed = 0

        while True:
            soup = self._fetch_search_page(startrow)
            summaries = list(self._parse_job_summaries(soup))
            if not summaries:
                self.logger.info("No results returned for startrow=%s; stopping.", startrow)
                break

            page_index += 1
            self.logger.info("Processing page %s (startrow=%s)", page_index, startrow)

            for summary in summaries:
                if summary.detail_url in seen_links:
                    self.logger.debug("Skipping duplicate detail url %s", summary.detail_url)
                    continue
                seen_links.add(summary.detail_url)

                try:
                    detail = self._fetch_job_detail(summary.detail_url)
                except Exception as exc:
                    self.logger.error("Failed to fetch detail for %s: %s", summary.detail_url, exc)
                    continue

                base_metadata = _compact_metadata(
                    (
                        ("job_id", summary.job_id),
                        ("department", summary.department),
                        ("facility", summary.facility),
                    )
                )
                detail_metadata = detail.pop("metadata", {})
                metadata = {**base_metadata, **(detail_metadata or {})}

                listing = JobListing(
                    **asdict(summary),
                    description_text=detail.get("description_text", ""),
                    description_html=detail.get("description_html"),
                    metadata=metadata,
                )
                yield listing
                processed += 1

                if limit is not None and processed >= limit:
                    self.logger.info("Reached limit=%s; stopping scrape.", limit)
                    return

                if self.delay:
                    time.sleep(self.delay)

            if max_pages is not None and page_index >= max_pages:
                self.logger.info("Reached max_pages=%s; stopping scrape.", max_pages)
                break

            startrow += len(summaries)

    def _fetch_search_page(self, startrow: int) -> BeautifulSoup:
        params = dict(DEFAULT_SEARCH_PARAMS)
        if startrow:
            params["startrow"] = startrow

        response = self.session.get(
            urljoin(BASE_URL, SEARCH_PATH),
            params=params,
            timeout=45,
        )
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")

    def _parse_job_summaries(self, soup: BeautifulSoup) -> Iterator[JobSummary]:
        rows = soup.select("table.searchResults tr.data-row")
        for row in rows:
            title_link = row.select_one("a.jobTitle-link")
            if not title_link:
                continue
            title = _text_or_none(title_link)
            detail_href = title_link.get("href")
            if not title or not detail_href:
                continue
            detail_url = urljoin(BASE_URL, detail_href)
            job_id = _job_id_from_url(detail_url)
            location = _text_or_none(row.select_one("span.jobLocation"))
            posted = _text_or_none(row.select_one("span.jobDate"))
            department = _text_or_none(row.select_one("span.jobDepartment"))
            facility = _text_or_none(row.select_one("span.jobFacility"))

            yield JobSummary(
                title=title,
                detail_url=detail_url,
                job_id=job_id,
                location=location,
                date_posted=posted,
                department=department,
                facility=facility,
            )

    def _fetch_job_detail(self, url: str) -> Dict[str, object]:
        response = self.session.get(url, timeout=45)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        description_elem = soup.select_one("div.job span.jobdescription") or soup.select_one(".jobdescription")
        description_text = ""
        description_html = None
        if description_elem:
            description_text = description_elem.get_text("\n", strip=True)
            description_html = str(description_elem)

        date_text = _text_or_none(soup.select_one("#job-date"))
        if date_text and date_text.lower().startswith("date:"):
            date_text = _normalize_whitespace(date_text.split(":", 1)[-1])

        location_text = _text_or_none(soup.select_one("#job-location .jobGeoLocation"))

        company_text = _text_or_none(soup.select_one("#job-company span"))

        apply_link = soup.select_one(".applylink a, a.dialogApplyBtn")
        apply_url = None
        if apply_link and apply_link.get("href"):
            apply_url = urljoin(BASE_URL, apply_link["href"])

        metadata = _compact_metadata(
            (
                ("detail_posted_date", date_text),
                ("detail_location", location_text),
                ("company", company_text),
                ("apply_url", apply_url),
            )
        )

        additional_fields = self._extract_additional_fields(soup)
        if additional_fields:
            metadata["additional_fields"] = additional_fields

        return {
            "description_text": description_text,
            "description_html": description_html,
            "metadata": metadata,
        }

    def _extract_additional_fields(self, soup: BeautifulSoup) -> Dict[str, str]:
        fields: Dict[str, str] = {}
        for row in soup.select(".jobfacts .job"):
            label = _text_or_none(row.select_one(".jobLabel"))
            value = _text_or_none(row.select_one(".jobDescription"))
            if label and value:
                fields[label] = value

        for item in soup.select(".jobDisplay .jobcontent li"):
            label_elem = item.select_one("span.jobLabel")
            value_elem = item.select_one("span.jobDesc")
            if not label_elem or not value_elem:
                continue
            label = _text_or_none(label_elem)
            value = _text_or_none(value_elem)
            if label and value:
                fields[label] = value

        return fields


def store_listing(listing: JobListing) -> None:
    metadata = dict(listing.metadata or {})

    if listing.job_id and "job_id" not in metadata:
        metadata["job_id"] = listing.job_id
    if listing.department and "department" not in metadata:
        metadata["department"] = listing.department
    if listing.facility and "facility" not in metadata:
        metadata["facility"] = listing.facility

    JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults={
            "title": listing.title[:255],
            "location": (listing.location or "")[:255],
            "date": (listing.date_posted or "")[:100],
            "description": (listing.description_text or "")[:10000],
            "metadata": metadata or None,
        },
    )


def run_scrape(max_pages: Optional[int], limit: Optional[int], delay: float) -> int:
    scraper = CenterPointEnergyJobScraper(delay=delay)
    count = 0
    for job in scraper.scrape(max_pages=max_pages, limit=limit):
        store_listing(job)
        count += 1
    return count


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CenterPoint Energy careers manual scraper")
    parser.add_argument("--max-pages", type=int, default=None, help="Stop after processing this many search result pages")
    parser.add_argument("--limit", type=int, default=None, help="Stop after processing this many job postings")
    parser.add_argument("--delay", type=float, default=0.25, help="Delay (seconds) between detail page fetches")
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
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    start = time.time()
    try:
        count = run_scrape(args.max_pages, args.limit, args.delay)
    except ScraperError as exc:
        logging.error("Scrape failed: %s", exc)
        return 1
    duration = time.time() - start
    dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
    summary = {
        "company": "CenterPoint Energy",
        "url": urljoin(BASE_URL, SEARCH_PATH),
        "count": count,
        "elapsed_seconds": duration,
        "dedupe": dedupe_summary,
    }
    logging.info("Summary: %s", json.dumps(summary))
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
