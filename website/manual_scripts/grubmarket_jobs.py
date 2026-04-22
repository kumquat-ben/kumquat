#!/usr/bin/env python3
"""Custom scraper for https://www.grubmarket.com/jobs."""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Generator, Iterable, List, Optional, Tuple
from urllib.parse import urljoin

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
ROOT_URL = "https://www.grubmarket.com/jobs"
OPENINGS_URL = "https://www.grubmarket.com/jobs/openings"
BASE_URL = "https://www.grubmarket.com"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": ROOT_URL,
}

REQUEST_TIMEOUT = (15, 45)
DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 30)

SCRAPER_QS = Scraper.objects.filter(company="GrubMarket", url=ROOT_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple GrubMarket scrapers found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="GrubMarket",
        url=ROOT_URL,
        code="custom-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the scraper cannot continue."""


@dataclass
class JobSummary:
    title: str
    raw_title: str
    link: str
    category: Optional[str]
    location: Optional[str]
    slug: Optional[str]


@dataclass
class JobListing(JobSummary):
    department: Optional[str]
    role: Optional[str]
    description_text: Optional[str]
    description_html: Optional[str]
    metadata: Dict[str, object]


class GrubMarketJobScraper:
    def __init__(self, delay: float = 0.2, session: Optional[requests.Session] = None) -> None:
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    def scrape(self, limit: Optional[int] = None) -> Generator[JobListing, None, None]:
        summaries = list(self._fetch_job_summaries())
        self.logger.info("Discovered %s job postings", len(summaries))

        yielded = 0
        for summary in summaries:
            detail = self._fetch_job_detail(summary.link)
            metadata = {
                **detail["metadata"],
                "category": summary.category,
                "location": summary.location,
                "slug": summary.slug,
                "title_raw": summary.raw_title,
            }
            listing = JobListing(**asdict(summary), **detail, metadata=metadata)
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
        response = self.session.get(OPENINGS_URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        items = soup.select(".career-open-jobs__item")
        if not items:
            raise ScraperError("No job listings found on the openings page.")

        seen_links = set()
        for item in items:
            category = _text_or_none(item.select_one(".career-open-jobs__title"))
            for anchor in item.select(".career-open-jobs__list-item a"):
                href = (anchor.get("href") or "").strip()
                if not href:
                    continue
                link = urljoin(BASE_URL, href)
                if link in seen_links:
                    continue
                seen_links.add(link)

                raw_title = anchor.get_text(strip=True)
                title, location = _split_title_location(raw_title)
                slug = href.strip("/").split("/")[-1] if href else None

                yield JobSummary(
                    title=title or raw_title,
                    raw_title=raw_title,
                    link=link,
                    category=category,
                    location=location,
                    slug=slug,
                )

    def _fetch_job_detail(self, url: str) -> Dict[str, Optional[object]]:
        response = self.session.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        department_raw = _text_or_none(soup.select_one(".job-listing-title__department"))
        role = _text_or_none(soup.select_one(".job-listing-title__role"))
        department = department_raw.replace(" at GrubMarket", "").strip() if department_raw else None

        description_text, description_html = _extract_description(soup)

        metadata = {
            "department_raw": department_raw,
            "department": department,
            "role": role,
            "source_url": url,
        }

        return {
            "department": department,
            "role": role,
            "description_text": description_text,
            "description_html": description_html,
            "metadata": metadata,
        }


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _text_or_none(element: Optional[object]) -> Optional[str]:
    if not element:
        return None
    text = element.get_text(" ", strip=True)
    return text or None


def _split_title_location(value: str) -> Tuple[str, Optional[str]]:
    if not value:
        return "", None
    if " - " in value:
        title, location = value.rsplit(" - ", 1)
        return title.strip(), location.strip() or None
    return value.strip(), None


def _extract_description(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    sections = soup.select(".job-listing-job-description__section")
    if sections:
        text_parts: List[str] = []
        html_parts: List[str] = []
        for section in sections:
            section_text = section.get_text("\n", strip=True)
            if section_text:
                text_parts.append(section_text)
            html_parts.append(str(section))
        text = "\n\n".join(text_parts).strip() if text_parts else None
        html = "\n".join(html_parts).strip() if html_parts else None
        return text or None, html or None

    container = soup.select_one(".job-listing-job-description")
    if not container:
        return None, None
    text = container.get_text("\n", strip=True).strip()
    return text or None, str(container).strip() or None


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def store_listing(listing: JobListing) -> None:
    JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.link,
        defaults={
            "title": (listing.title or "")[:255],
            "location": (listing.location or "")[:255],
            "date": "",
            "description": (listing.description_text or listing.description_html or "")[:10000],
            "metadata": listing.metadata,
        },
    )


def run_scrape(limit: Optional[int], delay: float) -> int:
    scraper = GrubMarketJobScraper(delay=delay)
    count = 0
    for job in scraper.scrape(limit=limit):
        store_listing(job)
        count += 1
    return count


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="GrubMarket jobs scraper")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--delay", type=float, default=0.2)
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
        "company": "GrubMarket",
        "url": ROOT_URL,
        "count": count,
        "elapsed_seconds": duration,
        "dedupe": dedupe_summary,
    }
    logging.info("Summary: %s", json.dumps(summary))
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
