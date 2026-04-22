#!/usr/bin/env python3
"""Manual scraper for https://www.legalos.ai/employment-based-visas/h1b."""
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
from urllib.parse import unquote, urljoin, urlparse

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
COMPANY_NAME = "LegalOS"
SOURCE_URL = "https://www.legalos.ai/employment-based-visas/h1b"
CAREERS_URL = "https://www.legalos.ai/careers"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 30)

SCRAPER_QS = Scraper.objects.filter(company=COMPANY_NAME, url=SOURCE_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple LegalOS scrapers found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company=COMPANY_NAME,
        url=SOURCE_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the scraper cannot continue."""


@dataclass
class JobListing:
    title: str
    detail_url: str
    location: Optional[str]
    employment_type: Optional[str]
    description: str
    date_posted: Optional[str]
    metadata: Dict[str, object]


class LegalOSH1BJobScraper:
    def __init__(self, delay: float = 0.2, session: Optional[requests.Session] = None) -> None:
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    def scrape(self, limit: Optional[int] = None) -> Iterable[JobListing]:
        listings = self._fetch_listings()
        self.logger.info("Discovered %s job postings", len(listings))

        yielded = 0
        for listing in listings:
            yield listing
            yielded += 1
            if limit is not None and yielded >= limit:
                return
            if self.delay:
                time.sleep(self.delay)

    # ------------------------------------------------------------------
    # Fetch + parse helpers
    # ------------------------------------------------------------------
    def _fetch_listings(self) -> List[JobListing]:
        primary_html = self._fetch_html(SOURCE_URL)
        listings = self._parse_job_cards(primary_html, SOURCE_URL)
        if listings:
            return listings

        self.logger.info("No job cards on %s; checking %s", SOURCE_URL, CAREERS_URL)
        fallback_html = self._fetch_html(CAREERS_URL)
        listings = self._parse_job_cards(fallback_html, CAREERS_URL)
        if listings:
            return listings

        listings = self._parse_json_ld_jobs(primary_html, SOURCE_URL)
        if listings:
            return listings

        listings = self._parse_json_ld_jobs(fallback_html, CAREERS_URL)
        if listings:
            return listings

        self.logger.warning("No job listings found on %s or %s", SOURCE_URL, CAREERS_URL)
        return []

    def _fetch_html(self, url: str) -> str:
        response = self.session.get(url, timeout=40)
        response.raise_for_status()
        return response.text

    def _parse_job_cards(self, html: str, base_url: str) -> List[JobListing]:
        soup = BeautifulSoup(html, "html.parser")
        cards = soup.select(".uui-career07_item")
        listings: List[JobListing] = []

        for card in cards:
            title = _text(card.select_one(".uui-career07_heading")) or "Untitled role"
            label = _text(card.select_one(".uui-badge div:last-child"))
            summary = _text(card.select_one(".uui-text-size-medium-4")) or ""

            detail_values = [
                _text(detail) for detail in card.select(".uui-career07_detail-wrapper")
            ]
            detail_values = [value for value in detail_values if value]
            location = detail_values[0] if detail_values else None
            employment_type = detail_values[1] if len(detail_values) > 1 else None

            link_tag = card.find("a", href=True)
            detail_url = base_url
            link_label = None
            if link_tag:
                href = link_tag.get("href") or ""
                link_label = _text(link_tag)
                if href.startswith("mailto:"):
                    detail_url = href
                else:
                    detail_url = urljoin(base_url, href)

            contact_email = _extract_mailto_email(detail_url)

            metadata = {
                "source_page": base_url,
                "role_label": label,
                "summary": summary,
                "details": detail_values,
                "link_label": link_label,
                "contact_email": contact_email,
            }

            listings.append(
                JobListing(
                    title=title,
                    detail_url=detail_url,
                    location=location,
                    employment_type=employment_type,
                    description=summary,
                    date_posted=None,
                    metadata=metadata,
                )
            )

        return listings

    def _parse_json_ld_jobs(self, html: str, base_url: str) -> List[JobListing]:
        soup = BeautifulSoup(html, "html.parser")
        scripts = soup.find_all("script", type="application/ld+json")
        listings: List[JobListing] = []

        for script in scripts:
            if not script.string:
                continue
            try:
                data = json.loads(script.string)
            except json.JSONDecodeError:
                continue
            for posting in _iter_json_ld_job_postings(data):
                title = (posting.get("title") or "Untitled role").strip()
                description = (posting.get("description") or "").strip()
                detail_url = posting.get("url") or base_url
                location = _extract_json_ld_location(posting)
                employment_type = _extract_json_ld_employment_type(posting)
                date_posted = posting.get("datePosted")
                metadata = {
                    "source_page": base_url,
                    "json_ld": posting,
                }
                listings.append(
                    JobListing(
                        title=title,
                        detail_url=detail_url,
                        location=location,
                        employment_type=employment_type,
                        description=description,
                        date_posted=date_posted,
                        metadata=metadata,
                    )
                )

        return listings


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _text(element: Optional[object]) -> Optional[str]:
    if element is None:
        return None
    if hasattr(element, "get_text"):
        text_value = element.get_text(" ", strip=True)
    else:
        text_value = str(element).strip()
    return text_value or None


def _extract_mailto_email(link: str) -> Optional[str]:
    if not link.startswith("mailto:"):
        return None
    parsed = urlparse(link)
    email = parsed.path or ""
    return unquote(email) or None


def _iter_json_ld_job_postings(data: object) -> Iterable[Dict[str, object]]:
    if isinstance(data, dict):
        if data.get("@type") == "JobPosting":
            yield data
        for key in ("@graph", "graph", "itemListElement"):
            value = data.get(key)
            if isinstance(value, list):
                for entry in value:
                    yield from _iter_json_ld_job_postings(entry)
        return
    if isinstance(data, list):
        for entry in data:
            yield from _iter_json_ld_job_postings(entry)


def _extract_json_ld_location(posting: Dict[str, object]) -> Optional[str]:
    raw = posting.get("jobLocation")
    if isinstance(raw, list) and raw:
        raw = raw[0]
    if isinstance(raw, dict):
        address = raw.get("address") or {}
        if isinstance(address, dict):
            parts = [
                address.get("addressLocality"),
                address.get("addressRegion"),
                address.get("addressCountry"),
            ]
            return ", ".join([part for part in parts if part])
    if isinstance(raw, str):
        return raw
    return None


def _extract_json_ld_employment_type(posting: Dict[str, object]) -> Optional[str]:
    raw = posting.get("employmentType")
    if isinstance(raw, list):
        raw = ", ".join([str(entry) for entry in raw if entry])
    if raw:
        return str(raw)
    return None


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def store_listing(listing: JobListing) -> None:
    JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults={
            "title": (listing.title or "")[:255],
            "location": (listing.location or "")[:255],
            "date": (listing.date_posted or "")[:100],
            "description": (listing.description or "")[:10000],
            "metadata": listing.metadata,
        },
    )


def run_scrape(limit: Optional[int], delay: float) -> int:
    scraper = LegalOSH1BJobScraper(delay=delay)
    count = 0
    for job in scraper.scrape(limit=limit):
        store_listing(job)
        count += 1
    return count


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="LegalOS H-1B jobs scraper")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--delay", type=float, default=0.2)
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
    )
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
        "url": SOURCE_URL,
        "count": count,
        "elapsed_seconds": duration,
        "dedupe": dedupe_summary,
    }
    logging.info("Summary: %s", json.dumps(summary))
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
