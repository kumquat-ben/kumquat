#!/usr/bin/env python3
"""Manual scraper for https://bellabeat.com/careers."""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Generator, Iterable, List, Optional

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
COMPANY_NAME = "Bellabeat"
CAREERS_URL = "https://bellabeat.com/careers"

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

SCRAPER_QS = Scraper.objects.filter(company=COMPANY_NAME, url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Bellabeat scrapers found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company=COMPANY_NAME,
        url=CAREERS_URL,
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
    department: Optional[str]
    location: Optional[str]
    description_text: str
    description_html: Optional[str]
    metadata: Dict[str, object]


class BellabeatCareersScraper:
    def __init__(self, delay: float = 0.2, session: Optional[requests.Session] = None) -> None:
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    def scrape(self, limit: Optional[int] = None) -> Generator[JobListing, None, None]:
        html = self._fetch_careers_page()
        listings = list(self._parse_listings(html))
        self.logger.info("Discovered %s job postings", len(listings))

        yielded = 0
        for listing in listings:
            yield listing
            yielded += 1
            if limit is not None and yielded >= limit:
                self.logger.info("Reached limit %s; stopping scrape", limit)
                return
            if self.delay:
                time.sleep(self.delay)

    def _fetch_careers_page(self) -> str:
        try:
            response = self.session.get(CAREERS_URL, timeout=40)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch careers page: {exc}") from exc
        return response.text

    def _parse_listings(self, html: str) -> Iterable[JobListing]:
        soup = BeautifulSoup(html, "html.parser")
        toggle_items = soup.select("div.elementor-toggle-item")
        if not toggle_items:
            raise ScraperError("No job listings found in Elementor toggle items.")

        seen_urls = set()
        for item in toggle_items:
            title_div = item.find("div", class_="elementor-tab-title")
            content_div = item.find("div", class_="elementor-tab-content")
            if not title_div or not content_div:
                continue

            anchor = title_div.find("a", class_="elementor-toggle-title")
            title_text = " ".join(anchor.stripped_strings) if anchor else " ".join(title_div.stripped_strings)
            if not title_text:
                continue

            department = None
            if anchor:
                dept_span = anchor.find("span")
                if dept_span:
                    department = dept_span.get_text(strip=True) or None

            title = title_text
            if department and title_text.startswith(department):
                title = title_text[len(department):].strip(" -–:|")
            if not title:
                title = title_text

            tab_id = title_div.get("id") or content_div.get("id")
            detail_url = f"{CAREERS_URL}#{tab_id}" if tab_id else CAREERS_URL
            if detail_url in seen_urls:
                continue
            seen_urls.add(detail_url)

            description_html = _inner_html(content_div)
            description_text = _html_to_text(description_html) or content_div.get_text("\n", strip=True)
            location = _extract_location(description_text)

            metadata: Dict[str, object] = {
                "department": department,
                "location": location,
                "elementor_tab_id": tab_id,
                "source": "bellabeat_careers",
            }

            mailtos = _extract_mailtos(content_div)
            if mailtos:
                metadata["apply_emails"] = sorted(mailtos)

            yield JobListing(
                title=title,
                detail_url=detail_url,
                department=department,
                location=location,
                description_text=description_text,
                description_html=description_html,
                metadata=metadata,
            )


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------
def _html_to_text(value: Optional[str]) -> str:
    if not value:
        return ""
    soup = BeautifulSoup(value, "html.parser")
    return soup.get_text("\n", strip=True).strip()


def _inner_html(node: BeautifulSoup) -> Optional[str]:
    contents = [str(child) for child in node.contents]
    payload = "".join(contents).strip()
    return payload or None


def _extract_location(text: str) -> Optional[str]:
    for line in text.splitlines():
        if line.lower().startswith("location:"):
            return line.split(":", 1)[1].strip() or None
    match = re.search(r"Location:\s*([^\n]+)", text, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip() or None
    return None


def _extract_mailtos(node: BeautifulSoup) -> List[str]:
    emails = []
    for anchor in node.select('a[href^="mailto:"]'):
        href = anchor.get("href") or ""
        address = href.split("mailto:", 1)[-1].split("?", 1)[0].strip()
        if address:
            emails.append(address)
    return sorted(set(emails))


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
            "date": "",
            "description": (listing.description_text or listing.description_html or "")[:10000],
            "metadata": listing.metadata,
        },
    )


def run_scrape(limit: Optional[int], delay: float) -> int:
    scraper = BellabeatCareersScraper(delay=delay)
    count = 0
    for job in scraper.scrape(limit=limit):
        store_listing(job)
        count += 1
    return count


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bellabeat careers scraper")
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
