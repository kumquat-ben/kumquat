#!/usr/bin/env python3
"""Manual scraper for https://www.lexius.ai/join-waitlist."""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence
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

from django.conf import settings  # noqa: E402

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
COMPANY_NAME = "Lexius"
CAREERS_URL = "https://www.lexius.ai/join-waitlist"
SITEMAP_URL = "https://www.lexius.ai/sitemap.xml"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
REQUEST_TIMEOUT = (10, 30)
DEFAULT_DELAY = 0.2
DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 600), 60)

SCRAPER_QS = Scraper.objects.filter(company=COMPANY_NAME, url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Lexius scrapers found; using id=%s", SCRAPER.id)
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
class LexiusJob:
    title: str
    link: str
    location: Optional[str]
    date_posted: Optional[str]
    description: str
    metadata: Dict[str, object]


# ---------------------------------------------------------------------------
# JSON-LD helpers
# ---------------------------------------------------------------------------

def _is_job_posting(node_type: object) -> bool:
    if isinstance(node_type, str):
        return node_type == "JobPosting"
    if isinstance(node_type, list):
        return "JobPosting" in node_type
    return False


def _collect_job_payloads(raw: object, found: List[Dict[str, object]]) -> None:
    if isinstance(raw, dict):
        if _is_job_posting(raw.get("@type")):
            found.append(raw)
        graph = raw.get("@graph")
        if isinstance(graph, list):
            for node in graph:
                _collect_job_payloads(node, found)
        for key, value in raw.items():
            if key == "@graph":
                continue
            _collect_job_payloads(value, found)
    elif isinstance(raw, list):
        for item in raw:
            _collect_job_payloads(item, found)


def _extract_job_payloads(html: str) -> List[Dict[str, object]]:
    soup = BeautifulSoup(html, "html.parser")
    payloads: List[Dict[str, object]] = []
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        raw_text = script.string or script.text or ""
        raw_text = raw_text.strip()
        if not raw_text:
            continue
        try:
            data = json.loads(raw_text)
        except json.JSONDecodeError:
            continue
        _collect_job_payloads(data, payloads)
    return payloads


def _clean_description(description_html: Optional[str]) -> str:
    if not description_html:
        return ""
    soup = BeautifulSoup(description_html, "html.parser")
    text = soup.get_text("\n", strip=True)
    lines = [line.strip() for line in text.splitlines()]
    return "\n".join(line for line in lines if line)


def _compose_location(job_locations: object, job_location_type: object) -> Optional[str]:
    entries: List[Dict[str, object]] = []
    if isinstance(job_locations, dict):
        entries = [job_locations]
    elif isinstance(job_locations, list):
        entries = [entry for entry in job_locations if isinstance(entry, dict)]

    locations: List[str] = []
    for entry in entries:
        address = entry.get("address")
        if not isinstance(address, dict):
            continue
        locality = (address.get("addressLocality") or "").strip()
        region = (address.get("addressRegion") or "").strip()
        country = (address.get("addressCountry") or "").strip()
        parts = [part for part in (locality, region) if part]
        if country and country not in parts:
            parts.append(country)
        if not parts:
            continue
        location = ", ".join(parts)
        if location not in locations:
            locations.append(location)

    if locations:
        return " | ".join(locations)

    if isinstance(job_location_type, str) and job_location_type:
        if "TELECOMMUTE" in job_location_type.upper() or "REMOTE" in job_location_type.upper():
            return "Remote"
    if isinstance(job_location_type, list):
        values = " ".join(str(value).upper() for value in job_location_type)
        if "TELECOMMUTE" in values or "REMOTE" in values:
            return "Remote"
    return None


def _extract_identifier(payload: Dict[str, object]) -> Optional[str]:
    identifier = payload.get("identifier")
    if isinstance(identifier, dict):
        value = identifier.get("value")
        if isinstance(value, str) and value.strip():
            return value.strip()
    if isinstance(identifier, str) and identifier.strip():
        return identifier.strip()
    return None


def _build_job_record(payload: Dict[str, object], source_url: str) -> Optional[LexiusJob]:
    title = (payload.get("title") or "").strip()
    if not title:
        return None

    raw_link = (payload.get("url") or "").strip()
    link = raw_link or source_url

    description = _clean_description(payload.get("description"))
    date_posted = None
    if payload.get("datePosted"):
        date_posted = str(payload["datePosted"])

    location = _compose_location(payload.get("jobLocation"), payload.get("jobLocationType"))

    metadata: Dict[str, object] = {
        "source_url": source_url,
    }
    identifier = _extract_identifier(payload)
    if identifier:
        metadata["identifier"] = identifier

    for key in (
        "employmentType",
        "hiringOrganization",
        "validThrough",
        "jobLocationType",
        "applicantLocationRequirements",
        "baseSalary",
        "industry",
        "directApply",
    ):
        value = payload.get(key)
        if value not in (None, "", [], {}):
            metadata[key] = value

    raw_locations = payload.get("jobLocation")
    if raw_locations:
        metadata["jobLocation"] = raw_locations

    return LexiusJob(
        title=title,
        link=link,
        location=location,
        date_posted=date_posted,
        description=description,
        metadata=metadata,
    )


# ---------------------------------------------------------------------------
# Scraper implementation
# ---------------------------------------------------------------------------
class LexiusWaitlistScraper:
    def __init__(self, *, delay: float = DEFAULT_DELAY, session: Optional[requests.Session] = None) -> None:
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    def _fetch_url(self, url: str) -> Optional[str]:
        try:
            response = self.session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.RequestException as exc:
            self.logger.warning("Failed to fetch %s: %s", url, exc)
            return None
        return response.text

    def _discover_urls(self) -> List[str]:
        urls: List[str] = [CAREERS_URL]
        sitemap_html = self._fetch_url(SITEMAP_URL)
        if not sitemap_html:
            return urls
        soup = BeautifulSoup(sitemap_html, "xml")
        for loc in soup.find_all("loc"):
            href = (loc.text or "").strip()
            if not href:
                continue
            urls.append(urljoin(CAREERS_URL, href))
        seen: List[str] = []
        unique_urls: List[str] = []
        for url in urls:
            if url in seen:
                continue
            seen.append(url)
            unique_urls.append(url)
        return unique_urls

    def scrape(self, *, limit: Optional[int] = None) -> Iterator[LexiusJob]:
        seen_keys: set[str] = set()
        yielded = 0
        for page_url in self._discover_urls():
            html = self._fetch_url(page_url)
            if not html:
                continue
            job_payloads = _extract_job_payloads(html)
            if not job_payloads:
                if self.delay:
                    time.sleep(self.delay)
                continue
            for payload in job_payloads:
                record = _build_job_record(payload, page_url)
                if not record:
                    continue
                key = record.link or f"{record.title}:{record.date_posted or page_url}"
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                yield record
                yielded += 1
                if limit is not None and yielded >= limit:
                    return
            if self.delay:
                time.sleep(self.delay)


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def store_listing(listing: LexiusJob) -> None:
    JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.link,
        defaults={
            "title": (listing.title or "")[:255],
            "location": (listing.location or "")[:255],
            "date": (listing.date_posted or "")[:100],
            "description": (listing.description or "")[:10000],
            "metadata": listing.metadata,
        },
    )


def run_scrape(limit: Optional[int], delay: float) -> int:
    scraper = LexiusWaitlistScraper(delay=delay)
    count = 0
    for job in scraper.scrape(limit=limit):
        store_listing(job)
        count += 1
    return count


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lexius waitlist scraper")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY)
    parser.add_argument("--log-level", type=str, default="INFO", choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"])
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
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
