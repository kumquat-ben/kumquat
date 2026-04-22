#!/usr/bin/env python3
"""Manual scraper for Globe Life careers."""

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
from typing import Dict, Iterable, Optional, Set
from urllib.parse import parse_qs, urljoin, urlparse

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
from django.db import IntegrityError  # noqa: E402

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

# ---------------------------------------------------------------------------
# Constants & configuration
# ---------------------------------------------------------------------------
ROOT_LANDING_URL = "https://careers.globelifeinsurance.com/jobs/jobs-by-category"
BASE_DOMAIN = "https://careers.globelifeinsurance.com"
REQUEST_TIMEOUT = (15, 45)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://careers.globelifeinsurance.com/",
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 1800), 30)
SCRAPER_QS = Scraper.objects.filter(company="Globe Life", url=ROOT_LANDING_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched; using the earliest (id=%s).", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Globe Life",
        url=ROOT_LANDING_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the scrape pipeline cannot proceed."""


@dataclass
class JobListing:
    link: str
    title: str
    location: Optional[str]
    description_text: str
    description_html: str
    metadata: Dict[str, object]


def _absolute_url(href: str) -> str:
    return urljoin(BASE_DOMAIN, href)


def _extract_job_id(url: str) -> Optional[str]:
    parsed = urlparse(url)
    candidates = parse_qs(parsed.query).get("jobid")
    if candidates:
        job_id = candidates[0].strip()
        if job_id:
            return job_id
    match = re.search(r"job-(\d+)", parsed.path or "")
    if match:
        return match.group(1)
    return None


def _normalize_whitespace(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    cleaned = re.sub(r"\s+", " ", value).strip()
    return cleaned or None


def _clean_text(html_fragment: Optional[str]) -> str:
    soup = BeautifulSoup(html_fragment or "", "html.parser")
    return soup.get_text("\n", strip=True).strip()


class GlobeLifeClient:
    def __init__(self, session: Optional[requests.Session] = None, delay: float = 0.0) -> None:
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.delay = max(0.0, delay)
        self.logger = logging.getLogger(self.__class__.__name__)

    def iter_listings(self, limit: Optional[int] = None) -> Iterable[JobListing]:
        soup = self._fetch_listing_page()
        listings_parent = soup.select_one("section.jobs-list div.container > div.row > div.col-md-12")
        if not listings_parent:
            raise ScraperError("Unable to locate Globe Life job listings container.")

        seen_links: Set[str] = set()
        processed = 0

        for block in listings_parent.find_all("div", recursive=False):
            category_node = block.select_one("div.jobs-list-category span")
            category = category_node.get_text(strip=True) if category_node else None

            for entry in block.select("div.jobs-list-entry"):
                anchor = entry.find("a")
                if not anchor:
                    continue

                href = (anchor.get("href") or "").strip()
                if not href:
                    continue

                link = _absolute_url(href)
                if "/jobs/job-details/" not in link:
                    self.logger.debug("Skipping non job-detail link %s", link)
                    continue

                if link in seen_links:
                    continue
                seen_links.add(link)

                title_node = anchor.find("div", class_="jobs-list-title")
                title = title_node.get_text(strip=True) if title_node else ""
                if not title:
                    self.logger.debug("Skipping job at %s due to missing title.", link)
                    continue

                location_node = anchor.find("div", class_="jobs-list-location")
                tracking_code = None
                location_text: Optional[str] = None
                if location_node:
                    tracking_node = location_node.find("span", class_="job-tracking-code")
                    if tracking_node:
                        tracking_code = tracking_node.get_text(strip=True).strip("()") or None
                        tracking_node.extract()
                    location_text = _normalize_whitespace(location_node.get_text(" ", strip=True))

                job_id = _extract_job_id(link)

                try:
                    detail = self._fetch_detail_page(link)
                except ScraperError as exc:
                    self.logger.warning("Skipping %s: %s", link, exc)
                    continue

                description_html = detail.get("description_html") or ""
                description_text = _clean_text(description_html) or "Description unavailable."
                detail_location = _normalize_whitespace(detail.get("job_header_location"))
                chosen_location = detail_location or location_text

                metadata: Dict[str, object] = {
                    "category": category,
                    "job_id": job_id,
                    "tracking_code": tracking_code,
                    "job_number": detail.get("job_number"),
                    "apply_links": detail.get("apply_links"),
                    "listing_location": location_text,
                    "detail_location": detail_location,
                    "job_header_title": detail.get("job_header_title"),
                }
                metadata = {key: value for key, value in metadata.items() if value not in (None, "", [])}

                yield JobListing(
                    link=link,
                    title=title,
                    location=chosen_location,
                    description_text=description_text,
                    description_html=description_html,
                    metadata=metadata,
                )

                processed += 1
                if limit and processed >= limit:
                    return
                if self.delay:
                    time.sleep(self.delay)

    def _fetch_listing_page(self) -> BeautifulSoup:
        try:
            resp = self.session.get(ROOT_LANDING_URL, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch Globe Life listings page: {exc}") from exc
        return BeautifulSoup(resp.text, "html.parser")

    def _fetch_detail_page(self, url: str) -> Dict[str, object]:
        try:
            resp = self.session.get(url, timeout=REQUEST_TIMEOUT, headers={"Referer": ROOT_LANDING_URL})
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch job detail at {url}: {exc}") from exc

        soup = BeautifulSoup(resp.text, "html.parser")
        job_header = soup.select_one("div.job-header")
        job_number = None
        job_header_title = None
        job_header_location = None

        if job_header:
            title_node = job_header.find("h1", class_="job-title")
            if title_node:
                job_header_title = title_node.get_text(strip=True)
            location_node = job_header.find("div", class_="job-location")
            if location_node:
                job_header_location = _normalize_whitespace(location_node.get_text(" ", strip=True))
            job_number_node = job_header.find("div", class_="job-number")
            if job_number_node:
                job_number_text = job_number_node.get_text(" ", strip=True)
                job_number = job_number_text.replace("Job number:", "").strip() or None

        job_body = soup.select_one("div.job-body")
        if not job_body:
            raise ScraperError("Job detail page is missing the job body section.")

        description_container = job_body.select_one("span[id$='lbDescription']")
        if description_container:
            description_html = description_container.decode_contents()
        else:
            description_html = job_body.decode_contents()

        apply_links = []
        for anchor in soup.select("div.apply-buttons a"):
            href = (anchor.get("href") or "").strip()
            if href:
                apply_links.append(href)

        return {
            "description_html": description_html,
            "job_number": job_number,
            "job_header_title": job_header_title,
            "job_header_location": job_header_location,
            "apply_links": apply_links,
        }


def store_listing(listing: JobListing) -> None:
    metadata = dict(listing.metadata)
    if listing.description_html:
        metadata.setdefault("description_html", listing.description_html)

    location_value = _normalize_whitespace(listing.location)
    defaults = {
        "title": listing.title[:255],
        "location": location_value[:255] if location_value else None,
        "date": "",
        "description": listing.description_text[:10000],
        "metadata": metadata,
    }
    try:
        JobPosting.objects.update_or_create(
            scraper=SCRAPER,
            link=listing.link,
            defaults=defaults,
        )
    except IntegrityError as exc:
        raise ScraperError(f"Failed to persist job at {listing.link}: {exc}") from exc


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Globe Life careers manual scraper")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of job records to process")
    parser.add_argument("--delay", type=float, default=0.0, help="Delay in seconds between job detail fetches")
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity",
    )
    return parser.parse_args(argv)


def run_scrape(args: argparse.Namespace) -> Dict[str, object]:
    client = GlobeLifeClient(delay=args.delay)
    processed = 0
    for listing in client.iter_listings(limit=args.limit):
        store_listing(listing)
        processed += 1
    dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
    return {"processed_jobs": processed, "deduplicated": dedupe_summary}


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    start = time.time()
    try:
        outcome = run_scrape(args)
    except ScraperError as exc:
        logging.error("Scrape failed: %s", exc)
        return 1
    duration = time.time() - start
    summary = {
        "company": "Globe Life",
        "site": ROOT_LANDING_URL,
        "elapsed_seconds": round(duration, 2),
        **outcome,
    }
    logging.info("Summary: %s", json.dumps(summary))
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
