#!/usr/bin/env python3
"""Manual scraper for https://stripe.com/jobs."""
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
from typing import Dict, Iterator, List, Optional, Tuple

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
CAREERS_URL = "https://stripe.com/jobs"
SEARCH_URL = "https://stripe.com/jobs/search"
COMPANY_NAME = "Stripe"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

JSON_HEADERS = {
    **DEFAULT_HEADERS,
    "Accept": "application/json",
}

REQUEST_TIMEOUT = (10, 30)
DEFAULT_DELAY = 0.25
DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 1800), 120)

SCRAPER_QS = Scraper.objects.filter(company=COMPANY_NAME, url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Stripe scraper rows found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company=COMPANY_NAME,
        url=CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
@dataclass
class JobSummary:
    job_id: Optional[str]
    job_slug: Optional[str]
    title: str
    detail_url: str
    departments: List[str]
    location: Optional[str]
    location_country_code: Optional[str]


@dataclass
class JobListing(JobSummary):
    description_text: Optional[str]
    description_html: Optional[str]
    metadata: Dict[str, object]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _normalize_text(text: str) -> str:
    lines = [line.strip() for line in text.splitlines()]
    return "\n".join(line for line in lines if line)


def _extract_job_id_and_slug(path: str) -> Tuple[Optional[str], Optional[str]]:
    match = re.search(r"/jobs/listing/([^/]+)/([0-9]+)", path)
    if not match:
        return None, None
    return match.group(2), match.group(1)


def _parse_listing_html(html: str) -> List[JobSummary]:
    soup = BeautifulSoup(html, "html.parser")
    listings: List[JobSummary] = []
    for row in soup.select("tr"):
        link = row.select_one("a.JobsListings__link")
        if not link:
            continue
        title = link.get_text(strip=True)
        href = link.get("href") or ""
        if not title or not href:
            continue
        job_id, job_slug = _extract_job_id_and_slug(href)
        departments = [
            item.get_text(strip=True)
            for item in row.select(".JobsListings__departmentsListItem")
            if item.get_text(strip=True)
        ]
        location_el = row.select_one(".JobsListings__locationDisplayName")
        location = location_el.get_text(strip=True) if location_el else None
        flag_img = row.select_one(".JobsListings__tableCell--country img")
        country_code = flag_img.get("alt") if flag_img else None
        detail_url = href if href.startswith("http") else f"https://stripe.com{href}"
        listings.append(
            JobSummary(
                job_id=job_id,
                job_slug=job_slug,
                title=title,
                detail_url=detail_url,
                departments=departments,
                location=location,
                location_country_code=country_code,
            )
        )
    return listings


def _parse_detail_properties(soup: BeautifulSoup) -> Dict[str, str]:
    properties: Dict[str, str] = {}
    for prop in soup.select(".JobDetailCardProperty"):
        title_el = prop.select_one(".JobDetailCardProperty__title")
        title = title_el.get_text(strip=True) if title_el else None
        if not title:
            continue
        text = prop.get_text("\n", strip=True)
        value = text.replace(title, "", 1).strip() if text else ""
        if value:
            properties[title] = value
    return properties


def _extract_description(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    section = soup.select_one(".JobsBodySection .ArticleMarkdown") or soup.select_one(".JobsBodySection")
    if not section:
        return None, None
    description_html = section.decode_contents()
    description_text = _normalize_text(section.get_text("\n", strip=True))
    return description_text, description_html


# ---------------------------------------------------------------------------
# Scraper implementation
# ---------------------------------------------------------------------------
class StripeJobsScraper:
    def __init__(
        self,
        *,
        session: Optional[requests.Session] = None,
        delay: float = DEFAULT_DELAY,
    ) -> None:
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.delay = max(0.0, delay)
        self.logger = logging.getLogger(self.__class__.__name__)

    def scrape(self, *, limit: Optional[int] = None) -> Iterator[JobListing]:
        skip = 0
        yielded = 0

        while True:
            payload = self._fetch_listing_payload(skip)
            listings = _parse_listing_html(payload.get("html") or "")
            if not listings:
                self.logger.info("No listings returned at skip=%s; stopping.", skip)
                break

            for summary in listings:
                detail = self._fetch_job_detail(summary.detail_url)
                listing = JobListing(**asdict(summary), **detail)
                yield listing
                yielded += 1
                if limit is not None and yielded >= limit:
                    return
                if self.delay:
                    time.sleep(self.delay)

            pagination = payload.get("viewContext", {}).get("pagination") or {}
            items_per_page = pagination.get("items_per_page") or len(listings)
            total = pagination.get("total")
            if total is not None and skip + items_per_page >= total:
                break
            if len(listings) < items_per_page:
                break
            skip += items_per_page

    def _fetch_listing_payload(self, skip: int) -> Dict[str, object]:
        params = {
            "view_type": "list",
            "skip": str(skip),
        }
        response = self.session.get(SEARCH_URL, params=params, headers=JSON_HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()

    def _fetch_job_detail(self, url: str) -> Dict[str, Optional[object]]:
        response = self.session.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        description_text, description_html = _extract_description(soup)
        properties = _parse_detail_properties(soup)
        apply_link = None
        apply_anchor = soup.select_one(".JobsDetailApplyCard a")
        if apply_anchor and apply_anchor.get("href"):
            apply_href = apply_anchor.get("href")
            apply_link = apply_href if apply_href.startswith("http") else f"https://stripe.com{apply_href}"

        metadata: Dict[str, object] = {
            "job_id": properties.get("Job ID"),
            "detail_properties": properties,
            "apply_url": apply_link,
        }
        return {
            "description_text": description_text,
            "description_html": description_html,
            "metadata": {k: v for k, v in metadata.items() if v not in (None, "", {}, [])},
        }


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------
def store_listing(listing: JobListing) -> None:
    metadata = {
        "job_id": listing.job_id,
        "job_slug": listing.job_slug,
        "departments": listing.departments,
        "location_country_code": listing.location_country_code,
        "detail": listing.metadata,
    }
    JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults={
            "title": (listing.title or "")[:255],
            "location": (listing.location or "")[:255],
            "date": "",
            "description": (listing.description_text or listing.description_html or "")[:10000],
            "metadata": {k: v for k, v in metadata.items() if v not in (None, "", {}, [])},
        },
    )


def run_scrape(limit: Optional[int], delay: float) -> int:
    scraper = StripeJobsScraper(delay=delay)
    count = 0
    for job in scraper.scrape(limit=limit):
        store_listing(job)
        count += 1
    return count


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stripe jobs scraper")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY)
    parser.add_argument("--log-level", type=str, default="INFO", choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"])
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    start = time.time()
    count = run_scrape(args.limit, args.delay)
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
