#!/usr/bin/env python3
"""Manual scraper for https://jobs.dropbox.com (open positions page)."""
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
COMPANY_NAME = "Dropbox"
CAREERS_URL = "https://jobs.dropbox.com"
JOB_LIST_URL = "https://jobs.dropbox.com/all-jobs"

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
REQUEST_TIMEOUT = (10, 40)

SCRAPER_QS = Scraper.objects.filter(company=COMPANY_NAME, url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Dropbox scrapers found; using id=%s", SCRAPER.id)
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
class JobSummary:
    posting_id: str
    title: str
    detail_url: str
    team: Optional[str]
    team_url: Optional[str]
    team_description: Optional[str]
    location: Optional[str]


@dataclass
class JobListing(JobSummary):
    description_text: Optional[str]
    description_html: Optional[str]
    metadata: Dict[str, object]


class DropboxCareersScraper:
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
            detail = self._fetch_job_detail(summary)
            listing = JobListing(**asdict(summary), **detail)
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
        response = self.session.get(JOB_LIST_URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        groups = soup.select("div.open-positions__listing-group")
        if not groups:
            raise ScraperError("No job listing groups found on Dropbox jobs page.")

        seen_urls = set()
        for group in groups:
            team_name = _text_or_none(group.select_one("h3.open-positions__dept-title"))
            team_link = group.select_one("h3.open-positions__dept-title a")
            team_url = team_link["href"].strip() if team_link and team_link.get("href") else None
            team_description = _text_or_none(group.select_one("h5.open-positions__listing-discription"))

            listings = group.select("li.open-positions__listing")
            for listing in listings:
                link = listing.select_one("a.open-positions__listing-link")
                detail_url = link["href"].strip() if link and link.get("href") else None
                if not detail_url:
                    continue
                if detail_url in seen_urls:
                    continue
                seen_urls.add(detail_url)

                title = _text_or_none(listing.select_one("h5.open-positions__listing-title"))
                if not title and link:
                    title = _text_or_none(link)
                if not title:
                    continue

                location = _text_or_none(listing.select_one("p.open-positions__listing-location"))
                if not location:
                    location = listing.get("data-location")

                team_value = listing.get("data-team") or team_name

                posting_id = _extract_posting_id(detail_url)
                if not posting_id:
                    continue

                yield JobSummary(
                    posting_id=posting_id,
                    title=title,
                    detail_url=detail_url,
                    team=team_value,
                    team_url=team_url,
                    team_description=team_description,
                    location=location,
                )

    def _fetch_job_detail(self, summary: JobSummary) -> Dict[str, Optional[object]]:
        response = self.session.get(summary.detail_url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        title = _text_or_none(soup.select_one("h1.jc01-title__title")) or summary.title
        location = _text_or_none(soup.select_one("h2.jc01-title__subtitle")) or summary.location

        content = soup.select_one("div.jc03-content")
        description_html = _inner_html(content) if content else None
        description_text = _html_to_text(description_html)
        if not description_text and content:
            description_text = content.get_text("\n", strip=True)

        apply_link = soup.find("a", href=re.compile(r"/apply/?$"))
        apply_url = apply_link["href"].strip() if apply_link and apply_link.get("href") else None
        section_headings = [heading.get_text(strip=True) for heading in soup.select("div.jc03-content h2")]

        metadata = {
            "posting_id": summary.posting_id,
            "team": summary.team,
            "team_url": summary.team_url,
            "team_description": summary.team_description,
            "location": location,
            "apply_url": apply_url,
            "section_headings": section_headings,
            "job_page": summary.detail_url,
        }

        return {
            "title": title,
            "location": location,
            "description_text": description_text,
            "description_html": description_html,
            "metadata": {key: value for key, value in metadata.items() if value not in (None, "", [])},
        }


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------
def _extract_posting_id(url: str) -> Optional[str]:
    match = re.search(r"/listing/(\\d+)", url)
    return match.group(1) if match else None


def _text_or_none(node: Optional[BeautifulSoup]) -> Optional[str]:
    if not node:
        return None
    text = node.get_text(" ", strip=True)
    return text or None


def _html_to_text(value: Optional[str]) -> str:
    if not value:
        return ""
    soup = BeautifulSoup(value, "html.parser")
    extracted = soup.get_text("\n", strip=True)
    lines = [line.strip() for line in extracted.splitlines()]
    return "\n".join(line for line in lines if line)


def _inner_html(node: Optional[BeautifulSoup]) -> Optional[str]:
    if not node:
        return None
    contents = [str(child) for child in node.contents]
    payload = "".join(contents).strip()
    return payload or None


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
    scraper = DropboxCareersScraper(delay=delay)
    count = 0
    for job in scraper.scrape(limit=limit):
        store_listing(job)
        count += 1
    return count


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dropbox careers scraper")
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
