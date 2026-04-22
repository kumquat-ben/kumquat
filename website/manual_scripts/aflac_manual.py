#!/usr/bin/env python3
"""Manual scraper for Aflac's SuccessFactors careers site."""
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
from typing import Dict, Iterable, List, Optional
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
# Constants & configuration
# ---------------------------------------------------------------------------
CAREERS_URL = "https://careers.aflac.com/search/job"
BASE_URL = "https://careers.aflac.com"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": BASE_URL,
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 600), 30)
SCRAPER_QS = Scraper.objects.filter(company="Aflac", url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched Aflac; using id=%s.", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Aflac",
        url=CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the Aflac scraper cannot proceed."""


@dataclass
class JobListing:
    title: str
    link: str
    location: Optional[str]
    date: Optional[str]
    description: str
    apply_url: Optional[str]
    metadata: Dict[str, object]


def _clean(text: Optional[str]) -> str:
    return (text or "").strip()


class AflacJobScraper:
    def __init__(
        self,
        *,
        delay: float = 0.2,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.delay = max(0.0, float(delay))
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

    def scrape(self, *, limit: Optional[int] = None) -> Iterable[JobListing]:
        start_row = 0
        fetched = 0
        total_jobs: Optional[int] = None

        while True:
            soup = self._fetch_search_page(start_row)
            rows = self._parse_rows(soup)
            if not rows:
                logging.info("No listings returned at startrow=%s; stopping.", start_row)
                break

            if total_jobs is None:
                total_jobs = self._extract_total_jobs(soup)
                logging.info("Discovered total jobs=%s at page size %s.", total_jobs, len(rows))

            for summary in rows:
                listing = self._build_listing(summary)
                if listing is None:
                    continue
                yield listing
                fetched += 1
                if limit is not None and fetched >= limit:
                    logging.info("Reached limit=%s; stopping.", limit)
                    return

            start_row += len(rows)
            if total_jobs is not None and start_row >= total_jobs:
                logging.info("Reached reported total jobs=%s; pagination complete.", total_jobs)
                return
            if self.delay:
                time.sleep(self.delay)

    def _fetch_search_page(self, start_row: int) -> BeautifulSoup:
        params = {"startrow": max(0, start_row)} if start_row else None
        try:
            response = self.session.get(CAREERS_URL, params=params, timeout=40)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch search page startrow={start_row}: {exc}") from exc
        return BeautifulSoup(response.text, "html.parser")

    def _extract_total_jobs(self, soup: BeautifulSoup) -> Optional[int]:
        label = soup.select_one("table#searchresults")
        if not label:
            return None
        aria = label.get("aria-label") or ""
        match = re.search(r"of\s+([\d,]+)", aria)
        if not match:
            return None
        try:
            return int(match.group(1).replace(",", ""))
        except ValueError:
            return None

    def _parse_rows(self, soup: BeautifulSoup) -> List[Dict[str, str]]:
        rows: List[Dict[str, str]] = []
        for row in soup.select("table#searchresults tbody tr.data-row"):
            link = row.select_one("a.jobTitle-link")
            if not link:
                continue
            href = link.get("href") or ""
            rows.append(
                {
                    "title": _clean(link.get_text(strip=True)),
                    "href": href,
                    "location": _clean(row.select_one(".colLocation").get_text(strip=True) if row.select_one(".colLocation") else ""),
                    "date": _clean(row.select_one(".colDate").get_text(strip=True) if row.select_one(".colDate") else ""),
                }
            )
        return rows

    def _fetch_job_detail(self, href: str) -> BeautifulSoup:
        url = urljoin(BASE_URL, href)
        try:
            response = self.session.get(url, timeout=40)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch job detail {url}: {exc}") from exc
        return BeautifulSoup(response.text, "html.parser")

    def _build_listing(self, summary: Dict[str, str]) -> Optional[JobListing]:
        title = summary.get("title") or ""
        href = summary.get("href") or ""
        if not title or not href:
            return None

        soup = self._fetch_job_detail(href)
        description_node = soup.select_one("span.jobdescription")
        apply_link = soup.select_one("a.apply")
        meta_date = soup.select_one('meta[itemprop="datePosted"]')

        return JobListing(
            title=title,
            link=urljoin(BASE_URL, href),
            location=summary.get("location") or None,
            date=meta_date.get("content") if meta_date and meta_date.has_attr("content") else summary.get("date"),
            description=_clean(description_node.get_text(" ", strip=True) if description_node else ""),
            apply_url=urljoin(BASE_URL, apply_link.get("href")) if apply_link and apply_link.get("href") else None,
            metadata={"source_list_date": summary.get("date")},
        )


def persist_listing(listing: JobListing) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255] or None,
        "date": (listing.date or "")[:100] or None,
        "description": listing.description[:10000],
        "metadata": {
            **listing.metadata,
            "apply_url": listing.apply_url,
        },
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.link,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Persisted Aflac job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Aflac (SuccessFactors) job listings.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum jobs to process.")
    parser.add_argument(
        "--delay",
        type=float,
        default=0.2,
        help="Seconds to sleep between requests (default: 0.2).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Output jobs instead of saving.")
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

    scraper = AflacJobScraper(delay=args.delay)
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

    logging.info("Aflac scraper finished - fetched=%(fetched)s created=%(created)s", totals)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
