#!/usr/bin/env python3
"""Manual scraper for Cisco careers listings.

This script crawls the public search interface at https://jobs.cisco.com and
persists the resulting jobs via the shared ``JobPosting`` model so operations
staff can trigger it ad hoc from the manual scripts dashboard.
"""
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
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup, Tag

# ---------------------------------------------------------------------------
# Django bootstrap (keeps parity with other manual scripts)
# ---------------------------------------------------------------------------
BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "website.settings")

import django  # noqa: E402

django.setup()

from django.conf import settings  # noqa: E402

from scrapers.models import Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings, persist_job_results  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CAREERS_LANDING_URL = "https://www.cisco.com/c/en/us/about/careers.html"
SEARCH_URL = "https://jobs.cisco.com/jobs/SearchJobs/"
AJAX_TOTAL_URL = "https://jobs.cisco.com/jobs/SearchJobsResultsAJAX/"
DETAIL_LANG_SUFFIX = "?lang=en_us"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": SEARCH_URL,
}
REQUEST_TIMEOUT = (10, 30)
PAGE_SIZE = 25

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 600), 60)
SCRAPER_QS = Scraper.objects.filter(company="Cisco", url=CAREERS_LANDING_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.getLogger(__name__).warning(
            "Multiple Scraper rows matched Cisco; using id=%s", SCRAPER.id
        )
else:
    SCRAPER = Scraper.objects.create(
        company="Cisco",
        url=CAREERS_LANDING_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the scraper encounters an unrecoverable error."""


@dataclass
class CiscoJob:
    job_id: str
    title: str
    link: str
    location: str
    actions: Optional[str]
    area_of_interest: Optional[str]
    alternate_location: Optional[str]
    date_posted: Optional[str] = None
    description: Optional[str] = None
    compensation_range: Optional[str] = None
    job_type: Optional[str] = None
    technology_interest: Optional[str] = None

    def to_payload(self) -> Dict[str, object]:
        metadata: Dict[str, object] = {
            "job_id": self.job_id,
            "actions": self.actions,
            "area_of_interest": self.area_of_interest,
            "alternate_location": self.alternate_location,
            "compensation_range": self.compensation_range,
            "job_type": self.job_type,
            "technology_interest": self.technology_interest,
        }
        metadata = {k: v for k, v in metadata.items() if v}
        return {
            "title": self.title[:255],
            "location": self.location[:255],
            "date": (self.date_posted or "")[:100],
            "link": self.link,
            "description": (self.description or "").strip(),
            "metadata": metadata or None,
        }


class CiscoCareersScraper:
    def __init__(
        self,
        *,
        session: Optional[requests.Session] = None,
        delay: float = 0.25,
    ) -> None:
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.delay = max(0.0, delay)
        self.logger = logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------------ #
    # Public API                                                         #
    # ------------------------------------------------------------------ #
    def scrape(
        self,
        *,
        max_pages: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Iterable[CiscoJob]:
        total = self._fetch_total_results()
        if total is None:
            self.logger.warning("Could not determine total job count; falling back to on-the-fly iteration.")
            total = float("inf")

        yielded = 0
        offset = 0
        page_index = 0

        while offset < total:
            if max_pages is not None and page_index >= max_pages:
                break

            listings = self._fetch_listing_page(offset)
            if not listings:
                break

            for job in listings:
                try:
                    self._enrich_job(job)
                except ScraperError as exc:
                    self.logger.error("Skipping %s due to detail error: %s", job.link, exc)
                    continue
                yield job
                yielded += 1
                if limit is not None and yielded >= limit:
                    return
                if self.delay:
                    time.sleep(self.delay)

            offset += PAGE_SIZE
            page_index += 1
            if self.delay:
                time.sleep(self.delay)

    # ------------------------------------------------------------------ #
    # Listing parsing                                                    #
    # ------------------------------------------------------------------ #
    def _fetch_total_results(self) -> Optional[int]:
        params = {"listFilterMode": "true"}
        try:
            resp = self.session.get(AJAX_TOTAL_URL, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as exc:
            self.logger.error("Failed to fetch total results: %s", exc)
            return None

        text = resp.text.strip()
        try:
            return int(text)
        except ValueError:
            self.logger.debug("Unexpected total count payload: %r", text)
            return None

    def _fetch_listing_page(self, offset: int) -> List[CiscoJob]:
        params = {
            "listFilterMode": "1",
            "search": "",
            "jobOffset": str(offset),
        }
        try:
            resp = self.session.get(SEARCH_URL, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch listing offset={offset}: {exc}") from exc

        soup = BeautifulSoup(resp.text, "html.parser")
        table = self._locate_results_table(soup)
        if not table:
            self.logger.warning("No results table found at offset=%s.", offset)
            return []

        jobs: List[CiscoJob] = []
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if not cells:
                continue
            try:
                job = self._parse_listing_row(cells)
            except ScraperError as exc:
                self.logger.error("Failed to parse row: %s", exc)
                continue
            jobs.append(job)
        return jobs

    @staticmethod
    def _locate_results_table(soup: BeautifulSoup) -> Optional[Tag]:
        for table in soup.find_all("table"):
            header = table.find("th")
            if header and "Job Title" in header.get_text():
                return table
        return None

    @staticmethod
    def _parse_listing_row(cells: List[Tag]) -> CiscoJob:
        link_tag = cells[0].find("a")
        if not link_tag or not link_tag.get("href"):
            raise ScraperError("Missing job link in listing row.")

        raw_title = link_tag.get_text(" ", strip=True)
        title = raw_title
        job_id = CiscoCareersScraper._extract_job_id(link_tag["href"])
        if job_id and raw_title.startswith(job_id):
            title = raw_title[len(job_id):].strip(" -")

        if len(cells) < 4:
            raise ScraperError("Listing row did not contain expected columns.")

        location = cells[3].get_text(" ", strip=True)
        actions = cells[1].get_text(" ", strip=True) or None
        area_of_interest = cells[2].get_text(" ", strip=True) or None
        alternate_location = ""
        if len(cells) > 4:
            alternate_location = cells[4].get_text(" ", strip=True)

        return CiscoJob(
            job_id=job_id or "",
            title=title or raw_title,
            link=link_tag["href"],
            location=location,
            actions=actions or None,
            area_of_interest=area_of_interest,
            alternate_location=alternate_location or None,
        )

    @staticmethod
    def _extract_job_id(url: str) -> Optional[str]:
        parsed = urlparse(url)
        segments = [segment for segment in parsed.path.split("/") if segment]
        if not segments:
            return None
        candidate = segments[-1]
        return candidate if candidate.isdigit() else None

    # ------------------------------------------------------------------ #
    # Detail enrichment                                                  #
    # ------------------------------------------------------------------ #
    def _enrich_job(self, job: CiscoJob) -> None:
        detail_url = job.link
        if "?" in detail_url:
            detail_url = f"{detail_url}&{DETAIL_LANG_SUFFIX.lstrip('?')}"
        else:
            detail_url = f"{detail_url}{DETAIL_LANG_SUFFIX}"

        try:
            resp = self.session.get(detail_url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch detail for {job.link}: {exc}") from exc

        if resp.url.rstrip("/").endswith("/jobs/Error"):
            raise ScraperError("Detail page redirected to error screen.")

        soup = BeautifulSoup(resp.text, "html.parser")
        self._extract_description(job, soup)
        self._extract_metadata(job, soup)
        self._extract_date(job, soup)

    @staticmethod
    def _extract_description(job: CiscoJob, soup: BeautifulSoup) -> None:
        container = soup.select_one(".job_description") or soup.select_one("#job_description")
        if not container:
            raise ScraperError("Job description container not found.")
        text = container.get_text("\n", strip=True)
        if not text:
            raise ScraperError("Job description was empty.")
        job.description = text

    @staticmethod
    def _extract_metadata(job: CiscoJob, soup: BeautifulSoup) -> None:
        label_map = {
            "Compensation Range": "compensation_range",
            "Job Type": "job_type",
            "Technology Interest": "technology_interest",
            "Area of Interest": "area_of_interest",
            "Location:": "location",
            "Alternate Location": "alternate_location",
            "Job Id": "job_id",
            "Actions": "actions",
        }

        for wrapper in soup.select(".fields-data_list li.fields-data_item"):
            label_el = wrapper.select_one(".fields-data_label")
            value_el = wrapper.select_one(".fields-data_value")
            if not label_el or not value_el:
                continue
            label = label_el.get_text(strip=True)
            value = value_el.get_text(" ", strip=True)
            field = label_map.get(label)
            if not field or not value:
                continue
            if field == "location":
                job.location = value
            elif field == "alternate_location":
                job.alternate_location = value or job.alternate_location
            elif field == "actions":
                job.actions = value or job.actions
            elif field == "area_of_interest":
                job.area_of_interest = value or job.area_of_interest
            elif field == "job_id":
                job.job_id = value or job.job_id
            else:
                setattr(job, field, value)

    @staticmethod
    def _extract_date(job: CiscoJob, soup: BeautifulSoup) -> None:
        ld_script = soup.find("script", {"type": "application/ld+json"})
        if not ld_script or not ld_script.string:
            return
        try:
            data = json.loads(ld_script.string)
        except json.JSONDecodeError:
            return
        date_posted = data.get("datePosted")
        if date_posted:
            job.date_posted = date_posted


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Cisco careers listings.")
    parser.add_argument("--max-pages", type=int, help="Maximum listing pages to crawl.")
    parser.add_argument("--limit", type=int, help="Maximum number of jobs to ingest.")
    parser.add_argument("--delay", type=float, default=0.25, help="Delay between requests in seconds.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch data but do not persist to the database.")
    parser.add_argument("--dedupe", action="store_true", help="Run deduplication after ingest.")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    logger = logging.getLogger("cisco_manual")

    scraper = CiscoCareersScraper(delay=args.delay)

    jobs: List[CiscoJob] = []
    try:
        for job in scraper.scrape(max_pages=args.max_pages, limit=args.limit):
            jobs.append(job)
    except ScraperError as exc:
        logger.error("Scraper failed: %s", exc)
        return 1

    if not jobs:
        logger.warning("No Cisco jobs collected; aborting.")
        return 1

    payload = {"jobs": [job.to_payload() for job in jobs]}
    logger.info("Collected %s Cisco jobs.", len(jobs))

    if args.dry_run:
        logger.info("Dry run requested; not persisting results.")
    else:
        summary = persist_job_results(SCRAPER, payload)
        logger.info("Persisted jobs: %s", summary)
        if args.dedupe:
            result = deduplicate_job_postings(scraper=SCRAPER, dry_run=False)
            logger.info("Deduplication result: %s", result)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
