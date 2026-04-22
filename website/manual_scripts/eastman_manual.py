#!/usr/bin/env python3
"""Manual scraper for Eastman careers (SuccessFactors platform).

This script pulls job listings from Eastman's public SuccessFactors portal and
persists them into the shared ``JobPosting`` table that backs the Eastman
``Scraper`` row.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple
from urllib.parse import urlencode, urljoin, urlparse

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

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_URL = "https://jobs.eastman.com"
LISTING_PATH = "/search/"
LISTING_URL = f"{BASE_URL}{LISTING_PATH}"
DEFAULT_PARAMS = {
    "createNewAlert": "false",
    "q": "",
    "locationsearch": "",
    "sortColumn": "referencedate",
    "sortDirection": "desc",
}
REQUEST_TIMEOUT = 40

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
}

# Ensure a single scraper entry exists for Eastman.
SCRAPER_QS = Scraper.objects.filter(company="Eastman", url=LISTING_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.getLogger(__name__).warning(
            "Multiple Scraper rows matched Eastman careers; using id=%s", SCRAPER.id
        )
else:
    SCRAPER = Scraper.objects.create(
        company="Eastman",
        url=LISTING_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=300,
    )


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
@dataclass
class JobSummary:
    title: str
    link: str
    location: Optional[str]
    date: Optional[str]
    job_id: Optional[str]


@dataclass
class JobDetail(JobSummary):
    description: str
    date_posted: Optional[str]
    apply_url: Optional[str]
    requisition_id: Optional[str]
    location_meta: Dict[str, str]


class EastmanScraperError(Exception):
    """Raised when the Eastman scraper encounters an unrecoverable error."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _normalize_whitespace(text: str) -> str:
    text = (text or "").replace("\r", "\n").replace("\xa0", " ")
    text = text.replace("\u2013", "-").replace("\u2014", "-").replace("\u2022", "-")
    text = re.sub(r"[ \t]+", " ", text)
    lines = [line.strip() for line in text.splitlines()]

    collapsed: List[str] = []
    blank_streak = 0
    for line in lines:
        if not line:
            if blank_streak == 0 and collapsed:
                collapsed.append("")
            blank_streak += 1
            continue
        collapsed.append(line)
        blank_streak = 0

    return "\n".join(collapsed).strip()


def _extract_job_id_from_url(url: str) -> Optional[str]:
    parsed = urlparse(url)
    match = re.search(r"/(\d+)/?$", parsed.path or "")
    if match:
        return match.group(1)
    return None


def _safe_text(node: Optional[BeautifulSoup], *, separator: str = " ") -> Optional[str]:
    if not node:
        return None
    return _normalize_whitespace(node.get_text(separator, strip=True))


# ---------------------------------------------------------------------------
# Scraper implementation
# ---------------------------------------------------------------------------
class EastmanJobScraper:
    def __init__(
        self,
        *,
        query: str = "",
        location: str = "",
        delay: float = 0.4,
        detail_delay: Optional[float] = None,
        session: Optional[requests.Session] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.params = dict(DEFAULT_PARAMS)
        if query:
            self.params["q"] = query
        if location:
            self.params["locationsearch"] = location

        self.delay = max(0.0, delay)
        self.detail_delay = detail_delay if detail_delay is not None else min(self.delay, 0.4)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------
    def scrape(self, *, limit: Optional[int] = None) -> Iterator[JobDetail]:
        fetched = 0
        seen_links: set[str] = set()
        start_row = 0
        total_results: Optional[int] = None
        page_size_hint: Optional[int] = None

        while True:
            html = self._fetch_listing_page(start_row=start_row)
            summaries, page_size, total = self._parse_listing_page(html)
            if not summaries:
                self.logger.debug("No job rows found at startrow=%s, stopping.", start_row)
                break

            if total_results is None and total:
                total_results = total
                self.logger.debug("Total results reported: %s", total_results)

            if page_size_hint is None and page_size:
                page_size_hint = page_size

            for summary in summaries:
                if summary.link in seen_links:
                    continue
                try:
                    detail = self._enrich_summary(summary)
                except EastmanScraperError as exc:
                    self.logger.warning("Failed to enrich %s: %s", summary.link, exc)
                    continue

                seen_links.add(summary.link)
                fetched += 1
                yield detail

                if limit is not None and fetched >= limit:
                    return

            if total_results is not None and page_size_hint:
                if start_row + page_size_hint >= total_results:
                    break
                start_row += page_size_hint
            else:
                start_row += len(summaries)

            if self.delay:
                time.sleep(self.delay)

    # ------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------
    def _fetch_listing_page(self, *, start_row: int) -> str:
        params = dict(self.params)
        if start_row:
            params["startrow"] = str(start_row)
        url = f"{BASE_URL}{LISTING_PATH}"

        self.logger.debug("Fetching listings: %s?%s", url, urlencode(params))
        response = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.text

    def _parse_listing_page(self, html_text: str) -> Tuple[List[JobSummary], Optional[int], Optional[int]]:
        soup = BeautifulSoup(html_text, "html.parser")
        rows = soup.select("tr.data-row")

        summaries: List[JobSummary] = []
        for row in rows:
            link_tag = row.select_one("a.jobTitle-link")
            if not link_tag or not link_tag.get("href"):
                continue
            title = _safe_text(link_tag, separator=" ")
            if not title:
                continue

            detail_url = urljoin(BASE_URL, link_tag["href"])
            location_tag = row.select_one("span.jobLocation")
            if location_tag:
                for extra in location_tag.select("small"):
                    extra.decompose()
            location = _safe_text(location_tag, separator=" ") if location_tag else None

            date_tag = row.select_one("span.jobDate")
            date_value = _safe_text(date_tag, separator=" ") if date_tag else None

            summaries.append(
                JobSummary(
                    title=title,
                    link=detail_url,
                    location=location,
                    date=date_value,
                    job_id=_extract_job_id_from_url(detail_url),
                )
            )

        table = soup.select_one("table#searchresults")
        total_results: Optional[int] = None
        page_size: Optional[int] = None
        if table and table.has_attr("aria-label"):
            label = table["aria-label"].replace("\u2013", "-")
            match = re.search(r"Results\s+(\d+)[\s-]+(\d+)\s+of\s+(\d+)", label)
            if match:
                start = int(match.group(1))
                end = int(match.group(2))
                total_results = int(match.group(3))
                if end >= start:
                    page_size = end - start + 1

        if page_size is None and summaries:
            page_size = len(summaries)

        return summaries, page_size, total_results

    def _enrich_summary(self, summary: JobSummary) -> JobDetail:
        self.logger.debug("Fetching job detail: %s", summary.link)
        response = self.session.get(summary.link, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        if self.detail_delay:
            time.sleep(self.detail_delay)

        soup = BeautifulSoup(response.text, "html.parser")

        description_node = soup.select_one("span.jobdescription")
        if not description_node:
            raise EastmanScraperError("Job description container missing.")
        description_text = _normalize_whitespace(description_node.get_text("\n", strip=True))

        apply_tag = soup.select_one("a.dialogApplyBtn")
        apply_url = None
        if apply_tag and apply_tag.get("href"):
            apply_url = urljoin(BASE_URL, apply_tag["href"])

        requisition_node = soup.select_one('[data-careersite-propertyid="customfield1"]')
        requisition_id = _safe_text(requisition_node, separator=" ")

        date_posted = None
        posted_meta = soup.select_one('meta[itemprop="datePosted"]')
        if posted_meta and posted_meta.get("content"):
            raw_date = posted_meta["content"].strip()
            parsed_date = _parse_date(raw_date)
            date_posted = parsed_date or raw_date

        location_meta = _extract_location_meta(soup)

        return JobDetail(
            title=summary.title,
            link=summary.link,
            location=summary.location,
            date=summary.date,
            job_id=summary.job_id,
            description=description_text,
            date_posted=date_posted,
            apply_url=apply_url,
            requisition_id=requisition_id,
            location_meta=location_meta,
        )


def _parse_date(raw: str) -> Optional[str]:
    """
    Convert SuccessFactors date strings like ``Fri Oct 10 07:00:00 UTC 2025`` into YYYY-MM-DD.
    """
    patterns = [
        "%a %b %d %H:%M:%S %Z %Y",
        "%a %b %d %H:%M:%S %z %Y",
        "%Y-%m-%d",
    ]
    for pattern in patterns:
        try:
            parsed = datetime.strptime(raw, pattern)
            return parsed.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _extract_location_meta(soup: BeautifulSoup) -> Dict[str, str]:
    meta: Dict[str, str] = {}
    address_node = soup.select_one('[itemprop="jobLocation"] [itemprop="address"]')
    if not address_node:
        return meta
    for key in ("addressLocality", "addressRegion", "postalCode", "addressCountry"):
        element = address_node.find("meta", attrs={"itemprop": key})
        if element and element.get("content"):
            meta[key] = element["content"].strip()
    return meta


def persist_job(detail: JobDetail) -> bool:
    description = detail.description[:10000]
    date_value = detail.date_posted or detail.date

    metadata_payload = {
        "job_id": detail.job_id,
        "requisition_id": detail.requisition_id,
        "date_posted": detail.date_posted,
        "apply_url": detail.apply_url,
    }
    metadata_payload = {key: value for key, value in metadata_payload.items() if value}
    if detail.location_meta:
        metadata_payload["location_meta"] = detail.location_meta

    defaults = {
        "title": detail.title[:255],
        "location": (detail.location or "")[:255] or None,
        "date": (date_value or "")[:100] or None,
        "description": description,
        "metadata": metadata_payload,
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=detail.link,
        defaults=defaults,
    )
    logging.getLogger("persist").debug("Persisted job %s (created=%s, id=%s)", obj.link, created, obj.id)
    return created


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape and persist Eastman job listings.")
    parser.add_argument("--query", default="", help="Keyword query (default: empty).")
    parser.add_argument("--location", default="", help="Location query (default: empty).")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument("--delay", type=float, default=0.4, help="Delay between listing page requests.")
    parser.add_argument(
        "--detail-delay",
        type=float,
        default=None,
        help="Optional delay (seconds) between detail page requests (default: min(delay, 0.4)).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Fetch jobs and print JSON without touching the database.")
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
    logger = logging.getLogger("eastman")

    scraper = EastmanJobScraper(
        query=args.query,
        location=args.location,
        delay=args.delay,
        detail_delay=args.detail_delay,
        logger=logger,
    )

    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    for job in scraper.scrape(limit=args.limit):
        totals["fetched"] += 1
        if args.dry_run:
            payload = {
                "title": job.title,
                "link": job.link,
                "location": job.location,
                "date": job.date,
                "job_id": job.job_id,
                "date_posted": job.date_posted,
                "requisition_id": job.requisition_id,
                "apply_url": job.apply_url,
                "location_meta": job.location_meta,
                "description": job.description,
            }
            print(json.dumps(payload, ensure_ascii=False))
            continue

        try:
            created = persist_job(job)
            if created:
                totals["created"] += 1
            else:
                totals["updated"] += 1
        except Exception as exc:  # pragma: no cover - persistence error handling
            logger.error("Failed to persist %s: %s", job.link, exc)
            totals["errors"] += 1

    exit_code = 0
    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        logger.info("Deduplication summary: %s", dedupe_summary)
        if totals["errors"]:
            exit_code = 1

    logger.info(
        "Eastman scraper finished - fetched=%(fetched)s created=%(created)s updated=%(updated)s errors=%(errors)s",
        totals,
    )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
