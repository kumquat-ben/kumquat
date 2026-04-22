#!/usr/bin/env python3
"""Custom scraper for https://www.ycombinator.com/companies/voltair/jobs."""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Generator, Iterable, List, Optional
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
COMPANY_NAME = "Voltair"
JOBS_URL = "https://www.ycombinator.com/companies/voltair/jobs"
BASE_URL = "https://www.ycombinator.com"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": JOBS_URL,
}

REQUEST_TIMEOUT = (15, 45)
DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 30)

SCRAPER_QS = Scraper.objects.filter(company=COMPANY_NAME, url=JOBS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Voltair scrapers found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company=COMPANY_NAME,
        url=JOBS_URL,
        code="custom-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the scraper cannot continue."""


@dataclass
class JobSummary:
    title: str
    detail_url: str
    location: Optional[str]
    compensation: Optional[str]
    equity: Optional[str]
    experience: Optional[str]
    summary_items: List[str]


@dataclass
class JobListing(JobSummary):
    description_text: Optional[str]
    description_html: Optional[str]
    metadata: Dict[str, object]


class VoltairJobScraper:
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
            detail = self._fetch_job_detail(summary.detail_url)
            metadata = {
                **detail["metadata"],
                "summary_items": summary.summary_items,
                "summary_location": summary.location,
                "summary_compensation": summary.compensation,
                "summary_equity": summary.equity,
                "summary_experience": summary.experience,
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
        response = self.session.get(JOBS_URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        rows = soup.select("div.flex.w-full.flex-row.items-start.justify-between.py-4")
        if not rows:
            rows = _fallback_job_rows(soup)

        if not rows:
            raise ScraperError("No job listings found on the Voltair jobs page.")

        for row in rows:
            link = row.find("a", href=True)
            if not link:
                continue
            detail_url = urljoin(BASE_URL, link["href"].strip())
            title = link.get_text(" ", strip=True)
            meta_items = _extract_meta_items(row)
            location = meta_items[0] if meta_items else None
            compensation = _first_currency_item(meta_items)
            equity = _first_equity_item(meta_items)
            experience = _first_experience_item(meta_items)

            yield JobSummary(
                title=title,
                detail_url=detail_url,
                location=location,
                compensation=compensation,
                equity=equity,
                experience=experience,
                summary_items=meta_items,
            )

    def _fetch_job_detail(self, url: str) -> Dict[str, Optional[object]]:
        response = self.session.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        h1 = soup.find("h1")
        if not h1:
            raise ScraperError(f"Missing job title on detail page: {url}")

        header_container = h1.parent
        summary_line = _extract_summary_line(header_container)
        summary_items = _split_summary_items(summary_line)
        detail_meta = _extract_detail_meta(header_container)

        section = h1.find_parent("section") or soup
        description_text, description_html = _extract_description(section)

        metadata = {
            "detail_url": url,
            "header_summary_items": summary_items,
            "detail_meta": detail_meta,
        }

        return {
            "description_text": description_text,
            "description_html": description_html,
            "metadata": metadata,
            "location": summary_items[-1] if summary_items else None,
            "compensation": _first_currency_item(summary_items),
            "equity": _first_equity_item(summary_items),
            "experience": detail_meta.get("Experience"),
        }


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _fallback_job_rows(soup: BeautifulSoup) -> List[object]:
    rows = []
    for link in soup.find_all("a", href=True):
        if "/companies/voltair/jobs/" not in link["href"]:
            continue
        row = link.find_parent("div")
        if row:
            rows.append(row)
    return rows


def _extract_meta_items(row: object) -> List[str]:
    meta = row.select_one("div.justify-left.flex.flex-row.flex-wrap.gap-x-2.gap-y-0.pr-2")
    if not meta:
        return []
    items = []
    for child in meta.find_all("div", recursive=False):
        text = _text_or_none(child)
        if text:
            items.append(text)
    return items


def _extract_summary_line(container: object) -> Optional[str]:
    if not container:
        return None
    line = container.find(
        "div",
        class_=lambda c: _class_contains(c, "flex", "flex-wrap", "items-center", "text-base"),
    )
    return _text_or_none(line)


def _split_summary_items(line: Optional[str]) -> List[str]:
    if not line:
        return []
    bullet = "\u2022"
    return [item.strip() for item in line.split(bullet) if item.strip()]


def _extract_detail_meta(container: object) -> Dict[str, str]:
    meta_container = None
    for div in container.find_all("div", recursive=False):
        if div.find("strong"):
            meta_container = div
            break
    if not meta_container:
        return {}
    meta: Dict[str, str] = {}
    for strong in meta_container.find_all("strong"):
        label = _text_or_none(strong)
        if not label:
            continue
        value_tag = strong.parent.find_next_sibling()
        value = _text_or_none(value_tag)
        if value:
            meta[label] = value
    return meta


def _extract_description(section: object) -> tuple[Optional[str], Optional[str]]:
    prose_blocks = list(section.select(".prose"))
    if not prose_blocks:
        return None, None

    used = set()
    text_blocks: List[str] = []
    html_blocks: List[str] = []

    for heading in section.find_all(["h2", "h3"]):
        heading_text = _text_or_none(heading)
        prose = heading.find_next(class_="prose")
        if not prose or prose in used:
            continue
        used.add(prose)
        prose_text = prose.get_text("\n", strip=True)
        if heading_text:
            text_blocks.append(f"{heading_text}\n{prose_text}")
            html_blocks.append(f"<h3>{heading_text}</h3>\n{prose}")
        else:
            text_blocks.append(prose_text)
            html_blocks.append(str(prose))

    if not text_blocks:
        for block in prose_blocks:
            text = block.get_text("\n", strip=True)
            if text:
                text_blocks.append(text)
                html_blocks.append(str(block))

    return "\n\n".join(text_blocks), "\n".join(html_blocks)


def _first_currency_item(items: List[str]) -> Optional[str]:
    for item in items:
        if "$" in item or "£" in item or "€" in item:
            return item
    return None


def _first_equity_item(items: List[str]) -> Optional[str]:
    for item in items:
        if "%" in item and "year" not in item.lower():
            return item
    return None


def _first_experience_item(items: List[str]) -> Optional[str]:
    for item in items:
        lower = item.lower()
        if "year" in lower or "years" in lower or "sophomore" in lower:
            return item
    return None


def _class_contains(value: Optional[object], *required: str) -> bool:
    if not value:
        return False
    if isinstance(value, str):
        classes = value.split()
    else:
        classes = list(value)
    return all(cls in classes for cls in required)


def _text_or_none(element: Optional[object]) -> Optional[str]:
    if not element:
        return None
    text = element.get_text(" ", strip=True)
    return text or None


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
    scraper = VoltairJobScraper(delay=delay)
    count = 0
    for job in scraper.scrape(limit=limit):
        store_listing(job)
        count += 1
    return count


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Voltair jobs scraper")
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
        "url": JOBS_URL,
        "count": count,
        "elapsed_seconds": duration,
        "dedupe": dedupe_summary,
    }
    logging.info("Summary: %s", json.dumps(summary))
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
