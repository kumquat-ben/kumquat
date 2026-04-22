#!/usr/bin/env python3
"""Manual scraper for Pocket jobs hosted on Notion."""
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
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import requests

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
COMPANY_NAME = "Pocket"
CAREERS_URL = "https://stone-station-c49.notion.site/Jobs-at-Pocket-26a67978de338047b6cbe30fdbc89923"
NOTION_DOMAIN = "https://stone-station-c49.notion.site"
NOTION_API_URL = "https://www.notion.so/api/v3/loadPageChunk"
ROOT_PAGE_ID = "26a67978-de33-8047-b6cb-e30fdbc89923"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}
REQUEST_TIMEOUT = (10, 30)
DEFAULT_DELAY = 0.25
DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 1800), 120)

SCRAPER_QS = Scraper.objects.filter(company=COMPANY_NAME, url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Pocket scraper rows found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company=COMPANY_NAME,
        url=CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


@dataclass
class PocketJob:
    title: str
    link: str
    location: Optional[str]
    description: str
    metadata: Dict[str, object]


class ScraperError(Exception):
    """Raised when the scraper cannot continue."""


# ---------------------------------------------------------------------------
# Notion helpers
# ---------------------------------------------------------------------------
def _normalize_page_id(page_id: str) -> str:
    compact = page_id.replace("-", "")
    if len(compact) != 32:
        raise ScraperError(f"Invalid page id: {page_id}")
    return f"{compact[:8]}-{compact[8:12]}-{compact[12:16]}-{compact[16:20]}-{compact[20:]}"


def _slugify(value: str) -> str:
    ascii_value = value.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^A-Za-z0-9]+", "-", ascii_value).strip("-")
    return slug or "job"


def _build_public_url(title: str, page_id: str) -> str:
    slug = _slugify(title)
    return f"{NOTION_DOMAIN}/{slug}-{page_id.replace('-', '')}"


def _fetch_page_record_map(session: requests.Session, page_id: str) -> Dict[str, Dict[str, dict]]:
    payload = {
        "pageId": _normalize_page_id(page_id),
        "limit": 200,
        "cursor": {"stack": []},
        "chunkNumber": 0,
        "verticalColumns": False,
    }
    response = session.post(NOTION_API_URL, json=payload, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    data = response.json()
    record_map = data.get("recordMap")
    if not record_map:
        raise ScraperError("Missing recordMap in Notion response.")
    return record_map


def _extract_rich_text(rich_text: Optional[Sequence[Sequence[object]]]) -> Tuple[str, List[str]]:
    if not rich_text:
        return "", []
    parts: List[str] = []
    links: List[str] = []
    for segment in rich_text:
        if not segment:
            continue
        text = str(segment[0])
        link = None
        if len(segment) > 1:
            for fmt in segment[1] or []:
                if isinstance(fmt, list) and len(fmt) > 1 and fmt[0] == "a":
                    link = fmt[1]
                    break
        if link:
            links.append(link)
            parts.append(f"{text} ({link})")
        else:
            parts.append(text)
    return "".join(parts).strip(), links


def _extract_block_text(block_value: dict) -> Tuple[str, List[str]]:
    title = block_value.get("properties", {}).get("title")
    return _extract_rich_text(title)


def _extract_ordered_lines(
    record_map: Dict[str, Dict[str, dict]],
    page_id: str,
) -> Tuple[List[str], List[str]]:
    block_map = record_map.get("block") or {}
    page_block = block_map.get(page_id, {}).get("value") or {}
    content_ids = page_block.get("content") or []
    lines: List[str] = []
    links: List[str] = []
    number_counter = 0

    for block_id in content_ids:
        block = block_map.get(block_id, {}).get("value")
        if not block:
            continue
        block_type = block.get("type")
        text, block_links = _extract_block_text(block)
        if not text:
            if block_type != "numbered_list":
                number_counter = 0
            continue
        links.extend(block_links)
        if block_type == "bulleted_list":
            lines.append(f"- {text}")
            number_counter = 0
        elif block_type == "numbered_list":
            number_counter += 1
            lines.append(f"{number_counter}. {text}")
        elif block_type in ("header", "sub_header", "sub_sub_header"):
            lines.append(text)
            number_counter = 0
        elif block_type == "quote":
            lines.append(f"> {text}")
            number_counter = 0
        else:
            lines.append(text)
            number_counter = 0

    return lines, links


def _extract_field(lines: Iterable[str], label: str) -> Optional[str]:
    pattern = re.compile(rf"^{re.escape(label)}\\s*:\\s*(.+)$", re.IGNORECASE)
    for line in lines:
        match = pattern.match(line.strip())
        if match:
            return match.group(1).strip()
    return None


# ---------------------------------------------------------------------------
# Scraper implementation
# ---------------------------------------------------------------------------
class PocketNotionScraper:
    def __init__(self, *, session: Optional[requests.Session] = None, delay: float = DEFAULT_DELAY) -> None:
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.delay = max(0.0, delay)
        self.logger = logging.getLogger(self.__class__.__name__)

    def _fetch_job_pages(self) -> List[Tuple[str, str]]:
        record_map = _fetch_page_record_map(self.session, ROOT_PAGE_ID)
        block_map = record_map.get("block") or {}
        page_block = block_map.get(ROOT_PAGE_ID, {}).get("value") or {}
        content_ids = page_block.get("content") or []
        jobs: List[Tuple[str, str]] = []
        for block_id in content_ids:
            block = block_map.get(block_id, {}).get("value")
            if not block or block.get("type") != "page":
                continue
            title, _ = _extract_block_text(block)
            if not title:
                continue
            jobs.append((block_id, title))
        return jobs

    def scrape(self, *, limit: Optional[int] = None) -> Iterable[PocketJob]:
        job_pages = self._fetch_job_pages()
        self.logger.info("Discovered %s job pages", len(job_pages))
        count = 0
        for page_id, title in job_pages:
            record_map = _fetch_page_record_map(self.session, page_id)
            lines, links = _extract_ordered_lines(record_map, page_id)
            description = "\n".join(lines)
            location = _extract_field(lines, "Location")
            compensation = _extract_field(lines, "Compensation")
            link = _build_public_url(title, page_id)
            metadata = {
                "source": "notion",
                "notion_page_id": page_id,
                "notion_page_url": link,
                "links": sorted(set(links)),
                "location": location,
                "compensation": compensation,
            }
            yield PocketJob(
                title=title,
                link=link,
                location=location,
                description=description,
                metadata=metadata,
            )
            count += 1
            if limit is not None and count >= limit:
                return
            if self.delay:
                time.sleep(self.delay)


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------
def store_listing(listing: PocketJob) -> None:
    JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.link,
        defaults={
            "title": (listing.title or "")[:255],
            "location": (listing.location or "")[:255],
            "date": "",
            "description": (listing.description or "")[:10000],
            "metadata": listing.metadata,
        },
    )


def run_scrape(limit: Optional[int], delay: float) -> int:
    scraper = PocketNotionScraper(delay=delay)
    count = 0
    for job in scraper.scrape(limit=limit):
        store_listing(job)
        count += 1
    return count


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pocket Notion careers scraper")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY)
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
