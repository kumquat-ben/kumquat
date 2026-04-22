#!/usr/bin/env python3
"""Custom scraper for https://www.scribd.com/docs/Career-Growth (Scribd documents)."""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Generator, Iterable, List, Optional, Set, Tuple

import requests

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
COMPANY_NAME = "Scribd Career and Growth"
CAREERS_URL = "https://www.scribd.com/docs/Career-Growth"
SEARCH_ENDPOINT = "https://www.scribd.com/search/query"
DEFAULT_CATEGORY_ID = 56263
DEFAULT_QUERY = "*"
CONTENT_TYPE = "document"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 30)

SCRAPER_QS = Scraper.objects.filter(company=COMPANY_NAME, url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scribd Career & Growth scrapers found; using id=%s", SCRAPER.id)
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
class DocumentListing:
    document_id: int
    title: str
    detail_url: str
    description: Optional[str]
    released_at: Optional[str]
    author_name: Optional[str]
    author_url: Optional[str]
    page_count: Optional[int]
    language: Optional[str]
    views: Optional[str]
    categories: List[Dict[str, object]]
    metadata: Dict[str, object]


class ScribdDocumentScraper:
    def __init__(
        self,
        delay: float = 0.8,
        max_retries: int = 4,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.delay = max(0.0, delay)
        self.max_retries = max(1, max_retries)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    def scrape(
        self,
        limit: Optional[int] = None,
        max_pages: int = 10,
        stop_after_empty_pages: int = 2,
    ) -> Generator[DocumentListing, None, None]:
        category_id = self._fetch_category_id()
        total_expected: Optional[int] = None
        seen_ids: Set[int] = set()
        empty_pages = 0

        for page in range(1, max_pages + 1):
            documents, total_count = self._fetch_documents_page(category_id, page)
            if total_count is not None:
                total_expected = total_count

            new_count = 0
            for document in documents:
                if document.document_id in seen_ids:
                    continue
                seen_ids.add(document.document_id)
                new_count += 1
                yield document
                if limit is not None and len(seen_ids) >= limit:
                    self.logger.info("Reached limit %s; stopping scrape", limit)
                    return

            if total_expected is not None and len(seen_ids) >= total_expected:
                self.logger.info("Collected all %s documents reported by the API", total_expected)
                return

            if new_count == 0:
                empty_pages += 1
                if empty_pages >= stop_after_empty_pages:
                    self.logger.info("No new documents after %s pages; stopping", empty_pages)
                    return
            else:
                empty_pages = 0

            if self.delay:
                time.sleep(self.delay)

        if total_expected is not None and len(seen_ids) < total_expected:
            self.logger.warning(
                "Stopped after %s pages with %s/%s documents. Increase --max-pages to continue.",
                max_pages,
                len(seen_ids),
                total_expected,
            )

    # ------------------------------------------------------------------
    # Fetch + parse helpers
    # ------------------------------------------------------------------
    def _fetch_category_id(self) -> int:
        try:
            payload = self._request_json(CAREERS_URL, params=None)
        except ScraperError:
            self.logger.warning("Falling back to default category id %s", DEFAULT_CATEGORY_ID)
            return DEFAULT_CATEGORY_ID

        category_id = (
            payload.get("currentCategory", {})
            .get("metadata", {})
            .get("id")
        )
        if not category_id:
            self.logger.warning("Category id missing; using default %s", DEFAULT_CATEGORY_ID)
            return DEFAULT_CATEGORY_ID
        return int(category_id)

    def _fetch_documents_page(self, category_id: int, page: int) -> Tuple[List[DocumentListing], Optional[int]]:
        params = {
            "query": DEFAULT_QUERY,
            "content_type": CONTENT_TYPE,
            "category_id": str(category_id),
            "page": str(page),
        }
        payload = self._request_json(SEARCH_ENDPOINT, params=params)
        if "error" in payload:
            raise ScraperError(f"Scribd search error: {payload.get('error')}")

        results = payload.get("results", {}).get("documents", {}).get("content", {})
        documents = results.get("documents") or []
        total_count = payload.get("total_results_count")

        listings = []
        for doc in documents:
            listing = self._parse_document(doc)
            if listing:
                listings.append(listing)

        self.logger.info("Page %s returned %s documents", page, len(listings))
        return listings, total_count

    def _parse_document(self, doc: Dict[str, object]) -> Optional[DocumentListing]:
        document_id = doc.get("id")
        title = doc.get("title")
        if not document_id or not title:
            return None

        detail_url = doc.get("reader_url") or f"https://www.scribd.com/document/{document_id}/"
        author = doc.get("author") or {}
        author_name = author.get("name") if isinstance(author, dict) else None
        author_url = doc.get("authorUrl")

        metadata = {
            "document_id": document_id,
            "author": author,
            "author_url": author_url,
            "page_count": doc.get("pageCount"),
            "language": doc.get("language"),
            "views": doc.get("views"),
            "availability": doc.get("availability"),
            "download_url": doc.get("downloadUrl"),
            "reader_url": detail_url,
            "document_type": doc.get("type"),
            "categories": doc.get("categories") or [],
        }

        return DocumentListing(
            document_id=int(document_id),
            title=str(title),
            detail_url=str(detail_url),
            description=doc.get("description"),
            released_at=doc.get("releasedAt"),
            author_name=author_name,
            author_url=author_url,
            page_count=doc.get("pageCount"),
            language=(doc.get("language") or {}).get("name") if isinstance(doc.get("language"), dict) else None,
            views=doc.get("views"),
            categories=doc.get("categories") or [],
            metadata=metadata,
        )

    def _request_json(self, url: str, params: Optional[Dict[str, str]]) -> Dict[str, object]:
        last_error: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=40)
                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    wait_seconds = float(retry_after) if retry_after else 2.0
                    self.logger.warning("Rate limited; sleeping %s seconds", wait_seconds)
                    time.sleep(wait_seconds)
                    continue
                response.raise_for_status()
                payload = response.json()
                if isinstance(payload, dict) and payload.get("retry_after"):
                    wait_seconds = float(payload.get("retry_after") or 1)
                    self.logger.warning("Retry requested by API; sleeping %s seconds", wait_seconds)
                    time.sleep(wait_seconds)
                    continue
                return payload
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                backoff = min(2 ** attempt, 10)
                self.logger.warning("Request failed (attempt %s/%s): %s", attempt, self.max_retries, exc)
                time.sleep(backoff)

        raise ScraperError(f"Failed to fetch JSON after {self.max_retries} attempts: {last_error}")


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------
def store_listing(listing: DocumentListing) -> None:
    JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults={
            "title": (listing.title or "")[:255],
            "location": "",
            "date": (listing.released_at or "")[:100],
            "description": (listing.description or "")[:10000],
            "metadata": listing.metadata,
        },
    )


def run_scrape(limit: Optional[int], delay: float, max_pages: int, stop_after_empty_pages: int) -> int:
    scraper = ScribdDocumentScraper(delay=delay)
    count = 0
    for doc in scraper.scrape(
        limit=limit,
        max_pages=max_pages,
        stop_after_empty_pages=stop_after_empty_pages,
    ):
        store_listing(doc)
        count += 1
    return count


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scribd Career & Growth documents scraper")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--delay", type=float, default=0.8)
    parser.add_argument("--max-pages", type=int, default=10)
    parser.add_argument("--stop-after-empty-pages", type=int, default=2)
    parser.add_argument("--log-level", type=str, default="INFO", choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"])
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    start = time.time()
    try:
        count = run_scrape(args.limit, args.delay, args.max_pages, args.stop_after_empty_pages)
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
