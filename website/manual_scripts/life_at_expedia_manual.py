#!/usr/bin/env python3
"""Manual scraper for https://lifeatexpediagroup.com (Expedia Group careers).

The public careers site renders job listings by requesting the WordPress theme
endpoint ``components/search/search/calcresults.php``. This script replicates
those calls, walks through each paginated response, enriches every posting with
detail data (JSON-LD, description HTML, apply URL), and persists the results
through Django's shared ``JobPosting`` model.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag

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
BASE_SITE_URL = "https://careers.expediagroup.com"
SEARCH_PAGE_URL = f"{BASE_SITE_URL}/jobs"
THEME_BASE_URL = f"{BASE_SITE_URL}/wp-content/themes/careers.expediagroup.com"
LIST_ENDPOINT = f"{THEME_BASE_URL}/components/search/search/calcresults.php"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": SEARCH_PAGE_URL,
    "Connection": "keep-alive",
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 60)

SCRAPER_QS = Scraper.objects.filter(company="Expedia Group", url=SEARCH_PAGE_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Expedia Group scrapers found; using id=%s", SCRAPER.id)
else:  # pragma: no cover - initialization path
    SCRAPER = Scraper.objects.create(
        company="Expedia Group",
        url=SEARCH_PAGE_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )

# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


class ScraperError(Exception):
    """Raised when the Expedia Group scraper cannot continue."""


@dataclass
class JobSummary:
    title: str
    detail_path: str
    detail_url: str
    location: Optional[str]
    team: Optional[str]
    job_id: Optional[str]


@dataclass
class ListingPageMeta:
    total_pages: Optional[int] = None
    total_results: Optional[int] = None
    query_string: Optional[str] = None
    results_display: Optional[str] = None


@dataclass
class JobListing(JobSummary):
    description_text: str
    description_html: Optional[str]
    date_posted: Optional[str]
    apply_url: Optional[str]
    info_list: List[str]
    metadata: Dict[str, object]


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------
def _element_text(element: Optional[Tag], *, separator: str = "\n\n") -> str:
    if not element:
        return ""
    return element.get_text(separator, strip=True).strip()


def _parse_int_input(soup: BeautifulSoup, element_id: str) -> Optional[int]:
    elem = soup.select_one(f"#{element_id}")
    if not elem:
        return None
    raw_value = elem.get("value")
    if raw_value is None:
        return None
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return None


def _parse_string_input(soup: BeautifulSoup, element_id: str) -> Optional[str]:
    elem = soup.select_one(f"#{element_id}")
    if not elem:
        return None
    value = elem.get("value")
    return value.strip() if value else None


def _extract_job_id(detail_path: str) -> Optional[str]:
    parts = [segment for segment in detail_path.split("/") if segment]
    if not parts:
        return None
    candidate = parts[-1]
    if re.search(r"[A-Z]-?\d", candidate):
        return candidate
    if len(parts) >= 2 and re.search(r"[A-Z]-?\d", parts[-2]):
        return parts[-2]
    return candidate or None


def _load_json_ld(soup: BeautifulSoup) -> Optional[Dict[str, object]]:
    scripts = soup.find_all("script", attrs={"type": "application/ld+json"})
    for script in scripts:
        if not script.string:
            continue
        try:
            data = json.loads(script.string)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict) and data.get("@type") == "JobPosting":
            return data
    return None


# ---------------------------------------------------------------------------
# Scraper implementation
# ---------------------------------------------------------------------------
class ExpediaGroupCareersScraper:
    def __init__(self, *, delay: float = 0.3, session: Optional[requests.Session] = None) -> None:
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    def scrape(
        self,
        *,
        max_pages: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Iterator[JobListing]:
        page_index = 0
        yielded = 0
        total_pages: Optional[int] = None

        while True:
            if max_pages is not None and page_index >= max_pages:
                self.logger.info("Reached max_pages=%s; stopping.", max_pages)
                break

            summaries, meta = self._fetch_page(page_index)
            if not summaries:
                self.logger.info("No listings returned for page %s; stopping.", page_index)
                break

            if page_index == 0 and meta:
                total_pages = meta.total_pages
                self.logger.info(
                    "Expedia Group careers reports %s total jobs across %s pages.",
                    meta.total_results,
                    meta.total_pages,
                )

            for summary in summaries:
                try:
                    detail = self._fetch_detail(summary.detail_url)
                except requests.RequestException as exc:
                    self.logger.error("Failed to fetch detail %s: %s", summary.detail_url, exc)
                    continue
                except ScraperError as exc:
                    self.logger.error("%s", exc)
                    continue

                metadata = dict(detail["metadata"])
                metadata.setdefault("team", summary.team)
                metadata.setdefault("listing_location", summary.location)
                metadata.setdefault("job_id", summary.job_id)
                metadata.setdefault("info_list", detail["info_list"])

                listing = JobListing(
                    title=summary.title,
                    detail_path=summary.detail_path,
                    detail_url=summary.detail_url,
                    location=summary.location,
                    team=summary.team,
                    job_id=summary.job_id,
                    description_text=detail["description_text"],
                    description_html=detail["description_html"],
                    date_posted=detail["date_posted"],
                    apply_url=detail["apply_url"],
                    info_list=detail["info_list"],
                    metadata=metadata,
                )
                yield listing
                yielded += 1

                if limit is not None and yielded >= limit:
                    self.logger.info("Reached limit=%s; stopping.", limit)
                    return

                if self.delay:
                    time.sleep(self.delay)

            page_index += 1
            if total_pages is not None and page_index >= total_pages:
                self.logger.info("Reached reported total_pages=%s; stopping.", total_pages)
                break

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _fetch_page(self, page: int) -> tuple[List[JobSummary], Optional[ListingPageMeta]]:
        params = {"mypage": page} if page > 0 else None
        try:
            response = self.session.get(LIST_ENDPOINT, params=params, timeout=(10, 45))
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch listing page {page}: {exc}") from exc

        soup = BeautifulSoup(response.text, "html.parser")
        summaries: List[JobSummary] = []
        for item in soup.select("li.Results__list__item"):
            summary = self._parse_summary(item)
            if summary:
                summaries.append(summary)

        meta = ListingPageMeta(
            total_pages=_parse_int_input(soup, "hiddennumpages"),
            total_results=_parse_int_input(soup, "hiddentotresults"),
            query_string=_parse_string_input(soup, "hiddenqs"),
            results_display=_parse_string_input(soup, "hiddenresultsdisplay"),
        )
        return summaries, meta

    def _parse_summary(self, item: Tag) -> Optional[JobSummary]:
        link_elem = item.select_one(".Results__list__content a.view-job-button[href]")
        if not link_elem:
            return None
        detail_path = link_elem.get("href", "").strip()
        if not detail_path:
            return None
        detail_url = urljoin(BASE_SITE_URL, detail_path)

        title_elem = item.select_one("h3.Results__list__title")
        title = title_elem.get_text(" ", strip=True) if title_elem else None
        if not title:
            return None

        location_elem = item.select_one("h4.Results__list__location")
        location = location_elem.get_text(" ", strip=True) if location_elem else None

        category_elem = item.select_one(".Results__list__content p")
        team = category_elem.get_text(" ", strip=True) if category_elem else None

        job_id = _extract_job_id(detail_path)

        return JobSummary(
            title=title,
            detail_path=detail_path,
            detail_url=detail_url,
            location=location,
            team=team,
            job_id=job_id,
        )

    def _fetch_detail(self, detail_url: str) -> Dict[str, object]:
        response = self.session.get(detail_url, timeout=(10, 60))
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        description_container = soup.select_one("div.Desc__copy")
        description_html = str(description_container) if description_container else None
        description_text = _element_text(description_container)
        if not description_text and description_container:
            description_text = description_container.get_text(" ", strip=True)

        info_items = [
            li.get_text(" ", strip=True)
            for li in soup.select("div.Desc__col ul.Info__list li")
            if li.get_text(strip=True)
        ]

        apply_elem = soup.select_one("a.Hero__button[href]") or soup.select_one("a.button--primary[href]")
        apply_href = apply_elem.get("href") if apply_elem else None
        apply_url = urljoin(BASE_SITE_URL, apply_href) if apply_href else None

        schema_data = _load_json_ld(soup) or {}
        date_posted = schema_data.get("datePosted")
        if not date_posted:
            for entry in info_items:
                if re.search(r"\d{1,2}/\d{1,2}/\d{4}", entry):
                    date_posted = entry.strip()
                    break

        metadata: Dict[str, object] = {}
        if schema_data:
            metadata["schema_org"] = schema_data
        if apply_url:
            metadata["apply_url"] = apply_url

        return {
            "description_text": description_text or "",
            "description_html": description_html,
            "date_posted": date_posted,
            "apply_url": apply_url,
            "info_list": info_items,
            "metadata": metadata,
        }


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------
def store_listing(listing: JobListing) -> bool:
    metadata = dict(listing.metadata or {})
    if listing.apply_url:
        metadata.setdefault("apply_url", listing.apply_url)

    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255] or None,
        "date": (listing.date_posted or "")[:100] or None,
        "description": (listing.description_text or "")[:10000],
        "metadata": metadata,
    }

    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults=defaults,
    )
    logging.getLogger("store_listing").debug(
        "Stored Expedia Group job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


# ---------------------------------------------------------------------------
# CLI wiring
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Expedia Group (lifeatexpediagroup.com) manual scraper.")
    parser.add_argument("--max-pages", type=int, default=None, help="Maximum listing pages to fetch.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to persist.")
    parser.add_argument("--delay", type=float, default=0.3, help="Delay (seconds) between requests.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print jobs as JSON instead of writing to the database.",
    )
    return parser.parse_args(argv)


def run_scrape(max_pages: Optional[int], limit: Optional[int], delay: float, dry_run: bool) -> Dict[str, object]:
    scraper = ExpediaGroupCareersScraper(delay=delay)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    try:
        for listing in scraper.scrape(max_pages=max_pages, limit=limit):
            totals["fetched"] += 1
            if dry_run:
                print(json.dumps(asdict(listing), default=str, ensure_ascii=False))
                continue
            try:
                created = store_listing(listing)
                if created:
                    totals["created"] += 1
                else:
                    totals["updated"] += 1
            except Exception as exc:  # pragma: no cover - persistence failure
                logging.error("Failed to store job %s: %s", listing.detail_url, exc)
                totals["errors"] += 1
    except requests.HTTPError as exc:
        logging.error("HTTP error while scraping Expedia Group careers: %s", exc)
        totals["errors"] += 1
    except requests.RequestException as exc:
        logging.error("Network error while scraping Expedia Group careers: %s", exc)
        totals["errors"] += 1
    except ScraperError as exc:
        logging.error("Expedia Group scraper stopped: %s", exc)
        totals["errors"] += 1

    if not dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["dedupe"] = dedupe_summary

    return totals


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()), format="%(levelname)s: %(message)s")

    totals = run_scrape(args.max_pages, args.limit, args.delay, args.dry_run)
    logging.info(
        "Expedia Group scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )

    if not args.dry_run and "dedupe" in totals:
        logging.info("Deduplication summary: %s", totals["dedupe"])
    return 0 if not totals["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
