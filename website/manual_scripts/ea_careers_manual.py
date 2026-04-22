#!/usr/bin/env python3
"""Manual scraper for Electronic Arts careers (Avature portal).

This script paginates the public job listings at
https://jobs.ea.com/en_US/careers/SearchJobs, enriches each record with
metadata from its detail page, and persists the results through the shared
``JobPosting`` Django model.
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
CAREERS_HOME_URL = "https://jobs.ea.com/en_US/careers"
SEARCH_URL = f"{CAREERS_HOME_URL}/SearchJobs"
PAGE_SIZE = 20
REQUEST_TIMEOUT = (15, 45)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 60)
SCRAPER_QS = Scraper.objects.filter(company="Electronic Arts", url=SEARCH_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Electronic Arts scrapers found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Electronic Arts",
        url=SEARCH_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised for unrecoverable scraping issues."""


@dataclass
class EAJobSummary:
    title: str
    detail_url: str
    location: Optional[str]
    role_id: Optional[str]
    worker_type: Optional[str]
    department: Optional[str]
    work_model: Optional[str]
    extra_locations: List[str]


@dataclass
class EAJobListing(EAJobSummary):
    description_text: Optional[str]
    description_html: Optional[str]
    metadata: Dict[str, object]


class EACareersScraper:
    def __init__(
        self,
        *,
        delay: float = 0.25,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------------ #
    # Public API                                                         #
    # ------------------------------------------------------------------ #
    def scrape(
        self, *, max_pages: Optional[int] = None, limit: Optional[int] = None
    ) -> Iterator[EAJobListing]:
        offset = 0
        page = 0
        yielded = 0
        total_results: Optional[int] = None

        while True:
            if max_pages is not None and page >= max_pages:
                self.logger.info("Reached max_pages=%s; stopping pagination.", max_pages)
                break

            soup = self._fetch_jobs_page(offset)
            if total_results is None:
                total_results = self._extract_total_results(soup)
                if total_results is not None:
                    self.logger.info("Electronic Arts reports %s open roles.", total_results)

            summaries = self._parse_jobs_page(soup)
            if not summaries:
                self.logger.info("No job summaries returned at offset=%s; stopping.", offset)
                break

            for summary in summaries:
                try:
                    detail = self._fetch_job_detail(summary.detail_url)
                except requests.HTTPError as exc:
                    self.logger.error("Failed to fetch detail for %s: %s", summary.detail_url, exc)
                    continue
                except requests.RequestException as exc:
                    self.logger.error("Network error fetching %s: %s", summary.detail_url, exc)
                    continue

                listing = EAJobListing(**asdict(summary), **detail)
                yield listing
                yielded += 1

                if limit is not None and yielded >= limit:
                    self.logger.info("Reached limit=%s; stopping.", limit)
                    return

                if self.delay:
                    time.sleep(self.delay)

            page += 1
            offset += PAGE_SIZE
            if self.delay:
                time.sleep(self.delay)

    # ------------------------------------------------------------------ #
    # Internal helpers                                                   #
    # ------------------------------------------------------------------ #
    def _fetch_jobs_page(self, offset: int) -> BeautifulSoup:
        params = {
            "jobRecordsPerPage": PAGE_SIZE,
            "jobOffset": offset,
            "listFilterMode": 1,
        }
        self.logger.debug("Fetching job list offset=%s", offset)
        response = self.session.get(SEARCH_URL, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")

    def _parse_jobs_page(self, soup: BeautifulSoup) -> List[EAJobSummary]:
        articles = soup.select("article.article--result")
        summaries: List[EAJobSummary] = []
        for article in articles:
            summary = self._parse_job_article(article)
            if summary:
                summaries.append(summary)
        return summaries

    def _parse_job_article(self, article: Tag) -> Optional[EAJobSummary]:
        link = article.select_one("a.link_result")
        if not link or not link.get("href"):
            self.logger.debug("Skipping article without job link.")
            return None

        title = link.get_text(" ", strip=True)
        detail_url = link["href"].strip()
        subtitle = article.select_one(".article__header__text__subtitle")

        location = None
        extra_locations: List[str] = []
        role_id = None
        worker_type = None
        department = None
        work_model = None

        if subtitle:
            primary_loc = subtitle.select_one(".list-item-location")
            if primary_loc:
                location = self._clean_text(primary_loc)

            for span in subtitle.find_all("span"):
                classes = span.get("class") or []
                text = self._clean_text(span)
                if not text:
                    continue
                if any(cls.startswith("list-item-jobPostingLocation") for cls in classes):
                    for nested in span.find_all("span"):
                        nested_text = self._clean_text(nested)
                        if nested_text:
                            extra_locations.append(nested_text)
                    continue
                if "list-item-id" in classes and not role_id:
                    role_id = text.replace("Role ID", "").strip() or text
                elif "list-item-workerType" in classes and not worker_type:
                    worker_type = text
                elif "list-item-department" in classes and not department:
                    department = text
                elif "list-item-workModel" in classes and not work_model:
                    work_model = text

        return EAJobSummary(
            title=title,
            detail_url=detail_url,
            location=location,
            role_id=role_id,
            worker_type=worker_type,
            department=department,
            work_model=work_model,
            extra_locations=list(dict.fromkeys(extra_locations)),
        )

    def _fetch_job_detail(self, detail_url: str) -> Dict[str, object]:
        self.logger.debug("Fetching job detail %s", detail_url)
        response = self.session.get(detail_url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        description_text, description_html = self._extract_description(soup)
        general_info, detailed_primary_location = self._extract_general_information(soup)

        metadata: Dict[str, object] = {}
        if general_info:
            metadata["general_information"] = general_info

        if detailed_primary_location:
            metadata.setdefault("detail_primary_location", detailed_primary_location)

        if description_html:
            metadata.setdefault("description_html", description_html)

        return {
            "description_text": description_text,
            "description_html": description_html,
            "metadata": metadata,
        }

    def _extract_description(self, soup: BeautifulSoup) -> tuple[Optional[str], Optional[str]]:
        for article in soup.select("article.article--details"):
            header = article.find("h3")
            if not header:
                continue
            if "Description" not in header.get_text(" ", strip=True):
                continue

            value_blocks = article.select(".article__content__view__field__value")
            html_parts: List[str] = []
            text_parts: List[str] = []
            for block in value_blocks:
                html_parts.append(str(block))
                text = block.get_text("\n", strip=True)
                if text:
                    text_parts.append(text)

            description_html = "\n".join(html_parts).strip() or None
            description_text = "\n\n".join(text_parts).strip() or None
            return description_text, description_html
        return None, None

    def _extract_general_information(
        self, soup: BeautifulSoup
    ) -> tuple[Dict[str, object], Optional[str]]:
        general_info: Dict[str, object] = {}
        primary_location: Optional[str] = None

        for article in soup.select("article.article--details"):
            header = article.find("h3")
            if not header:
                continue
            if "General Information" not in header.get_text(" ", strip=True):
                continue

            for field in article.select(".article__content__view__field"):
                value_elem = field.select_one(".article__content__view__field__value")
                if not value_elem:
                    continue

                label_elem = field.select_one(".article__content__view__field__label")
                label_text = label_elem.get_text(" ", strip=True) if label_elem else None
                strong = value_elem.find("strong")
                strong_text = strong.get_text(" ", strip=True) if strong else None
                if not label_text and strong_text:
                    label_text = strong_text

                value_text = value_elem.get_text(" ", strip=True)
                clean_value = value_text
                if strong_text and clean_value.startswith(strong_text):
                    clean_value = clean_value[len(strong_text) :].lstrip(": ").strip()

                if label_text:
                    general_info[label_text] = clean_value or value_text

                if strong_text and strong_text.lower().startswith("location"):
                    primary_location = clean_value or value_text
                    location_attributes = self._extract_location_attributes(value_elem)
                    if location_attributes:
                        general_info.setdefault("Location Attributes", location_attributes)
            break

        return general_info, primary_location

    @staticmethod
    def _extract_location_attributes(value_elem: Tag) -> Dict[str, str]:
        attributes: Dict[str, str] = {}
        for li in value_elem.select("li.MultipleDataSetField"):
            label_elem = li.select_one(".MultipleDataSetFieldLabel")
            value = li.select_one(".MultipleDataSetFieldValue")
            if not label_elem or not value:
                continue
            key = label_elem.get_text(" ", strip=True).rstrip(":")
            val = value.get_text(" ", strip=True)
            if key and val:
                attributes[key] = val
        return attributes

    @staticmethod
    def _extract_total_results(soup: BeautifulSoup) -> Optional[int]:
        legend = soup.select_one(".list-controls__text__legend")
        if not legend:
            return None
        text = legend.get_text(" ", strip=True)
        match = re.search(r"of\s+(\d+)", text)
        if not match:
            return None
        try:
            return int(match.group(1))
        except ValueError:
            return None

    @staticmethod
    def _clean_text(element: Optional[Tag]) -> Optional[str]:
        if not element:
            return None
        text = element.get_text(" ", strip=True)
        return text or None


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------
def store_listing(listing: EAJobListing) -> bool:
    metadata = dict(listing.metadata or {})
    general_info = metadata.get("general_information")
    if not isinstance(general_info, dict):
        general_info = {}

    role_id = listing.role_id or general_info.get("Role ID")
    worker_type = listing.worker_type or general_info.get("Worker Type")
    department = listing.department or general_info.get("Studio/Department")
    work_model = listing.work_model or general_info.get("Work Model")

    if role_id and "role_id" not in metadata:
        metadata["role_id"] = role_id
    if listing.extra_locations and "additional_locations" not in metadata:
        metadata["additional_locations"] = listing.extra_locations
    if worker_type and "worker_type" not in metadata:
        metadata["worker_type"] = worker_type
    if department and "department" not in metadata:
        metadata["department"] = department
    if work_model and "work_model" not in metadata:
        metadata["work_model"] = work_model

    detail_location = metadata.get("detail_primary_location")
    if not detail_location and isinstance(general_info, dict):
        detail_location = general_info.get("Locations")

    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or detail_location or "")[:255] or None,
        "date": None,
        "description": (listing.description_text or "")[:10000],
        "metadata": metadata,
    }

    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults=defaults,
    )
    logging.getLogger("store_listing").debug(
        "Stored EA job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Electronic Arts careers manual scraper.")
    parser.add_argument("--max-pages", type=int, default=None, help="Maximum listing pages to fetch.")
    parser.add_argument("--limit", type=int, default=None, help="Max number of jobs to persist.")
    parser.add_argument("--delay", type=float, default=0.25, help="Delay between HTTP requests.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print jobs as JSON instead of persisting them.",
    )
    return parser.parse_args(argv)


def run_scrape(max_pages: Optional[int], limit: Optional[int], delay: float, dry_run: bool) -> Dict[str, object]:
    scraper = EACareersScraper(delay=delay)
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
            except Exception as exc:  # pragma: no cover - defensive logging
                logging.error("Failed to store job %s: %s", listing.detail_url, exc)
                totals["errors"] += 1
    except requests.HTTPError as exc:
        logging.error("HTTP error while scraping EA careers: %s", exc)
        totals["errors"] += 1
    except requests.RequestException as exc:
        logging.error("Network error while scraping EA careers: %s", exc)
        totals["errors"] += 1
    except ScraperError as exc:
        logging.error("EA careers scraper stopped: %s", exc)
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
        "Electronic Arts scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )

    if not args.dry_run and "dedupe" in totals:
        logging.info("Deduplication summary: %s", totals["dedupe"])
    return 0 if not totals["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
