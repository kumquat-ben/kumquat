#!/usr/bin/env python3
"""Manual scraper for Dell Technologies careers (jobs.dell.com).

This script mirrors the public search experience exposed at
https://jobs.dell.com/en/search-jobs by iterating the paginated listings,
visiting each job detail page for structured metadata, and persisting the
results via the shared ``JobPosting`` Django model.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, List, Optional
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
# Constants
# ---------------------------------------------------------------------------
BASE_URL = "https://jobs.dell.com"
SEARCH_PATH = "/en/search-jobs"
SEARCH_URL = urljoin(BASE_URL, SEARCH_PATH)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.8",
    "Referer": SEARCH_URL,
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 900), 60)

SCRAPER_QS = Scraper.objects.filter(company="Dell Technologies", url=SEARCH_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Dell scrapers found; using id=%s", SCRAPER.id)
else:  # pragma: no cover - creation path
    SCRAPER = Scraper.objects.create(
        company="Dell Technologies",
        url=SEARCH_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised for unrecoverable errors while scraping Dell careers."""


@dataclass
class JobSummary:
    job_id: str
    title: str
    detail_url: str
    location: Optional[str]
    date_posted: Optional[str]
    categories: List[str]
    raw_job: Dict[str, Any]


@dataclass
class JobListing(JobSummary):
    description_text: Optional[str]
    description_html: Optional[str]
    metadata: Dict[str, Any]


class DellJobScraper:
    def __init__(self, *, delay: float = 0.3, session: Optional[requests.Session] = None) -> None:
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def scrape(
        self,
        *,
        max_pages: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Generator[JobListing, None, None]:
        page = 1
        yielded = 0
        total_pages: Optional[int] = None

        while True:
            if max_pages is not None and page > max_pages:
                self.logger.info("Reached max_pages=%s; stopping.", max_pages)
                break

            soup, jobs_map, page_meta = self._fetch_search_page(page)
            if total_pages is None:
                total_pages = page_meta.get("total_pages")
                self.logger.info(
                    "Discovered %s Dell job pages (reported total results=%s).",
                    total_pages,
                    page_meta.get("total_results"),
                )

            summaries = list(self._parse_job_summaries(soup, jobs_map))
            if not summaries:
                self.logger.info("No job summaries returned for page %s; ending scrape.", page)
                break

            for summary in summaries:
                try:
                    detail = self._fetch_job_detail(summary)
                except ScraperError as exc:
                    self.logger.warning("Skipping job %s detail error: %s", summary.job_id, exc)
                    continue

                listing = JobListing(
                    job_id=summary.job_id,
                    title=summary.title,
                    detail_url=summary.detail_url,
                    location=summary.location,
                    date_posted=detail.get("date_posted") or summary.date_posted,
                    categories=summary.categories,
                    raw_job=summary.raw_job,
                    description_text=detail.get("description_text"),
                    description_html=detail.get("description_html"),
                    metadata=detail.get("metadata") or {},
                )
                yield listing
                yielded += 1

                if limit is not None and yielded >= limit:
                    self.logger.info("Reached limit=%s; stopping scrape.", limit)
                    return

                if self.delay:
                    time.sleep(self.delay)

            page += 1
            if total_pages is not None and page > total_pages:
                self.logger.info("Reached final reported page (%s); stopping.", total_pages)
                break

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _fetch_search_page(
        self,
        page: int,
    ) -> tuple[BeautifulSoup, Dict[str, Dict[str, Any]], Dict[str, Optional[int]]]:
        params = {"p": page}
        try:
            response = self.session.get(SEARCH_URL, params=params, timeout=45)
            response.raise_for_status()
        except requests.RequestException as exc:  # pragma: no cover - network failures
            raise ScraperError(f"Failed to fetch search page {page}: {exc}") from exc

        soup = BeautifulSoup(response.text, "html.parser")
        jobs_map = self._extract_jobs_map(response.text)

        section = soup.select_one("#search-results")
        total_pages = _safe_int(section.get("data-total-pages")) if section else None
        total_results = _safe_int(section.get("data-total-results")) if section else None

        return soup, jobs_map, {"total_pages": total_pages, "total_results": total_results}

    def _extract_jobs_map(self, html: str) -> Dict[str, Dict[str, Any]]:
        marker = '"Jobs":['
        marker_index = html.find(marker)
        if marker_index == -1:
            self.logger.warning("Jobs JSON marker not found on page.")
            return {}

        start_index = html.find("[", marker_index)
        if start_index == -1:
            raise ScraperError("Malformed jobs JSON payload.")

        depth = 0
        end_index = None
        for idx, char in enumerate(html[start_index:], start=start_index):
            if char == "[":
                depth += 1
            elif char == "]":
                depth -= 1
                if depth == 0:
                    end_index = idx
                    break

        if end_index is None:
            raise ScraperError("Could not locate closing bracket for jobs JSON payload.")

        jobs_json = html[start_index : end_index + 1]
        try:
            jobs_array = json.loads(jobs_json)
        except json.JSONDecodeError as exc:
            raise ScraperError(f"Failed to decode jobs JSON payload: {exc}") from exc

        jobs_map: Dict[str, Dict[str, Any]] = {}
        for job in jobs_array:
            job_id = job.get("ID") or job.get("JobID")
            if job_id is None:
                continue
            jobs_map[str(job_id)] = job
        return jobs_map

    def _parse_job_summaries(
        self,
        soup: BeautifulSoup,
        jobs_map: Dict[str, Dict[str, Any]],
    ) -> Iterable[JobSummary]:
        for item in soup.select("#search-results-list li"):
            anchor = item.find("a", attrs={"data-job-id": True})
            if not anchor:
                continue

            job_id = (anchor.get("data-job-id") or "").strip()
            detail_path = (anchor.get("href") or "").strip()
            title_elem = anchor.find("h2")
            title = _normalize_whitespace(title_elem.get_text(" ", strip=True)) if title_elem else ""

            if not job_id or not detail_path or not title:
                self.logger.debug("Skipping incomplete job summary: %s", job_id or detail_path)
                continue

            location_elem = anchor.select_one(".job-info.job-location")
            location = (
                _normalize_whitespace(location_elem.get_text(" ", strip=True)) if location_elem else None
            )

            job_data = jobs_map.get(job_id, {})
            posted_raw = job_data.get("PostedDate") or job_data.get("InsertedDate")
            categories = [
                cat.get("Name")
                for cat in (job_data.get("Categories") or [])
                if isinstance(cat, dict) and cat.get("Name")
            ]

            yield JobSummary(
                job_id=job_id,
                title=title,
                detail_url=urljoin(BASE_URL, detail_path),
                location=location,
                date_posted=_normalize_iso_date(posted_raw),
                categories=categories,
                raw_job=job_data,
            )

    def _fetch_job_detail(self, summary: JobSummary) -> Dict[str, Any]:
        try:
            response = self.session.get(summary.detail_url, timeout=45)
            response.raise_for_status()
        except requests.RequestException as exc:  # pragma: no cover - network failures
            raise ScraperError(f"Failed to fetch job detail {summary.detail_url}: {exc}") from exc

        soup = BeautifulSoup(response.text, "html.parser")
        job_ld = _extract_jobposting_jsonld(soup)

        description_html = None
        description_text = None
        date_posted = summary.date_posted
        metadata: Dict[str, Any] = {
            "job_id": summary.job_id,
            "categories": summary.categories,
            "source": "jobs.dell.com",
        }

        raw_job = summary.raw_job or {}
        apply_url = raw_job.get("ApplyUrl") or raw_job.get("TBApplyUrl")
        if apply_url:
            metadata["apply_url"] = apply_url
        for key in ("ExternalReferenceCode", "JobType", "JobStatus", "JobLevel"):
            value = raw_job.get(key)
            if value:
                metadata[key.lower()] = value
        if raw_job.get("PostedDate"):
            metadata.setdefault("posted_date_iso", raw_job.get("PostedDate"))
        if raw_job.get("InsertedDate"):
            metadata.setdefault("inserted_date_iso", raw_job.get("InsertedDate"))
        if raw_job.get("Locations"):
            metadata["locations"] = [
                {
                    "country": loc.get("Country"),
                    "country_code": loc.get("CountryCode"),
                    "region": loc.get("Division1"),
                    "city": loc.get("City"),
                    "formatted": loc.get("FormattedName"),
                }
                for loc in raw_job.get("Locations", [])
                if isinstance(loc, dict)
            ]

        if job_ld:
            description_html = job_ld.get("description") or description_html
            date_posted = _normalize_iso_date(job_ld.get("datePosted") or date_posted)
            employment_type = job_ld.get("employmentType")
            if employment_type:
                metadata["employment_type"] = employment_type
            identifier = job_ld.get("identifier")
            if identifier:
                metadata["identifier"] = identifier
            hiring_org = job_ld.get("hiringOrganization")
            if hiring_org:
                metadata["hiring_organization"] = hiring_org
            job_location = job_ld.get("jobLocation")
            if job_location:
                metadata["job_location"] = job_location

        if not description_html:
            description_html = raw_job.get("DescriptionHtml") or raw_job.get("Description")

        if description_html:
            description_text = _html_to_text(description_html)

        metadata = {key: value for key, value in metadata.items() if value not in (None, "", [])}

        return {
            "description_html": description_html,
            "description_text": description_text,
            "date_posted": date_posted,
            "metadata": metadata,
        }


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------
def _normalize_whitespace(text: str) -> str:
    return " ".join(text.replace("\xa0", " ").split())


def _html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for br in soup.find_all("br"):
        br.replace_with("\n")
    text = soup.get_text("\n", strip=True)
    lines = [line.strip() for line in text.splitlines()]
    return "\n".join(line for line in lines if line)


def _extract_jobposting_jsonld(soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
    for script in soup.select('script[type="application/ld+json"]'):
        if not script.string:
            continue
        try:
            payload = json.loads(script.string)
        except json.JSONDecodeError:
            continue

        if isinstance(payload, dict):
            if payload.get("@type") == "JobPosting":
                return payload
        elif isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict) and item.get("@type") == "JobPosting":
                    return item
    return None


def _normalize_iso_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    candidates = [cleaned]
    if cleaned.endswith("Z"):
        candidates.append(cleaned[:-1] + "+00:00")
    for candidate in candidates:
        try:
            dt = datetime.fromisoformat(candidate)
            return dt.date().isoformat()
        except ValueError:
            continue
    for fmt in ("%m/%d/%Y", "%d/%m/%Y"):
        try:
            dt = datetime.strptime(cleaned, fmt)
            return dt.date().isoformat()
        except ValueError:
            continue
    return cleaned


def _safe_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------
def store_listing(listing: JobListing) -> bool:
    metadata = dict(listing.metadata or {})
    if listing.description_html:
        metadata.setdefault("description_html", listing.description_html)

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
        "Stored Dell job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def run_scrape(max_pages: Optional[int], limit: Optional[int], delay: float) -> Dict[str, int]:
    scraper = DellJobScraper(delay=delay)
    totals = {"processed": 0, "created": 0}

    for listing in scraper.scrape(max_pages=max_pages, limit=limit):
        created = store_listing(listing)
        totals["processed"] += 1
        if created:
            totals["created"] += 1

    return totals


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dell Technologies manual scraper.")
    parser.add_argument("--max-pages", type=int, default=None, help="Maximum search pages to fetch.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of job postings.")
    parser.add_argument("--delay", type=float, default=0.3, help="Delay between requests in seconds.")
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    start = time.time()

    try:
        totals = run_scrape(args.max_pages, args.limit, args.delay)
    except ScraperError as exc:
        logging.error("Dell scrape failed: %s", exc)
        return 1

    dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
    elapsed = time.time() - start
    summary = {
        "company": "Dell Technologies",
        "url": SEARCH_URL,
        "processed": totals["processed"],
        "created": totals["created"],
        "elapsed_seconds": round(elapsed, 2),
        "dedupe": dedupe_summary,
    }
    logging.info("Dell scrape summary: %s", json.dumps(summary))
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
