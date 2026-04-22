#!/usr/bin/env python3
"""Manual scraper for https://www.usajobs.gov/search/results/.

This scraper uses the public-facing USAJOBS search workflow:
    1. Bootstrap a `requests.Session` against the search landing page so that we
       inherit the necessary Akamai cookies.
    2. Page through the `/Search/ExecuteSearch` JSON endpoint (the same one the
       SPA uses) to enumerate job summaries.
    3. For each job summary, visit the corresponding detail page to capture the
       rich announcement content (Summary, Duties, Requirements, etc.).
    4. Upsert records into `JobPosting`, keyed by the canonical job URL.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, Generator, Iterable, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from pathlib import Path

# ---------------------------------------------------------------------------
# Django setup (makes script runnable via management dashboard)
# ---------------------------------------------------------------------------
BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "website.settings")

import django

django.setup()

from django.conf import settings

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

# ---------------------------------------------------------------------------
# Constants & defaults
# ---------------------------------------------------------------------------
BASE_URL = "https://www.usajobs.gov"
SEARCH_LANDING_URL = urljoin(BASE_URL, "/search/results/")
EXECUTE_SEARCH_URL = urljoin(BASE_URL, "/Search/ExecuteSearch")
JOB_URL_TEMPLATE = urljoin(BASE_URL, "/job/{doc_id}")

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.8",
    "Referer": SEARCH_LANDING_URL,
    "Origin": BASE_URL,
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 30)
SCRAPER_QS = Scraper.objects.filter(company="USAJOBS", url=SEARCH_LANDING_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched; using the earliest (id=%s).", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="USAJOBS",
        url=SEARCH_LANDING_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the scraper encounters an unrecoverable error."""


@dataclass
class JobSummary:
    """Minimal fields returned by the ExecuteSearch API."""

    document_id: str
    position_id: Optional[str]
    title: str
    agency: Optional[str]
    department: Optional[str]
    location: Optional[str]
    date_display: Optional[str]
    work_schedule: Optional[str]
    work_type: Optional[str]
    salary_display: Optional[str]
    hiring_paths: List[Dict[str, Any]]
    api_payload: Dict[str, Any]

    @property
    def detail_url(self) -> str:
        raw = self.api_payload.get("PositionURI") or ""
        if raw.startswith("https://www.usajobs.gov:443"):
            raw = raw.replace(":443", "", 1)
        if raw:
            return raw
        return JOB_URL_TEMPLATE.format(doc_id=self.document_id)


@dataclass
class JobListing(JobSummary):
    """Extended job data after visiting the announcement page."""

    description_text: Optional[str]
    description_html: Optional[str]
    sections: Dict[str, str]
    overview: Dict[str, Any]
    detail_metadata: Dict[str, Any]


class USAJobsScraper:
    def __init__(
        self,
        *,
        keyword: Optional[str] = None,
        locations: Optional[List[str]] = None,
        results_per_page: int = 500,
        delay: float = 0.35,
        max_workers: int = 6,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.keyword = keyword.strip() if keyword else None
        self.locations = [loc.strip() for loc in (locations or []) if loc.strip()]
        self.results_per_page = max(1, int(results_per_page))
        self.delay = max(0.0, float(delay))
        self.max_workers = max(1, int(max_workers))
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)
        self._thread_local = threading.local()
        self._bootstrap_session()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def scrape(
        self,
        *,
        max_pages: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Generator[JobListing, None, None]:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        produced = 0

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_map = {}
            for summary in self._iter_summaries(max_pages=max_pages, limit=limit):
                if limit is not None and produced >= limit:
                    break
                future = executor.submit(self._enrich_summary, summary)
                future_map[future] = summary

            for future in as_completed(future_map):
                summary = future_map[future]
                try:
                    listing = future.result()
                except Exception as exc:  # pragma: no cover - defensive logging
                    self.logger.error("Failed to enrich job %s: %s", summary.document_id, exc)
                    continue
                produced += 1
                yield listing

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _bootstrap_session(self) -> None:
        """Hit the landing page once to secure required cookies."""
        resp = self.session.get(SEARCH_LANDING_URL, timeout=30)
        resp.raise_for_status()

    def _iter_summaries(
        self,
        *,
        max_pages: Optional[int],
        limit: Optional[int],
    ) -> Iterable[JobSummary]:
        page = 1
        seen = 0

        while True:
            payload = self._build_payload(page)
            self.logger.debug("Fetching page %s with payload=%s", page, payload)
            resp = self.session.post(EXECUTE_SEARCH_URL, json=payload, timeout=40)
            if resp.status_code != 200:
                raise ScraperError(f"ExecuteSearch returned {resp.status_code} on page {page}")

            data = resp.json()
            jobs = data.get("Jobs") or []
            if not jobs:
                self.logger.info("No jobs returned on page %s; stopping pagination.", page)
                break

            for job in jobs:
                summary = self._build_summary(job)
                seen += 1
                yield summary
                if limit is not None and seen >= limit:
                    return

            pager = data.get("Pager") or {}
            has_next = bool(pager.get("HasNextPage"))
            last_page = int(pager.get("LastPageIndex") or pager.get("NumberOfPages") or page)

            page += 1
            if max_pages is not None and page > max_pages:
                break
            if limit is not None and seen >= limit:
                break
            if not has_next or page > last_page:
                break
            time.sleep(self.delay)

    def _build_payload(self, page: int) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "Page": str(page),
            "ResultsPerPage": str(self.results_per_page),
        }
        if self.keyword:
            payload["Keyword"] = self.keyword
        if self.locations:
            payload["LocationName"] = self.locations
        return payload

    def _build_summary(self, job: Dict[str, Any]) -> JobSummary:
        doc_id = str(job.get("DocumentID") or "").strip()
        if not doc_id:
            raise ScraperError("Encountered job without DocumentID.")
        return JobSummary(
            document_id=doc_id,
            position_id=job.get("PositionID"),
            title=(job.get("Title") or "").strip(),
            agency=(job.get("Agency") or "").strip() or None,
            department=(job.get("Department") or "").strip() or None,
            location=(job.get("Location") or "").strip() or None,
            date_display=(job.get("DateDisplay") or "").strip() or None,
            work_schedule=(job.get("WorkSchedule") or "").strip() or None,
            work_type=(job.get("WorkType") or "").strip() or None,
            salary_display=(job.get("SalaryDisplay") or "").strip() or None,
            hiring_paths=job.get("HiringPath") or [],
            api_payload=job,
        )

    def _enrich_summary(self, summary: JobSummary) -> JobListing:
        html = self._fetch_job_html(summary.detail_url)
        description_html, description_text, sections, overview = self._extract_detail_sections(html)
        detail_meta = {
            "document_id": summary.document_id,
            "position_id": summary.position_id,
            "detail_url": summary.detail_url,
            "sections": list(sections.keys()),
        }

        return JobListing(
            **asdict(summary),
            description_text=description_text,
            description_html=description_html,
            sections=sections,
            overview=overview,
            detail_metadata=detail_meta,
        )

    def _fetch_job_html(self, url: str) -> str:
        session = self._get_detail_session()
        response = session.get(url, timeout=45)
        if response.status_code != 200:
            raise ScraperError(f"Failed to fetch detail page ({response.status_code}) for {url}")
        return response.text

    def _get_detail_session(self) -> requests.Session:
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update(DEFAULT_HEADERS)
            session.cookies.update(self.session.cookies)
            self._thread_local.session = session
        else:
            session.cookies.update(self.session.cookies)
        return session

    def _extract_detail_sections(
        self,
        html: str,
    ) -> (Optional[str], Optional[str], Dict[str, str], Dict[str, Any]):
        soup = BeautifulSoup(html, "html.parser")
        root = soup.select_one("div.apply-joa-defaults")
        if not root:
            return None, None, {}, {}

        sections: Dict[str, str] = {}
        for container in root.select("div[id^=joa-]"):
            heading_elem = container.find("h2")
            if not heading_elem:
                continue
            heading = heading_elem.get_text(strip=True)
            if not heading or heading in sections:
                continue
            body_html_parts: List[str] = []
            for child in container.find_all(recursive=False):
                if child is heading_elem:
                    continue
                body_html_parts.append(str(child))
            body_html = "\n".join(body_html_parts).strip()
            sections[heading] = body_html

        description_html_parts: List[str] = []
        description_text_parts: List[str] = []
        for heading, body_html in sections.items():
            section_soup = BeautifulSoup(body_html, "html.parser")
            text = section_soup.get_text("\n", strip=True)
            description_html_parts.append(f"<h2>{heading}</h2>\n{body_html}")
            if text:
                description_text_parts.append(f"{heading}\n{text}")

        description_html = "\n\n".join(description_html_parts) if description_html_parts else None
        description_text = "\n\n".join(description_text_parts) if description_text_parts else None

        overview = self._extract_overview(root)
        return description_html, description_text, sections, overview

    def _extract_overview(self, root: BeautifulSoup) -> Dict[str, Any]:
        overview_container = root.select_one("#joa-summary .page-section") or root.select_one(
            ".page-section"
        )
        if not overview_container:
            return {}

        overview: Dict[str, Any] = {}

        for dl in overview_container.find_all("dl"):
            dt = dl.find("dt")
            dd = dl.find("dd")
            if not dt or not dd:
                continue
            label = dt.get_text(strip=True)
            value = dd.get_text("\n", strip=True)
            if label:
                overview[label] = value

        badge = overview_container.find("div", class_="badge")
        if badge:
            overview.setdefault("Status", badge.get_text(strip=True))

        locations_block = overview_container.select_one("#allLocations")
        if locations_block:
            locations = [
                loc.get_text(" ", strip=True)
                for loc in locations_block.select("div.font-bold")
                if loc.get_text(strip=True)
            ]
            if locations:
                overview["Locations"] = locations

        return overview


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------
def store_listing(listing: JobListing) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255],
        "date": (listing.date_display or "")[:100],
        "description": (listing.description_text or listing.description_html or "")[:10000],
        "metadata": {
            "agency": listing.agency,
            "department": listing.department,
            "work_schedule": listing.work_schedule,
            "work_type": listing.work_type,
            "salary_display": listing.salary_display,
            "hiring_paths": listing.hiring_paths,
            "api_payload": listing.api_payload,
            "sections": listing.sections,
            "overview": listing.overview,
            "detail": listing.detail_metadata,
        },
    }

    _, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults=defaults,
    )
    return created


def run_scrape(
    *,
    keyword: Optional[str],
    locations: Optional[List[str]],
    max_pages: Optional[int],
    limit: Optional[int],
    results_per_page: int,
    delay: float,
    max_workers: int,
) -> Dict[str, Any]:
    scraper = USAJobsScraper(
        keyword=keyword,
        locations=locations,
        results_per_page=results_per_page,
        delay=delay,
        max_workers=max_workers,
    )

    total = 0
    created = 0

    for listing in scraper.scrape(max_pages=max_pages, limit=limit):
        total += 1
        if store_listing(listing):
            created += 1

    return {
        "company": SCRAPER.company,
        "url": SCRAPER.url,
        "processed": total,
        "created": created,
        "updated": total - created,
        "keyword": keyword,
        "locations": locations,
        "results_per_page": results_per_page,
    }


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="USAJOBS manual scraper")
    parser.add_argument("--keyword", type=str, default=None, help="Keyword filter (optional)")
    parser.add_argument(
        "--location",
        action="append",
        dest="locations",
        default=None,
        help="Location filter (repeat for multiple locations)",
    )
    parser.add_argument("--max-pages", type=int, default=None, help="Maximum number of result pages")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process")
    parser.add_argument(
        "--results-per-page",
        type=int,
        default=500,
        help="How many jobs to request per ExecuteSearch page (default: 500)",
    )
    parser.add_argument("--delay", type=float, default=0.35, help="Delay (seconds) between page fetches")
    parser.add_argument(
        "--max-workers",
        type=int,
        default=6,
        help="Max concurrent workers for fetching job details (default: 6)",
    )
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
        summary = run_scrape(
            keyword=args.keyword,
            locations=args.locations,
            max_pages=args.max_pages,
            limit=args.limit,
            results_per_page=args.results_per_page,
            delay=args.delay,
            max_workers=args.max_workers,
        )
    except ScraperError as exc:
        logging.error("Scrape failed: %s", exc)
        return 1
    except Exception:  # pragma: no cover - defensive logging
        logging.exception("Unexpected error during USAJOBS scrape")
        return 1
    dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
    summary["elapsed_seconds"] = round(time.time() - start, 2)
    summary["dedupe"] = dedupe_summary
    logging.info("Summary: %s", json.dumps(summary))
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
