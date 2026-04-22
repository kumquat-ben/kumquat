#!/usr/bin/env python3
"""Manual scraper for Globe Life careers (Workday-powered).

Globe Life exposes public staffing content on https://careers.globelifeinsurance.com,
but the live job inventory is served by a Workday tenant hosted at
https://gen.wd1.myworkdayjobs.com/en-US/careers. This script talks directly to
the same JSON endpoints the site relies on, enriches each posting with detail
page metadata, and persists results into ``scrapers.JobPosting``.
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
from typing import Dict, Generator, Iterable, Optional
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

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CAREERS_URL = "https://careers.globelifeinsurance.com"
WORKDAY_ROOT = "https://gen.wd1.myworkdayjobs.com"
TENANT = "gen"
PORTAL = "careers"
CXS_BASE = f"{WORKDAY_ROOT}/wday/cxs/{TENANT}/{PORTAL}"
JOBS_ENDPOINT = f"{CXS_BASE}/jobs"
JOB_DETAIL_BASE = f"{WORKDAY_ROOT}/en-US/{PORTAL}"
SESSION_SEED_URL = f"{WORKDAY_ROOT}/en-US/{PORTAL}"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

SCRAPER_QS = Scraper.objects.filter(company="Globe Life", url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched Globe Life; using id=%s.", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Globe Life",
        url=CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=300,
    )


class ScraperError(Exception):
    """Raised for unrecoverable errors while scraping Globe Life."""


@dataclass
class JobSummary:
    job_id: Optional[str]
    title: str
    detail_path: str
    detail_url: str
    location_text: Optional[str]
    posted_on: Optional[str]
    remote_type: Optional[str]


@dataclass
class JobListing(JobSummary):
    description: str
    date_posted: Optional[str]
    metadata: Dict[str, object]


class GlobeLifeJobScraper:
    def __init__(
        self,
        *,
        page_size: int = 20,
        delay: float = 0.25,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.page_size = max(1, page_size)
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)
        self._bootstrapped = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def scrape(self, *, limit: Optional[int] = None) -> Generator[JobListing, None, None]:
        fetched = 0
        for summary in self._iter_summaries(limit=limit):
            try:
                listing = self._enrich_summary(summary)
            except Exception as exc:  # pragma: no cover - defensive logging
                self.logger.error("Failed to enrich job %s: %s", summary.detail_url, exc)
                continue
            yield listing
            fetched += 1
            if limit is not None and fetched >= limit:
                return

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _iter_summaries(self, *, limit: Optional[int]) -> Iterable[JobSummary]:
        offset = 0
        retrieved = 0
        total: Optional[int] = None

        self._ensure_session_bootstrap()

        while True:
            payload = {
                "limit": self.page_size,
                "offset": offset,
                "searchText": "",
                "appliedFacets": {},
                "userPreferredLanguage": "en-US",
            }
            self.logger.debug("Requesting jobs offset=%s", offset)
            response = self.session.post(JOBS_ENDPOINT, json=payload, timeout=40)
            if response.status_code == 400 and not self._bootstrapped:
                self.logger.info("Workday API returned 400; retrying after session bootstrap.")
                self._ensure_session_bootstrap(force=True)
                response = self.session.post(JOBS_ENDPOINT, json=payload, timeout=40)
            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                snippet = response.text[:200].strip()
                self.logger.error(
                    "Workday jobs request failed (%s): %s", response.status_code, snippet
                )
                raise ScraperError(f"Workday jobs request failed: {exc}") from exc

            data = response.json()
            job_postings = data.get("jobPostings") or []
            if not job_postings:
                self.logger.info("No job postings returned at offset %s; stopping.", offset)
                return

            if total is None:
                try:
                    total = int(data.get("total") or 0)
                except (TypeError, ValueError):
                    total = None

            for raw in job_postings:
                detail_path = raw.get("externalPath") or ""
                detail_url = detail_path
                if detail_path:
                    detail_url = (
                        detail_path
                        if detail_path.startswith("http")
                        else urljoin(
                            JOB_DETAIL_BASE.rstrip("/") + "/", detail_path.lstrip("/")
                        )
                    )
                summary = JobSummary(
                    job_id=(raw.get("bulletFields") or [None])[0],
                    title=(raw.get("title") or "").strip(),
                    detail_path=detail_path,
                    detail_url=detail_url,
                    location_text=self._clean_text(raw.get("locationsText")),
                    posted_on=self._clean_text(raw.get("postedOn")),
                    remote_type=self._clean_text(raw.get("remoteType")),
                )
                if not summary.title or not summary.detail_url:
                    self.logger.debug("Skipping invalid job summary payload: %s", raw)
                    continue
                yield summary
                retrieved += 1
                if limit is not None and retrieved >= limit:
                    return

            offset += self.page_size
            if total is not None and offset >= total:
                self.logger.info("Reached reported Workday total (%s); stopping.", total)
                return

            if self.delay:
                time.sleep(self.delay)

    def _enrich_summary(self, summary: JobSummary) -> JobListing:
        detail_html = self._fetch_detail_html(summary.detail_url)
        json_ld: Dict[str, object] = {}

        try:
            json_ld = self._extract_json_ld(detail_html)
        except ScraperError as exc:
            self.logger.warning("Missing JSON-LD for %s (%s)", summary.detail_url, exc)

        raw_description = ""
        if isinstance(json_ld, dict):
            raw_description = str(json_ld.get("description") or "")

        description_text = self._normalize_description(raw_description)
        if not description_text:
            fallback = self._extract_fallback_description(detail_html)
            description_text = fallback or "Description unavailable."

        date_posted = summary.posted_on
        if isinstance(json_ld, dict):
            raw_date = self._clean_text(json_ld.get("datePosted"))
            date_posted = raw_date or date_posted

        metadata: Dict[str, object] = {
            "job_id": summary.job_id,
            "posted_on_text": summary.posted_on,
            "locations_text": summary.location_text,
            "remote_type": summary.remote_type,
            "detail_path": summary.detail_path,
        }
        if isinstance(json_ld, dict) and json_ld:
            metadata["json_ld"] = json_ld

        return JobListing(
            **summary.__dict__,
            description=description_text,
            date_posted=date_posted,
            metadata=metadata,
        )

    def _fetch_detail_html(self, url: str) -> str:
        headers = dict(self.session.headers)
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        response = self.session.get(url, headers=headers, timeout=40)
        response.raise_for_status()

        content_type = response.headers.get("Content-Type", "")
        if "application/json" in content_type:
            try:
                data = response.json()
            except ValueError:
                return response.text
            redirect_path = data.get("url")
            if redirect_path:
                redirect_url = (
                    redirect_path
                    if redirect_path.startswith("http")
                    else urljoin(WORKDAY_ROOT, redirect_path)
                )
                return self._fetch_detail_html(redirect_url)
        return response.text

    @staticmethod
    def _extract_json_ld(html_text: str) -> Dict[str, object]:
        soup = BeautifulSoup(html_text, "html.parser")
        script_tag = soup.find("script", attrs={"type": "application/ld+json"})
        if not script_tag:
            raise ScraperError("Job detail JSON-LD payload not found.")
        raw_json = script_tag.string or script_tag.get_text()
        if not raw_json:
            raise ScraperError("Job detail JSON-LD payload empty.")
        try:
            data = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            raise ScraperError(f"Failed to parse JSON-LD: {exc}") from exc
        if isinstance(data, dict):
            return data
        if isinstance(data, list) and data:
            first = data[0]
            if isinstance(first, dict):
                return first
        raise ScraperError("Unexpected JSON-LD structure.")

    @staticmethod
    def _extract_fallback_description(html_text: str) -> str:
        soup = BeautifulSoup(html_text, "html.parser")
        container = soup.find("div", attrs={"data-automation-id": "richTextArea"})
        if not container:
            container = soup.find("div", attrs={"data-automation-id": "jobPostingDescription"})
        if not container:
            container = soup.find("div", class_="GWTCKEditor-Disabled")
        if not container:
            return ""
        text = container.get_text("\n", strip=True)
        return GlobeLifeJobScraper._clean_text(text) or ""

    @staticmethod
    def _normalize_description(raw: str) -> str:
        raw = GlobeLifeJobScraper._clean_text(raw) or ""
        if not raw:
            return ""
        if "â" in raw or "Â" in raw:
            try:
                raw = raw.encode("latin-1", "ignore").decode("utf-8", "ignore")
            except UnicodeEncodeError:
                pass
        soup = BeautifulSoup(raw, "html.parser")
        text = soup.get_text("\n", strip=True)
        lines = [line.strip() for line in text.splitlines()]
        cleaned = "\n".join(line for line in lines if line)
        return cleaned

    @staticmethod
    def _clean_text(value: Optional[object]) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        text = (
            text.replace("\r", "\n")
            .replace("\xa0", " ")
            .replace("\u202f", " ")
            .replace("\u200b", "")
        )
        return text or None

    def _ensure_session_bootstrap(self, force: bool = False) -> None:
        if self._bootstrapped and not force:
            return
        self.logger.debug("Bootstrapping Workday session via %s", SESSION_SEED_URL)
        resp = self.session.get(SESSION_SEED_URL, timeout=40)
        resp.raise_for_status()
        self._bootstrapped = True


def persist_listing(listing: JobListing) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location_text or "")[:255] or None,
        "date": (listing.date_posted or listing.posted_on or "")[:100] or None,
        "description": listing.description[:10000],
        "metadata": listing.metadata,
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Persisted Globe Life job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Globe Life Workday careers job listings.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument(
        "--page-size",
        type=int,
        default=20,
        help="Number of jobs to request per Workday API page.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.25,
        help="Seconds to sleep between Workday pagination requests.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and display jobs without writing to the database.",
    )
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

    scraper = GlobeLifeJobScraper(page_size=args.page_size, delay=args.delay)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    for listing in scraper.scrape(limit=args.limit):
        totals["fetched"] += 1
        if args.dry_run:
            print(json.dumps(listing.__dict__, default=str, ensure_ascii=False))
            continue

        try:
            created = persist_listing(listing)
            if created:
                totals["created"] += 1
            else:
                totals["updated"] += 1
        except Exception as exc:  # pragma: no cover - persistence error path
            logging.error("Failed to persist job %s: %s", listing.detail_url, exc)
            totals["errors"] += 1

    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["dedupe"] = dedupe_summary

    logging.info(
        "Globe Life scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )
    return 0 if not totals["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

