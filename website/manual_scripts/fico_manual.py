#!/usr/bin/env python3
"""Manual scraper for https://www.fico.com/en/careers (Workday-powered).

The script uses FICO's Workday CxS endpoints to collect paginated job listings,
enriches each posting by reading the corresponding detail page, and persists the
results through the shared Django `JobPosting` model. Run it from the project
root or via the manual scripts dashboard.
"""
from __future__ import annotations

import argparse
import html
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, Optional
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
# Constants & configuration
# ---------------------------------------------------------------------------
CAREERS_URL = "https://www.fico.com/en/careers"
WORKDAY_ROOT = "https://fico.wd1.myworkdayjobs.com"
TENANT = "fico"
PORTAL = "External"
CXS_BASE = f"{WORKDAY_ROOT}/wday/cxs/{TENANT}/{PORTAL}"
JOBS_ENDPOINT = f"{CXS_BASE}/jobs"
JOB_DETAIL_BASE = f"{WORKDAY_ROOT}/en-US/{PORTAL}"
SESSION_SEED_URL = JOB_DETAIL_BASE

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

DEFAULT_PAGE_SIZE = 20
DEFAULT_DELAY = 0.25
DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 600), 300)

SCRAPER_QS = Scraper.objects.filter(company="FICO", url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple FICO Scraper rows found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="FICO",
        url=CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the scraper encounters a non-recoverable error."""


@dataclass
class JobSummary:
    job_id: Optional[str]
    title: str
    detail_path: str
    detail_url: str
    location_text: Optional[str]
    posted_on: Optional[str]
    time_type: Optional[str]


@dataclass
class JobListing(JobSummary):
    description: str
    date_posted: Optional[str]
    metadata: Dict[str, Any]


class FICOJobScraper:
    def __init__(
        self,
        *,
        page_size: int = DEFAULT_PAGE_SIZE,
        delay: float = DEFAULT_DELAY,
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
            except Exception as exc:
                self.logger.error("Failed to enrich %s: %s", summary.detail_url, exc)
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
            response = self.session.post(JOBS_ENDPOINT, json=payload, timeout=40)
            if response.status_code == 400 and not self._bootstrapped:
                self.logger.info("Retrying Workday jobs request after session bootstrap")
                self._ensure_session_bootstrap(force=True)
                response = self.session.post(JOBS_ENDPOINT, json=payload, timeout=40)

            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                snippet = response.text[:200].strip()
                raise ScraperError(f"Workday jobs request failed: {exc} :: {snippet}") from exc

            data = response.json()
            job_postings = data.get("jobPostings") or []
            if not job_postings:
                self.logger.info("No job postings returned at offset %s; stopping", offset)
                break

            if total is None:
                total = _safe_int(data.get("total"))
                if total is not None:
                    self.logger.info("FICO Workday reports %s active jobs.", total)

            for raw in job_postings:
                detail_path = _strip_or_none(raw.get("externalPath")) or ""
                detail_url = detail_path
                if detail_path and not detail_path.startswith("http"):
                    detail_url = urljoin(f"{JOB_DETAIL_BASE.rstrip('/')}/", detail_path.lstrip("/"))

                title = _strip_or_none(raw.get("title"))
                if not title or not detail_url:
                    self.logger.debug("Skipping invalid job payload: %s", raw)
                    continue

                bullet_fields = raw.get("bulletFields") or []
                job_id = _strip_or_none(bullet_fields[0]) if bullet_fields else None

                summary = JobSummary(
                    job_id=job_id,
                    title=title,
                    detail_path=detail_path,
                    detail_url=detail_url,
                    location_text=_strip_or_none(raw.get("locationsText")),
                    posted_on=_strip_or_none(raw.get("postedOn")),
                    time_type=_strip_or_none(raw.get("timeType")),
                )
                yield summary
                retrieved += 1
                if limit is not None and retrieved >= limit:
                    return

            offset += self.page_size
            if total is not None and offset >= total:
                self.logger.info("Reached reported Workday total %s; stopping.", total)
                break

            if self.delay:
                time.sleep(self.delay)

    def _enrich_summary(self, summary: JobSummary) -> JobListing:
        detail_html = self._fetch_detail_html(summary.detail_url)
        json_ld = self._extract_json_ld(detail_html)

        description = "Description unavailable."
        if isinstance(json_ld, dict):
            raw_description = json_ld.get("description") or ""
            if raw_description:
                description = _normalize_description(raw_description)

        date_posted = summary.posted_on
        if isinstance(json_ld, dict):
            date_posted = _strip_or_none(json_ld.get("datePosted")) or date_posted

        metadata: Dict[str, Any] = {
            "job_id": summary.job_id,
            "workday_path": summary.detail_path,
            "locations_text": summary.location_text,
            "posted_on_text": summary.posted_on,
            "time_type": summary.time_type,
            "detail_url": summary.detail_url,
            "apply_url": _build_apply_url(summary.detail_url),
        }

        if isinstance(json_ld, dict):
            identifier = json_ld.get("identifier")
            if isinstance(identifier, dict):
                metadata["identifier"] = {k: v for k, v in identifier.items() if k != "@type"}
            employment_type = _strip_or_none(json_ld.get("employmentType"))
            if employment_type:
                metadata["employment_type"] = employment_type
            hiring_org = json_ld.get("hiringOrganization")
            if isinstance(hiring_org, dict):
                metadata["hiring_organization"] = {k: v for k, v in hiring_org.items() if k != "@type"}
            job_location = json_ld.get("jobLocation")
            if isinstance(job_location, dict):
                cleaned_location = {}
                for key, value in job_location.items():
                    if key == "@type":
                        continue
                    if isinstance(value, dict):
                        cleaned_location[key] = {k: v for k, v in value.items() if k != "@type"}
                    else:
                        cleaned_location[key] = value
                metadata["job_location"] = cleaned_location
            metadata["date_posted_iso"] = json_ld.get("datePosted")

        metadata = {key: value for key, value in metadata.items() if value not in (None, "", [], {})}

        listing_payload = asdict(summary)
        listing_payload.update(
            {
                "description": description[:10000],
                "date_posted": date_posted,
                "metadata": metadata,
            }
        )

        return JobListing(**listing_payload)

    def _fetch_detail_html(self, url: str) -> str:
        headers = dict(self.session.headers)
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        response = self.session.get(url, headers=headers, timeout=40)
        response.raise_for_status()

        if "application/json" in response.headers.get("Content-Type", ""):
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
    def _extract_json_ld(html_text: str) -> Dict[str, Any]:
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
        return data if isinstance(data, dict) else {"raw": data}

    def _ensure_session_bootstrap(self, force: bool = False) -> None:
        if self._bootstrapped and not force:
            return
        resp = self.session.get(SESSION_SEED_URL, timeout=40)
        resp.raise_for_status()
        self._bootstrapped = True


def persist_listing(listing: JobListing) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location_text or "")[:255] or None,
        "date": (listing.date_posted or "")[:100] or None,
        "description": listing.description[:10000],
        "metadata": listing.metadata,
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Persisted FICO job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape FICO Workday job listings.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum jobs to process.")
    parser.add_argument(
        "--page-size", type=int, default=DEFAULT_PAGE_SIZE, help="Jobs per Workday request."
    )
    parser.add_argument(
        "--delay", type=float, default=DEFAULT_DELAY, help="Seconds to pause between requests."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and print jobs without writing to the database.",
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

    scraper = FICOJobScraper(page_size=args.page_size, delay=args.delay)
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
        except Exception as exc:
            logging.error("Failed to persist %s: %s", listing.detail_url, exc)
            totals["errors"] += 1

    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["dedupe"] = dedupe_summary

    logging.info(
        "FICO scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )
    if "dedupe" in totals:
        logging.info("Deduplication summary: %s", totals["dedupe"])
    return 0 if not totals["errors"] else 1


def _strip_or_none(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    stripped = value.strip()
    return stripped or None


def _safe_int(value: Optional[object]) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _normalize_description(raw_html: str) -> str:
    text = html.unescape(raw_html)
    soup = BeautifulSoup(text, "html.parser")
    normalized = soup.get_text("\n", strip=True)
    return normalized.replace("\u202f", " ").replace("\xa0", " ").strip()


def _build_apply_url(detail_url: str) -> Optional[str]:
    if not detail_url:
        return None
    return detail_url.rstrip("/") + "/apply"


if __name__ == "__main__":
    raise SystemExit(main())
