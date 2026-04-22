#!/usr/bin/env python3
"""Manual scraper for Broadridge (Workday-powered careers site).

The Broadridge careers experience is powered by Workday and exposed at
https://broadridge.wd5.myworkdayjobs.com/Careers. This script mirrors the
requests the front end issues, normalizes the results, and stores them through
the Django ORM so they appear inside Kumquat.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Django bootstrap
# ---------------------------------------------------------------------------
CURRENT_FILE = Path(__file__).resolve()
parents = list(CURRENT_FILE.parents)
default_backend_dir = parents[2] if len(parents) > 2 else parents[-1]
BACKEND_DIR = next(
    (candidate for candidate in parents if (candidate / "manage.py").exists()),
    default_backend_dir,
)
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
CAREERS_URL = "https://broadridge.wd5.myworkdayjobs.com/Careers"
WORKDAY_ROOT = "https://broadridge.wd5.myworkdayjobs.com"
TENANT = "broadridge"
PORTAL = "Careers"
CXS_BASE = f"{WORKDAY_ROOT}/wday/cxs/{TENANT}/{PORTAL}"
JOBS_ENDPOINT = f"{CXS_BASE}/jobs"
JOB_DETAIL_BASE = CAREERS_URL
SESSION_SEED_URL = f"{WORKDAY_ROOT}/en-US/{PORTAL}"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": WORKDAY_ROOT,
    "Referer": SESSION_SEED_URL,
}

SCRAPER_QS = Scraper.objects.filter(company="Broadridge", url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning(
            "Multiple Scraper rows matched Broadridge; using id=%s.", SCRAPER.id
        )
else:
    SCRAPER = Scraper.objects.create(
        company="Broadridge",
        url=CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=900,
    )


class ScraperError(RuntimeError):
    """Raised when the scrape pipeline cannot continue safely."""


@dataclass
class JobSummary:
    job_id: Optional[str]
    title: str
    detail_path: str
    detail_url: str
    location_text: Optional[str]
    posted_on: Optional[str]


@dataclass
class JobListing(JobSummary):
    description: str
    metadata: Dict[str, object]


class BroadridgeJobScraper:
    """Client for interacting with the Broadridge Workday job listings."""

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
    def scrape(self, *, limit: Optional[int] = None) -> Iterator[JobListing]:
        fetched = 0
        for summary in self._iter_summaries(limit=limit):
            try:
                listing = self._enrich_summary(summary)
            except Exception as exc:  # pragma: no cover - defensive guard
                self.logger.error("Failed to enrich job %s: %s", summary.detail_url, exc)
                continue

            yield listing
            fetched += 1
            if limit is not None and fetched >= limit:
                break

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _iter_summaries(self, *, limit: Optional[int]) -> Iterator[JobSummary]:
        self._ensure_session_bootstrap()

        offset = 0
        retrieved = 0
        total: Optional[int] = None
        retry_bootstrap = False

        while True:
            payload = {
                "limit": self.page_size,
                "offset": offset,
                "searchText": "",
                "appliedFacets": {},
                "userPreferredLanguage": "en-US",
            }

            self.logger.debug("Requesting Workday jobs offset=%s", offset)
            response = self.session.post(JOBS_ENDPOINT, json=payload, timeout=40)

            if response.status_code in (400, 401, 422) and not retry_bootstrap:
                self.logger.info(
                    "Workday returned status %s; retrying after session bootstrap.",
                    response.status_code,
                )
                self._ensure_session_bootstrap(force=True)
                retry_bootstrap = True
                response = self.session.post(JOBS_ENDPOINT, json=payload, timeout=40)

            retry_bootstrap = False

            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                snippet = response.text[:200].strip()
                raise ScraperError(
                    f"Workday jobs request failed ({response.status_code}): {snippet}"
                ) from exc

            data = response.json()
            job_postings = data.get("jobPostings") or []
            if not job_postings:
                self.logger.info("No job postings found at offset %s; stopping.", offset)
                return

            if total is None:
                try:
                    total = int(data.get("total") or 0)
                except (TypeError, ValueError):
                    total = None

            for raw in job_postings:
                detail_path = (raw.get("externalPath") or "").strip()
                detail_url = detail_path
                if detail_path and not detail_path.startswith("http"):
                    detail_url = urljoin(JOB_DETAIL_BASE.rstrip("/") + "/", detail_path.lstrip("/"))

                summary = JobSummary(
                    job_id=_first_non_empty(raw.get("bulletFields")),
                    title=_clean_text(raw.get("title")) or "Untitled Job",
                    detail_path=detail_path,
                    detail_url=detail_url,
                    location_text=_clean_text(raw.get("locationsText")),
                    posted_on=_clean_text(raw.get("postedOn")),
                )

                if not summary.detail_url:
                    self.logger.debug("Skipping malformed job payload: %s", raw)
                    continue

                yield summary
                retrieved += 1
                if limit is not None and retrieved >= limit:
                    return

            offset += self.page_size
            if total is not None and offset >= total:
                self.logger.info("Reached Workday reported total (%s); stopping.", total)
                return

            if self.delay:
                time.sleep(self.delay)

    def _enrich_summary(self, summary: JobSummary) -> JobListing:
        detail_html = self._fetch_detail_html(summary.detail_url)

        try:
            json_ld = self._extract_json_ld(detail_html)
        except ScraperError as exc:
            self.logger.warning("Missing JSON-LD for %s (%s)", summary.detail_url, exc)
            json_ld = {}

        description_raw = ""
        date_posted = summary.posted_on
        location_override: Optional[str] = None

        if isinstance(json_ld, dict):
            description_raw = str(json_ld.get("description") or "")
            date_posted = _clean_text(json_ld.get("datePosted")) or date_posted
            job_location = json_ld.get("jobLocation")
            if isinstance(job_location, dict):
                address = job_location.get("address")
                if isinstance(address, dict):
                    location_override = _clean_text(
                        ", ".join(
                            part
                            for part in (
                                address.get("addressLocality"),
                                address.get("addressRegion"),
                                address.get("addressCountry"),
                            )
                            if part
                        )
                    )

        description = _normalize_description(description_raw)
        if not description:
            description = self._fallback_description(detail_html) or "Description unavailable."

        metadata: Dict[str, object] = {
            "job_id": summary.job_id,
            "detail_path": summary.detail_path,
            "posted_on_text": summary.posted_on,
        }
        if json_ld:
            metadata["json_ld"] = json_ld

        summary_dict = asdict(summary)
        summary_dict["posted_on"] = date_posted
        if location_override:
            summary_dict["location_text"] = location_override

        return JobListing(
            **summary_dict,
            description=description,
            metadata=metadata,
        )

    # ------------------------------------------------------------------
    # Networking helpers
    # ------------------------------------------------------------------
    def _fetch_detail_html(self, url: str) -> str:
        headers = dict(self.session.headers)
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        response = self.session.get(url, headers=headers, timeout=40)

        if response.status_code == 403:
            raise ScraperError("Access denied while fetching job detail page.")

        response.raise_for_status()

        content_type = response.headers.get("Content-Type", "")
        if "application/json" in content_type:
            try:
                payload = response.json()
            except ValueError:
                return response.text
            redirect_path = payload.get("url")
            if redirect_path:
                redirect_url = (
                    redirect_path
                    if redirect_path.startswith("http")
                    else urljoin(WORKDAY_ROOT, redirect_path)
                )
                return self._fetch_detail_html(redirect_url)
        return response.text

    def _extract_json_ld(self, html_text: str) -> Dict[str, object]:
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
        if isinstance(data, list):
            for entry in data:
                if isinstance(entry, dict):
                    return entry
        raise ScraperError("Unexpected JSON-LD structure.")

    def _fallback_description(self, html_text: str) -> str:
        soup = BeautifulSoup(html_text, "html.parser")
        container = soup.find("div", attrs={"data-automation-id": "richTextArea"})
        if not container:
            container = soup.find("div", attrs={"data-automation-id": "jobPostingDescription"})
        if not container:
            return ""
        text = container.get_text("\n", strip=True)
        return _clean_text(text) or ""

    def _ensure_session_bootstrap(self, *, force: bool = False) -> None:
        if self._bootstrapped and not force:
            return

        headers = dict(self.session.headers)
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"

        try:
            response = self.session.get(SESSION_SEED_URL, headers=headers, timeout=40)
            if response.status_code >= 400:
                self.logger.warning(
                    "Session bootstrap returned status %s; continuing anyway.",
                    response.status_code,
                )
        except requests.RequestException as exc:  # pragma: no cover - network guard
            self.logger.warning("Failed to bootstrap Workday session: %s", exc)
        finally:
            self._bootstrapped = True


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


def _normalize_description(raw: str) -> str:
    cleaned = _clean_text(raw) or ""
    if not cleaned:
        return ""
    soup = BeautifulSoup(cleaned, "html.parser")
    text = soup.get_text("\n", strip=True)
    lines = [line.strip() for line in text.splitlines()]
    return "\n".join(line for line in lines if line)


def _first_non_empty(items: Optional[Iterable[object]]) -> Optional[str]:
    if not items:
        return None
    for item in items:
        cleaned = _clean_text(item)
        if cleaned:
            return cleaned
    return None


def persist_listing(listing: JobListing) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location_text or "")[:255] or None,
        "date": (listing.posted_on or "")[:100] or None,
        "description": listing.description[:10000],
        "metadata": listing.metadata,
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Persisted Broadridge job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape Broadridge Workday careers job listings."
    )
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
        help="Seconds to sleep between pagination requests (default: 0.25).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print jobs instead of storing them.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(levelname)s: %(message)s",
    )

    scraper = BroadridgeJobScraper(page_size=args.page_size, delay=args.delay)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    for listing in scraper.scrape(limit=args.limit):
        totals["fetched"] += 1
        if args.dry_run:
            print(json.dumps(asdict(listing), default=str, ensure_ascii=False))
            continue

        try:
            created = persist_listing(listing)
            if created:
                totals["created"] += 1
            else:
                totals["updated"] += 1
        except Exception as exc:  # pragma: no cover - persistence safeguards
            logging.error("Failed to persist job %s: %s", listing.detail_url, exc)
            totals["errors"] += 1

    if not args.dry_run:
        totals["dedupe"] = deduplicate_job_postings(scraper=SCRAPER)

    logging.info(
        "Broadridge scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )

    return 0 if totals["errors"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

