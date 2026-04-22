#!/usr/bin/env python3
"""Manual scraper for Expeditors careers (SmartRecruiters-powered).

The careers page at https://www.expeditors.com/careers/jobs embeds the
SmartRecruiters widget. This script pages through the public widget API,
visits each job detail page for rich content, and stores the results via the
shared Django `JobPosting` model so they can surface in Kumquat dashboards.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

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
CAREERS_PAGE_URL = "https://www.expeditors.com/careers/jobs"
SMARTRECRUITERS_SEARCH_ENDPOINT = (
    "https://www.smartrecruiters.com/job-api/public/search/widgets/Expeditors/postings"
)
SMARTRECRUITERS_COMPANY = "Expeditors"
SMARTRECRUITERS_LANG_CODE = "en_US"
JOB_PAGE_BASE_URL = "https://jobs.smartrecruiters.com/Expeditors"
APPLY_URL_TEMPLATE = (
    "https://jobs.smartrecruiters.com/oneclick-ui/company/{company}/publication/{publication}"
    "?dcr_ci={company}"
)

API_TIMEOUT = 45
DETAIL_TIMEOUT = 45
DEFAULT_PAGE_SIZE = 50
DEFAULT_DETAIL_DELAY = 0.35

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}
API_HEADERS = {**DEFAULT_HEADERS, "Accept": "application/json"}
HTML_HEADERS = {
    **DEFAULT_HEADERS,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": CAREERS_PAGE_URL,
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 60)
SCRAPER_QS = Scraper.objects.filter(company="Expeditors", url=CAREERS_PAGE_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Expeditors scrapers found; using id=%s.", SCRAPER.id)
else:  # pragma: no cover - initial bootstrap path
    SCRAPER = Scraper.objects.create(
        company="Expeditors",
        url=CAREERS_PAGE_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when an unrecoverable scraping issue occurs."""


@dataclass
class JobListing:
    job_vacancy_id: Optional[str]
    uuid: Optional[str]
    publication_id: str
    url_job_name: str
    title: str
    detail_url: str
    location: Optional[str]
    released_date: Optional[str]
    employment_type: Optional[str]
    department: Optional[str]
    ref_number: Optional[str]
    remote: bool
    country: Optional[str]
    description_text: str
    description_html: Optional[str]
    metadata: Dict[str, object]


def _iso_date_from_ms(timestamp: Optional[int]) -> Optional[str]:
    if not timestamp:
        return None
    try:
        dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
        return dt.date().isoformat()
    except (OverflowError, OSError, ValueError, TypeError):
        return None


def _compose_location(raw: Dict[str, object]) -> Optional[str]:
    parts: List[str] = []
    city = (raw.get("location") or "").strip()
    region = (raw.get("regionAbbreviation") or "").strip()
    country = (raw.get("countryName") or "").strip()
    for component in (city, region, country):
        if component and component not in parts:
            parts.append(component)
    return ", ".join(parts) if parts else None


def _compact_metadata(data: Dict[str, object]) -> Dict[str, object]:
    return {
        key: value
        for key, value in data.items()
        if value not in (None, "", [], {})
    }


class ExpeditorsJobScraper:
    def __init__(
        self,
        *,
        page_size: int = DEFAULT_PAGE_SIZE,
        detail_delay: float = DEFAULT_DETAIL_DELAY,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.page_size = max(1, page_size)
        self.detail_delay = max(0.0, detail_delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    def scrape(self, *, limit: Optional[int] = None) -> Iterable[JobListing]:
        offset = 0
        yielded = 0
        total: Optional[int] = None

        while True:
            payload = self._fetch_postings(offset=offset, limit=self.page_size)
            results = payload.get("results") or []
            if not results:
                self.logger.info("No results returned at offset %s; stopping pagination.", offset)
                break

            total = total or payload.get("numFound")
            for raw in results:
                try:
                    listing = self._build_listing(raw)
                except ScraperError as exc:
                    job_id = raw.get("jobVacancyId") or raw.get("publicationId")
                    self.logger.warning("Skipping job %s (%s)", job_id, exc)
                    continue

                yield listing
                yielded += 1

                if limit is not None and yielded >= limit:
                    return

                if self.detail_delay:
                    time.sleep(self.detail_delay)

            offset += len(results)
            if total is not None and offset >= int(total):
                self.logger.info("Reached total count %s; pagination complete.", total)
                break

    def _fetch_postings(self, *, offset: int, limit: int) -> Dict[str, object]:
        params = {
            "dcr_ci": SMARTRECRUITERS_COMPANY,
            "offset": offset,
            "limit": limit,
            "langCode": SMARTRECRUITERS_LANG_CODE,
            "locationType": "ANY",
            "fq": "",
            "customFields": "",
        }
        self.logger.debug("Requesting postings offset=%s limit=%s", offset, limit)
        response = self.session.get(
            SMARTRECRUITERS_SEARCH_ENDPOINT,
            headers=API_HEADERS,
            params=params,
            timeout=API_TIMEOUT,
        )
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:  # pragma: no cover - defensive logging
            snippet = response.text[:400].strip()
            raise ScraperError(f"SmartRecruiters postings request failed: {exc} | {snippet}") from exc
        try:
            return response.json()
        except json.JSONDecodeError as exc:  # pragma: no cover - defensive logging
            raise ScraperError(f"Failed to decode postings JSON: {exc}") from exc

    def _build_listing(self, raw: Dict[str, object]) -> JobListing:
        publication_id = str(raw.get("publicationId") or "").strip()
        url_job_name = (raw.get("urlJobName") or "").strip()
        title = (raw.get("vacancyName") or "").strip()

        if not publication_id or not url_job_name or not title:
            raise ScraperError("Missing publicationId, urlJobName, or title.")

        detail_url = f"{JOB_PAGE_BASE_URL}/{publication_id}-{url_job_name}"
        description_text, description_html = self._fetch_and_parse_detail(detail_url)
        released_date = _iso_date_from_ms(raw.get("releasedDate"))

        apply_url = APPLY_URL_TEMPLATE.format(
            company=SMARTRECRUITERS_COMPANY,
            publication=publication_id,
        )

        metadata = _compact_metadata(
            {
                "job_vacancy_id": raw.get("jobVacancyId"),
                "uuid": raw.get("uuid"),
                "company_identifier": raw.get("companyIdentifier"),
                "ref_number": raw.get("refNumber"),
                "department": raw.get("department"),
                "department_id": raw.get("departmentId"),
                "type_of_employment": raw.get("typeOfEmployment"),
                "region": raw.get("regionAbbreviation"),
                "country": raw.get("countryName"),
                "country_code": raw.get("countryAbbreviation"),
                "location_remote": bool(raw.get("locationRemote")),
                "released_date_ms": raw.get("releasedDate"),
                "custom_field_values": raw.get("customFieldValues"),
                "apply_url": apply_url,
                "language_code": raw.get("languageCode"),
                "description_html": description_html,
            }
        )

        return JobListing(
            job_vacancy_id=str(raw.get("jobVacancyId") or "").strip() or None,
            uuid=str(raw.get("uuid") or "").strip() or None,
            publication_id=publication_id,
            url_job_name=url_job_name,
            title=title,
            detail_url=detail_url,
            location=_compose_location(raw),
            released_date=released_date,
            employment_type=(raw.get("typeOfEmployment") or "").strip() or None,
            department=(raw.get("department") or "").strip() or None,
            ref_number=(raw.get("refNumber") or "").strip() or None,
            remote=bool(raw.get("locationRemote")),
            country=(raw.get("countryName") or "").strip() or None,
            description_text=description_text,
            description_html=description_html,
            metadata=metadata,
        )

    def _fetch_and_parse_detail(self, url: str) -> Tuple[str, Optional[str]]:
        self.logger.debug("Fetching job detail page %s", url)
        response = self.session.get(url, headers=HTML_HEADERS, timeout=DETAIL_TIMEOUT)
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            snippet = response.text[:200].strip()
            raise ScraperError(f"Job detail fetch failed: {exc} | {snippet}") from exc

        soup = BeautifulSoup(response.text, "html.parser")
        main = soup.select_one("main.jobad-main")
        sections = main.select("section.job-section") if main else []

        if sections:
            description_html = "\n".join(str(section) for section in sections)
            description_text = "\n\n".join(section.get_text("\n", strip=True) for section in sections)
        elif main:
            description_html = str(main)
            description_text = main.get_text("\n", strip=True)
        else:
            description_html = None
            description_text = soup.get_text("\n", strip=True)

        description_text = (description_text or "").replace("\xa0", " ").replace("\u202f", " ").strip()
        if not description_text:
            description_text = "Description unavailable."

        return description_text, description_html


def persist_listing(listing: JobListing) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255] or None,
        "date": (listing.released_date or "")[:100] or None,
        "description": (listing.description_text or "")[:10000],
        "metadata": listing.metadata,
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Saved Expeditors job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Expeditors careers listings.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help="Number of jobs to request per SmartRecruiters API page (default: 50).",
    )
    parser.add_argument(
        "--detail-delay",
        type=float,
        default=DEFAULT_DETAIL_DELAY,
        help="Seconds to sleep between job detail requests (default: 0.35).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch jobs but do not write them to the database.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s: %(message)s")

    scraper = ExpeditorsJobScraper(page_size=args.page_size, detail_delay=args.detail_delay)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    for listing in scraper.scrape(limit=args.limit):
        totals["fetched"] += 1

        if args.dry_run:
            print(json.dumps(listing.__dict__, ensure_ascii=False, default=str))
            continue

        try:
            created = persist_listing(listing)
            if created:
                totals["created"] += 1
            else:
                totals["updated"] += 1
        except Exception as exc:  # pragma: no cover - defensive persistence path
            logging.error("Failed to persist job %s: %s", listing.detail_url, exc)
            totals["errors"] += 1

    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["dedupe"] = dedupe_summary

    logging.info(
        "Expeditors scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )
    return 0 if not totals["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
