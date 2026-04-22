#!/usr/bin/env python3
"""Manual scraper for Corpay careers (Dayforce + Workday)."""
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
from typing import Dict, Iterable, Iterator, List, Optional

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
from django.db import IntegrityError  # noqa: E402

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

# ---------------------------------------------------------------------------
# Constants & configuration
# ---------------------------------------------------------------------------
CAREERS_URL = "https://www.corpay.com/careers"
SCRAPER_QS = Scraper.objects.filter(company="Corpay", url=CAREERS_URL).order_by("id")
DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 3600), 120)

if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched Corpay; using id=%s.", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Corpay",
        url=CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )

UA_STRING = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/127.0.0.0 Safari/537.36"
)

DAYFORCE_BASE_URL = "https://jobs.dayforcehcm.com"
DAYFORCE_CLIENT_NAMESPACE = "corpay"
DAYFORCE_JOB_BOARD_CODE = "candidateportal"
DAYFORCE_CULTURE_CODE = "en-US"
DAYFORCE_CAREERS_URL = f"{DAYFORCE_BASE_URL}/{DAYFORCE_CLIENT_NAMESPACE}/{DAYFORCE_JOB_BOARD_CODE}"
DAYFORCE_SEARCH_URL = f"{DAYFORCE_BASE_URL}/api/jobposting/search"
DAYFORCE_CSRF_URL = f"{DAYFORCE_BASE_URL}/api/auth/csrf"
DAYFORCE_DETAIL_TEMPLATE = (
    f"{DAYFORCE_BASE_URL}/api/jobposting/{{client_namespace}}/{{culture_code}}/{{job_board_id}}/{{job_posting_id}}"
)
DAYFORCE_PUBLIC_URL_TEMPLATE = (
    f"{DAYFORCE_BASE_URL}/{{client_namespace}}/{{job_board_code}}/jobs/{{job_posting_id}}"
)

WORKDAY_HOST = "https://corpay.wd103.myworkdayjobs.com"
WORKDAY_SITE = "Ext_001"
WORKDAY_JOBS_ENDPOINT = f"{WORKDAY_HOST}/wday/cxs/corpay/{WORKDAY_SITE}/jobs"
WORKDAY_PUBLIC_BASE = f"{WORKDAY_HOST}/en-US/{WORKDAY_SITE}/"
WORKDAY_JOB_SEARCH_URL = f"{WORKDAY_HOST}/{WORKDAY_SITE}"
WORKDAY_DETAIL_PREFIX = f"{WORKDAY_HOST}/wday/cxs/corpay/{WORKDAY_SITE}"
WORKDAY_DEFAULT_PAGE_SIZE = 20

# ---------------------------------------------------------------------------
# Exceptions & data structures
# ---------------------------------------------------------------------------


class ScraperError(RuntimeError):
    """Raised when the scrape pipeline cannot proceed."""


@dataclass
class JobRecord:
    title: str
    link: str
    location: Optional[str]
    date: Optional[str]
    description: str
    metadata: Dict[str, object]
    source: str


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------
def _html_to_text(value: Optional[str]) -> str:
    if not value:
        return ""
    soup = BeautifulSoup(value, "html.parser")
    text = soup.get_text("\n", strip=True)
    lines = [line.strip() for line in text.splitlines()]
    return "\n".join(line for line in lines if line)


def _normalize_dayforce_locations(locations: Optional[Iterable[Dict[str, object]]]) -> Optional[str]:
    if not locations:
        return None
    dedup: Dict[str, None] = {}
    for raw in locations:
        if not isinstance(raw, dict):
            continue
        formatted = (raw.get("formattedAddress") or "").strip()
        if formatted:
            dedup.setdefault(formatted, None)
            continue
        city = (raw.get("cityName") or "").strip()
        state = (raw.get("stateCode") or "").strip()
        country = (raw.get("isoCountryCode") or "").strip()
        parts = [part for part in (city, state, country) if part]
        if parts:
            dedup.setdefault(", ".join(parts), None)
    if not dedup:
        return None
    return "; ".join(dedup.keys())


# ---------------------------------------------------------------------------
# Dayforce client
# ---------------------------------------------------------------------------


class DayforceCorpayClient:
    def __init__(self, *, culture_code: str = DAYFORCE_CULTURE_CODE, delay: float = 0.0) -> None:
        self.client_namespace = DAYFORCE_CLIENT_NAMESPACE
        self.job_board_code = DAYFORCE_JOB_BOARD_CODE
        self.culture_code = culture_code
        self.delay = max(0.0, delay)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": UA_STRING,
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Origin": DAYFORCE_BASE_URL,
                "Referer": DAYFORCE_CAREERS_URL,
            }
        )
        self.csrf_token: Optional[str] = None
        self.logger = logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------------ #
    # HTTP helpers                                                       #
    # ------------------------------------------------------------------ #
    def _ensure_csrf_token(self, *, force: bool = False) -> None:
        if self.csrf_token and not force:
            return
        try:
            response = self.session.get(DAYFORCE_CSRF_URL, timeout=30)
            response.raise_for_status()
        except requests.RequestException as exc:  # pragma: no cover - network guard
            raise ScraperError(f"Failed to fetch CSRF token: {exc}") from exc
        payload = response.json()
        token = payload.get("csrfToken")
        if not token:
            raise ScraperError("Missing csrfToken in CSRF response payload.")
        self.csrf_token = token

    def _post_search(self, *, pagination_start: int) -> Dict[str, object]:
        self._ensure_csrf_token()
        payload = {
            "clientNamespace": self.client_namespace,
            "jobBoardCode": self.job_board_code,
            "cultureCode": self.culture_code,
            "paginationStart": pagination_start,
        }
        headers = {"X-CSRF-TOKEN": self.csrf_token or ""}
        try:
            response = self.session.post(
                DAYFORCE_SEARCH_URL,
                json=payload,
                headers=headers,
                timeout=30,
            )
            if response.status_code == 403:
                self.logger.info("Dayforce search returned 403; refreshing CSRF token.")
                self._ensure_csrf_token(force=True)
                headers["X-CSRF-TOKEN"] = self.csrf_token or ""
                response = self.session.post(
                    DAYFORCE_SEARCH_URL,
                    json=payload,
                    headers=headers,
                    timeout=30,
                )
            response.raise_for_status()
        except requests.RequestException as exc:  # pragma: no cover - network guard
            raise ScraperError(f"Dayforce search request failed: {exc}") from exc
        return response.json()

    def _fetch_detail(self, *, job_board_id: int, job_posting_id: int) -> Optional[Dict[str, object]]:
        detail_url = DAYFORCE_DETAIL_TEMPLATE.format(
            client_namespace=self.client_namespace,
            culture_code=self.culture_code,
            job_board_id=job_board_id,
            job_posting_id=job_posting_id,
        )
        try:
            response = self.session.get(detail_url, timeout=30)
            if response.status_code == 404:
                return None
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Dayforce detail request failed: {exc}") from exc
        return response.json()

    # ------------------------------------------------------------------ #
    # Public API                                                         #
    # ------------------------------------------------------------------ #
    def iter_jobs(
        self,
        *,
        limit: Optional[int] = None,
        skip_details: bool = False,
    ) -> Iterator[JobRecord]:
        fetched = 0
        pagination_start = 0

        while True:
            payload = self._post_search(pagination_start=pagination_start)
            job_postings = payload.get("jobPostings") or []
            if not job_postings:
                self.logger.info("Dayforce returned no postings at offset %s.", pagination_start)
                break

            count = payload.get("count", len(job_postings))
            offset = payload.get("offset", pagination_start)
            max_count = payload.get("maxCount")

            for summary in job_postings:
                job_posting_id = summary.get("jobPostingId")
                job_board_id = summary.get("jobBoardId")
                if job_posting_id is None or job_board_id is None:
                    continue

                detail_payload = None
                if not skip_details:
                    try:
                        detail_payload = self._fetch_detail(
                            job_board_id=job_board_id,
                            job_posting_id=job_posting_id,
                        )
                    except ScraperError as exc:
                        self.logger.warning(
                            "Dayforce detail fetch failed for %s: %s", job_posting_id, exc
                        )
                        detail_payload = None

                record = self._build_record(summary=summary, detail=detail_payload)
                yield record
                fetched += 1
                if limit is not None and fetched >= limit:
                    return

            pagination_start = offset + count
            if max_count is not None and pagination_start >= max_count:
                break
            if self.delay:
                time.sleep(self.delay)

    def _build_record(
        self,
        *,
        summary: Dict[str, object],
        detail: Optional[Dict[str, object]],
    ) -> JobRecord:
        job_posting_id = summary.get("jobPostingId")
        if job_posting_id is None:
            raise ScraperError("Dayforce summary missing jobPostingId.")

        link = DAYFORCE_PUBLIC_URL_TEMPLATE.format(
            client_namespace=self.client_namespace,
            job_board_code=self.job_board_code,
            job_posting_id=job_posting_id,
        )

        summary_locations = summary.get("postingLocations")
        detail_locations = detail.get("postingLocations") if detail else None
        location = _normalize_dayforce_locations(detail_locations) or _normalize_dayforce_locations(
            summary_locations
        )

        description_html = None
        if detail:
            content = detail.get("jobPostingContent") or {}
            parts = [
                content.get("jobDescriptionHeader"),
                content.get("jobDescription"),
                content.get("jobDescriptionFooter"),
            ]
            description_html = "\n".join([part for part in parts if part])
        if not description_html:
            description_html = summary.get("jobDescription")

        description_text = _html_to_text(description_html)

        metadata: Dict[str, object] = {
            "source": "dayforce",
            "jobPostingId": job_posting_id,
            "jobBoardId": summary.get("jobBoardId"),
            "jobReqId": summary.get("jobReqId"),
            "jobPostingLocations": summary.get("postingLocations"),
            "hasVirtualLocation": summary.get("hasVirtualLocation"),
            "postingStartTimestampUTC": summary.get("postingStartTimestampUTC"),
            "postingExpiryTimestampUTC": summary.get("postingExpiryTimestampUTC"),
            "searchScore": summary.get("searchScore"),
        }

        if detail:
            metadata.update(
                {
                    "jobPostingContent": detail.get("jobPostingContent"),
                    "jobPostingAttributes": detail.get("jobPostingAttributes"),
                    "postingType": detail.get("postingType"),
                    "relocationEligible": detail.get("relocationEligible"),
                    "createdTimestampUTC": detail.get("createdTimestampUTC"),
                    "lastModifiedTimestampUTC": detail.get("lastModifiedTimestampUTC"),
                }
            )
        if description_html:
            metadata["description_html"] = description_html

        return JobRecord(
            title=(summary.get("jobTitle") or "").strip(),
            link=link,
            location=location,
            date=(summary.get("postingStartTimestampUTC") or "").strip() or None,
            description=description_text or "Description unavailable.",
            metadata=metadata,
            source="dayforce",
        )


# ---------------------------------------------------------------------------
# Workday client
# ---------------------------------------------------------------------------


class WorkdayCorpayClient:
    def __init__(
        self,
        *,
        page_size: int = WORKDAY_DEFAULT_PAGE_SIZE,
        delay: float = 0.25,
    ) -> None:
        self.page_size = max(1, min(page_size, WORKDAY_DEFAULT_PAGE_SIZE))
        self.delay = max(0.0, delay)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": UA_STRING,
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/json",
                "Referer": WORKDAY_JOB_SEARCH_URL,
            }
        )
        self.logger = logging.getLogger(self.__class__.__name__)
        self._bootstrapped = False

    def _ensure_bootstrap(self, *, force: bool = False) -> None:
        if self._bootstrapped and not force:
            return
        response = self.session.get(WORKDAY_JOB_SEARCH_URL, timeout=30)
        response.raise_for_status()
        self._bootstrapped = True

    def _fetch_page(self, *, offset: int) -> Dict[str, object]:
        payload = {
            "appliedFacets": {},
            "limit": self.page_size,
            "offset": offset,
            "searchText": "",
            "userPreferredLanguage": "en-US",
        }
        response = self.session.post(WORKDAY_JOBS_ENDPOINT, json=payload, timeout=30)
        if response.status_code in {400, 403}:
            self.logger.info("Workday API returned %s; retrying after bootstrap.", response.status_code)
            self._ensure_bootstrap(force=True)
            response = self.session.post(WORKDAY_JOBS_ENDPOINT, json=payload, timeout=30)
        response.raise_for_status()
        return response.json()

    def _fetch_detail(self, *, external_path: str) -> Dict[str, object]:
        detail_url = f"{WORKDAY_DETAIL_PREFIX}{external_path}"
        response = self.session.get(detail_url, timeout=30)
        response.raise_for_status()
        return response.json()

    def iter_jobs(self, *, limit: Optional[int] = None) -> Iterator[JobRecord]:
        offset = 0
        fetched = 0

        while True:
            page = self._fetch_page(offset=offset)
            postings = page.get("jobPostings") or []
            if not postings:
                self.logger.info("Workday returned no postings at offset %s.", offset)
                break

            for summary in postings:
                external_path = (summary.get("externalPath") or "").strip()
                if not external_path:
                    continue
                detail = self._fetch_detail(external_path=external_path)
                record = self._build_record(summary=summary, detail=detail)
                yield record
                fetched += 1
                if limit is not None and fetched >= limit:
                    return
                if self.delay:
                    time.sleep(self.delay)

            offset += len(postings)
            total = page.get("total")
            if total is not None:
                try:
                    total_int = int(total)
                except (TypeError, ValueError):
                    total_int = None
                if total_int is not None and offset >= total_int:
                    break

            if self.delay:
                time.sleep(self.delay)

    def _build_record(self, *, summary: Dict[str, object], detail: Dict[str, object]) -> JobRecord:
        external_path = summary.get("externalPath") or ""
        detail_url = urljoin(WORKDAY_PUBLIC_BASE, external_path.lstrip("/"))
        info = detail.get("jobPostingInfo") or {}
        description_html = info.get("jobDescription") or ""
        description_text = _html_to_text(description_html) or "Description unavailable."
        bullet_fields = summary.get("bulletFields") or []

        metadata: Dict[str, object] = {
            "source": "workday",
            "workdayExternalPath": external_path,
            "jobPostingId": info.get("jobPostingId"),
            "jobReqId": info.get("jobReqId"),
            "timeType": info.get("timeType"),
            "jobPostingInfo": info,
            "bulletFields": bullet_fields,
            "similarJobs": detail.get("similarJobs"),
            "hiringOrganization": detail.get("hiringOrganization"),
        }
        if description_html:
            metadata["description_html"] = description_html

        return JobRecord(
            title=(summary.get("title") or "").strip(),
            link=detail_url,
            location=(summary.get("locationsText") or "").strip() or None,
            date=(summary.get("postedOn") or "").strip() or None,
            description=description_text,
            metadata=metadata,
            source="workday",
        )


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------


def persist_job(record: JobRecord, *, dry_run: bool = False) -> str:
    if dry_run:
        print(json.dumps(record.__dict__, default=str, ensure_ascii=False))
        return "skipped"

    defaults = {
        "title": record.title[:255],
        "location": (record.location or "")[:255] or None,
        "date": (record.date or "")[:100] or None,
        "description": record.description[:10000],
        "metadata": record.metadata,
    }
    try:
        obj, created = JobPosting.objects.update_or_create(
            scraper=SCRAPER,
            link=record.link,
            defaults=defaults,
        )
    except IntegrityError as exc:  # pragma: no cover - DB safety
        logging.warning("Failed to persist job %s due to integrity error: %s", record.link, exc)
        return "error"

    if created:
        return "created"
    return "updated"


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Corpay careers (Dayforce + Workday).")
    parser.add_argument("--dayforce-limit", type=int, default=None, help="Maximum Dayforce jobs to process.")
    parser.add_argument("--workday-limit", type=int, default=None, help="Maximum Workday jobs to process.")
    parser.add_argument(
        "--skip-dayforce", action="store_true", help="Skip fetching Dayforce (North America) jobs."
    )
    parser.add_argument(
        "--skip-workday", action="store_true", help="Skip fetching Workday (international) jobs."
    )
    parser.add_argument(
        "--dayforce-delay", type=float, default=0.0, help="Seconds to sleep between Dayforce result pages."
    )
    parser.add_argument(
        "--workday-delay", type=float, default=0.25, help="Seconds to sleep between Workday requests."
    )
    parser.add_argument(
        "--dayforce-skip-details",
        action="store_true",
        help="Skip Dayforce detail hydration (faster, but reduced descriptions).",
    )
    parser.add_argument("--workday-page-size", type=int, default=WORKDAY_DEFAULT_PAGE_SIZE)
    parser.add_argument("--dry-run", action="store_true", help="Fetch jobs without writing to the database.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    logger = logging.getLogger("corpay_manual")
    start_time = time.time()
    stats = {
        "dayforce": {"fetched": 0, "created": 0, "updated": 0, "skipped": 0, "errors": 0},
        "workday": {"fetched": 0, "created": 0, "updated": 0, "skipped": 0, "errors": 0},
    }

    if not args.skip_dayforce:
        logger.info("Fetching Dayforce (North America) postings.")
        dayforce_client = DayforceCorpayClient(delay=args.dayforce_delay)
        try:
            for record in dayforce_client.iter_jobs(
                limit=args.dayforce_limit,
                skip_details=args.dayforce_skip_details,
            ):
                stats["dayforce"]["fetched"] += 1
                result = persist_job(record, dry_run=args.dry_run)
                if result in {"created", "updated"}:
                    stats["dayforce"][result] += 1
                elif result == "skipped":
                    stats["dayforce"]["skipped"] += 1
                else:
                    stats["dayforce"]["errors"] += 1
        except ScraperError as exc:
            logger.error("Dayforce scrape failed: %s", exc)
            stats["dayforce"]["errors"] += 1

    if not args.skip_workday:
        logger.info("Fetching Workday (international) postings.")
        workday_client = WorkdayCorpayClient(page_size=args.workday_page_size, delay=args.workday_delay)
        try:
            for record in workday_client.iter_jobs(limit=args.workday_limit):
                stats["workday"]["fetched"] += 1
                result = persist_job(record, dry_run=args.dry_run)
                if result in {"created", "updated"}:
                    stats["workday"][result] += 1
                elif result == "skipped":
                    stats["workday"]["skipped"] += 1
                else:
                    stats["workday"]["errors"] += 1
        except ScraperError as exc:
            logger.error("Workday scrape failed: %s", exc)
            stats["workday"]["errors"] += 1

    dedupe_stats: Optional[Dict[str, object]] = None
    if not args.dry_run:
        dedupe_stats = deduplicate_job_postings(scraper=SCRAPER)
        logger.info(
            "Deduplicated postings: removed=%s, considered=%s",
            dedupe_stats.get("removed") if dedupe_stats else None,
            dedupe_stats.get("considered") if dedupe_stats else None,
        )

    duration = time.time() - start_time
    summary = {
        "scraper_id": SCRAPER.id,
        "company": SCRAPER.company,
        "careers_url": CAREERS_URL,
        "dayforce": stats["dayforce"],
        "workday": stats["workday"],
        "dedupe": dedupe_stats,
        "dry_run": args.dry_run,
        "elapsed_seconds": round(duration, 2),
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }

    print(json.dumps(summary, default=str))
    if stats["dayforce"]["errors"] or stats["workday"]["errors"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
