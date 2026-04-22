#!/usr/bin/env python3
"""Manual scraper for Dayforce's public careers site.

The Dayforce careers experience is backed by a JSON API that requires a
per-session CSRF token. This script replays the same workflow as the SPA:

    1. Bootstrap a ``requests.Session`` and obtain a CSRF token from
       ``/api/auth/csrf`` so subsequent POSTs are authorized.
    2. Page through the ``/api/jobposting/search`` endpoint, yielding job
       summaries in batches of 25.
    3. Hydrate each summary with its job-detail payload fetched from
       ``/api/jobposting/{clientNamespace}/{cultureCode}/{jobBoardId}/{jobPostingId}``.
    4. Upsert the postings into the existing ``JobPosting`` table via the
       Django ORM.
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
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Django setup (keeps the script runnable from the management dashboard)
# ---------------------------------------------------------------------------
BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "website.settings")

import django  # noqa: E402
from django.conf import settings  # noqa: E402
from django.db import IntegrityError  # noqa: E402

django.setup()

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

# ---------------------------------------------------------------------------
# Constants & configuration
# ---------------------------------------------------------------------------
BASE_URL = "https://jobs.dayforcehcm.com"
CAREERS_LANDING_URL = "https://www.dayforce.com/who-we-are/careers"
DEFAULT_CLIENT_NAMESPACE = "mydayforce"
DEFAULT_JOB_BOARD_CODE = "alljobs"
DEFAULT_CULTURE_CODE = "en-US"
SEARCH_URL = f"{BASE_URL}/api/jobposting/search"
CSRF_URL = f"{BASE_URL}/api/auth/csrf"
DETAIL_URL_TEMPLATE = (
    f"{BASE_URL}/api/jobposting/{{client_namespace}}/{{culture_code}}/{{job_board_id}}/{{job_posting_id}}"
)
JOB_PAGE_URL_TEMPLATE = (
    f"{BASE_URL}/{{client_namespace}}/{{job_board_code}}/jobs/{{job_posting_id}}"
)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

REQUEST_TIMEOUT = 30

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 30)
SCRAPER_QS = Scraper.objects.filter(company="Dayforce", url=CAREERS_LANDING_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched; using the earliest (id=%s).", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Dayforce",
        url=CAREERS_LANDING_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the scraper encounters an unrecoverable error."""


@dataclass
class DayforceJob:
    job_posting_id: int
    job_req_id: Optional[int]
    title: str
    link: str
    location: Optional[str]
    posted_date: Optional[str]
    apply_url: Optional[str]
    description_html: Optional[str]
    description_text: str
    metadata: Dict[str, object]


def _html_to_text(value: Optional[str]) -> str:
    if not value:
        return ""
    soup = BeautifulSoup(value, "html.parser")
    return soup.get_text("\n", strip=True).strip()


def _normalize_location(locations: Optional[Iterable[Dict[str, object]]]) -> Optional[str]:
    if not locations:
        return None
    deduped: Dict[str, None] = {}
    for loc in locations:
        if not isinstance(loc, dict):
            continue
        formatted = (loc.get("formattedAddress") or "").strip()
        if formatted:
            deduped.setdefault(formatted, None)
            continue

        city = (loc.get("cityName") or "").strip()
        state = (loc.get("stateCode") or "").strip()
        country = (loc.get("isoCountryCode") or "").strip()
        components = [component for component in (city, state, country) if component]
        if components:
            deduped.setdefault(", ".join(components), None)
    if not deduped:
        return None
    return "; ".join(deduped.keys())


class DayforceClient:
    def __init__(
        self,
        *,
        client_namespace: str,
        job_board_code: str,
        culture_code: str = DEFAULT_CULTURE_CODE,
        delay: float = 0.0,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.client_namespace = client_namespace
        self.job_board_code = job_board_code
        self.culture_code = culture_code
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.session.headers.setdefault(
            "Referer",
            f"{BASE_URL}/{self.client_namespace}/{self.job_board_code}",
        )
        self.session.headers.setdefault("Origin", BASE_URL)
        self.csrf_token: Optional[str] = None
        self.logger = logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------------ #
    # HTTP helpers                                                       #
    # ------------------------------------------------------------------ #
    def _ensure_csrf_token(self, *, force: bool = False) -> None:
        if self.csrf_token and not force:
            return

        self.logger.debug("Requesting CSRF token.")
        try:
            resp = self.session.get(CSRF_URL, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as exc:  # pragma: no cover - network guard
            raise ScraperError(f"Failed to fetch CSRF token: {exc}") from exc

        payload = resp.json()
        token = payload.get("csrfToken")
        if not token:
            raise ScraperError("Missing csrfToken in CSRF response payload.")
        self.csrf_token = token
        self.logger.debug("Obtained CSRF token.")

    def _post_search(self, payload: Dict[str, object]) -> Dict[str, object]:
        self._ensure_csrf_token()
        headers = {"X-CSRF-TOKEN": self.csrf_token}
        try:
            resp = self.session.post(
                SEARCH_URL,
                json=payload,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 403:
                # Token may be stale; refresh once and retry.
                self.logger.info("Received 403 from search endpoint; refreshing CSRF token.")
                self._ensure_csrf_token(force=True)
                resp = self.session.post(
                    SEARCH_URL,
                    json=payload,
                    headers={"X-CSRF-TOKEN": self.csrf_token},
                    timeout=REQUEST_TIMEOUT,
                )
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Search request failed: {exc}") from exc
        return resp.json()

    def _fetch_detail(self, *, job_board_id: int, job_posting_id: int) -> Optional[Dict[str, object]]:
        detail_url = DETAIL_URL_TEMPLATE.format(
            client_namespace=self.client_namespace,
            culture_code=self.culture_code,
            job_board_id=job_board_id,
            job_posting_id=job_posting_id,
        )
        try:
            resp = self.session.get(detail_url, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 404:
                self.logger.debug("Detail endpoint returned 404 for job %s.", job_posting_id)
                return None
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch job detail {job_posting_id}: {exc}") from exc
        return resp.json()

    # ------------------------------------------------------------------ #
    # Public API                                                         #
    # ------------------------------------------------------------------ #
    def iter_job_payloads(
        self,
        *,
        max_results: Optional[int] = None,
        skip_details: bool = False,
    ) -> Iterator[Tuple[Dict[str, object], Optional[Dict[str, object]]]]:
        fetched = 0
        pagination_start = 0

        while True:
            search_payload = {
                "clientNamespace": self.client_namespace,
                "jobBoardCode": self.job_board_code,
                "cultureCode": self.culture_code,
                "paginationStart": pagination_start,
            }
            result = self._post_search(search_payload)

            jobs = result.get("jobPostings") or []
            if not jobs:
                self.logger.info("No job postings returned at offset %s; stopping.", pagination_start)
                break

            count = result.get("count", len(jobs))
            offset = result.get("offset", pagination_start)
            max_count = result.get("maxCount")

            for job in jobs:
                detail_payload = None
                if not skip_details:
                    try:
                        detail_payload = self._fetch_detail(
                            job_board_id=job.get("jobBoardId"),
                            job_posting_id=job.get("jobPostingId"),
                        )
                    except ScraperError as exc:  # pragma: no cover - defensive log
                        self.logger.warning("Detail fetch failed for %s: %s", job.get("jobPostingId"), exc)
                        detail_payload = None

                yield job, detail_payload
                fetched += 1

                if max_results is not None and fetched >= max_results:
                    return

            pagination_start = offset + count
            if max_count is not None and pagination_start >= max_count:
                break
            if self.delay:
                time.sleep(self.delay)


def build_job(
    summary: Dict[str, object],
    detail: Optional[Dict[str, object]],
    *,
    client_namespace: str,
    job_board_code: str,
    culture_code: str,
) -> DayforceJob:
    job_posting_id = summary.get("jobPostingId")
    if job_posting_id is None:
        raise ScraperError("Job summary missing jobPostingId.")
    job_board_id = summary.get("jobBoardId")
    title = (summary.get("jobTitle") or "").strip()
    job_req_id = summary.get("jobReqId")

    link = JOB_PAGE_URL_TEMPLATE.format(
        client_namespace=client_namespace,
        job_board_code=job_board_code,
        job_posting_id=job_posting_id,
    )

    summary_locations = summary.get("postingLocations")
    detail_locations = None
    if detail:
        detail_locations = detail.get("postingLocations")

    location = _normalize_location(detail_locations) or _normalize_location(summary_locations)
    posted_date = summary.get("postingStartTimestampUTC")

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
        "jobPostingId": job_posting_id,
        "jobReqId": job_req_id,
        "jobBoardId": job_board_id,
        "hasVirtualLocation": summary.get("hasVirtualLocation"),
        "postingStartTimestampUTC": summary.get("postingStartTimestampUTC"),
        "postingExpiryTimestampUTC": summary.get("postingExpiryTimestampUTC"),
        "searchScore": summary.get("searchScore"),
        "source": "dayforce_api",
    }

    if detail_locations:
        metadata["postingLocations"] = detail_locations
    elif summary_locations:
        metadata["postingLocations"] = summary_locations

    if detail:
        metadata.update(
            {
                "jobPostingAttributes": detail.get("jobPostingAttributes"),
                "jobPostingContent": detail.get("jobPostingContent"),
                "postingType": detail.get("postingType"),
                "relocationEligible": detail.get("relocationEligible"),
                "createdTimestampUTC": detail.get("createdTimestampUTC"),
                "lastModifiedTimestampUTC": detail.get("lastModifiedTimestampUTC"),
            }
        )

    return DayforceJob(
        job_posting_id=job_posting_id,
        job_req_id=job_req_id,
        title=title,
        link=link,
        location=location,
        posted_date=posted_date,
        apply_url=link,
        description_html=description_html,
        description_text=description_text,
        metadata=metadata,
    )


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Dayforce careers and store postings.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument("--delay", type=float, default=0.0, help="Delay between search pages (seconds).")
    parser.add_argument(
        "--client-namespace",
        default=DEFAULT_CLIENT_NAMESPACE,
        help="Client namespace used by the job board.",
    )
    parser.add_argument(
        "--job-board-code",
        default=DEFAULT_JOB_BOARD_CODE,
        help="Job board code used by the Dayforce career site.",
    )
    parser.add_argument(
        "--culture-code",
        default=DEFAULT_CULTURE_CODE,
        help="Culture code (locale) to request from the API.",
    )
    parser.add_argument(
        "--skip-details",
        action="store_true",
        help="Skip the per-job detail request (description will come from the summary payload).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and log jobs without writing to the database.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )
    return parser.parse_args(argv)


def persist_jobs(jobs: Iterable[DayforceJob], *, dry_run: bool = False) -> Dict[str, int]:
    if dry_run:
        return {"created": 0, "updated": 0}

    created = 0
    updated = 0
    for job in jobs:
        defaults = {
            "title": job.title[:255],
            "location": (job.location or "")[:255],
            "date": (job.posted_date or "")[:100],
            "description": job.description_text[:10000],
            "metadata": job.metadata,
        }
        try:
            obj, created_flag = JobPosting.objects.update_or_create(
                scraper=SCRAPER,
                link=job.link,
                defaults=defaults,
            )
        except IntegrityError as exc:  # pragma: no cover - DB safety
            logging.warning("Integrity error while saving %s: %s", job.link, exc)
            continue

        if created_flag:
            created += 1
        else:
            updated += 1

        if job.description_html:
            obj.metadata = {**obj.metadata, "description_html": job.description_html}
            obj.save(update_fields=["metadata"])

    return {"created": created, "updated": updated}


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    logging.info(
        "Starting Dayforce scrape (namespace=%s, board=%s, culture=%s, limit=%s).",
        args.client_namespace,
        args.job_board_code,
        args.culture_code,
        args.limit,
    )

    client = DayforceClient(
        client_namespace=args.client_namespace,
        job_board_code=args.job_board_code,
        culture_code=args.culture_code,
        delay=args.delay,
    )

    harvested_jobs: List[DayforceJob] = []
    for summary, detail in client.iter_job_payloads(
        max_results=args.limit,
        skip_details=args.skip_details,
    ):
        try:
            job = build_job(
                summary,
                detail,
                client_namespace=args.client_namespace,
                job_board_code=args.job_board_code,
                culture_code=args.culture_code,
            )
        except ScraperError as exc:
            logging.warning("Skipping job due to parse error: %s", exc)
            continue

        harvested_jobs.append(job)

    logging.info("Fetched %s job summaries.", len(harvested_jobs))

    persistence_stats = persist_jobs(harvested_jobs, dry_run=args.dry_run)
    dedupe_stats: Optional[Dict[str, object]] = None

    if not args.dry_run:
        dedupe_stats = deduplicate_job_postings(scraper=SCRAPER)
        logging.info(
            "Persistence complete (created=%s, updated=%s, dedup_removed=%s).",
            persistence_stats["created"],
            persistence_stats["updated"],
            dedupe_stats.get("removed", 0) if dedupe_stats else 0,
        )
    else:
        logging.info("Dry run complete; skipping persistence.")

    summary = {
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "scraper_id": SCRAPER.id,
        "company": SCRAPER.company,
        "fetched": len(harvested_jobs),
        "created": persistence_stats["created"],
        "updated": persistence_stats["updated"],
        "deduplicated": (dedupe_stats or {}).get("removed", 0),
        "dry_run": args.dry_run,
    }

    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ScraperError as exc:
        logging.error("Scraper failed: %s", exc)
        print(json.dumps({"error": str(exc)}))
        sys.exit(1)
