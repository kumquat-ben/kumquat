#!/usr/bin/env python3
"""Manual scraper for the Cigna Healthcare careers hub (Phenom platform).

The script mirrors the conventions used across the other manual scrapers:

1. Bootstraps Django so that we can leverage the shared `Scraper` and
   `JobPosting` models.
2. Crawls the public job search endpoint that powers
   https://jobs.thecignagroup.com/us/en/cigna-healthcare-careers by paging
   through `search-results?keywords=Cigna+Healthcare`.
3. Visits each job detail page to capture the full HTML description.
4. Upserts the resulting postings into the existing `JobPosting` table.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, Tuple

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
from django.db import transaction  # noqa: E402
from django.utils import timezone  # noqa: E402

from scrapers.models import JobPosting, Scraper  # noqa: E402

# ---------------------------------------------------------------------------
# Constants & configuration
# ---------------------------------------------------------------------------
BASE_DOMAIN = "https://jobs.thecignagroup.com"
SEARCH_PATH = "/us/en/search-results"
DETAIL_PATH_TEMPLATE = "/us/en/job/{job_seq}"

SEARCH_URL = f"{BASE_DOMAIN}{SEARCH_PATH}"
DETAIL_URL_TEMPLATE = f"{BASE_DOMAIN}{DETAIL_PATH_TEMPLATE}"

DEFAULT_KEYWORDS = "Cigna Healthcare"
REQUEST_TIMEOUT: Tuple[int, int] = (10, 40)
LISTING_DELAY_SECONDS = 0.2
DETAIL_DELAY_SECONDS = 0.1
PAGE_SIZE = 10  # Phenom renders ten listings per page when paging with ?from=

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 900), 60)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def emit(event: str, data: Dict[str, Any]) -> None:
    """Print a structured JSON event that upstream tooling can capture."""
    print(json.dumps({"event": event, "data": data}, ensure_ascii=False))
    sys.stdout.flush()


def extract_ddo(html_text: str) -> Dict[str, Any]:
    """Extract the Phenom DDO payload embedded in the HTML."""
    marker = "phApp.ddo = "
    start = html_text.find(marker)
    if start == -1:
        raise ValueError("Unable to locate phApp.ddo payload.")
    start += len(marker)

    while start < len(html_text) and html_text[start].isspace():
        start += 1

    if start >= len(html_text) or html_text[start] != "{":
        raise ValueError("Unexpected phApp.ddo payload structure.")

    depth = 0
    end = start
    for idx in range(start, len(html_text)):
        char = html_text[idx]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                end = idx
                break
    else:
        raise ValueError("Unable to parse phApp.ddo payload (unterminated object).")

    payload = html_text[start : end + 1]
    return json.loads(payload)


def clean_html(fragment: Optional[str]) -> str:
    if not fragment:
        return ""
    soup = BeautifulSoup(fragment, "html.parser")
    return soup.get_text("\n", strip=True).strip()


def compact_metadata(items: Iterable[Tuple[str, Any]]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in items:
        if value is None:
            continue
        if isinstance(value, str):
            trimmed = value.strip()
            if not trimmed:
                continue
            result[key] = trimmed
            continue
        if isinstance(value, (list, dict)) and not value:
            continue
        result[key] = value
    return result


def fetch_listing_page(
    session: requests.Session,
    *,
    keywords: str,
    offset: int = 0,
) -> Tuple[List[Dict[str, Any]], int]:
    params = {"keywords": keywords}
    if offset:
        params["from"] = offset

    response = session.get(SEARCH_URL, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    ddo = extract_ddo(response.text)
    refine = ddo.get("eagerLoadRefineSearch") or {}
    data = refine.get("data") or {}
    jobs = data.get("jobs") or []
    total_hits = refine.get("totalHits") or refine.get("hits") or len(jobs)
    return jobs, int(total_hits)


def fetch_job_detail(session: requests.Session, job_seq: str) -> Dict[str, Any]:
    detail_url = DETAIL_URL_TEMPLATE.format(job_seq=job_seq)
    response = session.get(detail_url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    ddo = extract_ddo(response.text)
    job_detail = ((ddo.get("jobDetail") or {}).get("data") or {}).get("job") or {}
    return job_detail


def build_job_record(summary: Dict[str, Any], detail: Dict[str, Any]) -> Dict[str, Any]:
    job_seq = (summary.get("jobSeqNo") or detail.get("jobSeqNo") or "").strip()
    title = (summary.get("title") or detail.get("title") or "").strip()
    location = (
        summary.get("cityStateCountry")
        or summary.get("location")
        or detail.get("cityStateCountry")
        or detail.get("location")
        or ""
    )
    location = location.strip() if isinstance(location, str) else location

    posted_date = summary.get("postedDate") or detail.get("postedDate") or ""

    structure = detail.get("structureData") or {}
    description_html = (
        structure.get("description")
        or detail.get("ml_Description")
        or detail.get("description")
        or ""
    )
    description_text = clean_html(description_html)

    metadata = compact_metadata(
        (
            ("job_seq_no", job_seq),
            ("req_id", summary.get("reqId") or detail.get("reqId")),
            ("job_id", summary.get("jobId") or detail.get("jobId")),
            ("job_requisition_id", detail.get("jobRequisitionId")),
            ("category", summary.get("category") or detail.get("category")),
            ("sub_category", summary.get("subCategory") or detail.get("subCategory")),
            ("type", summary.get("type") or detail.get("type")),
            ("time_type", detail.get("timeType")),
            ("position_type", detail.get("positionType")),
            ("worker_type", detail.get("workerType")),
            ("job_level", detail.get("jobLevel")),
            ("career_step", detail.get("careerStep")),
            ("job_family", detail.get("jobFamily")),
            ("job_family_group", detail.get("jobFamilyGroup")),
            ("team_id", summary.get("teamId") or detail.get("teamId")),
            ("industry", summary.get("industry") or detail.get("industry")),
            ("ml_skills", summary.get("ml_skills") or detail.get("ml_skills")),
            ("apply_url", summary.get("applyUrl") or detail.get("applyUrl")),
            ("external_apply", summary.get("externalApply") or detail.get("externalApply")),
            ("city", summary.get("city") or detail.get("city")),
            ("state", summary.get("state") or detail.get("state")),
            ("country", summary.get("country") or detail.get("country")),
            ("latitude", summary.get("latitude") or detail.get("latitude")),
            ("longitude", summary.get("longitude") or detail.get("longitude")),
            ("multi_location", summary.get("multi_location") or detail.get("multi_location")),
            ("multi_location_array", summary.get("multi_location_array") or detail.get("multi_location_array")),
            ("description_teaser", summary.get("descriptionTeaser") or detail.get("descriptionTeaser")),
            ("description_html", description_html),
            ("posted_date", posted_date),
            ("date_created", summary.get("dateCreated") or detail.get("dateCreated")),
            ("job_visibility", summary.get("jobVisibility") or detail.get("jobVisibility")),
        )
    )

    return {
        "job_seq": job_seq,
        "title": title,
        "location": location,
        "date": posted_date,
        "link": DETAIL_URL_TEMPLATE.format(job_seq=job_seq) if job_seq else summary.get("applyUrl"),
        "description": description_text,
        "metadata": metadata,
    }


def iter_job_records(
    session: requests.Session,
    *,
    keywords: str,
    max_pages: Optional[int] = None,
    max_results: Optional[int] = None,
    listing_delay: float = LISTING_DELAY_SECONDS,
    detail_delay: float = DETAIL_DELAY_SECONDS,
) -> Iterator[Dict[str, Any]]:
    seen_sequences: Set[str] = set()
    offset = 0
    page = 0
    fetched = 0
    total_hits: Optional[int] = None

    while True:
        if max_pages is not None and page >= max_pages:
            break

        summaries, total_hits = fetch_listing_page(session, keywords=keywords, offset=offset)
        if not summaries:
            break

        emit(
            "log",
            {
                "step": "listing",
                "detail": f"Fetched {len(summaries)} summaries at offset {offset}",
                "page": page + 1,
                "offset": offset,
                "total_hits": total_hits,
            },
        )

        for summary in summaries:
            job_seq = (summary.get("jobSeqNo") or "").strip()
            if not job_seq:
                continue
            if job_seq in seen_sequences:
                continue

            seen_sequences.add(job_seq)

            detail = fetch_job_detail(session, job_seq)
            record = build_job_record(summary, detail)
            yield record

            fetched += 1
            if max_results is not None and fetched >= max_results:
                return

            if detail_delay > 0:
                time.sleep(detail_delay)

        page += 1
        offset += PAGE_SIZE

        if total_hits is not None and offset >= total_hits:
            break

        if listing_delay > 0:
            time.sleep(listing_delay)


def persist_jobs(scraper: Scraper, jobs: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    created = 0
    updated = 0
    processed = 0

    for job in jobs:
        processed += 1
        link = job.get("link")
        title = (job.get("title") or "").strip()
        if not link or not title:
            continue

        defaults = {
            "title": title[:255],
            "location": (job.get("location") or "")[:255],
            "date": (job.get("date") or "")[:100],
            "description": (job.get("description") or ""),
            "metadata": job.get("metadata") or {},
        }

        obj, created_flag = JobPosting.objects.update_or_create(
            scraper=scraper,
            link=link,
            defaults=defaults,
        )
        if created_flag:
            created += 1
        else:
            updated += 1

    scraper.last_run = timezone.now()
    scraper.save(update_fields=["last_run"])

    return {"processed": processed, "created": created, "updated": updated}


def get_scraper_record(target_url: str) -> Scraper:
    scraper, created_flag = Scraper.objects.get_or_create(
        company="Cigna Healthcare",
        url=target_url,
        defaults={
            "code": "manual-script",
            "interval_hours": 24,
            "timeout_seconds": DEFAULT_TIMEOUT_SECONDS,
        },
    )
    if created_flag:
        emit("log", {"step": "scraper", "detail": f"Created Scraper id={scraper.id}"})
    return scraper


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Manual scraper for the Cigna Healthcare careers landing page.",
    )
    parser.add_argument("--keywords", default=DEFAULT_KEYWORDS, help="Keyword filter to pass to the job search.")
    parser.add_argument("--max-pages", type=int, default=None, help="Limit the number of listing pages to crawl.")
    parser.add_argument("--max-results", type=int, default=None, help="Limit the total number of job postings to ingest.")
    parser.add_argument("--listing-delay", type=float, default=LISTING_DELAY_SECONDS, help="Delay (seconds) between listing page requests.")
    parser.add_argument("--detail-delay", type=float, default=DETAIL_DELAY_SECONDS, help="Delay (seconds) between detail page requests.")
    parser.add_argument("--dry-run", action="store_true", help="Collect the jobs but skip database writes.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging to stderr.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )

    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    emit(
        "log",
        {
            "step": "start",
            "detail": "Beginning job collection",
            "search_url": SEARCH_URL,
            "keywords": args.keywords,
            "dry_run": args.dry_run,
        },
    )

    scraper = get_scraper_record(f"{SEARCH_URL}?keywords={args.keywords.replace(' ', '+')}")

    jobs_iterator = iter_job_records(
        session,
        keywords=args.keywords,
        max_pages=args.max_pages,
        max_results=args.max_results,
        listing_delay=args.listing_delay,
        detail_delay=args.detail_delay,
    )

    records: List[Dict[str, Any]] = list(jobs_iterator)

    if args.dry_run:
        summary = {
            "processed": len(records),
            "created": 0,
            "updated": 0,
        }
    else:
        with transaction.atomic():
            summary = persist_jobs(scraper, records)

    emit(
        "result",
        {
            "company": scraper.company,
            "scraper_id": scraper.id,
            "url": scraper.url,
            "summary": summary,
            "dry_run": args.dry_run,
        },
    )


if __name__ == "__main__":
    main()
