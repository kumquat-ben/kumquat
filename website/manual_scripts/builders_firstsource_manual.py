#!/usr/bin/env python3
"""Manual scraper for Builders FirstSource (https://www.bldr.com/join-our-team/jobs-search)."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional

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

from django.conf import settings  # noqa: E402

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CAREERS_URL = "https://www.bldr.com/join-our-team/jobs-search"
API_URL = "https://careers.bldr.com/api/jobs"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://careers.bldr.com/",
}
DEFAULT_PAGE_SIZE = 100
DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 3600), 60)

SCRAPER_QS = Scraper.objects.filter(company="Builders FirstSource", url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Builders FirstSource scrapers found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Builders FirstSource",
        url=CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the scraper encounters an unrecoverable issue."""


@dataclass
class JobRecord:
    title: str
    link: str
    location: Optional[str]
    date: Optional[str]
    description_text: str
    description_html: Optional[str]
    metadata: Dict[str, object]


def fetch_page(
    session: requests.Session,
    *,
    page: int,
    page_size: int,
    timeout: int,
) -> Dict[str, object]:
    params = {"page": page, "limit": page_size}
    response = session.get(API_URL, params=params, timeout=timeout)
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        snippet = response.text[:200].strip()
        raise ScraperError(
            f"Builders FirstSource API request failed (page={page}): {exc} :: {snippet}"
        ) from exc

    try:
        return response.json()
    except ValueError as exc:  # pragma: no cover - defensive path
        raise ScraperError(f"Builders FirstSource API response was not valid JSON (page={page}).") from exc


def html_to_text(value: Optional[str]) -> str:
    if not value:
        return ""
    soup = BeautifulSoup(value, "html.parser")
    text = soup.get_text("\n", strip=True)
    # Collapse excessive blank lines while keeping paragraph boundaries.
    lines = [line.strip() for line in text.splitlines()]
    cleaned: List[str] = []
    for line in lines:
        if line:
            cleaned.append(line)
        elif cleaned and cleaned[-1] != "":
            cleaned.append("")
    return "\n".join(cleaned).strip()


def parse_job(data: Dict[str, object]) -> Optional[JobRecord]:
    def as_str(value: object) -> str:
        return value.strip() if isinstance(value, str) else ""

    title = as_str(data.get("title"))

    meta_block_raw = data.get("meta_data")
    meta_block = meta_block_raw if isinstance(meta_block_raw, dict) else {}
    canonical_url = as_str(meta_block.get("canonical_url"))

    if not title or not canonical_url:
        return None

    full_location = as_str(data.get("full_location"))
    short_location = as_str(data.get("short_location"))
    city = as_str(data.get("city"))
    state = as_str(data.get("state"))
    country_code = as_str(data.get("country_code"))

    location_parts = [part for part in [full_location, short_location] if part]
    if not location_parts:
        city_parts = [part for part in [city, state, country_code] if part]
        location_parts.append(", ".join(city_parts) if city_parts else "")

    location = location_parts[0] if location_parts else None
    location = location or None

    description_raw = data.get("description")
    description_html = description_raw if isinstance(description_raw, str) else None
    description_text = html_to_text(description_html) or title

    categories_raw = data.get("categories")
    categories: List[str] = []
    if isinstance(categories_raw, list):
        for entry in categories_raw:
            if isinstance(entry, dict):
                name = entry.get("name")
                if isinstance(name, str) and name.strip():
                    categories.append(name.strip())

    metadata: Dict[str, object] = {
        "req_id": data.get("req_id"),
        "apply_url": data.get("apply_url"),
        "ats_code": data.get("ats_code"),
        "categories": categories,
        "street_address": data.get("street_address"),
        "city": city or None,
        "state": state or None,
        "country_code": country_code or None,
        "postal_code": data.get("postal_code"),
        "latitude": data.get("latitude"),
        "longitude": data.get("longitude"),
        "location_type": data.get("location_type"),
        "full_location": full_location or None,
        "short_location": short_location or None,
        "multipleLocations": data.get("multipleLocations"),
        "meta_data": meta_block,
        "update_date": data.get("update_date"),
        "create_date": data.get("create_date"),
        "description_html": description_html,
    }

    return JobRecord(
        title=title,
        link=canonical_url,
        location=location,
        date=(data.get("update_date") or data.get("create_date")),  # type: ignore[arg-type]
        description_text=description_text,
        description_html=description_html,
        metadata=metadata,
    )


def iter_job_records(
    session: requests.Session,
    *,
    limit: Optional[int],
    page_size: int,
    max_pages: Optional[int],
    timeout: int,
) -> Iterator[JobRecord]:
    fetched = 0
    page = 1
    total_count: Optional[int] = None

    while True:
        if max_pages is not None and page > max_pages:
            break

        payload = fetch_page(session, page=page, page_size=page_size, timeout=timeout)
        jobs = payload.get("jobs") or []
        if not isinstance(jobs, list) or not jobs:
            logging.info("No jobs returned for page %s; stopping iteration.", page)
            break

        if total_count is None:
            total_count_val = payload.get("totalCount")
            if isinstance(total_count_val, int):
                total_count = total_count_val

        for entry in jobs:
            if not isinstance(entry, dict):
                continue

            job_data = entry.get("data")
            if not isinstance(job_data, dict):
                continue

            record = parse_job(job_data)
            if not record:
                continue

            yield record
            fetched += 1

            if limit is not None and fetched >= limit:
                return

        page += 1

        if limit is not None and fetched >= limit:
            break

        if total_count is not None and page_size > 0 and (page - 1) * page_size >= total_count:
            break


def persist_job(job: JobRecord) -> bool:
    defaults = {
        "title": job.title[:255],
        "location": (job.location or "")[:255] or None,
        "date": (job.date or "")[:100] or None,
        "description": job.description_text[:10000],
        "metadata": job.metadata,
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=job.link,
        defaults=defaults,
    )
    logging.getLogger("persist").debug("Stored job %s (created=%s)", obj.id, created)
    return created


def run(
    *,
    limit: Optional[int],
    page_size: int,
    max_pages: Optional[int],
    timeout: int,
) -> Dict[str, int]:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    summary = {"fetched": 0, "created": 0, "updated": 0, "skipped": 0}

    for job in iter_job_records(
        session,
        limit=limit,
        page_size=page_size,
        max_pages=max_pages,
        timeout=timeout,
    ):
        summary["fetched"] += 1
        try:
            created = persist_job(job)
        except Exception as exc:  # pragma: no cover - persistence failures are unexpected
            logging.error("Failed to persist job %s: %s", job.link, exc)
            summary["skipped"] += 1
            continue

        if created:
            summary["created"] += 1
        else:
            summary["updated"] += 1

        if limit is not None and summary["fetched"] >= limit:
            break

    return summary


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Builders FirstSource manual scraper")
    parser.add_argument("--limit", type=int, default=None, help="Process at most this many jobs.")
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help=f"Number of jobs to request per page (default {DEFAULT_PAGE_SIZE}).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Stop after this many pages (helps with testing).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=min(DEFAULT_TIMEOUT_SECONDS, 120),
        help="HTTP request timeout in seconds.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(levelname)s %(name)s: %(message)s",
    )

    try:
        summary = run(
            limit=args.limit,
            page_size=max(args.page_size, 1),
            max_pages=args.max_pages,
            timeout=args.timeout,
        )
    except ScraperError as exc:
        logging.error("Builders FirstSource scrape failed: %s", exc)
        return 1

    dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
    summary["dedupe"] = dedupe_summary

    logging.info(
        "Builders FirstSource scrape finished fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s skipped=%(skipped)s",
        summary,
    )
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
