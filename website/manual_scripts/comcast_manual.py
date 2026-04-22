#!/usr/bin/env python3
"""Manual scraper for Comcast careers listings.

This script uses the public sitemap exposed at jobs.comcast.com to discover
current job detail pages, extracts the JSON-LD `JobPosting` payload from each
page, and persists the results via the Django ORM to `JobPosting`. It is meant
to be triggered manually from the operations dashboard.
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
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, Union

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
from django.db import IntegrityError  # noqa: E402

django.setup()

from django.conf import settings  # noqa: E402

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CAREERS_LANDING_URL = "https://corporate.comcast.com/careers"
JOBS_SITEMAP_URL = "https://jobs.comcast.com/sitemap.xml"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": CAREERS_LANDING_URL,
}
REQUEST_TIMEOUT: Tuple[int, int] = (10, 30)

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 600), 60)
SCRAPER_QS = Scraper.objects.filter(company="Comcast", url=CAREERS_LANDING_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched Comcast careers; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Comcast",
        url=CAREERS_LANDING_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when the Comcast manual scraper encounters an unrecoverable issue."""


@dataclass
class JobRecord:
    title: str
    link: str
    location: str
    date_posted: Optional[str]
    description: str
    metadata: Dict[str, object]


def configure_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def fetch_sitemap_urls(session: requests.Session) -> List[str]:
    response = session.get(JOBS_SITEMAP_URL, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    soup = BeautifulSoup(response.content.decode("utf-8-sig"), "xml")
    job_urls: List[str] = []
    for loc in soup.find_all("loc"):
        href = (loc.text or "").strip()
        if "/job/" in href:
            job_urls.append(href)
    logging.info("Discovered %s job URLs via sitemap.", len(job_urls))
    return job_urls


def _select_job_payload(raw: Union[Dict[str, object], List[object]]) -> Optional[Dict[str, object]]:
    """Depth-first search through JSON-LD payload to find the JobPosting node."""
    if isinstance(raw, dict):
        if raw.get("@type") == "JobPosting":
            return raw
        graph = raw.get("@graph")
        if isinstance(graph, list):
            for node in graph:
                result = _select_job_payload(node)  # type: ignore[arg-type]
                if result:
                    return result
    elif isinstance(raw, list):
        for node in raw:
            result = _select_job_payload(node)  # type: ignore[arg-type]
            if result:
                return result
    return None


def extract_job_payload(html: str) -> Optional[Dict[str, object]]:
    soup = BeautifulSoup(html, "html.parser")
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        raw_text = script.string or script.text or ""
        raw_text = raw_text.strip()
        if not raw_text:
            continue
        try:
            data = json.loads(raw_text)
        except json.JSONDecodeError:
            continue
        job_payload = _select_job_payload(data)
        if job_payload:
            return job_payload
    return None


def _compose_location(addresses: Sequence[Dict[str, object]]) -> str:
    seen: List[str] = []
    formatted_locations: List[str] = []
    for entry in addresses:
        address = entry.get("address")
        if not isinstance(address, dict):
            continue
        locality = (address.get("addressLocality") or "").strip()
        region = (address.get("addressRegion") or "").strip()
        country = (address.get("addressCountry") or "").strip()
        comps = [part for part in (locality, region) if part]
        if country and country not in comps:
            comps.append(country)
        if not comps:
            continue
        formatted = ", ".join(comps)
        if formatted not in seen:
            seen.append(formatted)
            formatted_locations.append(formatted)
    return " | ".join(formatted_locations)


def _clean_description(description_html: Optional[str]) -> str:
    if not description_html:
        return ""
    soup = BeautifulSoup(description_html, "html.parser")
    text = soup.get_text("\n", strip=True)
    lines = [line.strip() for line in text.splitlines()]
    filtered = "\n".join([line for line in lines if line])
    return filtered.strip()


def build_job_record(job_payload: Dict[str, object], page_url: str) -> JobRecord:
    title = (job_payload.get("title") or "").strip()
    if not title:
        raise ScraperError(f"Job payload at {page_url} missing 'title'.")

    raw_link = (job_payload.get("url") or "").strip()
    link = raw_link or page_url

    job_location = job_payload.get("jobLocation") or []
    if isinstance(job_location, dict):
        job_location = [job_location]
    if not isinstance(job_location, list):
        job_location = []

    location_str = _compose_location(
        [entry for entry in job_location if isinstance(entry, dict)]
    )

    description_text = _clean_description(job_payload.get("description"))

    metadata: Dict[str, object] = {}
    for key in ("identifier", "employmentType", "industry", "directApply"):
        value = job_payload.get(key)
        if value not in (None, "", [], {}):
            metadata[key] = value

    base_salary = job_payload.get("baseSalary")
    if isinstance(base_salary, dict) and base_salary:
        metadata["baseSalary"] = base_salary

    raw_locations = job_payload.get("jobLocation")
    if raw_locations:
        metadata["jobLocation"] = raw_locations

    date_posted = None
    if job_payload.get("datePosted"):
        date_posted = str(job_payload["datePosted"])

    return JobRecord(
        title=title,
        link=link,
        location=location_str,
        date_posted=date_posted,
        description=description_text,
        metadata=metadata,
    )


def iter_job_records(
    session: requests.Session,
    *,
    urls: Iterable[str],
    delay: float = 0.0,
) -> Iterable[JobRecord]:
    for index, url in enumerate(urls, start=1):
        try:
            response = session.get(url, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        except Exception as exc:
            logging.error("Failed to fetch job URL %s (%s).", url, exc)
            continue

        job_payload = extract_job_payload(response.text)
        if not job_payload:
            logging.warning("JSON-LD payload missing at %s; skipping.", url)
            continue

        try:
            record = build_job_record(job_payload, url)
        except ScraperError as exc:
            logging.error(str(exc))
            continue

        logging.debug("Prepared record %s: %s", index, record.title)
        yield record

        if delay:
            time.sleep(delay)


def persist_records(records: Iterable[JobRecord]) -> Dict[str, int]:
    created = 0
    updated = 0
    for record in records:
        defaults = {
            "title": record.title[:255],
            "location": record.location[:255],
            "date": (record.date_posted or "")[:100],
            "description": record.description[:10000],
            "metadata": record.metadata or None,
        }
        try:
            _, created_flag = JobPosting.objects.update_or_create(
                scraper=SCRAPER,
                link=record.link,
                defaults=defaults,
            )
        except IntegrityError as exc:
            logging.error("Failed to persist job %s (%s).", record.link, exc)
            continue

        if created_flag:
            created += 1
        else:
            updated += 1

    return {"created": created, "updated": updated}


def main() -> None:
    parser = argparse.ArgumentParser(description="Manual Comcast careers scraper.")
    parser.add_argument("--limit", type=int, help="Maximum number of jobs to process.")
    parser.add_argument("--delay", type=float, default=0.0, help="Delay (seconds) between job page requests.")
    parser.add_argument("--skip-dedup", action="store_true", help="Skip post-processing deduplication.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging.")
    args = parser.parse_args()

    configure_logging(args.verbose)
    logging.info("Starting Comcast careers scrape. limit=%s delay=%s", args.limit, args.delay)

    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    try:
        job_urls = fetch_sitemap_urls(session)
    except Exception as exc:
        raise ScraperError(f"Unable to fetch sitemap: {exc}") from exc

    if args.limit:
        job_urls = job_urls[: args.limit]
        logging.info("Processing first %s job URLs due to --limit.", len(job_urls))

    records = iter_job_records(session, urls=job_urls, delay=args.delay or 0.0)
    summary = persist_records(records)
    logging.info("Persisted postings: created=%s updated=%s", summary["created"], summary["updated"])

    if not args.skip_dedup:
        dedup_result = deduplicate_job_postings(scraper=SCRAPER, dry_run=False)
        logging.info(
            "Deduplication removed %(removed)s duplicates across %(duplicate_groups)s groups.",
            dedup_result,
        )

    logging.info("Comcast careers scrape complete.")


if __name__ == "__main__":
    main()

