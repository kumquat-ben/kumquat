#!/usr/bin/env python3
"""Manual scraper for Federal Realty Investment Trust careers listings.

The public careers page embeds an iCIMS-powered `Jobs` JavaScript array that
contains all open positions. This script downloads the page, extracts and
normalizes that payload, and persists the resulting jobs via the shared Django
`JobPosting` model for manual/on-demand runs.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from dataclasses import asdict, dataclass
from typing import Dict, Iterable, List, Optional

import requests
from bs4 import BeautifulSoup
from pathlib import Path

# ---------------------------------------------------------------------------
# Django setup
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
CAREERS_URL = "https://www.federalrealty.com/about/careers/"
JOBS_REGEX = re.compile(r"var\s+Jobs\s*=\s*(\[[\s\S]*?\]);", re.IGNORECASE)
REQUEST_TIMEOUT = (10, 40)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": CAREERS_URL,
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 60)
SCRAPER_QS = Scraper.objects.filter(company="Federal Realty Investment Trust", url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched Federal Realty; using id=%s.", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Federal Realty Investment Trust",
        url=CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


# ---------------------------------------------------------------------------
# Data structures & helpers
# ---------------------------------------------------------------------------
@dataclass
class JobListing:
    job_id: Optional[str]
    title: str
    link: str
    location: Optional[str]
    updated_date: Optional[str]
    description_text: str
    description_html: Optional[str]
    metadata: Dict[str, object]


def collapse_whitespace(value: Optional[str]) -> str:
    if not value:
        return ""
    return " ".join(value.split())


def html_to_text(value: Optional[str]) -> str:
    if not value:
        return ""
    soup = BeautifulSoup(value, "html.parser")
    return soup.get_text("\n", strip=True).strip()


def clean_metadata(data: Dict[str, object]) -> Dict[str, object]:
    return {key: value for key, value in data.items() if value not in (None, "", [], {})}


# ---------------------------------------------------------------------------
# Scraping logic
# ---------------------------------------------------------------------------
def fetch_careers_page(session: requests.Session) -> str:
    logging.debug("Fetching Federal Realty careers page: %s", CAREERS_URL)
    response = session.get(CAREERS_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.text


def extract_jobs_payload(html: str) -> List[Dict[str, object]]:
    match = JOBS_REGEX.search(html)
    if not match:
        raise ValueError("Unable to locate Jobs array in careers page.")
    jobs_json = match.group(1)
    payload = json.loads(jobs_json)
    if not isinstance(payload, list):
        raise ValueError("Jobs payload is not a list.")
    return payload


def build_listing(job: Dict[str, object]) -> JobListing:
    details = job.get("details") or {}
    joblocation = details.get("joblocation") or {}
    sections_html = {
        "overview": details.get("overview"),
        "responsibilities": details.get("responsibilities"),
        "qualifications": details.get("qualifications"),
    }
    combined_html_parts = [section for section in sections_html.values() if section]
    description_html = "\n\n".join(combined_html_parts) if combined_html_parts else None
    description_text_parts = [html_to_text(section) for section in combined_html_parts]
    description_text = "\n\n".join(part for part in description_text_parts if part)

    metadata = clean_metadata(
        {
            "icims_id": job.get("id"),
            "icims_self": job.get("self"),
            "portal_url": job.get("portalUrl"),
            "updated_datetime": job.get("updatedDate"),
            "job_code": details.get("jobid"),
            "folder": details.get("folder"),
            "position_category": (details.get("positioncategory") or {}).get("formattedvalue"),
            "eeo_category": (details.get("eeocategory") or {}).get("formattedvalue"),
            "field14981": details.get("field14981"),
            "field21639": details.get("field21639"),
            "field47164": details.get("field47164"),
            "joblocation": joblocation,
            "sections_html": {key: value for key, value in sections_html.items() if value},
        }
    )

    listing = JobListing(
        job_id=(details.get("jobid") or job.get("id")),
        title=collapse_whitespace(details.get("jobtitle")),
        link=job.get("portalUrl"),
        location=collapse_whitespace(joblocation.get("value")),
        updated_date=details.get("updateddate") or job.get("updatedDate"),
        description_text=description_text,
        description_html=description_html,
        metadata=metadata,
    )

    if not listing.title:
        raise ValueError(f"Job {job.get('id')} missing title.")
    if not listing.link:
        raise ValueError(f"Job {job.get('id')} missing portal URL.")
    return listing


def iter_listings(session: requests.Session) -> Iterable[JobListing]:
    html = fetch_careers_page(session)
    raw_jobs = extract_jobs_payload(html)
    logging.info("Discovered %s open roles on Federal Realty careers page.", len(raw_jobs))
    for job in raw_jobs:
        try:
            yield build_listing(job)
        except Exception as exc:  # pragma: no cover - defensive logging
            logging.error("Skipping job due to parse error: %s", exc)


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------
def store_listing(listing: JobListing) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": listing.location[:255] if listing.location else None,
        "date": (listing.updated_date or "")[:100],
        "description": listing.description_text[:10000],
        "metadata": {
            **listing.metadata,
            **({"description_html": listing.description_html} if listing.description_html else {}),
        },
    }

    job_posting, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.link,
        defaults=defaults,
    )
    logging.debug(
        "%s job posting id=%s title=%s",
        "Created" if created else "Updated",
        job_posting.id,
        listing.title,
    )
    return created


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[Iterable[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manual scraper for Federal Realty careers page.")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap on the number of listings to process.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and print listings without writing to the database.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def run(limit: Optional[int], dry_run: bool) -> Dict[str, object]:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}
    try:
        for idx, listing in enumerate(iter_listings(session), start=1):
            if limit is not None and idx > limit:
                break

            totals["fetched"] += 1
            if dry_run:
                print(json.dumps(asdict(listing), default=str, ensure_ascii=False))
                continue

            try:
                created = store_listing(listing)
                if created:
                    totals["created"] += 1
                else:
                    totals["updated"] += 1
            except Exception as exc:  # pragma: no cover - persistence error path
                logging.error("Failed to persist job %s: %s", listing.link, exc)
                totals["errors"] += 1
    except requests.HTTPError as exc:
        logging.error("HTTP error while fetching jobs: %s", exc)
        totals["errors"] += 1
    except requests.RequestException as exc:
        logging.error("Network error while fetching jobs: %s", exc)
        totals["errors"] += 1
    except ValueError as exc:
        logging.error("Failed to parse jobs payload: %s", exc)
        totals["errors"] += 1

    if not dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["dedupe"] = dedupe_summary
    return totals


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()), format="%(levelname)s: %(message)s")

    totals = run(args.limit, args.dry_run)
    logging.info(
        "Federal Realty careers scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )

    if not args.dry_run and "dedupe" in totals:
        logging.info("Deduplication summary: %s", totals["dedupe"])
    return 0 if not totals["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
