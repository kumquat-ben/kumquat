#!/usr/bin/env python3
"""Manual scraper for Centene careers feed (https://www.centene.com/careers.html).

This script pulls from the public RSS feed at https://jobs.centene.com/us/en/jobs/xml/?rss=true
and stores the results via the Django ORM.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional
import xml.etree.ElementTree as ET

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
CAREERS_URL = "https://www.centene.com/careers.html"
FEED_URL = "https://jobs.centene.com/us/en/jobs/xml/?rss=true"
DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 3600), 60)

SCRAPER_QS = Scraper.objects.filter(company="Centene", url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Centene scrapers found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Centene",
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
    metadata: Dict[str, Optional[str]]


def fetch_feed(session: requests.Session, *, timeout: int) -> ET.Element:
    response = session.get(FEED_URL, timeout=timeout)
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        snippet = response.text[:200].strip()
        raise ScraperError(f"Centene RSS feed request failed: {exc} :: {snippet}") from exc

    try:
        return ET.fromstring(response.content)
    except ET.ParseError as exc:
        raise ScraperError("Centene RSS feed payload could not be parsed as XML") from exc


def iter_job_elements(root: ET.Element) -> Iterator[ET.Element]:
    return iter(root.findall("job"))


def parse_job(elem: ET.Element) -> Optional[JobRecord]:
    def text_or_none(tag: str) -> Optional[str]:
        value = elem.findtext(tag)
        if value is None:
            return None
        value = value.strip()
        return value or None

    title = text_or_none("title")
    link = text_or_none("url")
    if not title or not link:
        return None

    city = text_or_none("city")
    state = text_or_none("state")
    country = text_or_none("country")
    location = city or ", ".join(filter(None, [state, country])) or None

    description_html = text_or_none("description")
    description_text = html_to_text(description_html) if description_html else title

    metadata = {
        "requisition_id": text_or_none("requisitionid"),
        "reference_number": text_or_none("referencenumber"),
        "api_job_id": text_or_none("apijobid"),
        "city": city,
        "state": state,
        "country": country,
        "postal_code": text_or_none("postalcode"),
        "job_type": text_or_none("jobtype"),
        "category": text_or_none("category"),
        "source_name": text_or_none("sourcename"),
        "remote_type": text_or_none("remotetype"),
        "last_activity_date": text_or_none("lastactivitydate"),
        "description_html": description_html,
    }

    return JobRecord(
        title=title,
        link=link,
        location=location,
        date=text_or_none("date"),
        description_text=description_text,
        description_html=description_html,
        metadata=metadata,
    )


def html_to_text(value: str) -> str:
    soup = BeautifulSoup(unescape(value), "html.parser")
    text = soup.get_text("\n", strip=True)
    return text.replace("\u202f", " ").replace("\xa0", " ").strip()


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


def run(limit: Optional[int] = None, *, timeout: int = 60) -> Dict[str, int]:
    session = requests.Session()
    summary = {"fetched": 0, "created": 0, "updated": 0, "skipped": 0}

    root = fetch_feed(session, timeout=timeout)
    for elem in iter_job_elements(root):
        if limit is not None and summary["fetched"] >= limit:
            break

        job = parse_job(elem)
        if not job:
            summary["skipped"] += 1
            continue

        summary["fetched"] += 1
        try:
            created = persist_job(job)
        except Exception as exc:  # pragma: no cover - persistence failure is unexpected
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
    parser = argparse.ArgumentParser(description="Centene careers manual scraper")
    parser.add_argument("--limit", type=int, default=None, help="Process at most this many jobs.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=min(DEFAULT_TIMEOUT_SECONDS, 120),
        help="Feed request timeout in seconds.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(levelname)s %(name)s: %(message)s",
    )

    try:
        summary = run(limit=args.limit, timeout=args.timeout)
    except ScraperError as exc:
        logging.error("Centene scrape failed: %s", exc)
        return 1

    dedupe = deduplicate_job_postings(scraper=SCRAPER)
    summary["dedupe"] = dedupe

    logging.info(
        "Centene scrape finished fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s skipped=%(skipped)s",
        summary,
    )
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
