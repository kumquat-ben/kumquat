#!/usr/bin/env python3
"""Manual scraper for Coterra Energy careers (Recruitee-powered listings)."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional

import requests
from bs4 import BeautifulSoup

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
SCRAPER_COMPANY = "Coterra Energy"
SCRAPER_URL = "https://www.coterra.com/careers"
COMPANY_SLUG = "coterraenergy"
OFFERS_ENDPOINT = f"https://api.recruitee.com/c/{COMPANY_SLUG}/careers/offers"
REQUEST_TIMEOUT = (10, 30)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.coterra.com/careers/",
}
DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 600), 60)
LOGGER = logging.getLogger("coterra_manual")

SCRAPER_QS = Scraper.objects.filter(company=SCRAPER_COMPANY, url=SCRAPER_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        LOGGER.warning("Multiple Scraper rows matched; using id=%s.", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company=SCRAPER_COMPANY,
        url=SCRAPER_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )
    LOGGER.info("Created new Scraper row for %s (id=%s).", SCRAPER_COMPANY, SCRAPER.id)


# ---------------------------------------------------------------------------
# Data containers & helpers
# ---------------------------------------------------------------------------
@dataclass
class JobRecord:
    title: str
    link: str
    location: Optional[str]
    date_posted: Optional[str]
    description: str
    metadata: Dict[str, object]


def html_to_text(raw_html: Optional[str]) -> str:
    """Convert HTML fragments to normalized plain text."""
    if not raw_html:
        return ""
    soup = BeautifulSoup(raw_html, "html.parser")
    text = soup.get_text("\n", strip=True)
    text = text.replace("\xa0", " ").replace("\u202f", " ")
    return "\n".join(line.strip() for line in text.splitlines() if line.strip())


def parse_published_at(published_at: Optional[str]) -> Optional[str]:
    if not published_at:
        return None
    try:
        dt = datetime.strptime(published_at, "%Y-%m-%d %H:%M:%S %Z")
    except ValueError:
        return published_at.strip()
    dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def build_location(offer: Dict[str, object]) -> Optional[str]:
    location = (offer.get("location") or "").strip()
    if location:
        return location

    formatted: List[str] = []
    for loc in offer.get("locations") or []:
        city = (loc.get("city") or loc.get("name") or "").strip()
        state = (loc.get("state") or loc.get("state_code") or "").strip()
        country = (loc.get("country") or loc.get("country_code") or "").strip()
        parts = [part for part in (city, state, country) if part]
        formatted_location = ", ".join(parts)
        if formatted_location and formatted_location not in formatted:
            formatted.append(formatted_location)

    if formatted:
        return " / ".join(formatted)
    return None


def offer_to_job(offer: Dict[str, object]) -> Optional[JobRecord]:
    title = (offer.get("title") or "").strip()
    link = (offer.get("careers_url") or offer.get("careers_apply_url") or "").strip()
    if not title or not link:
        return None

    description_html = offer.get("description") or ""
    requirements_html = offer.get("requirements") or ""
    description_text = html_to_text(description_html)
    if requirements_html:
        requirements_text = html_to_text(requirements_html)
        if requirements_text:
            description_text = f"{description_text}\n\nRequirements:\n{requirements_text}".strip()

    if not description_text:
        description_text = "Description unavailable."

    metadata: Dict[str, object] = {
        "id": offer.get("id"),
        "slug": offer.get("slug"),
        "apply_url": offer.get("careers_apply_url"),
        "remote": offer.get("remote"),
        "hybrid": offer.get("hybrid"),
        "employment_type_code": offer.get("employment_type_code"),
        "department": offer.get("department"),
        "tags": offer.get("tags"),
        "locations": offer.get("locations"),
        "mailbox_email": offer.get("mailbox_email"),
        "published_at_raw": offer.get("published_at"),
        "description_html": description_html,
        "requirements_html": requirements_html or None,
        "dynamic_fields": offer.get("dynamic_fields"),
        "open_questions": offer.get("open_questions"),
    }
    metadata = {key: value for key, value in metadata.items() if value not in (None, "", [])}

    job = JobRecord(
        title=title,
        link=link,
        location=build_location(offer),
        date_posted=parse_published_at(offer.get("published_at")),
        description=description_text,
        metadata=metadata,
    )
    return job


def fetch_offers(session: requests.Session) -> List[Dict[str, object]]:
    response = session.get(OFFERS_ENDPOINT, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    try:
        payload = response.json()
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed to decode offers payload: {exc}") from exc

    offers = payload.get("offers")
    if not isinstance(offers, list):
        raise RuntimeError("Unexpected payload structure - `offers` list missing.")

    return offers


def iter_jobs(offers: Iterable[Dict[str, object]], *, limit: Optional[int] = None) -> Iterator[JobRecord]:
    count = 0
    for offer in offers:
        job = offer_to_job(offer)
        if not job:
            continue
        yield job
        count += 1
        if limit is not None and count >= limit:
            break


def persist_job(scraper: Scraper, job: JobRecord) -> bool:
    defaults = {
        "title": job.title[:255],
        "location": (job.location or "")[:255] or None,
        "date": (job.date_posted or "")[:100] or None,
        "description": job.description[:10000],
        "metadata": job.metadata,
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=scraper,
        link=job.link,
        defaults=defaults,
    )
    LOGGER.debug("Persisted job '%s' (id=%s, created=%s).", obj.title, obj.id, created)
    return created


# ---------------------------------------------------------------------------
# CLI plumbing
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Coterra Energy careers listings.")
    parser.add_argument("--limit", type=int, default=None, help="Optional cap on number of jobs processed.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and print jobs without writing to the database.")
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

    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    try:
        offers = fetch_offers(session)
    except Exception as exc:  # pragma: no cover - network/JSON failure
        LOGGER.error("Failed to retrieve offers: %s", exc)
        return 1

    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    for job in iter_jobs(offers, limit=args.limit):
        totals["fetched"] += 1
        if args.dry_run:
            print(
                json.dumps(
                    {
                        "title": job.title,
                        "link": job.link,
                        "location": job.location,
                        "date_posted": job.date_posted,
                    },
                    ensure_ascii=False,
                )
            )
            continue

        try:
            created = persist_job(SCRAPER, job)
        except Exception as exc:  # pragma: no cover - persistence failure
            LOGGER.error("Failed to persist job %s: %s", job.link, exc)
            totals["errors"] += 1
            continue

        if created:
            totals["created"] += 1
        else:
            totals["updated"] += 1

    if args.dry_run:
        LOGGER.info(
            "Dry run complete - fetched %s offers (no database writes).",
            totals["fetched"],
        )
    else:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["dedupe"] = dedupe_summary
        LOGGER.info(
            "Coterra scraper finished - fetched=%(fetched)s created=%(created)s "
            "updated=%(updated)s errors=%(errors)s",
            totals,
        )
        LOGGER.info("Deduplication summary: %s", dedupe_summary)

    return 0 if totals["errors"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
