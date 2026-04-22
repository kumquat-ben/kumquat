#!/usr/bin/env python3
"""Manual scraper for Darden careers (Paradox-powered portal).

This script fetches open roles from https://dardenrscjobs.recruiting.com via
the documented Paradox REST endpoints and persists them into the existing
``JobPosting`` table tied to the Darden scraper entry.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Django bootstrap (mirrors other manual scripts)
# ---------------------------------------------------------------------------
BACKEND_DIR = Path(__file__).resolve().parents[1]
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
CAREERS_URL = "https://www.darden.com/careers"
BASE_URL = "https://dardenrscjobs.recruiting.com"
JOBS_ENDPOINT = f"{BASE_URL}/api/get-jobs"
REQUEST_TIMEOUT = (10, 30)
PRELOAD_PATTERN = re.compile(
    r"window\.__PRELOAD_STATE__\s*=\s*(\{.*?\})\s*;\s*window\.__BUILD__",
    re.DOTALL,
)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/127.0.0.0 Safari/537.36"
)
HTML_ACCEPT = (
    "text/html,application/xhtml+xml,application/xml;q=0.9,"
    "image/avif,image/webp,*/*;q=0.8"
)
JSON_ACCEPT = "application/json, text/plain, */*"

SCRAPER_QS = Scraper.objects.filter(company="Darden", url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows matched Darden careers; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="Darden",
        url=CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=300,
    )


@dataclass
class JobRecord:
    title: str
    link: str
    location: Optional[str]
    date: Optional[str]
    description: str
    metadata: Dict[str, Any]


class DardenJobScraper:
    def __init__(
        self,
        *,
        page_size: int = 100,
        delay: float = 0.0,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.page_size = max(1, min(page_size, 200))
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept-Language": "en-US,en;q=0.9",
            }
        )
        self._site_languages: List[str] = []
        self._disable_switch_search_mode = False
        self._default_radius = 15
        self._enable_kilometers = False
        self._base_params: Dict[str, Any] = {}

        self._bootstrap_site_state()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def scrape(self, *, limit: Optional[int] = None) -> Iterable[JobRecord]:
        fetched = 0
        page_number = 1
        total_expected: Optional[int] = None

        while True:
            payload = self._fetch_page(page_number)
            jobs = payload.get("jobs") or []
            if not jobs:
                logging.debug("No jobs found for page %s; stopping.", page_number)
                break

            total_expected = payload.get("totalJob") or total_expected

            for job in jobs:
                record = self._normalize_job(job)
                yield record
                fetched += 1
                if limit is not None and fetched >= limit:
                    return

            if total_expected is not None and fetched >= int(total_expected):
                break

            page_number += 1
            if self.delay:
                time.sleep(self.delay)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _bootstrap_site_state(self) -> None:
        logging.debug("Bootstrapping session from %s", BASE_URL)
        response = self.session.get(
            BASE_URL,
            headers={"Accept": HTML_ACCEPT},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        state = extract_preload_state(response.text)
        company = state.get("company") or {}
        page_attrs = company.get("page_attributes") or {}

        self._site_languages = company.get("site_available_languages") or []
        self._disable_switch_search_mode = bool(page_attrs.get("disable_switch_search_mode"))
        self._default_radius = int(page_attrs.get("jobs_radius_default_radius_item") or 15)
        self._enable_kilometers = bool(page_attrs.get("jobs_radius_enable_kilometers"))

        params = state.get("jobSearch", {}).get("params") or {}
        self._base_params = {k: v for k, v in params.items() if v not in (None, "", [], {})}
        logging.debug(
            "Bootstrapped state: radius=%s, languages=%s, disable_switch=%s",
            self._default_radius,
            self._site_languages,
            self._disable_switch_search_mode,
        )

    def _fetch_page(self, page_number: int) -> Dict[str, Any]:
        query = {
            "radius": self._default_radius,
            "page_size": self.page_size,
            "page_number": page_number,
            "enable_kilometers": str(self._enable_kilometers).lower(),
        }
        query.update(self._base_params)

        headers = {
            "Accept": JSON_ACCEPT,
            "Referer": f"{BASE_URL}/",
            "Origin": BASE_URL,
        }
        response = self.session.post(
            JOBS_ENDPOINT,
            params=query,
            json={
                "disable_switch_search_mode": self._disable_switch_search_mode,
                "site_available_languages": self._site_languages,
            },
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        if response.status_code == 401:
            logging.error("Received 401 from jobs endpoint; cookies=%s", self.session.cookies.get_dict())
        response.raise_for_status()
        return response.json()

    def _normalize_job(self, raw: Dict[str, Any]) -> JobRecord:
        title = (raw.get("title") or "").strip()
        detail_path = (raw.get("originalURL") or "").lstrip("/")
        link = urljoin(f"{BASE_URL}/", detail_path) if detail_path else raw.get("applyURL") or BASE_URL

        locations: List[Dict[str, Any]] = raw.get("locations") or []
        location_text = format_locations(locations, is_remote=bool(raw.get("isRemote")))

        description_html = raw.get("description") or ""
        description_text = html_to_text(description_html)

        metadata: Dict[str, Any] = {}
        add_meta(metadata, "unique_id", raw.get("uniqueID"))
        add_meta(metadata, "requisition_id", raw.get("requisitionID") or raw.get("reference"))
        add_meta(metadata, "reference", raw.get("reference"))
        add_meta(metadata, "code", raw.get("code"))
        add_meta(metadata, "source_id", raw.get("sourceID"))
        add_meta(metadata, "apply_url", raw.get("applyURL"))
        add_meta(metadata, "custom_apply_link", raw.get("customApplyLink"))
        add_meta(metadata, "original_url", raw.get("originalURL"))
        add_meta(metadata, "company_name", raw.get("companyName"))
        add_meta(metadata, "account_name", raw.get("accountName"))
        add_meta(metadata, "is_remote", bool(raw.get("isRemote")), keep_false=True)
        add_meta(metadata, "employment_type", raw.get("employmentType"))
        add_meta(metadata, "employment_status", raw.get("employmentStatus"))
        add_meta(metadata, "posting_type", raw.get("postingType"))
        add_meta(metadata, "updated_date", raw.get("updatedDate"))
        add_meta(metadata, "end_date", raw.get("endDate"))
        add_meta(metadata, "commute_time_duration", raw.get("commuteTimeDuration"))

        categories = [c.get("name") for c in raw.get("categories") or [] if c.get("name")]
        if categories:
            metadata["categories"] = categories

        custom_categories = [c.get("name") for c in raw.get("customCategories") or [] if c.get("name")]
        if custom_categories:
            metadata["custom_categories"] = custom_categories

        custom_fields_list = raw.get("customFields") or []
        custom_fields = {
            field["cfKey"]: field.get("value")
            for field in custom_fields_list
            if field.get("cfKey")
        }
        if custom_fields:
            metadata["custom_fields"] = {k: v for k, v in custom_fields.items() if v not in (None, "")}

        job_card_extra = raw.get("jobCardExtraFields") or {}
        if job_card_extra:
            metadata["job_card_extra_fields"] = job_card_extra

        if locations:
            metadata["locations"] = [shrink_location(loc) for loc in locations]

        if description_html:
            metadata["description_html"] = description_html

        date_value = (raw.get("updatedDate") or "")[:10] or None

        return JobRecord(
            title=title,
            link=link,
            location=location_text[:255] or None,
            date=date_value,
            description=description_text[:10000],
            metadata=metadata,
        )


def extract_preload_state(html_text: str) -> Dict[str, Any]:
    match = PRELOAD_PATTERN.search(html_text)
    if not match:
        raise RuntimeError("Unable to locate window.__PRELOAD_STATE__ payload.")
    payload = match.group(1)
    return json.loads(payload)


def html_to_text(fragment: Optional[str]) -> str:
    if not fragment:
        return ""
    soup = BeautifulSoup(fragment, "html.parser")
    text = soup.get_text("\n", strip=True)
    return re.sub(r"\n{3,}", "\n\n", text)


def format_locations(locations: List[Dict[str, Any]], *, is_remote: bool) -> str:
    labels: List[str] = []
    for loc in locations:
        label = (
            loc.get("locationText")
            or loc.get("locationParsedText")
            or loc.get("locationName")
        )
        if not label:
            city = loc.get("city")
            state = loc.get("stateAbbr") or loc.get("state")
            country = loc.get("country")
            parts = [city or ""]
            if state:
                parts[-1] = f"{city}, {state}" if city else state
            if country:
                if parts[-1]:
                    parts[-1] = f"{parts[-1]}, {country}"
                else:
                    parts[-1] = country
            label = parts[-1]
        label = (label or "").strip()
        if label and label not in labels:
            labels.append(label)

    if not labels and is_remote:
        labels.append("Remote")

    return " | ".join(labels)


def add_meta(store: Dict[str, Any], key: str, value: Any, *, keep_false: bool = False) -> None:
    if isinstance(value, bool):
        if value or keep_false:
            store[key] = value
        return
    if value in (None, "", [], {}):
        return
    store[key] = value


def shrink_location(location: Dict[str, Any]) -> Dict[str, Any]:
    keys = [
        "locationID",
        "locationName",
        "locationText",
        "locationParsedText",
        "street",
        "city",
        "state",
        "stateAbbr",
        "postalCode",
        "country",
        "latitude",
        "longitude",
        "isRemote",
    ]
    reduced = {key: location.get(key) for key in keys if location.get(key) not in (None, "")}
    return reduced


def persist_job(record: JobRecord) -> bool:
    defaults = {
        "title": record.title[:255],
        "location": record.location,
        "date": (record.date or "")[:100] or None,
        "description": record.description,
        "metadata": record.metadata,
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=record.link,
        defaults=defaults,
    )
    logging.getLogger("persist").debug("Persisted job '%s' (created=%s, id=%s)", obj.title, created, obj.id)
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape jobs from dardenrscjobs.recruiting.com")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument(
        "--page-size",
        type=int,
        default=100,
        help="Number of jobs to request per API call (default: 100).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Seconds to sleep between API requests.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and display jobs without modifying the database.",
    )
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

    scraper = DardenJobScraper(page_size=args.page_size, delay=args.delay)
    stats = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    try:
        for job in scraper.scrape(limit=args.limit):
            stats["fetched"] += 1
            if args.dry_run:
                print(json.dumps(job.__dict__, ensure_ascii=False, default=str))
                continue

            try:
                created = persist_job(job)
                if created:
                    stats["created"] += 1
                else:
                    stats["updated"] += 1
            except Exception as exc:  # pragma: no cover - defensive logging
                logging.error("Failed to persist job %s: %s", job.link, exc)
                stats["errors"] += 1
    except Exception as exc:  # pragma: no cover - defensive logging
        logging.exception("Scraper crashed: %s", exc)
        return 1

    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        stats["dedupe"] = dedupe_summary

    logging.info(
        "Darden scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        stats,
    )
    return 0 if not stats["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
