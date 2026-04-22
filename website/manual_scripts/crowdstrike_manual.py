#!/usr/bin/env python3
"""Manual scraper for CrowdStrike careers (Workday-powered)."""
from __future__ import annotations

import argparse
import html
import json
import logging
import math
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Generator, Iterable, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Django bootstrap
BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "website.settings")

import django  # noqa: E402

django.setup()

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

CAREERS_URL = "https://www.crowdstrike.com/en-us/careers/"
WORKDAY_ROOT = "https://crowdstrike.wd5.myworkdayjobs.com"
TENANT = "crowdstrike"
PORTAL = "crowdstrikecareers"
CXS_BASE = f"{WORKDAY_ROOT}/wday/cxs/{TENANT}/{PORTAL}"
JOBS_ENDPOINT = f"{CXS_BASE}/jobs"
JOB_DETAIL_BASE = f"{WORKDAY_ROOT}/en-US/{PORTAL}"
SESSION_SEED_URL = f"{WORKDAY_ROOT}/en-US/{PORTAL}"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

SCRAPER_QS = Scraper.objects.filter(company="CrowdStrike", url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple CrowdStrike scrapers found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="CrowdStrike",
        url=CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=300,
    )


class ScraperError(Exception):
    pass


@dataclass
class JobSummary:
    job_id: Optional[str]
    title: str
    detail_path: str
    detail_url: str
    location_text: Optional[str]
    posted_on: Optional[str]


@dataclass
class JobListing(JobSummary):
    description: str
    date_posted: Optional[str]
    metadata: Dict[str, object]
    normalized_location: Optional[str] = None
    location_latitude: Optional[float] = None
    location_longitude: Optional[float] = None
    location_place_id: Optional[str] = None


class CrowdStrikeJobScraper:
    def __init__(
        self,
        *,
        page_size: int = 20,
        delay: float = 0.25,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.page_size = max(1, page_size)
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)
        self._bootstrapped = False

    def scrape(self, *, limit: Optional[int] = None) -> Generator[JobListing, None, None]:
        fetched = 0
        for summary in self._iter_summaries(limit=limit):
            try:
                listing = self._enrich_summary(summary)
            except Exception as exc:
                self.logger.error("Failed to enrich %s: %s", summary.detail_url, exc)
                continue
            yield listing
            fetched += 1
            if limit is not None and fetched >= limit:
                return

    def _iter_summaries(self, *, limit: Optional[int]) -> Iterable[JobSummary]:
        offset = 0
        retrieved = 0
        total: Optional[int] = None
        self._ensure_session_bootstrap()

        while True:
            payload = {
                "limit": self.page_size,
                "offset": offset,
                "searchText": "",
                "appliedFacets": {},
                "userPreferredLanguage": "en-US",
            }
            response = self.session.post(JOBS_ENDPOINT, json=payload, timeout=40)
            if response.status_code == 400 and not self._bootstrapped:
                self.logger.info("Retrying Workday jobs request after session bootstrap")
                self._ensure_session_bootstrap(force=True)
                response = self.session.post(JOBS_ENDPOINT, json=payload, timeout=40)

            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                snippet = response.text[:200].strip()
                raise ScraperError(f"Workday jobs request failed: {exc} :: {snippet}") from exc

            data = response.json()
            job_postings = data.get("jobPostings") or []
            if not job_postings:
                self.logger.info("No job postings at offset %s; stopping", offset)
                return

            if total is None:
                total = _safe_int(data.get("total"))

            for raw in job_postings:
                detail_path = raw.get("externalPath") or ""
                detail_url = detail_path
                if detail_path:
                    detail_url = (
                        detail_path
                        if detail_path.startswith("http")
                        else urljoin(f"{JOB_DETAIL_BASE.rstrip('/')}/", detail_path.lstrip("/"))
                    )
                title = (raw.get("title") or "").strip()
                if not title or not detail_url:
                    self.logger.debug("Skipping invalid payload: %s", raw)
                    continue

                job_id = None
                bullet = raw.get("bulletFields") or []
                if bullet:
                    job_id = (bullet[0] or "").strip() or None

                summary = JobSummary(
                    job_id=job_id,
                    title=title,
                    detail_path=detail_path,
                    detail_url=detail_url,
                    location_text=_strip_or_none(raw.get("locationsText")),
                    posted_on=_strip_or_none(raw.get("postedOn")),
                )
                yield summary
                retrieved += 1
                if limit is not None and retrieved >= limit:
                    return

            offset += self.page_size
            if total is not None and offset >= total:
                self.logger.info("Reached reported Workday total %s; stopping", total)
                return

            if self.delay:
                time.sleep(self.delay)

    def _enrich_summary(self, summary: JobSummary) -> JobListing:
        detail_html = self._fetch_detail_html(summary.detail_url)
        json_ld = self._extract_json_ld(detail_html)
        location_info = _extract_location_info(json_ld)

        description_text = ""
        if isinstance(json_ld, dict):
            raw_description = (json_ld.get("description") or "").strip()
            if raw_description:
                description_text = _normalize_description(raw_description)

        if not description_text:
            soup = BeautifulSoup(detail_html, "html.parser")
            body = soup.find("body")
            description_text = body.get_text("\n", strip=True) if body else ""
            if description_text:
                description_text = _maybe_repair_encoding(description_text)

        date_posted = None
        if isinstance(json_ld, dict):
            date_posted = _strip_or_none(json_ld.get("datePosted"))

        metadata: Dict[str, object] = {
            "job_id": summary.job_id,
            "posted_on_text": summary.posted_on,
            "locations_text": summary.location_text,
            "detail_path": summary.detail_path,
            "date_posted_iso": date_posted,
            "structured_locations": location_info["structured"],
        }
        if isinstance(json_ld, dict):
            metadata["json_ld"] = json_ld

        summary_data = dict(summary.__dict__)
        if location_info.get("display"):
            summary_data["location_text"] = location_info["display"]

        return JobListing(
            **summary_data,
            description=description_text or "Description unavailable.",
            date_posted=date_posted or summary.posted_on,
            metadata=metadata,
            normalized_location=location_info.get("normalized"),
            location_latitude=location_info.get("latitude"),
            location_longitude=location_info.get("longitude"),
            location_place_id=location_info.get("place_id"),
        )

    def _fetch_detail_html(self, url: str) -> str:
        headers = dict(self.session.headers)
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        response = self.session.get(url, headers=headers, timeout=40)
        response.raise_for_status()

        if "application/json" in response.headers.get("Content-Type", ""):
            try:
                data = response.json()
            except ValueError:
                return response.text
            redirect_path = data.get("url")
            if redirect_path:
                redirect_url = (
                    redirect_path
                    if redirect_path.startswith("http")
                    else urljoin(WORKDAY_ROOT, redirect_path)
                )
                return self._fetch_detail_html(redirect_url)
        return response.text

    def _extract_json_ld(self, html_text: str) -> Optional[Dict[str, object]]:
        soup = BeautifulSoup(html_text, "html.parser")
        script_tag = soup.find("script", attrs={"type": "application/ld+json"})
        if not script_tag:
            self.logger.debug("Detail page missing JSON-LD script tag.")
            return None
        raw_json = script_tag.string or script_tag.get_text()
        if not raw_json:
            self.logger.debug("JSON-LD script tag present but empty.")
            return None
        try:
            data = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            self.logger.warning("Failed to parse JSON-LD payload: %s", exc)
            return None

        if isinstance(data, dict):
            return data

        if isinstance(data, list) and data:
            first = data[0]
            if isinstance(first, dict):
                return first
            return {"raw": data}

        return {"raw": data}

    def _ensure_session_bootstrap(self, force: bool = False) -> None:
        if self._bootstrapped and not force:
            return
        resp = self.session.get(SESSION_SEED_URL, timeout=40)
        resp.raise_for_status()
        self._bootstrapped = True


def persist_listing(listing: JobListing) -> bool:
    location_value = listing.location_text or listing.normalized_location
    defaults = {
        "title": listing.title[:255],
        "location": (location_value or "")[:255] or None,
        "normalized_location": (listing.normalized_location or "")[:255] or None,
        "location_latitude": listing.location_latitude,
        "location_longitude": listing.location_longitude,
        "location_place_id": listing.location_place_id,
        "date": (listing.date_posted or "")[:100] or None,
        "description": listing.description[:10000],
        "metadata": listing.metadata,
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Persisted CrowdStrike job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape CrowdStrike careers job listings.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum jobs to process.")
    parser.add_argument("--page-size", type=int, default=20, help="Jobs per Workday request.")
    parser.add_argument(
        "--delay", type=float, default=0.25, help="Seconds to sleep between page requests."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and print jobs without writing to the database.",
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

    scraper = CrowdStrikeJobScraper(page_size=args.page_size, delay=args.delay)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    for listing in scraper.scrape(limit=args.limit):
        totals["fetched"] += 1
        if args.dry_run:
            print(json.dumps(listing.__dict__, default=str, ensure_ascii=False))
            continue

        try:
            created = persist_listing(listing)
            if created:
                totals["created"] += 1
            else:
                totals["updated"] += 1
        except Exception as exc:
            logging.error("Failed to persist %s: %s", listing.detail_url, exc)
            totals["errors"] += 1

    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["dedupe"] = dedupe_summary

    logging.info(
        "CrowdStrike scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )
    return 0 if not totals["errors"] else 1


def _strip_or_none(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    stripped = value.strip()
    return stripped or None


def _safe_int(value: Optional[object]) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _safe_float(value: Optional[object]) -> Optional[float]:
    try:
        result = float(value) if value is not None else None
    except (TypeError, ValueError):
        return None
    if result is None:
        return None
    if math.isnan(result) or math.isinf(result):
        return None
    return result


def _maybe_repair_encoding(value: str) -> str:
    if not value or not any(ch in value for ch in ("â", "Ã", "Â")):
        return value
    try:
        repaired = value.encode("latin-1").decode("utf-8")
    except UnicodeError:
        return value
    return repaired


def _extract_location_info(json_ld: Optional[Dict[str, object]]) -> Dict[str, object]:
    result: Dict[str, object] = {
        "display": None,
        "normalized": None,
        "latitude": None,
        "longitude": None,
        "place_id": None,
        "structured": [],
    }
    if not isinstance(json_ld, dict):
        return result

    job_location = json_ld.get("jobLocation")
    if not job_location:
        return result

    candidates: List[Dict[str, object]] = []
    if isinstance(job_location, list):
        candidates = [loc for loc in job_location if isinstance(loc, dict)]
    elif isinstance(job_location, dict):
        candidates = [job_location]

    for entry in candidates:
        address = entry.get("address")
        address_dict = address if isinstance(address, dict) else {}

        name = _strip_or_none(entry.get("name"))
        locality = _strip_or_none(address_dict.get("addressLocality"))
        region = _strip_or_none(address_dict.get("addressRegion"))
        country = _strip_or_none(address_dict.get("addressCountry"))
        postal_code = _strip_or_none(address_dict.get("postalCode"))

        geo = entry.get("geo")
        if isinstance(geo, dict):
            latitude = _safe_float(geo.get("latitude"))
            longitude = _safe_float(geo.get("longitude"))
        else:
            latitude = _safe_float(entry.get("latitude"))
            longitude = _safe_float(entry.get("longitude"))

        components: List[str] = []
        for part in (name, locality, region, country):
            if part and part not in components:
                components.append(part)
        formatted = ", ".join(components) if components else None

        normalized_parts = [part for part in (locality, region, country) if part]
        normalized = ", ".join(normalized_parts) if normalized_parts else formatted

        structured_entry = {
            "name": name,
            "locality": locality,
            "region": region,
            "country": country,
            "postal_code": postal_code,
            "formatted": formatted,
            "normalized": normalized,
            "latitude": latitude,
            "longitude": longitude,
            "raw": entry,
        }
        result["structured"].append(structured_entry)

        if result["display"] is None and formatted:
            result["display"] = formatted
        if result["normalized"] is None and normalized:
            result["normalized"] = normalized
        if result["latitude"] is None and latitude is not None:
            result["latitude"] = latitude
        if result["longitude"] is None and longitude is not None:
            result["longitude"] = longitude

    return result


def _normalize_description(raw_html: str) -> str:
    text = html.unescape(raw_html)
    text = _maybe_repair_encoding(text)
    soup = BeautifulSoup(text, "html.parser")
    normalized = soup.get_text("\n", strip=True)
    normalized = _maybe_repair_encoding(normalized)
    return normalized.replace("\u202f", " ").replace("\xa0", " ").strip()


if __name__ == "__main__":
    raise SystemExit(main())
