#!/usr/bin/env python3
"""Manual scraper for Domino's (jobs.dominos.com).

This script fetches publicly listed Domino's jobs, enriches each listing with
metadata from its detail page JSON-LD payload, and stores/updates
``JobPosting`` rows associated with the Domino's scraper entry.
"""
from __future__ import annotations

import argparse
import html
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional
from urllib.parse import urljoin

try:
    import cloudscraper
except ImportError as exc:  # pragma: no cover - handled at runtime
    raise SystemExit(
        "The `cloudscraper` package is required to run this script. "
        "Install it via `pip install cloudscraper`."
    ) from exc

import requests
from bs4 import BeautifulSoup, Tag  # type: ignore[import]

# ---------------------------------------------------------------------------
# Django bootstrap (matches existing manual script conventions)
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
# Constants & configuration
# ---------------------------------------------------------------------------
BASE_URL = "https://jobs.dominos.com"
CAREERS_URL = f"{BASE_URL}/us/jobs/"
CATEGORY_PATHS = {
    "stores": f"{BASE_URL}/us/jobs/stores/",
    "supply-chain": f"{BASE_URL}/us/jobs/supply-chain/",
    "corporate": f"{BASE_URL}/us/jobs/corporate/",
}

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

SCRAPER_QS = Scraper.objects.filter(company="Domino's", url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning(
            "Multiple Scraper rows matched Domino's careers; using id=%s", SCRAPER.id
        )
else:
    SCRAPER = Scraper.objects.create(
        company="Domino's",
        url=CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=300,
    )


class ScraperError(Exception):
    """Raised when the Domino's scraper encounters a fatal error."""


@dataclass(frozen=True)
class JobCard:
    title: str
    link: str
    location_text: Optional[str]
    distance_note: Optional[str]
    posting_type: Optional[str]
    apply_url: Optional[str]
    category: str


@dataclass(frozen=True)
class JobListing:
    title: str
    link: str
    location: Optional[str]
    date_posted: Optional[str]
    description: str
    metadata: Dict[str, object]


# ---------------------------------------------------------------------------
# Domino's client
# ---------------------------------------------------------------------------
class DominosJobClient:
    def __init__(
        self,
        *,
        categories: Optional[Iterable[str]] = None,
        delay: float = 0.25,
        session: Optional[cloudscraper.CloudScraper] = None,
    ) -> None:
        self.categories = list(categories) if categories else list(CATEGORY_PATHS.keys())
        self.delay = max(0.0, delay)
        self.session = session or self._create_session()
        self.logger = logging.getLogger(self.__class__.__name__)

    @staticmethod
    def _create_session() -> cloudscraper.CloudScraper:
        scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "desktop": True}
        )
        scraper.headers.update(DEFAULT_HEADERS)
        return scraper

    # Public API -----------------------------------------------------------
    def iter_job_cards(self) -> Iterator[JobCard]:
        for category in self.categories:
            url = CATEGORY_PATHS.get(category)
            if not url:
                self.logger.warning("Unknown category '%s'; skipping.", category)
                continue

            response = self._request_with_retry("GET", url)
            soup = BeautifulSoup(response.text, "html.parser")
            cards = soup.select(".card.card-job.js-job")
            self.logger.info("Fetched %s job cards from %s", len(cards), url)

            for card in cards:
                link_tag = card.select_one("h2.card-title a")
                if not link_tag or not link_tag.get("href"):
                    self.logger.debug("Skipping card without link: %s", card)
                    continue

                link_url = urljoin(BASE_URL, link_tag["href"])
                title = link_tag.get_text(strip=True)
                location_text, distance_note = self._parse_location(card)
                posting_type = self._text_or_none(card.select_one("li.posting-type"))
                apply_tag = card.select_one("a.btn.btn-primary")
                apply_url = None
                if apply_tag and apply_tag.get("href"):
                    apply_url = urljoin(BASE_URL, apply_tag["href"])

                yield JobCard(
                    title=title,
                    link=link_url,
                    location_text=location_text,
                    distance_note=distance_note,
                    posting_type=posting_type,
                    apply_url=apply_url,
                    category=category,
                )

            if self.delay:
                time.sleep(self.delay)

    def fetch_listing(self, job: JobCard) -> JobListing:
        response = self._request_with_retry("GET", job.link)
        soup = BeautifulSoup(response.text, "html.parser")
        json_ld = self._extract_jobposting_payload(soup)
        if not json_ld:
            raise ScraperError(f"No JobPosting JSON-LD found for {job.link}")

        description = _normalize_description(json_ld.get("description"))
        location = _derive_location(json_ld.get("jobLocation")) or job.location_text

        metadata: Dict[str, object] = {
            "category": job.category,
            "posting_type": job.posting_type,
            "distance_note": job.distance_note,
            "apply_url": job.apply_url,
            "employment_type": json_ld.get("employmentType"),
            "hiring_organization": json_ld.get("hiringOrganization"),
            "industry": json_ld.get("industry"),
            "identifier": json_ld.get("identifier"),
            "base_salary": json_ld.get("baseSalary"),
            "valid_through": json_ld.get("validThrough"),
            "job_location_raw": json_ld.get("jobLocation"),
            "work_hours": json_ld.get("workHours"),
            "direct_apply": json_ld.get("directApply"),
        }
        metadata = {key: value for key, value in metadata.items() if value is not None}

        return JobListing(
            title=job.title,
            link=job.link,
            location=location,
            date_posted=json_ld.get("datePosted"),
            description=description,
            metadata=metadata,
        )

    # Internal helpers ----------------------------------------------------
    def _request_with_retry(self, method: str, url: str, *, retries: int = 3) -> requests.Response:
        last_exc: Optional[Exception] = None
        for attempt in range(1, retries + 1):
            try:
                response = self.session.request(method, url, timeout=60)
                if response.status_code == 404:
                    raise ScraperError(f"Endpoint not found ({response.status_code}): {url}")
                response.raise_for_status()
                return response
            except Exception as exc:
                last_exc = exc
                self.logger.warning(
                    "Request %s %s failed on attempt %s/%s: %s",
                    method,
                url,
                    attempt,
                    retries,
                    exc,
                )
                if attempt < retries:
                    time.sleep(max(self.delay, 0.5))
        raise ScraperError(f"Request {method} {url} failed after {retries} attempts") from last_exc

    @staticmethod
    def _parse_location(card: Tag) -> tuple[Optional[str], Optional[str]]:
        location_elem = card.select_one("li.location")
        if not location_elem:
            return None, None
        strong = location_elem.find("strong")
        primary = strong.get_text(strip=True) if strong else None
        full_text = location_elem.get_text(separator=" ", strip=True)
        distance = None
        if primary:
            remainder = full_text.replace(primary, "", 1).strip(" ,-")
            distance = remainder or None
        else:
            distance = full_text or None
        return primary, distance

    @staticmethod
    def _text_or_none(elem: Optional[Tag]) -> Optional[str]:
        if not elem:
            return None
        text = elem.get_text(strip=True)
        return text or None

    @staticmethod
    def _extract_jobposting_payload(soup: BeautifulSoup) -> Optional[Dict[str, object]]:
        for script in soup.find_all("script", {"type": "application/ld+json"}):
            try:
                payload = json.loads(script.string or "")
            except json.JSONDecodeError:  # pragma: no cover - defensive
                continue
            if isinstance(payload, dict) and payload.get("@type") == "JobPosting":
                return payload
        return None


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------
def _normalize_description(raw_html: Optional[str]) -> str:
    if not raw_html:
        return ""
    unescaped = html.unescape(raw_html)
    soup = BeautifulSoup(unescaped, "html.parser")
    return soup.get_text("\n").strip()


def _derive_location(job_location: object) -> Optional[str]:
    if not job_location:
        return None
    if isinstance(job_location, dict):
        return _format_location(job_location)
    if isinstance(job_location, list):
        pieces = [_format_location(item) for item in job_location]
        deduped = []
        for piece in pieces:
            if piece and piece not in deduped:
                deduped.append(piece)
        return "; ".join(deduped) if deduped else None
    return None


def _format_location(location_obj: object) -> Optional[str]:
    if not isinstance(location_obj, dict):
        return None
    name = location_obj.get("name")
    if isinstance(name, str) and name.strip():
        return name.strip()
    address = location_obj.get("address")
    if isinstance(address, dict):
        parts = [
            address.get("addressLocality"),
            address.get("addressRegion"),
        ]
        parts = [part for part in parts if isinstance(part, str) and part.strip()]
        if parts:
            return ", ".join(p.strip() for p in parts)
        for fallback in ("name", "streetAddress", "postalCode"):
            value = address.get(fallback)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def _persist_listing(listing: JobListing) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255] or None,
        "date": (listing.date_posted or "")[:100] or None,
        "description": listing.description[:10000],
        "metadata": listing.metadata,
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.link,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Stored Domino's job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


# ---------------------------------------------------------------------------
# CLI orchestration
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manual Domino's jobs scraper")
    parser.add_argument(
        "--categories",
        nargs="+",
        choices=sorted(CATEGORY_PATHS.keys()),
        help="Limit scraping to specific categories (default: all).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of job detail pages to process.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.25,
        help="Delay (in seconds) between page requests (default: 0.25).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and print job payloads without writing to the database.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.",
    )
    return parser.parse_args(argv)


def run(args: argparse.Namespace) -> Dict[str, object]:
    client = DominosJobClient(categories=args.categories, delay=args.delay)
    seen_links = set()
    processed = 0
    created = 0
    updated = 0
    errors = 0
    dry_payloads: List[Dict[str, object]] = []

    for job_card in client.iter_job_cards():
        if job_card.link in seen_links:
            continue
        if args.limit is not None and processed >= args.limit:
            break
        seen_links.add(job_card.link)

        try:
            listing = client.fetch_listing(job_card)
        except ScraperError as exc:
            logging.error("Failed to fetch %s: %s", job_card.link, exc)
            errors += 1
            continue

        processed += 1

        if args.dry_run:
            payload = {
                "title": listing.title,
                "link": listing.link,
                "location": listing.location,
                "date_posted": listing.date_posted,
                "metadata": listing.metadata,
            }
            payload["description"] = listing.description
            dry_payloads.append(payload)
            continue

        try:
            if _persist_listing(listing):
                created += 1
            else:
                updated += 1
        except Exception as exc:  # pragma: no cover - persistence safety net
            logging.error("Failed to persist %s: %s", listing.link, exc)
            errors += 1

        if args.limit is not None and processed >= args.limit:
            break

    dedupe_summary: Optional[Dict[str, object]] = None
    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)

    result = {
        "company": "Domino's",
        "source": CAREERS_URL,
        "categories": args.categories or list(CATEGORY_PATHS.keys()),
        "processed": processed,
        "created": created,
        "updated": updated,
        "errors": errors,
        "deduplicated": dedupe_summary,
        "dry_run_payloads": dry_payloads if args.dry_run else None,
    }
    return result


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(levelname)s: %(message)s",
    )

    start = time.time()
    try:
        outcome = run(args)
    except ScraperError as exc:
        logging.error("Scraper failed: %s", exc)
        return 1

    outcome["elapsed_seconds"] = round(time.time() - start, 2)
    print(json.dumps(outcome, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
