#!/usr/bin/env python3
"""Manual scraper for Delta Air Lines careers (Avature-powered).

This script uses Playwright to negotiate the AWS WAF challenge presented by
https://delta.avature.net/ and iterates through the public search results,
fetching individual job detail metadata via the JSON-LD payload available on
each job page. The collected jobs are mapped into the existing `JobPosting`
model associated with the "Delta Air Lines" scraper entry.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import Browser, Page, Playwright, TimeoutError as PlaywrightTimeoutError, sync_playwright

from pathlib import Path

# ---------------------------------------------------------------------------
# Django bootstrap
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
CAREERS_URL = "https://delta.avature.net/en_US/careers/SearchJobs/"
COMPANY_NAME = "Delta Air Lines, Inc."
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/127.0.0.0 Safari/537.36"
)

SCRAPER_QS = Scraper.objects.filter(company=COMPANY_NAME, url=CAREERS_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning(
            "Multiple Scraper rows matched %s; using id=%s", COMPANY_NAME, SCRAPER.id
        )
else:
    SCRAPER = Scraper.objects.create(
        company=COMPANY_NAME,
        url=CAREERS_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=300,
    )


@dataclass
class DeltaJob:
    """Container for a single job posting."""

    title: str
    link: str
    location: Optional[str]
    date_posted: Optional[str]
    description_text: str
    description_html: Optional[str]
    ref_code: Optional[str]
    metadata: Dict[str, object]

    def to_defaults(self) -> Dict[str, object]:
        return {
            "title": self.title[:255],
            "location": (self.location or "")[:255] or None,
            "date": (self.date_posted or "")[:100] or None,
            "description": self.description_text[:10000],
            "metadata": self.metadata,
        }


class DeltaCareersScraper:
    """Handles listing pagination and detail enrichment for Delta careers."""

    def __init__(self, *, delay: float = 1.0, max_pages: Optional[int] = None) -> None:
        self.delay = max(delay, 0.0)
        self.max_pages = max_pages
        self.logger = logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def scrape(self, *, limit: Optional[int] = None) -> Iterable[DeltaJob]:
        count = 0
        with sync_playwright() as playwright:
            browser = self._launch_browser(playwright)
            try:
                page = browser.new_page(user_agent=USER_AGENT)
                self._navigate_to(page, CAREERS_URL)
                self._wait_for_job_cards(page)
                session = self._build_session_from_context(page)

                pages_processed = 0
                seen_links: set[str] = set()

                while True:
                    soup = BeautifulSoup(page.content(), "html.parser")
                    listings = self._parse_listing_page(soup, current_url=page.url)
                    if not listings:
                        self.logger.warning(
                            "No job listings found on %s; breaking.", page.url
                        )
                        break

                    for listing in listings:
                        if listing["link"] in seen_links:
                            continue
                        seen_links.add(listing["link"])
                        job = self._enrich_listing(session, listing)
                        if not job:
                            continue
                        yield job
                        count += 1
                        if limit is not None and count >= limit:
                            return

                    pages_processed += 1
                    if self.max_pages and pages_processed >= self.max_pages:
                        self.logger.info(
                            "Reached max_pages=%s; stopping pagination.", self.max_pages
                        )
                        break

                    next_url = self._get_next_page_url(soup, current_url=page.url)
                    if not next_url:
                        break

                    time.sleep(self.delay)
                    self._navigate_to(page, next_url)
                    self._wait_for_job_cards(page)
            finally:
                browser.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _launch_browser(playwright: Playwright) -> Browser:
        return playwright.chromium.launch(headless=True)

    def _navigate_to(self, page: Page, url: str) -> None:
        self.logger.debug("Navigating to %s", url)
        try:
            page.goto(url, wait_until="load", timeout=90_000)
        except PlaywrightTimeoutError as exc:  # pragma: no cover - network failure
            raise RuntimeError(f"Timed out navigating to {url}") from exc

    @staticmethod
    def _wait_for_job_cards(page: Page) -> None:
        page.wait_for_selector("li.list__item", timeout=20_000)

    def _build_session_from_context(self, page: Page) -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            }
        )
        for cookie in page.context.cookies():
            # Use empty domain for cookies like aws-waf-token (domain begins with dot)
            try:
                session.cookies.set(cookie["name"], cookie["value"], domain=cookie["domain"])
            except requests.cookies.CookieConflictError:
                session.cookies.set(cookie["name"], cookie["value"])
        return session

    @staticmethod
    def _parse_listing_page(soup: BeautifulSoup, *, current_url: str) -> List[Dict[str, Optional[str]]]:
        listings: List[Dict[str, Optional[str]]] = []
        for item in soup.select("ul.list li.list__item"):
            title_anchor = item.select_one("div.list__item__text__title a")
            if not title_anchor:
                continue
            link = urljoin(current_url, title_anchor.get("href", "").strip())
            title = title_anchor.get_text(strip=True)
            if not link or not title:
                continue

            subtitle_spans = item.select("div.list__item__text__subtitle span")
            location = subtitle_spans[0].get_text(strip=True) if subtitle_spans else None
            ref_code = None
            if len(subtitle_spans) > 1:
                ref_code = subtitle_spans[1].get_text(strip=True) or None

            listings.append(
                {
                    "title": title,
                    "link": link,
                    "location": location,
                    "ref_code": ref_code,
                }
            )
        return listings

    def _enrich_listing(
        self, session: requests.Session, listing: Dict[str, Optional[str]]
    ) -> Optional[DeltaJob]:
        try:
            response = session.get(listing["link"], timeout=40)
            response.raise_for_status()
        except requests.RequestException as exc:  # pragma: no cover - network failure path
            self.logger.error("Failed to fetch job detail %s: %s", listing["link"], exc)
            return None

        soup = BeautifulSoup(response.text, "html.parser")
        json_ld = self._extract_json_ld(soup, listing["link"])

        raw_description_html = json_ld.get("description") if json_ld else ""
        description_html = raw_description_html or None
        description_text = self._html_to_text(raw_description_html) or "Description unavailable."
        date_posted = (json_ld.get("datePosted") or "").strip() if json_ld else None

        metadata: Dict[str, object] = {
            "ref_code": listing.get("ref_code"),
            "detail_url": listing["link"],
        }
        if json_ld:
            sanitized_json_ld = dict(json_ld)
            if "description" in sanitized_json_ld:
                sanitized_json_ld["description_length"] = len(
                    sanitized_json_ld.pop("description") or ""
                )
            metadata["json_ld"] = sanitized_json_ld

        job_location = None
        if json_ld and isinstance(json_ld.get("jobLocation"), dict):
            job_location = json_ld["jobLocation"]
            metadata["job_location"] = job_location
            if not listing.get("location"):
                address = job_location.get("address") or {}
                city = (address.get("addressLocality") or "").strip()
                region = (address.get("addressRegion") or "").strip()
                country = (address.get("addressCountry") or "").strip()
                location_parts = [part for part in (city, region, country) if part]
                if location_parts:
                    listing["location"] = ", ".join(location_parts)

        return DeltaJob(
            title=listing["title"],
            link=listing["link"],
            location=listing.get("location"),
            date_posted=date_posted or None,
            description_text=description_text,
            description_html=description_html,
            ref_code=listing.get("ref_code"),
            metadata=metadata,
        )

    @staticmethod
    def _html_to_text(raw_html: str) -> str:
        if not raw_html:
            return ""
        soup = BeautifulSoup(raw_html, "html.parser")
        text = soup.get_text("\n", strip=True)
        text = text.replace("\xa0", " ").replace("\u202f", " ")
        return "\n".join(line.strip() for line in text.splitlines() if line.strip())

    @staticmethod
    def _extract_json_ld(soup: BeautifulSoup, link: str) -> Dict[str, object]:
        script = soup.find("script", attrs={"type": "application/ld+json"})
        if not script or not script.string:
            logging.getLogger("DeltaCareersScraper").warning(
                "JSON-LD payload not found for %s", link
            )
            return {}
        try:
            return json.loads(script.string)
        except json.JSONDecodeError as exc:  # pragma: no cover - defensive path
            logging.getLogger("DeltaCareersScraper").error(
                "Failed to decode JSON-LD for %s: %s", link, exc
            )
            return {}

    @staticmethod
    def _get_next_page_url(soup: BeautifulSoup, *, current_url: str) -> Optional[str]:
        next_anchor = soup.select_one("a.paginationNextLink")
        if not next_anchor:
            return None
        href = next_anchor.get("href")
        if not href:
            return None
        return urljoin(current_url, href)


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------
def persist_job(job: DeltaJob) -> bool:
    defaults = job.to_defaults()
    if job.description_html:
        defaults["metadata"] = dict(defaults["metadata"])
        defaults["metadata"]["description_html"] = job.description_html
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=job.link,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Persisted job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Delta Air Lines careers listings.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument(
        "--max-pages", type=int, default=None, help="Optional cap on paginated result pages."
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.75,
        help="Seconds to sleep between pagination requests (default: 0.75).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print jobs without saving to the DB.")
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

    scraper = DeltaCareersScraper(delay=args.delay, max_pages=args.max_pages)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    for job in scraper.scrape(limit=args.limit):
        totals["fetched"] += 1
        if args.dry_run:
            print(
                json.dumps(
                    {
                        "title": job.title,
                        "link": job.link,
                        "location": job.location,
                        "date_posted": job.date_posted,
                        "ref_code": job.ref_code,
                        "description_preview": job.description_text[:200],
                    },
                    ensure_ascii=False,
                )
            )
            continue

        try:
            created = persist_job(job)
            if created:
                totals["created"] += 1
            else:
                totals["updated"] += 1
        except Exception as exc:  # pragma: no cover - persistence failure path
            logging.error("Failed to persist job %s: %s", job.link, exc)
            totals["errors"] += 1

    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        totals["dedupe"] = dedupe_summary

    logging.info(
        "Delta careers scraper finished - fetched=%(fetched)s created=%(created)s "
        "updated=%(updated)s errors=%(errors)s",
        totals,
    )
    return 0 if not totals["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

