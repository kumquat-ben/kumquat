#!/usr/bin/env python3
"""Manual scraper for Concentrix United States job listings."""

from __future__ import annotations

import argparse
import ast
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Generator, Iterable, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "website.settings")

import django  # noqa: E402

django.setup()

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

SEARCH_URL = "https://jobs.concentrix.com/job-search/?keyword=&country=United+States+Of+America"
AJAX_ENDPOINT = "https://jobs.concentrix.com/wp-admin/admin-ajax.php"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
}

SCRAPER_QS = Scraper.objects.filter(company="Concentrix", url=SEARCH_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
else:
    SCRAPER = Scraper.objects.create(
        company="Concentrix",
        url=SEARCH_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=300,
    )


class ScraperError(Exception):
    """Raised when the scraper encounters an unrecoverable error."""


@dataclass
class JobCard:
    title: str
    link: str
    location: Optional[str]
    category: Optional[str]
    summary: Optional[str]


@dataclass
class JobDetail(JobCard):
    description: str
    date: Optional[str]
    job_id: Optional[str]
    apply_url: Optional[str]
    data_layer: Dict[str, str]


def _clean_text(raw: str) -> str:
    text = (raw or "").replace("\r", "\n").replace("\xa0", " ")
    text = text.replace("\u2022", "-").replace("\u2013", "-").replace("\u2014", "-")
    text = text.replace("\u2019", "'").replace("\u2018", "'").replace("\u201c", '"').replace("\u201d", '"')
    lines = [line.strip() for line in text.splitlines()]
    return "\n".join(line for line in lines if line)


class ConcentrixJobScraper:
    def __init__(
        self,
        *,
        country: str = "United States Of America",
        jobs_per_page: int = 20,
        delay: float = 0.25,
        session: Optional[requests.Session] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.country = country
        self.jobs_per_page = max(1, jobs_per_page)
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.session.headers.setdefault("Referer", SEARCH_URL)
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    def scrape(self, *, limit: Optional[int] = None) -> Generator[JobDetail, None, None]:
        fetched = 0
        jobs_shown = 0

        while True:
            payload = self._fetch_listing_page(jobs_shown)
            html_fragment = payload.get("output") or ""
            cards = list(self._parse_cards(html_fragment))
            if not cards:
                self.logger.info("No job cards returned at offset %s; stopping.", jobs_shown)
                return

            for card in cards:
                try:
                    detail = self._enrich_card(card)
                except ScraperError as exc:
                    self.logger.warning("Failed to enrich job %s: %s", card.link, exc)
                    continue
                yield detail
                fetched += 1
                if limit is not None and fetched >= limit:
                    return

            if not payload.get("has_more"):
                self.logger.debug("API reported no more jobs; stopping pagination.")
                return

            next_offset = payload.get("jobs_shown")
            if isinstance(next_offset, int) and next_offset > jobs_shown:
                jobs_shown = next_offset
            else:
                jobs_shown += self.jobs_per_page

            if self.delay:
                time.sleep(self.delay)

    def _fetch_listing_page(self, jobs_shown: int) -> Dict[str, object]:
        data = {
            "action": "gd_jobs_query_pagination",
            "country[]": self.country,
            "jobs_shown": max(0, jobs_shown),
            "jobs_per_page": self.jobs_per_page,
            "keyword": "",
            "wh": "false",
        }
        response = self.session.post(AJAX_ENDPOINT, data=data, timeout=40)
        response.raise_for_status()

        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            raise ScraperError(f"Listing JSON decode failed: {exc}") from exc

        if not payload.get("success"):
            raise ScraperError(f"API returned success=False: {payload}")

        data_payload = payload.get("data")
        if not isinstance(data_payload, dict):
            raise ScraperError("Unexpected payload structure for listings.")
        return data_payload

    def _parse_cards(self, html_fragment: str) -> Iterable[JobCard]:
        soup = BeautifulSoup(html_fragment, "html.parser")
        for wrapper in soup.select("div.job"):
            link_tag = wrapper.find("a", href=True)
            if not link_tag:
                continue
            title_tag = link_tag.find("h3")
            title = (title_tag.get_text(" ", strip=True) if title_tag else "").strip()
            if not title:
                continue

            link = urljoin(SEARCH_URL, link_tag["href"].strip())
            location_tag = link_tag.select_one(".job-location")
            category_tag = link_tag.select_one(".job-category .tag")
            summary_tag = link_tag.find("p")

            yield JobCard(
                title=title,
                link=link,
                location=(location_tag.get_text(" ", strip=True) if location_tag else None),
                category=(category_tag.get_text(" ", strip=True) if category_tag else None),
                summary=_clean_text(summary_tag.get_text(" ", strip=True)) if summary_tag else None,
            )

    def _enrich_card(self, card: JobCard) -> JobDetail:
        response = self.session.get(card.link, timeout=40)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        body = soup.select_one("div.container.job-details div.body")

        description = card.summary or ""
        apply_url = None
        job_id = None
        if body:
            apply_tag = body.select_one("a.apply_now_link")
            if apply_tag:
                apply_url = apply_tag.get("href") or apply_tag.get("data-apply")
                job_id = apply_tag.get("data-originalid") or apply_tag.get("data-job-id")

            body_clone = BeautifulSoup(str(body), "html.parser")
            for extra in body_clone.select("div.more-link"):
                extra.decompose()
            description = _clean_text(body_clone.get_text("\n", strip=True))

        date_str = self._extract_post_date(soup)
        data_layer = self._extract_data_layer(response.text)

        return JobDetail(
            title=card.title,
            link=card.link,
            location=card.location,
            category=card.category,
            summary=card.summary,
            description=description,
            date=date_str,
            job_id=job_id,
            apply_url=apply_url,
            data_layer=data_layer,
        )

    def _extract_post_date(self, soup: BeautifulSoup) -> Optional[str]:
        meta = soup.find("meta", attrs={"property": "article:modified_time"})
        if not meta or not meta.get("content"):
            return None
        value = meta["content"].strip()
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed.strftime("%Y-%m-%d")
        except ValueError:
            return value[:10]

    def _extract_data_layer(self, html_text: str) -> Dict[str, str]:
        marker = "window.dataLayer.push("
        start = html_text.find(marker)
        if start == -1:
            return {}
        start += len(marker)
        end = html_text.find(");", start)
        if end == -1:
            return {}
        snippet = html_text[start:end]
        try:
            data = ast.literal_eval(snippet)
        except (SyntaxError, ValueError):
            return {}
        return {str(key): str(value) for key, value in data.items()}


def persist_job(detail: JobDetail) -> bool:
    description_text = detail.description[:10000]
    defaults = {
        "title": detail.title[:255],
        "location": (detail.location or "")[:255] or None,
        "date": (detail.date or "")[:100] or None,
        "description": description_text,
        "metadata": {
            "category": detail.category,
            "summary": detail.summary,
            "job_id": detail.job_id,
            "apply_url": detail.apply_url,
            "data_layer": detail.data_layer,
        },
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=detail.link,
        defaults=defaults,
    )
    logging.getLogger("persist").debug("Persisted %s (created=%s, id=%s)", obj.link, created, obj.id)
    return created


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Concentrix job listings for a specific country.")
    parser.add_argument("--country", default="United States Of America", help="Country filter as shown in the Concentrix UI.")
    parser.add_argument("--jobs-per-page", type=int, default=20, help="Number of jobs requested per pagination call.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument("--delay", type=float, default=0.25, help="Seconds to sleep between listing page requests.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch jobs and print JSON without touching the database.")
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
    logger = logging.getLogger("concentrix")

    scraper = ConcentrixJobScraper(
        country=args.country,
        jobs_per_page=args.jobs_per_page,
        delay=args.delay,
        logger=logger,
    )

    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    for detail in scraper.scrape(limit=args.limit):
        totals["fetched"] += 1
        if args.dry_run:
            payload = {
                "title": detail.title,
                "link": detail.link,
                "location": detail.location,
                "category": detail.category,
                "date": detail.date,
                "apply_url": detail.apply_url,
                "job_id": detail.job_id,
                "summary": detail.summary,
                "description": detail.description,
                "data_layer": detail.data_layer,
            }
            print(json.dumps(payload, ensure_ascii=False))
            continue

        try:
            created = persist_job(detail)
            if created:
                totals["created"] += 1
            else:
                totals["updated"] += 1
        except Exception as exc:  # pragma: no cover - persistence error handling
            logger.error("Failed to persist %s: %s", detail.link, exc)
            totals["errors"] += 1

    exit_code = 0
    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        logger.info("Deduplication summary: %s", dedupe_summary)
        if totals["errors"]:
            exit_code = 1

    logger.info(
        "Concentrix scraper finished - fetched=%(fetched)s created=%(created)s updated=%(updated)s errors=%(errors)s",
        totals,
    )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

