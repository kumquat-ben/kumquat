#!/usr/bin/env python3
"""Manual scraper for EMCOR Group's iCIMS-powered careers listings."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

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

django.setup()

from django.conf import settings  # noqa: E402

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CAREERS_LANDING_URL = "https://emcorgroup.com/careers"
JOB_SEARCH_URL = "https://careers-emcorgroup.icims.com/jobs/search"
REQUEST_TIMEOUT = (15, 45)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": CAREERS_LANDING_URL,
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 60)
SCRAPER_QS = Scraper.objects.filter(company="EMCOR Group", url=CAREERS_LANDING_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.getLogger(__name__).warning(
            "Multiple Scraper rows matched EMCOR Group; using id=%s.", SCRAPER.id
        )
else:
    SCRAPER = Scraper.objects.create(
        company="EMCOR Group",
        url=CAREERS_LANDING_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
class ScraperError(RuntimeError):
    """Raised when the EMCOR careers scrape pipeline cannot proceed."""


def collapse_whitespace(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = " ".join(value.replace("\xa0", " ").split())
    return cleaned or None


def strip_query_param(url: str, param: str) -> str:
    if not url:
        return url
    parts = urlsplit(url)
    filtered = [
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if key.lower() != param.lower()
    ]
    new_query = urlencode(filtered, doseq=True)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))


def canonical_job_url(url: str) -> str:
    if not url:
        return url
    stripped = strip_query_param(url, "in_iframe")
    parts = urlsplit(stripped)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def html_to_text(html: Optional[str]) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text("\n", strip=True).strip()


def parse_location(value: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    cleaned = collapse_whitespace(value)
    if not cleaned:
        return None, None, None, None
    parts = [piece.strip() for piece in cleaned.split("-") if piece.strip()]
    if len(parts) >= 3:
        country, state = parts[0], parts[1]
        city = "-".join(parts[2:])
    elif len(parts) == 2:
        country, state = parts[0], None
        city = parts[1]
    else:
        country, state, city = None, None, cleaned
    display = ", ".join([piece for piece in (city, state, country) if piece])
    return display or cleaned, city, state, country


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
@dataclass
class ListingSummary:
    title: str
    detail_url_iframe: str
    detail_url: str
    job_id: Optional[str]
    summary_text: Optional[str]
    summary_html: Optional[str]
    location: Optional[str]
    city: Optional[str]
    state: Optional[str]
    country: Optional[str]
    company: Optional[str]
    category: Optional[str]
    position_type: Optional[str]
    location_type: Optional[str]
    posted_date_display: Optional[str]
    posted_date_exact: Optional[str]
    metadata: Dict[str, object]


@dataclass
class JobListing:
    job_id: Optional[str]
    title: str
    detail_url: str
    apply_url: Optional[str]
    location: Optional[str]
    city: Optional[str]
    state: Optional[str]
    country: Optional[str]
    posted_date: Optional[str]
    description_text: str
    description_html: Optional[str]
    metadata: Dict[str, object]


# ---------------------------------------------------------------------------
# Client implementation
# ---------------------------------------------------------------------------
class EMCORClient:
    def __init__(self, *, delay: float = 0.25, session: Optional[requests.Session] = None) -> None:
        self.delay = max(0.0, delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    def iter_listings(
        self,
        *,
        limit: Optional[int] = None,
        max_pages: Optional[int] = None,
        start_page: int = 0,
    ) -> Iterable[JobListing]:
        page = max(0, start_page)
        processed = 0

        while True:
            if max_pages is not None and (page - start_page) >= max_pages:
                self.logger.info("Reached max_pages=%s; stopping pagination.", max_pages)
                break

            soup = self._fetch_page(page)
            summaries = self._parse_page(soup)
            if not summaries:
                self.logger.info("No job listings discovered on page %s; ending scrape.", page)
                break

            for summary in summaries:
                try:
                    listing = self._hydrate_listing(summary)
                except ScraperError as exc:
                    self.logger.error("Skipping %s due to error: %s", summary.detail_url, exc)
                    continue

                yield listing
                processed += 1
                if limit is not None and processed >= limit:
                    self.logger.info("Limit %s reached; stopping scrape.", limit)
                    return

            page += 1
            if self.delay:
                time.sleep(self.delay)

    def _fetch_page(self, page: int) -> BeautifulSoup:
        params = {"pr": page, "in_iframe": "1", "searchRelation": "keyword_all"}
        self.logger.debug("Fetching job list page %s with params=%s", page, params)
        try:
            response = self.session.get(JOB_SEARCH_URL, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch listings page {page}: {exc}") from exc
        return BeautifulSoup(response.text, "html.parser")

    def _parse_page(self, soup: BeautifulSoup) -> List[ListingSummary]:
        container = soup.select_one("div.iCIMS_JobsTable")
        if not container:
            return []

        summaries: List[ListingSummary] = []
        for row in container.select("div.row"):
            title_link = row.select_one("div.col-xs-12.title a")
            title_node = title_link.select_one("h3") if title_link else None
            title = (
                collapse_whitespace(title_node.get_text(" ", strip=True))
                if title_node
                else collapse_whitespace(title_link.get_text(" ", strip=True)) if title_link else None
            )
            href = title_link["href"].strip() if title_link and title_link.get("href") else None
            if not title or not href:
                continue

            detail_url_iframe = urljoin(JOB_SEARCH_URL, href)
            detail_url = canonical_job_url(detail_url_iframe)

            description_node = row.select_one("div.col-xs-12.description")
            summary_html = description_node.decode_contents() if description_node else None
            summary_text = (
                collapse_whitespace(description_node.get_text(" ", strip=True)) if description_node else None
            )

            location_span = row.select_one("div.col-xs-6.header.left span:not(.sr-only)")
            location_raw = collapse_whitespace(
                location_span.get_text(" ", strip=True) if location_span else None
            )
            location, city, state, country = parse_location(location_raw)

            id_span = row.select_one("div.col-xs-6.header.right span:not(.sr-only)")
            job_id = collapse_whitespace(id_span.get_text(" ", strip=True) if id_span else None)

            header_fields: Dict[str, object] = {}
            posted_date_display: Optional[str] = None
            posted_date_exact: Optional[str] = None
            for tag in row.select("div.col-xs-12.additionalFields div.iCIMS_JobHeaderTag"):
                label_node = tag.select_one("dt")
                value_node = tag.select_one("dd")
                label = collapse_whitespace(label_node.get_text(" ", strip=True) if label_node else None)
                if not label:
                    continue
                value_text = collapse_whitespace(value_node.get_text(" ", strip=True) if value_node else None)
                header_fields[label.lower()] = value_text
                if label.lower() == "posted date":
                    posted_date_display = value_text
                    if value_node:
                        span = value_node.select_one("span")
                        if span and span.has_attr("title"):
                            posted_date_exact = collapse_whitespace(span["title"])

            summaries.append(
                ListingSummary(
                    title=title,
                    detail_url_iframe=detail_url_iframe,
                    detail_url=detail_url,
                    job_id=job_id,
                    summary_text=summary_text,
                    summary_html=summary_html,
                    location=location,
                    city=city,
                    state=state,
                    country=country,
                    company=collapse_whitespace(header_fields.get("company")),
                    category=collapse_whitespace(header_fields.get("category")),
                    position_type=collapse_whitespace(header_fields.get("position type")),
                    location_type=collapse_whitespace(header_fields.get("location type")),
                    posted_date_display=posted_date_display,
                    posted_date_exact=posted_date_exact,
                    metadata={
                        "row_fields": header_fields,
                        "list_location_raw": location_raw,
                        "summary_html": summary_html,
                    },
                )
            )

        return summaries

    def _hydrate_listing(self, summary: ListingSummary) -> JobListing:
        detail_html = self._fetch_detail(summary.detail_url_iframe)
        detail_data = self._parse_detail(detail_html)

        sections = detail_data["sections"]
        description_html = "\n\n".join(section["html"] for section in sections if section["html"]) or None
        description_text = "\n\n".join(section["text"] for section in sections if section["text"]).strip()
        if not description_text:
            description_text = summary.summary_text or "Description unavailable."

        apply_url = detail_data.get("apply_url")
        if apply_url:
            apply_url = strip_query_param(apply_url, "in_iframe")

        metadata = {
            "job_id": summary.job_id,
            "company": summary.company,
            "category": summary.category,
            "position_type": summary.position_type,
            "location_type": summary.location_type,
            "posted_date_display": summary.posted_date_display,
            "posted_date_exact": summary.posted_date_exact,
            "summary_text": summary.summary_text,
            "detail_sections": sections,
            "detail_url_iframe": summary.detail_url_iframe,
            "apply_url_iframe": detail_data.get("apply_url"),
        }
        metadata.update(detail_data.get("metadata", {}))
        metadata.update(summary.metadata)

        return JobListing(
            job_id=summary.job_id,
            title=summary.title,
            detail_url=summary.detail_url,
            apply_url=apply_url,
            location=summary.location,
            city=summary.city,
            state=summary.state,
            country=summary.country,
            posted_date=summary.posted_date_exact or summary.posted_date_display,
            description_text=description_text,
            description_html=description_html,
            metadata=metadata,
        )

    def _fetch_detail(self, url: str) -> str:
        self.logger.debug("Fetching job detail: %s", url)
        try:
            response = self.session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise ScraperError(f"Failed to fetch job detail {url}: {exc}") from exc
        return response.text

    def _parse_detail(self, html: str) -> Dict[str, object]:
        soup = BeautifulSoup(html, "html.parser")
        sections: List[Dict[str, str]] = []

        for heading in soup.select("h2.iCIMS_InfoMsg.iCIMS_InfoField_Job"):
            title = collapse_whitespace(heading.get_text(" ", strip=True))
            content_container = heading.find_next_sibling("div", class_="iCIMS_InfoMsg_Job")
            if not content_container:
                continue
            expandable = content_container.select_one("div.iCIMS_Expandable_Text") or content_container
            section_html = expandable.decode_contents().strip()
            section_text = html_to_text(section_html)
            sections.append({"title": title, "html": section_html, "text": section_text})

        apply_link = soup.select_one("a.iCIMS_ApplyOnlineButton")
        apply_url = apply_link.get("href") if apply_link and apply_link.get("href") else None
        if apply_url:
            apply_url = urljoin(JOB_SEARCH_URL, apply_url)

        header_fields: Dict[str, str] = {}
        for tag in soup.select("div.iCIMS_JobHeaderTag"):
            label_node = tag.select_one("dt")
            value_node = tag.select_one("dd")
            label = collapse_whitespace(label_node.get_text(" ", strip=True) if label_node else None)
            value = collapse_whitespace(value_node.get_text(" ", strip=True) if value_node else None)
            if label:
                header_fields[label.lower()] = value

        metadata = {
            "detail_header_fields": header_fields,
        }

        return {"sections": sections, "apply_url": apply_url, "metadata": metadata}


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------
def persist_listing(listing: JobListing) -> bool:
    defaults = {
        "title": listing.title[:255],
        "location": (listing.location or "")[:255] or None,
        "date": (listing.posted_date or "")[:100] or None,
        "description": listing.description_text[:10000],
        "metadata": {
            **listing.metadata,
            **({"description_html": listing.description_html} if listing.description_html else {}),
            **({"apply_url": listing.apply_url} if listing.apply_url else {}),
            "city": listing.city,
            "state": listing.state,
            "country": listing.country,
        },
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.detail_url,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Stored job '%s' (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape EMCOR Group iCIMS careers listings.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to process.")
    parser.add_argument("--max-pages", type=int, default=None, help="Maximum number of list pages to traverse.")
    parser.add_argument("--start-page", type=int, default=0, help="Page index to start from (default: 0).")
    parser.add_argument("--delay", type=float, default=0.25, help="Seconds to sleep between list pages.")
    parser.add_argument("--dry-run", action="store_true", help="Emit JSON without touching the database.")
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
    logger = logging.getLogger("emcor")

    client = EMCORClient(delay=args.delay)
    totals = {"fetched": 0, "created": 0, "updated": 0, "errors": 0}

    for listing in client.iter_listings(limit=args.limit, max_pages=args.max_pages, start_page=args.start_page):
        totals["fetched"] += 1
        if args.dry_run:
            payload = {
                "job_id": listing.job_id,
                "title": listing.title,
                "detail_url": listing.detail_url,
                "apply_url": listing.apply_url,
                "location": listing.location,
                "city": listing.city,
                "state": listing.state,
                "country": listing.country,
                "posted_date": listing.posted_date,
                "description": listing.description_text,
            }
            print(json.dumps(payload, ensure_ascii=False))
            continue

        try:
            created = persist_listing(listing)
        except Exception as exc:  # pragma: no cover - defensive persistence logging
            logger.error("Failed to persist %s: %s", listing.detail_url, exc)
            totals["errors"] += 1
            continue

        if created:
            totals["created"] += 1
        else:
            totals["updated"] += 1

    exit_code = 0
    if not args.dry_run:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        logger.info("Deduplication summary: %s", dedupe_summary)
        if totals["errors"]:
            exit_code = 1

    logger.info(
        "EMCOR scraper finished - fetched=%(fetched)s created=%(created)s updated=%(updated)s errors=%(errors)s",
        totals,
    )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
