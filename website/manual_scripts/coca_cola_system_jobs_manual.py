#!/usr/bin/env python3
"""Manual scraper for Coca-Cola System job board links.

The System Jobs landing page curates external job board links for the global
network of Coca-Cola bottlers and brands. This script extracts those entries,
normalises the metadata, and writes/updates `JobPosting` records so operations
teams can direct candidates to the correct partner careers sites.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse

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

from django.db import transaction  # noqa: E402

from scrapers.models import JobPosting, Scraper  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TARGET_URL = "https://www.coca-colacompany.com/careers/job-search/systemjobs"
BASE_URL = "https://www.coca-colacompany.com"
SCRAPER_NAME = "Coca-Cola System Jobs"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


@dataclass
class JobBoardEntry:
    link: str
    title: str
    regions: Set[str] = field(default_factory=set)
    image_url: Optional[str] = None
    image_mobile_url: Optional[str] = None
    image_alt: Optional[str] = None
    component_id: Optional[str] = None
    component_type: Optional[str] = None
    component_title: Optional[str] = None
    component_modified: Optional[str] = None

    @property
    def location_label(self) -> Optional[str]:
        if not self.regions:
            return None
        # Remove the catch-all "All" unless it is the only category.
        filtered = [r for r in self.regions if r.lower() != "all"] or list(self.regions)
        filtered = sorted(set(filtered), key=str.lower)
        if len(filtered) == 1:
            return filtered[0]
        return " / ".join(filtered)

    def as_metadata(self) -> Dict[str, object]:
        return {
            "regions": sorted(self.regions, key=str.lower),
            "image": {
                "alt": self.image_alt,
                "url": self.image_url,
                "mobile_url": self.image_mobile_url,
            },
            "component": {
                "id": self.component_id,
                "type": self.component_type,
                "title": self.component_title,
                "modified": self.component_modified,
            },
            "source": TARGET_URL,
        }

    def description(self) -> str:
        region_text = ", ".join(sorted(self.regions, key=str.lower)) or "All"
        return (
            f"External careers site for {self.title}. Regions: {region_text}. "
            f"Scraped from Coca-Cola System Jobs."
        )


def get_scraper() -> Scraper:
    defaults = {
        "code": "manual-script",
        "interval_hours": 24,
        "timeout_seconds": 300,
        "active": True,
    }
    scraper, created = Scraper.objects.get_or_create(
        company=SCRAPER_NAME,
        url=TARGET_URL,
        defaults=defaults,
    )
    if created:
        logging.info("Created Scraper record id=%s for %s", scraper.id, SCRAPER_NAME)
    return scraper


def _normalise_text(value: str) -> str:
    if not value:
        return value
    try:
        return value.encode("latin-1").decode("utf-8")
    except UnicodeError:
        return value


def clean_title(raw_title: str, link: str) -> str:
    raw_title = _normalise_text((raw_title or "").strip())
    if raw_title:
        cleaned = re.split(r"\b(?:logo|button)\b", raw_title, flags=re.IGNORECASE)[0].strip()
        if not cleaned:
            cleaned = raw_title
    else:
        cleaned = ""

    if cleaned:
        cleaned = (
            cleaned.replace("\u00ad", "-")
            .replace("\u2010", "-")
            .replace("\u2011", "-")
            .replace("\u2013", "-")
            .replace("\u2014", "-")
        )
        cleaned = re.sub(r"\bCoca[\s\u00A0]+Cola\b", "Coca-Cola", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\bCoca\s+Coca\b", "Coca-Cola", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s+", " ", cleaned).strip(" -")
        return cleaned

    parsed = urlparse(link)
    host = parsed.netloc.replace("www.", "")
    return host or link


def parse_data_layer(raw: str) -> Dict[str, object]:
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        logging.debug("Failed to decode data layer payload: %s", raw[:120])
        return {}


def _find_region_tabs_container(soup: BeautifulSoup):
    for container in soup.select("div.cmp-tabs"):
        labels = [li.get_text(strip=True) for li in container.select("li.cmp-tabs__tab")]
        if labels and "All" in labels and len(labels) > 1:
            return container
    return None


def collect_entries(html: str) -> List[JobBoardEntry]:
    soup = BeautifulSoup(html, "html.parser")
    entries: Dict[str, JobBoardEntry] = {}
    tabs_container = _find_region_tabs_container(soup)
    if not tabs_container:
        logging.warning("Unable to locate region tabs on the System Jobs page.")
        return []

    for tab in tabs_container.select("li.cmp-tabs__tab"):
        label = tab.get_text(strip=True)
        panel_id = tab.get("aria-controls")
        if not panel_id:
            continue
        panel = soup.select_one(f"#{panel_id}")
        if not panel:
            continue

        for block in panel.select(".cmp-adaptive-image[data-cmp-data-layer]"):
            anchor = block.select_one("a[href]")
            if not anchor:
                continue
            href = anchor.get("href", "").strip()
            if not href:
                continue
            absolute_link = urljoin(BASE_URL, href)

            img = block.select_one("img")
            alt_text = _normalise_text((img.get("alt") or "").strip()) if img else ""
            image_src = urljoin(BASE_URL, img["src"]) if img and img.get("src") else None

            data_layer = parse_data_layer(block.get("data-cmp-data-layer", ""))
            component_id, component_info = next(iter(data_layer.items())) if data_layer else (None, None)

            image_mobile = None
            component_modified = None
            component_type = None
            component_title = None
            if isinstance(component_info, dict):
                component_modified = component_info.get("repo:modifyDate")
                component_type = component_info.get("@type")
                component_title = component_info.get("dc:title")
                image_info = component_info.get("image") or {}
                image_mobile = image_info.get("repo:mobile:path")
                if not image_src and image_info.get("repo:path"):
                    image_src = urljoin(BASE_URL, image_info["repo:path"])
            if image_mobile:
                image_mobile = urljoin(BASE_URL, image_mobile)

            entry = entries.get(absolute_link)
            if entry is None:
                title = clean_title(alt_text, absolute_link)
                entry = JobBoardEntry(
                    link=absolute_link,
                    title=title,
                    image_url=image_src,
                    image_mobile_url=image_mobile,
                    image_alt=alt_text or None,
                    component_id=component_id,
                    component_type=component_type,
                    component_title=component_title,
                    component_modified=component_modified,
                )
                entries[absolute_link] = entry
            entry.regions.add(label)

    return sorted(entries.values(), key=lambda e: e.title.lower())


def fetch_html(session: requests.Session, *, timeout: int) -> str:
    response = session.get(TARGET_URL, timeout=timeout)
    response.raise_for_status()
    return response.text


def persist_entries(scraper: Scraper, entries: List[JobBoardEntry], *, dry_run: bool = False) -> Dict[str, int]:
    created = 0
    updated = 0
    skipped = 0

    if dry_run:
        for entry in entries:
            logging.info(
                "[dry-run] %s -> %s (%s)",
                entry.title,
                entry.link,
                ", ".join(sorted(entry.regions)),
            )
        return {"created": 0, "updated": 0, "skipped": len(entries)}

    with transaction.atomic():
        for entry in entries:
            defaults = {
                "title": entry.title[:255],
                "location": (entry.location_label or "")[:255] or None,
                "description": entry.description()[:10000],
                "metadata": entry.as_metadata(),
            }
            obj, created_flag = JobPosting.objects.update_or_create(
                scraper=scraper,
                link=entry.link,
                defaults=defaults,
            )
            if created_flag:
                created += 1
                logging.info("Created JobPosting id=%s %s", obj.id, entry.title)
            else:
                updated += 1
    return {"created": created, "updated": updated, "skipped": skipped}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and parse entries without writing to the database.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="HTTP timeout (seconds) for the landing page request.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    try:
        html = fetch_html(session, timeout=args.timeout)
    except requests.RequestException as exc:
        logging.error("Failed to fetch %s: %s", TARGET_URL, exc)
        return 2

    entries = collect_entries(html)
    if not entries:
        logging.warning("No job board entries discovered on the System Jobs page.")

    scraper = get_scraper()
    result = persist_entries(scraper, entries, dry_run=args.dry_run)

    logging.info(
        "Finished. Entries=%s created=%s updated=%s skipped=%s",
        len(entries),
        result["created"],
        result["updated"],
        result["skipped"],
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
