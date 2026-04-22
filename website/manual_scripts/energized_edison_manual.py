#!/usr/bin/env python3
"""
Manual scraper for Energized by Edison's lineworker scholarship spotlight.

The article at ``ARTICLE_URL`` highlights Edison International's lineworker
scholarship program and links to the 2025 recipient showcase. This script
collects the profile data for each highlighted scholar and persists it in the
shared ``JobPosting`` table so operations teams can surface the opportunities
inside Kumquat.
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from urllib.parse import urljoin, urlparse, urlunparse

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
ARTICLE_URL = "https://energized.edison.com/stories/paving-a-career-path-in-linework"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.8",
    "Referer": ARTICLE_URL,
}
REQUEST_TIMEOUT = (15, 45)

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 6000), 30)
SCRAPER_QS = Scraper.objects.filter(company="Edison International", url=ARTICLE_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning(
            "Multiple Scraper rows matched Edison International article; using id=%s.", SCRAPER.id
        )
else:
    SCRAPER = Scraper.objects.create(
        company="Edison International",
        url=ARTICLE_URL,
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(Exception):
    """Raised when expected content is missing or a network call fails."""


@dataclass
class ScholarshipProfile:
    slug: str
    name: str
    link: str
    location: Optional[str]
    description: str
    metadata: Dict[str, object]


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------
def strip_tracking_query(url: str) -> str:
    """Remove query parameters and fragments from a URL."""
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, "", ""))


def html_to_text(html: str) -> str:
    """Normalize HTML into readable plain text."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    for tag in soup.find_all(attrs={"class": lambda value: value and "msocomtxt" in value}):
        tag.decompose()
    text = soup.get_text("\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def extract_blockquotes(soup: BeautifulSoup) -> List[str]:
    quotes: List[str] = []
    for block in soup.find_all("blockquote"):
        text = block.get_text(" ", strip=True)
        if text:
            quotes.append(text)
    return quotes


def extract_profile_image(profile: BeautifulSoup) -> Optional[str]:
    wrapper = profile.select_one(".profile-image")
    if not wrapper:
        return None

    style_attr = wrapper.get("style") or ""
    match = re.search(r"url\(([^)]+)\)", style_attr)
    if match:
        return match.group(1).strip(' "\'')

    img = wrapper.find("img")
    if img and img.get("src"):
        return img["src"]
    return None


def build_thumbnail_map(soup: BeautifulSoup) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for toggle in soup.select("#profile_gallery .profile-thmb a.profile-thmb-toggle"):
        href = toggle.get("href") or ""
        if not href.startswith("#"):
            continue
        slug = href.lstrip("#")
        img = toggle.find("img")
        if not img or not img.get("src"):
            continue
        mapping[slug] = img["src"]
    return mapping


def fetch_html(session: requests.Session, url: str) -> str:
    try:
        response = session.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException as exc:  # pragma: no cover - network guard
        raise ScraperError(f"Request failed for {url}: {exc}") from exc
    return response.text


def parse_article(session: requests.Session) -> Dict[str, Optional[str]]:
    html = fetch_html(session, ARTICLE_URL)
    soup = BeautifulSoup(html, "html.parser")

    title_node = soup.find("h1")
    title = title_node.get_text(strip=True) if title_node else None

    date_text = None
    published_iso = None

    date_node = soup.select_one(".story-date")
    if date_node:
        raw = date_node.get_text(" ", strip=True).replace("\xa0", " ")
        match = re.search(r"Published on\s*(.+)", raw)
        date_text = match.group(1).strip() if match else raw.strip()

    if not date_text:
        time_node = soup.find("time", attrs={"pubdate": True}) or soup.find(
            "time", attrs={"datetime": True}
        )
        if time_node:
            raw_attr = time_node.get("datetime") or time_node.get("pubdate")
            if raw_attr:
                published_iso = raw_attr.split("T")[0]
            text = time_node.get_text(" ", strip=True).replace("\xa0", " ")
            if text:
                match = re.search(r"Published on\s*(.+)", text)
                date_text = match.group(1).strip() if match else text.strip()

    if not published_iso and date_text:
        try:
            published_iso = datetime.strptime(date_text, "%B %d, %Y").date().isoformat()
        except ValueError:
            published_iso = date_text

    scholarship_url: Optional[str] = None
    for anchor in soup.select('a[href*="lineworker-scholarship"]'):
        href = anchor.get("href")
        if not href:
            continue
        candidate = urljoin(ARTICLE_URL, href)
        netloc = urlparse(candidate).netloc
        if "energized.edison.com" in netloc:
            scholarship_url = candidate
            break
        if scholarship_url is None:
            scholarship_url = candidate

    if not scholarship_url:
        raise ScraperError("Could not locate the scholarship link inside the article.")

    return {
        "title": title,
        "published_date": published_iso,
        "scholarship_url": scholarship_url,
    }


def fetch_scholarship_profiles(
    session: requests.Session,
    scholarship_url: str,
    *,
    limit: Optional[int] = None,
) -> List[ScholarshipProfile]:
    canonical_url = strip_tracking_query(scholarship_url)
    attempt_order: Iterable[str] = []
    if canonical_url:
        attempt_order = [canonical_url]
        if canonical_url != scholarship_url:
            attempt_order = [canonical_url, scholarship_url]
    else:
        attempt_order = [scholarship_url]

    html: Optional[str] = None
    used_url: Optional[str] = None
    for candidate in attempt_order:
        try:
            html_candidate = fetch_html(session, candidate)
        except ScraperError:
            continue
        html = html_candidate
        used_url = candidate
        break

    if html is None or used_url is None:
        raise ScraperError("Unable to retrieve scholarship profile content.")

    soup = BeautifulSoup(html, "html.parser")
    thumb_map = build_thumbnail_map(soup)

    profiles: List[ScholarshipProfile] = []
    for idx, profile_node in enumerate(soup.select("#profile_gallery .profile"), start=1):
        slug = profile_node.get("id")
        if not slug:
            logging.debug("Skipping profile without id attribute.")
            continue

        name_node = profile_node.select_one(".profile-title")
        if not name_node:
            logging.debug("Skipping profile %s with missing title.", slug)
            continue
        name = name_node.get_text(" ", strip=True)
        if not name:
            logging.debug("Skipping profile %s due to empty name.", slug)
            continue

        location_node = profile_node.select_one(".profile-subtitle")
        location = location_node.get_text(" ", strip=True) if location_node else None
        location = location or None

        body_node = profile_node.select_one(".profile-body")
        if not body_node:
            logging.debug("Skipping profile %s due to missing body content.", slug)
            continue

        body_html = body_node.decode_contents()
        body_soup = BeautifulSoup(body_html, "html.parser")
        quotes = extract_blockquotes(body_soup)
        description = html_to_text(body_html)

        metadata: Dict[str, object] = {
            "profile_slug": slug,
            "profile_index": idx,
            "profile_image": extract_profile_image(profile_node),
            "thumbnail": thumb_map.get(slug),
            "quotes": quotes or None,
            "scholarship_page": used_url,
        }
        metadata = {key: value for key, value in metadata.items() if value not in (None, "", [])}

        profile = ScholarshipProfile(
            slug=slug,
            name=name,
            link=f"{used_url}#{slug}",
            location=location,
            description=description,
            metadata=metadata,
        )
        profiles.append(profile)
        logging.debug("Parsed scholarship profile %s (%s).", profile.name, profile.slug)

        if limit is not None and len(profiles) >= limit:
            break

    if not profiles:
        raise ScraperError("No scholarship profiles were discovered on the scholarship page.")
    return profiles


def store_profiles(
    profiles: Iterable[ScholarshipProfile],
    *,
    published_date: Optional[str],
) -> Dict[str, int]:
    created = 0
    updated = 0
    for profile in profiles:
        title = f"Lineworker Scholarship Spotlight – {profile.name}"
        metadata = dict(profile.metadata)
        metadata.setdefault("source_article", ARTICLE_URL)

        defaults = {
            "title": title[:255],
            "location": (profile.location or "")[:255] or None,
            "date": (published_date or "")[:100] or None,
            "description": profile.description[:10000],
            "metadata": metadata,
        }

        obj, created_flag = JobPosting.objects.update_or_create(
            scraper=SCRAPER,
            link=profile.link,
            defaults=defaults,
        )

        if created_flag:
            created += 1
            logging.info("Created job posting id=%s for %s", obj.id, profile.name)
        else:
            updated += 1
            logging.info("Updated job posting id=%s for %s", obj.id, profile.name)

    return {"created": created, "updated": updated, "total": created + updated}


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape the Edison lineworker spotlight page.")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of scholarship profiles to process.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (default: INFO).",
    )
    parser.add_argument(
        "--no-dedupe",
        action="store_true",
        help="Skip deduplication after persisting results.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    logging.info("Starting Energized Edison lineworker scraper.")

    with requests.Session() as session:
        session.headers.update(DEFAULT_HEADERS)

        article_data = parse_article(session)
        scholarship_url = article_data["scholarship_url"]
        published_date = article_data["published_date"]

        logging.info("Article published date: %s", published_date or "unknown")
        logging.info("Discovered scholarship page: %s", scholarship_url)

        profiles = fetch_scholarship_profiles(session, scholarship_url, limit=args.limit)
        logging.info("Found %s scholarship profiles.", len(profiles))

    summary = store_profiles(profiles, published_date=published_date)

    dedupe_summary: Optional[Dict[str, object]] = None
    if not args.no_dedupe:
        dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
        logging.info(
            "Deduplication removed %(removed)s duplicates across %(duplicate_groups)s groups.",
            dedupe_summary,
        )

    print(
        f"Processed {summary['total']} profiles "
        f"(created={summary['created']}, updated={summary['updated']})."
    )
    if dedupe_summary:
        print(
            "Deduplication: "
            f"removed={dedupe_summary['removed']} groups={dedupe_summary['duplicate_groups']}"
        )


if __name__ == "__main__":
    main()
