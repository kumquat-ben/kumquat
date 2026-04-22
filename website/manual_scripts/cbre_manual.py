#!/usr/bin/env python3
"""Manual scraper for the CBRE careers site (https://careers.cbre.com)."""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple
from xml.etree import ElementTree as ET

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
BASE_URL = "https://careers.cbre.com"
SEARCH_PAGE = "/en_US/careers/SearchJobs/"
FEED_PATH = "/en_US/careers/SearchJobs/feed/"
DETAIL_PREFIX = "/careers/JobDetail/"
FEED_PAGE_SIZE = 50
REQUEST_TIMEOUT = (10, 40)
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
    "image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

WAF_TOKEN_SCRIPT = textwrap.dedent(
    """
    const {JSDOM} = require('jsdom');
    const vm = require('node:vm');

    async function getToken() {
      const headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'sec-ch-ua': '"Chromium";v="127", "Not)A;Brand";v="24", "Google Chrome";v="127"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1'
      };
      const url = 'https://careers.cbre.com/en_US/careers/SearchJobs/?jobRecordsPerPage=50&';
      const pageResp = await fetch(url, { headers });
      const html = await pageResp.text();
      const match = html.match(/window\\.gokuProps\\s*=\\s*(\\{.*?\\});/s);
      if (!match) {
        throw new Error('Unable to locate gokuProps on search page');
      }
      const gokuProps = JSON.parse(match[1]);
      const challengeMatch = html.match(/<script src="(https:\\/\\/[^"]+challenge\\.js)"/);
      if (!challengeMatch) {
        throw new Error('Unable to locate challenge script URL');
      }
      const challengeJs = await (await fetch(challengeMatch[1])).text();
      const dom = new JSDOM('<!doctype html><html><body></body></html>', {
        runScripts: 'outside-only',
        url,
        pretendToBeVisual: true,
      });
      const { window } = dom;
      Object.defineProperty(window, 'fetch', { value: fetch });
      Object.defineProperty(window.navigator, 'userAgent', { value: headers['User-Agent'] });
      Object.defineProperty(window.navigator, 'language', { value: 'en-US' });
      Object.defineProperty(window.navigator, 'languages', { value: ['en-US', 'en'] });
      Object.defineProperty(window, 'crypto', { value: global.crypto });
      window.atob = (str) => Buffer.from(str, 'base64').toString('binary');
      window.btoa = (str) => Buffer.from(str, 'binary').toString('base64');
      window.gokuProps = gokuProps;
      window.awsWafCookieDomainList = [];

      const script = new vm.Script(challengeJs);
      script.runInContext(dom.getInternalVMContext());

      const api = window.AwsWafIntegration;
      await api.saveReferrer();
      const forceRefresh = await api.checkForceRefresh();
      if (forceRefresh) {
        await api.forceRefreshToken();
      }
      const token = await api.getToken();
      const cookies = window.document.cookie || '';
      console.log(JSON.stringify({ token, cookies }));
    }

    getToken().then(() => process.exit(0)).catch((err) => {
      console.error('ERROR', err);
      process.exit(1);
    });
    """
).strip()

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 900), 60)

SCRAPER_QS = Scraper.objects.filter(
    company="CBRE",
    url=f"{BASE_URL}{SEARCH_PAGE}",
).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Scraper rows for CBRE detected; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company="CBRE",
        url=f"{BASE_URL}{SEARCH_PAGE}",
        code="manual-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class ScraperError(RuntimeError):
    """Raised when the scraper encounters an unrecoverable error."""


@dataclass
class JobRecord:
    title: str
    link: str
    job_id: Optional[str]
    posted_date: Optional[str]
    location: Optional[str]
    role_type: Optional[str]
    areas_of_interest: List[str]
    description_html: str
    description_text: str
    metadata: Dict[str, object]


# ---------------------------------------------------------------------------
# Networking helpers
# ---------------------------------------------------------------------------
def _obtain_waf_cookie(logger: logging.Logger) -> str:
    """Execute the embedded Node.js script to solve the AWS WAF challenge."""
    try:
        result = subprocess.run(
            ["node", "-"],
            input=WAF_TOKEN_SCRIPT,
            text=True,
            capture_output=True,
            check=True,
        )
    except FileNotFoundError as exc:  # pragma: no cover - environment specific
        raise ScraperError("Node.js runtime not found; required to solve AWS WAF challenge.") from exc
    except subprocess.CalledProcessError as exc:  # pragma: no cover - diagnostic path
        stderr = exc.stderr.strip() if exc.stderr else ""
        stdout = exc.stdout.strip() if exc.stdout else ""
        logger.error("WAF helper script failed. stdout=%s stderr=%s", stdout, stderr)
        raise ScraperError("Failed to acquire AWS WAF token.") from exc

    payload_line = result.stdout.strip().splitlines()
    if not payload_line:
        raise ScraperError("Node.js helper returned no output when solving WAF challenge.")

    try:
        payload = json.loads(payload_line[-1])
    except json.JSONDecodeError as exc:  # pragma: no cover
        raise ScraperError(f"Unexpected JSON payload from WAF helper: {payload_line[-1]!r}") from exc

    token = (payload.get("token") or "").strip()
    cookies = (payload.get("cookies") or "").strip()
    if not token or "aws-waf-token=" not in cookies:
        raise ScraperError("Node.js helper did not provide the expected aws-waf-token cookie.")
    logger.debug("Obtained aws-waf-token (%s chars)", len(token))
    return cookies


def _prime_session(cookie_header: str, logger: logging.Logger) -> requests.Session:
    """Return a session pre-loaded with the required cookies."""
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    # Apply cookies from Node helper.
    for chunk in cookie_header.split(";"):
        chunk = chunk.strip()
        if not chunk or "=" not in chunk:
            continue
        name, value = chunk.split("=", 1)
        session.cookies.set(name.strip(), value.strip(), domain="careers.cbre.com")

    search_url = f"{BASE_URL}{SEARCH_PAGE}?jobRecordsPerPage={FEED_PAGE_SIZE}&"
    resp = session.get(search_url, timeout=REQUEST_TIMEOUT)
    try:
        resp.raise_for_status()
    except requests.HTTPError as exc:  # pragma: no cover
        raise ScraperError(f"Failed to prime session via search page: {exc}") from exc

    logger.debug("Session cookies after priming: %s", dict(session.cookies.get_dict()))
    return session


def _fetch_feed_page(
    session: requests.Session,
    *,
    offset: int,
) -> List[Tuple[str, str, str]]:
    """Return a list of (title, link, pubDate) tuples from the RSS feed."""
    params = {"jobRecordsPerPage": FEED_PAGE_SIZE, "jobOffset": offset}
    feed_url = f"{BASE_URL}{FEED_PATH}"
    resp = session.get(feed_url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()

    root = ET.fromstring(resp.text)
    channel = root.find("channel")
    if channel is None:
        return []

    records: List[Tuple[str, str, str]] = []
    for item in channel.findall("item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub_date = (item.findtext("pubDate") or "").strip()
        if not link:
            continue
        records.append((title, link, pub_date))
    return records


def _fetch_job_detail(
    session: requests.Session,
    *,
    url: str,
) -> JobRecord:
    resp = session.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    title_el = soup.select_one(".article__content__view__field.view--title .article__content__view__field__value")
    title = (title_el.get_text(strip=True) if title_el else "").strip() or soup.title.get_text(strip=True)

    job_id = None
    posted = None
    role_type = None
    location = None
    areas_of_interest: List[str] = []

    description_block = None

    for field in soup.select(".article__content__view__field"):
        label_el = field.select_one(".article__content__view__field__label")
        value_el = field.select_one(".article__content__view__field__value")
        if not value_el:
            continue
        value_text = value_el.get_text(" ", strip=True)
        if not label_el:
            if description_block is None and value_text:
                description_block = value_el
            continue
        label = label_el.get_text(strip=True).lower()
        if label == "job id":
            job_id = value_text
        elif label == "posted":
            posted = value_text
        elif label == "role type":
            role_type = value_text
        elif label == "location(s)":
            location = value_text
        elif label == "areas of interest":
            if value_text:
                areas_of_interest.append(value_text)

    description_container = description_block
    description_html = ""
    if description_container:
        description_html = str(description_container)

    description_text = ""
    if description_html:
        text_soup = BeautifulSoup(description_html, "html.parser")
        description_text = text_soup.get_text("\n", strip=True)

    metadata: Dict[str, object] = {
        "areas_of_interest": areas_of_interest,
        "role_type": role_type,
    }

    return JobRecord(
        title=title or "",
        link=url,
        job_id=job_id,
        posted_date=posted,
        location=location,
        role_type=role_type,
        areas_of_interest=areas_of_interest,
        description_html=description_html,
        description_text=description_text,
        metadata=metadata,
    )


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------
def store_job(record: JobRecord) -> bool:
    defaults = {
        "title": record.title[:255],
        "location": (record.location or "")[:255] or None,
        "date": (record.posted_date or "")[:100] or None,
        "description": record.description_text[:10000],
        "metadata": {
            **record.metadata,
            "job_id": record.job_id,
            "description_html": record.description_html,
        },
    }
    obj, created = JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=record.link,
        defaults=defaults,
    )
    logging.getLogger("persist").debug(
        "Persisted CBRE job %s (created=%s, id=%s)", obj.title, created, obj.id
    )
    return created


# ---------------------------------------------------------------------------
# Scraper orchestration
# ---------------------------------------------------------------------------
def iterate_feed(session: requests.Session, *, limit: Optional[int], logger: logging.Logger) -> Iterator[Tuple[str, str, str]]:
    offset = 0
    fetched = 0
    while True:
        records = _fetch_feed_page(session, offset=offset)
        if not records:
            logger.debug("No records returned at offset=%s; stopping pagination.", offset)
            return
        for record in records:
            yield record
            fetched += 1
            if limit is not None and fetched >= limit:
                return
        offset += len(records)


def run_scraper(*, limit: Optional[int], logger: logging.Logger) -> Dict[str, int]:
    summary = {
        "fetched": 0,
        "created": 0,
        "updated": 0,
        "errors": 0,
    }

    waf_cookie = _obtain_waf_cookie(logger)
    session = _prime_session(waf_cookie, logger)

    for title, link, published in iterate_feed(session, limit=limit, logger=logger):
        summary["fetched"] += 1
        try:
            record = _fetch_job_detail(session, url=link)
            if not record.posted_date and published:
                record.posted_date = published
            if not record.title:
                record.title = title
            created = store_job(record)
            if created:
                summary["created"] += 1
            else:
                summary["updated"] += 1
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("Failed to process job %s: %s", link, exc)
            summary["errors"] += 1

    return summary


# ---------------------------------------------------------------------------
# CLI glue
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CBRE careers manual scraper")
    parser.add_argument("--limit", type=int, default=None, help="Only process the first N jobs.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity (default: INFO)",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    logger = logging.getLogger("cbre.manual")

    try:
        totals = run_scraper(limit=args.limit, logger=logger)
    except ScraperError as exc:
        logger.error("Scraper failed: %s", exc)
        return 1

    dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
    totals["dedupe"] = dedupe_summary

    logger.info("Summary: %s", json.dumps(totals, ensure_ascii=False))
    print(json.dumps(totals, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
