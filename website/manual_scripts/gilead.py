#!/usr/bin/env python3
"""Standalone scraper for the Gilead job board hosted on Yello."""

from __future__ import annotations

import json
import sys
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag

BASE_URL = "https://gilead.yello.co"
JOB_BOARD_ID = "v42vD4vKxb3AkKvV93YsrQ"
SEARCH_URL = f"{BASE_URL}/job_boards/{JOB_BOARD_ID}/search"
DETAIL_URL_TEMPLATE = f"{BASE_URL}/jobs/{{job_id}}"
REQUEST_TIMEOUT: Tuple[int, int] = (10, 30)
LOCALE = "en"
DETAIL_DELAY = 0.05
PAGE_DELAY = 0.15

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
)
COMMON_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept-Language": "en-US,en;q=0.9",
}
JSON_HEADERS = {**COMMON_HEADERS, "Accept": "application/json"}
HTML_HEADERS = {
    **COMMON_HEADERS,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": BASE_URL,
}


def emit(event: str, data: Dict[str, object]) -> None:
    print(json.dumps({"event": event, "data": data}, ensure_ascii=False))
    sys.stdout.flush()


def clean_text(fragment: Optional[str]) -> str:
    soup = BeautifulSoup(fragment or "", "html.parser")
    return soup.get_text("\n", strip=True).strip()


def fetch_page(session: requests.Session, page_number: int) -> Dict[str, object]:
    params = {"locale": LOCALE, "page_number": page_number}
    response = session.get(SEARCH_URL, params=params, headers=JSON_HEADERS, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def parse_listing_item(li_tag: Tag) -> Optional[Dict[str, object]]:
    anchor = li_tag.select_one("a.search-results__req_title")
    if not anchor:
        return None

    href = anchor.get("href") or ""
    if not href.startswith("/jobs/"):
        return None

    link = urljoin(BASE_URL, href)
    job_id = href.split("/jobs/")[-1].split("?")[0]

    info_spans = li_tag.select("div.search-results__jobinfo span")
    location = info_spans[0].get_text(" ", strip=True) if len(info_spans) > 0 else ""
    department = info_spans[1].get_text(" ", strip=True) if len(info_spans) > 1 else ""
    schedule = info_spans[2].get_text(" ", strip=True) if len(info_spans) > 2 else ""

    posted_node = li_tag.select_one("div.search-results__post-time")
    posted = posted_node.get_text(strip=True) if posted_node else ""

    listing = {
        "job_id": job_id,
        "title": anchor.get_text(strip=True),
        "link": link,
        "location": location,
        "department": department or None,
        "schedule": schedule or None,
        "posted": posted,
    }
    return listing


def fetch_detail(session: requests.Session, job_id: str) -> Dict[str, object]:
    params = {"job_board_id": JOB_BOARD_ID, "locale": LOCALE}
    url = DETAIL_URL_TEMPLATE.format(job_id=job_id)
    response = session.get(url, params=params, headers=HTML_HEADERS, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    description_div = soup.select_one(".job-details__description")
    description_html = str(description_div) if description_div else ""
    description_text = clean_text(description_html)

    summary_container = soup.select_one(".details-top__container")
    summary_spans = summary_container.find_all("span") if summary_container else []
    summary_department = summary_spans[1].get_text(" ", strip=True) if len(summary_spans) > 1 else None
    summary_schedule = summary_spans[2].get_text(" ", strip=True) if len(summary_spans) > 2 else None

    sidebar_fields: Dict[str, str] = {}
    for group in soup.select(".secondary-details__group"):
        title = group.select_one(".secondary-details__title")
        content = group.select_one(".secondary-details__content")
        if title and content:
            sidebar_fields[title.get_text(strip=True)] = content.get_text(strip=True)

    detail: Dict[str, object] = {
        "description_html": description_html,
        "description_text": description_text,
        "department": summary_department or sidebar_fields.get("Department"),
        "schedule": summary_schedule or sidebar_fields.get("Full Time/Part Time"),
        "sidebar_fields": sidebar_fields,
    }
    return detail


def collect_jobs(session: requests.Session) -> List[Dict[str, object]]:
    jobs: List[Dict[str, object]] = []
    seen_ids = set()
    page_number = 1

    while True:
        page_payload = fetch_page(session, page_number)
        soup = BeautifulSoup(page_payload.get("html") or "", "html.parser")
        items = soup.select("li.search-results__item")

        for item in items:
            listing = parse_listing_item(item)
            if not listing:
                continue
            job_id = listing["job_id"]
            if job_id in seen_ids:
                continue

            detail = fetch_detail(session, job_id)
            seen_ids.add(job_id)

            sidebar = detail.get("sidebar_fields") or {}
            metadata: Dict[str, object] = {
                "department": detail.get("department") or listing.get("department"),
                "schedule": detail.get("schedule") or listing.get("schedule"),
                "job_board_id": JOB_BOARD_ID,
                "job_requisition_id": sidebar.get("Job Requisition ID"),
                "job_level": sidebar.get("Job Level"),
                "remote_type": sidebar.get("Remote Type"),
                "sidebar_fields": sidebar or None,
                "search_posted_label": listing.get("posted") or None,
            }

            job_record = {
                "title": listing["title"],
                "location": listing["location"],
                "date": listing["posted"],
                "link": listing["link"],
                "description": detail.get("description_text") or "",
                "metadata": {k: v for k, v in metadata.items() if v},
            }
            jobs.append(job_record)
            if DETAIL_DELAY:
                time.sleep(DETAIL_DELAY)

        if not page_payload.get("more_requisitions"):
            break
        page_number += 1
        if PAGE_DELAY:
            time.sleep(PAGE_DELAY)

    return jobs


def main() -> None:
    session = requests.Session()
    session.headers.update(COMMON_HEADERS)

    emit(
        "log",
        {
            "step": "start",
            "detail": f"Fetching paginated listings from {SEARCH_URL}",
            "job_board_id": JOB_BOARD_ID,
        },
    )

    try:
        jobs = collect_jobs(session)
    except Exception as exc:  # pragma: no cover - defensive for runtime usage
        emit("log", {"step": "error", "detail": str(exc)})
        raise

    emit(
        "result",
        {
            "company": "Gilead Sciences",
            "url": SEARCH_URL,
            "jobs": jobs,
            "count": len(jobs),
        },
    )


if __name__ == "__main__":
    main()
