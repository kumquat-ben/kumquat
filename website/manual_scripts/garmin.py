#!/usr/bin/env python3
"""Standalone scraper for the Garmin careers site powered by iCIMS/JIBE."""

from __future__ import annotations

import json
import sys
import time
from typing import Dict, Iterable, List, Optional, Set, Tuple

import requests

BASE_URL = "https://careers.garmin.com"
API_URL = f"{BASE_URL}/api/jobs"
DETAIL_URL_TEMPLATE = f"{BASE_URL}/jobs/{{slug}}"
REQUEST_TIMEOUT: Tuple[int, int] = (10, 30)
LANGUAGE = "en-us"
PAGE_LIMIT = 100
PAGE_DELAY = 0.1

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
)
COMMON_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}


def emit(event: str, data: Dict[str, object]) -> None:
    print(json.dumps({"event": event, "data": data}, ensure_ascii=False))
    sys.stdout.flush()


def collapse_whitespace(text: Optional[str]) -> str:
    if not text:
        return ""
    return " ".join(text.split())


def join_sections(sections: Iterable[Optional[str]]) -> str:
    parts: List[str] = []
    for section in sections:
        collapsed = collapse_whitespace(section)
        if collapsed:
            parts.append(collapsed)
    return "\n\n".join(parts)


def job_to_record(payload: Dict[str, object]) -> Dict[str, object]:
    data = payload.get("data") or {}
    slug = str(data.get("slug") or "").strip()
    description = join_sections(
        [
            data.get("description"),
            data.get("responsibilities"),
            data.get("qualifications"),
        ]
    )

    metadata = {
        "slug": slug or None,
        "req_id": data.get("req_id"),
        "language": data.get("language"),
        "employment_type": data.get("employment_type"),
        "category": data.get("category"),
        "categories": data.get("categories"),
        "tags": data.get("tags"),
        "tags1": data.get("tags1"),
        "tags2": data.get("tags2"),
        "tags3": data.get("tags3"),
        "tags7": data.get("tags7"),
        "posted_date": data.get("posted_date"),
        "posting_expiry_date": data.get("posting_expiry_date"),
        "update_date": data.get("update_date"),
        "create_date": data.get("create_date"),
        "apply_url": data.get("apply_url"),
        "location_name": data.get("location_name"),
        "street_address": data.get("street_address"),
        "city": data.get("city"),
        "state": data.get("state"),
        "country": data.get("country"),
        "postal_code": data.get("postal_code"),
        "latitude": data.get("latitude"),
        "longitude": data.get("longitude"),
        "multipleLocations": data.get("multipleLocations"),
    }

    clean_metadata = {key: value for key, value in metadata.items() if value not in (None, "", [], {})}

    record = {
        "title": collapse_whitespace(data.get("title")),
        "location": data.get("short_location") or data.get("full_location") or collapse_whitespace(data.get("location_name")),
        "date": data.get("posted_date"),
        "link": DETAIL_URL_TEMPLATE.format(slug=slug) if slug else data.get("apply_url"),
        "apply_url": data.get("apply_url"),
        "description": description,
        "metadata": clean_metadata,
    }
    return record


def fetch_page(session: requests.Session, page_number: int) -> Dict[str, object]:
    params = {
        "language": LANGUAGE,
        "limit": PAGE_LIMIT,
        "page": page_number,
    }
    response = session.get(API_URL, params=params, headers=COMMON_HEADERS, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def collect_jobs(session: requests.Session) -> List[Dict[str, object]]:
    jobs: List[Dict[str, object]] = []
    seen: Set[str] = set()
    total_expected: Optional[int] = None
    page_number = 1

    while True:
        payload = fetch_page(session, page_number)
        total_expected = payload.get("totalCount") or total_expected
        batch = payload.get("jobs") or []

        for item in batch:
            data = item.get("data") or {}
            slug = str(data.get("slug") or "").strip()
            unique_id = data.get("req_id") or slug
            if not unique_id or unique_id in seen:
                continue
            seen.add(unique_id)
            jobs.append(job_to_record(item))

        if not batch:
            break
        if total_expected is not None and len(seen) >= int(total_expected):
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
            "detail": f"Fetching Garmin job data from {API_URL}",
            "language": LANGUAGE,
            "page_limit": PAGE_LIMIT,
        },
    )

    try:
        jobs = collect_jobs(session)
    except Exception as exc:  # pragma: no cover
        emit("log", {"step": "error", "detail": str(exc)})
        raise

    emit(
        "result",
        {
            "company": "Garmin",
            "url": API_URL,
            "jobs": jobs,
            "count": len(jobs),
        },
    )


if __name__ == "__main__":
    main()
