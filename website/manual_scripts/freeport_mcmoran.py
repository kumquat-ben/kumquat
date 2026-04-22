#!/usr/bin/env python3
"""Standalone scraper for the Freeport-McMoRan career site (SuccessFactors)."""

from __future__ import annotations

import json
import re
import sys
import time
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import parse_qsl, urljoin

import requests
from bs4 import BeautifulSoup, Tag

BASE_URL = "https://jobs.fcx.com"
SEARCH_URL = f"{BASE_URL}/search/"
REQUEST_TIMEOUT: Tuple[int, int] = (10, 30)
PAGE_DELAY = 0.2
DETAIL_DELAY = 0.1

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
)
COMMON_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept-Language": "en-US,en;q=0.9",
}
HTML_HEADERS = {
    **COMMON_HEADERS,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": SEARCH_URL,
}


def emit(event: str, data: Dict[str, object]) -> None:
    print(json.dumps({"event": event, "data": data}, ensure_ascii=False))
    sys.stdout.flush()


def clean_text(fragment: Optional[str]) -> str:
    soup = BeautifulSoup(fragment or "", "html.parser")
    return soup.get_text("\n", strip=True).strip()


def _select_first_text(tag: Tag, candidates: Iterable[str]) -> str:
    for selector in candidates:
        node = tag.select_one(selector)
        if node:
            return node.get_text(" ", strip=True)
    return ""


def _extract_field(li_tag: Tag, field: str) -> str:
    suffixes = [
        f"-desktop-section-{field}-value",
        f"-mobile-section-{field}-value",
        f"-tablet-section-{field}-value",
    ]
    selectors = [f"div[id$='{suffix}']" for suffix in suffixes]
    fallback_selector = f".section-field.{field}"

    value = _select_first_text(li_tag, selectors)
    if value:
        return value

    fallback = li_tag.select_one(fallback_selector)
    if fallback:
        parts = list(fallback.stripped_strings)
        if parts and ":" in parts[0]:
            parts = parts[1:]
        return " ".join(parts)
    return ""


def fetch_search_config(session: requests.Session) -> Dict[str, object]:
    response = session.get(SEARCH_URL, headers=HTML_HEADERS, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    script_text = ""
    for script in soup.find_all("script"):
        if script.string and "j2w.SearchResults.init" in script.string:
            script_text = script.string
            break

    if not script_text:
        raise RuntimeError("Unable to locate the search configuration script.")

    def extract(pattern: str) -> str:
        match = re.search(pattern, script_text)
        if not match:
            raise RuntimeError(f"Missing pattern '{pattern}' in search configuration.")
        return match.group(1)

    api_endpoint = extract(r'apiEndpoint:\s*"([^"]+)"')
    search_query = extract(r'searchQuery:\s*"([^"]*)"')
    per_page = int(extract(r'jobRecordsPerPage:\s*parseInt\("(\d+)"\)'))
    total = int(extract(r'jobRecordsFound:\s*parseInt\("(\d+)"\)'))

    params = dict(parse_qsl(search_query.lstrip("?"), keep_blank_values=True))

    endpoint_path = api_endpoint.strip()
    if not endpoint_path.startswith("/"):
        endpoint_path = f"/{endpoint_path}"
    if not endpoint_path.endswith("/"):
        endpoint_path = f"{endpoint_path}/"

    return {
        "endpoint_path": endpoint_path,
        "search_query": search_query,
        "params": params,
        "per_page": per_page,
        "total": total,
    }


def fetch_tile_page(
    session: requests.Session,
    endpoint_path: str,
    params: Dict[str, str],
    startrow: int,
) -> BeautifulSoup:
    merged_params = dict(params)
    if startrow:
        merged_params["startrow"] = str(startrow)

    url = urljoin(BASE_URL, endpoint_path)
    response = session.get(url, params=merged_params, headers=HTML_HEADERS, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def parse_listing(li_tag: Tag) -> Optional[Dict[str, object]]:
    anchor = li_tag.select_one("a.jobTitle-link")
    if not anchor:
        return None

    url_fragment = li_tag.get("data-url") or anchor.get("href") or ""
    link = urljoin(BASE_URL, url_fragment)
    job_id = url_fragment.rstrip("/").split("/")[-1] if url_fragment else ""

    listing = {
        "job_id": job_id,
        "title": anchor.get_text(strip=True),
        "link": link,
        "requisition_id": _extract_field(li_tag, "customfield1") or None,
        "department": _extract_field(li_tag, "department") or None,
        "location": _extract_field(li_tag, "location") or None,
        "posted": _extract_field(li_tag, "date") or None,
        "row_index": int(li_tag.get("data-row-index") or 0),
    }
    return listing


def fetch_job_detail(session: requests.Session, url: str) -> Dict[str, object]:
    response = session.get(url, headers=HTML_HEADERS, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    description_node = soup.select_one(".jobdescription")
    description_html = str(description_node) if description_node else ""
    description_text = clean_text(description_html)

    return {
        "description_html": description_html,
        "description_text": description_text,
    }


def collect_jobs(session: requests.Session) -> List[Dict[str, object]]:
    config = fetch_search_config(session)
    endpoint_path = config["endpoint_path"]
    params = config["params"]
    per_page = config["per_page"]
    total = config["total"]

    jobs: List[Dict[str, object]] = []
    seen_ids: Set[str] = set()

    emit(
        "log",
        {
            "step": "search_config",
            "endpoint": endpoint_path,
            "per_page": per_page,
            "total": total,
            "params": params,
        },
    )

    startrow = 0
    while True:
        soup = fetch_tile_page(session, endpoint_path, params, startrow)
        items = soup.select("li.job-tile")
        if not items:
            if startrow == 0:
                emit("log", {"step": "no_results"})
            break

        for li_tag in items:
            listing = parse_listing(li_tag)
            if not listing:
                continue

            job_id = listing.get("job_id") or listing["link"]
            if job_id in seen_ids:
                continue
            seen_ids.add(job_id)

            detail = fetch_job_detail(session, listing["link"])

            metadata = {
                "job_id": listing.get("job_id"),
                "requisition_id": listing.get("requisition_id"),
                "department": listing.get("department"),
                "search_row_index": listing.get("row_index"),
                "search_query": config["search_query"],
            }
            metadata = {k: v for k, v in metadata.items() if v}

            jobs.append(
                {
                    "title": listing["title"],
                    "location": listing.get("location") or "",
                    "date": listing.get("posted") or "",
                    "link": listing["link"],
                    "description": detail.get("description_text") or "",
                    "metadata": metadata,
                }
            )

            if DETAIL_DELAY:
                time.sleep(DETAIL_DELAY)

        startrow += per_page
        if startrow >= total:
            break
        if PAGE_DELAY:
            time.sleep(PAGE_DELAY)

    return jobs


def main() -> None:
    session = requests.Session()
    session.headers.update(COMMON_HEADERS)

    emit(
        "log",
        {"step": "start", "detail": f"Fetching job listings from {SEARCH_URL}"},
    )

    try:
        jobs = collect_jobs(session)
    except Exception as exc:  # pragma: no cover - runtime safety
        emit("log", {"step": "error", "detail": str(exc)})
        raise

    emit(
        "result",
        {
            "company": "Freeport-McMoRan",
            "url": SEARCH_URL,
            "jobs": jobs,
            "count": len(jobs),
        },
    )


if __name__ == "__main__":
    main()
