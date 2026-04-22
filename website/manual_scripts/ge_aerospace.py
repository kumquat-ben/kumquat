#!/usr/bin/env python3
"""Standalone scraper for GE Aerospace's Phenom-powered careers site."""

from __future__ import annotations

import json
import os
import sys
import time
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

BASE_DOMAIN = "https://careers.geaerospace.com"
SEARCH_PATH = "/global/en/search-results"
DETAIL_PATH = "/global/en/job/{job_seq}"
ROOT_URL = f"{BASE_DOMAIN}{SEARCH_PATH}"
REQUEST_TIMEOUT: Tuple[int, int] = (10, 30)
PAGE_DELAY = float(os.environ.get("GE_AERO_PAGE_DELAY", "0.1"))
DETAIL_DELAY = float(os.environ.get("GE_AERO_DETAIL_DELAY", "0.05"))
MAX_PAGES = int(os.environ.get("GE_AERO_MAX_PAGES", "0")) or None
MAX_RESULTS = int(os.environ.get("GE_AERO_MAX_RESULTS", "0")) or None

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": f"{BASE_DOMAIN}/global/en/home",
}


class ScraperError(Exception):
    """Raised when the scraper encounters an unrecoverable issue."""


def emit(event: str, data: Dict[str, object]) -> None:
    print(json.dumps({"event": event, "data": data}, ensure_ascii=False))
    sys.stdout.flush()


def _clean_text(fragment: Optional[str]) -> str:
    soup = BeautifulSoup(fragment or "", "html.parser")
    return soup.get_text("\n", strip=True).strip()


def _extract_ddo(html: str) -> Dict[str, object]:
    marker = "phApp.ddo = "
    start = html.find(marker)
    if start == -1:
        raise ScraperError("Unable to locate phApp.ddo payload.")

    start += len(marker)
    while start < len(html) and html[start].isspace():
        start += 1
    if start >= len(html) or html[start] != "{":
        raise ScraperError("Unexpected phApp.ddo payload format.")

    depth = 0
    end = start
    for idx in range(start, len(html)):
        ch = html[idx]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = idx
                break
    else:
        raise ScraperError("Unterminated phApp.ddo payload.")

    payload = html[start : end + 1].replace("\n", " ")
    try:
        return json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ScraperError(f"Failed to decode phApp.ddo payload: {exc}") from exc


def _fetch_listing_page(session: requests.Session, offset: int) -> Dict[str, object]:
    params = {"from": offset} if offset else None
    url = f"{BASE_DOMAIN}{SEARCH_PATH}"
    try:
        response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise ScraperError(f"Failed to fetch listing page at offset {offset}: {exc}") from exc

    ddo = _extract_ddo(response.text)
    refine = ddo.get("eagerLoadRefineSearch") or {}
    data = refine.get("data") or {}
    jobs = data.get("jobs") or []

    return {
        "jobs": jobs,
        "total_hits": refine.get("totalHits") or refine.get("hits"),
    }


def _fetch_job_detail(session: requests.Session, job_seq: str) -> Dict[str, object]:
    detail_url = f"{BASE_DOMAIN}{DETAIL_PATH.format(job_seq=job_seq)}"
    try:
        response = session.get(detail_url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise ScraperError(f"Failed to fetch job detail for {job_seq}: {exc}") from exc

    ddo = _extract_ddo(response.text)
    detail = ((ddo.get("jobDetail") or {}).get("data") or {}).get("job") or {}
    if not detail:
        emit(
            "log",
            {
                "step": "warn",
                "detail": f"No job detail payload returned for {job_seq}",
            },
        )
    return detail


def _structure_metadata(summary: Dict[str, object], detail: Dict[str, object]) -> Dict[str, object]:
    structure = detail.get("structureData") or {}
    metadata: Dict[str, object] = {
        "job_id": summary.get("jobId") or detail.get("jobId"),
        "job_seq_no": summary.get("jobSeqNo") or detail.get("jobSeqNo"),
        "req_id": summary.get("reqId") or detail.get("reqId"),
        "job_type": summary.get("jobType") or detail.get("jobType"),
        "category": summary.get("category") or detail.get("category"),
        "job_family": detail.get("jobFamily"),
        "job_families": detail.get("jobFamilies"),
        "business": summary.get("business") or detail.get("business"),
        "business_segment": summary.get("businessSegment") or detail.get("businessSegment"),
        "experience_level": summary.get("experienceLevel") or detail.get("experienceLevel"),
        "city": summary.get("city") or detail.get("city"),
        "state": summary.get("state") or detail.get("state"),
        "country": summary.get("country") or detail.get("country"),
        "city_state_country": summary.get("cityStateCountry") or detail.get("cityStateCountry"),
        "location": summary.get("location") or detail.get("location"),
        "is_multi_location": summary.get("isMultiLocation") or detail.get("isMultiLocation"),
        "multi_location": summary.get("multi_location") or detail.get("multi_location"),
        "multi_location_array": summary.get("multi_location_array") or detail.get("multi_location_array"),
        "posted_date": summary.get("postedDate") or detail.get("postedDate"),
        "date_created": summary.get("dateCreated") or detail.get("dateCreated"),
        "apply_url": summary.get("applyUrl") or detail.get("applyUrl"),
        "external_apply": summary.get("externalApply") or detail.get("externalApply"),
        "description_teaser": summary.get("descriptionTeaser") or detail.get("descriptionTeaser"),
        "ml_skills": summary.get("ml_skills") or detail.get("ml_skills"),
        "ml_highlight": detail.get("ml_highlight"),
        "job_visibility": summary.get("jobVisibility") or detail.get("jobVisibility"),
        "job_requisition_id": detail.get("jobRequisitionId"),
        "company": detail.get("company") or detail.get("companyName"),
        "latitude": summary.get("latitude") or detail.get("latitude"),
        "longitude": summary.get("longitude") or detail.get("longitude"),
        "map_query_location": detail.get("mapQueryLocation"),
        "structure_data": {
            key: value
            for key, value in {
                "description": structure.get("description"),
                "employmentType": structure.get("employmentType"),
                "jobLocation": structure.get("jobLocation"),
                "identifier": structure.get("identifier"),
            }.items()
            if value
        }
        or None,
    }
    return {key: value for key, value in metadata.items() if value not in (None, "", [], {})}


def _build_job_record(summary: Dict[str, object], detail: Dict[str, object]) -> Dict[str, object]:
    job_seq = (summary.get("jobSeqNo") or detail.get("jobSeqNo") or "").strip()
    link = f"{BASE_DOMAIN}{DETAIL_PATH.format(job_seq=job_seq)}" if job_seq else ROOT_URL

    description_html = (
        detail.get("description")
        or (detail.get("structureData") or {}).get("description")
        or detail.get("ml_Description")
        or ""
    )
    description_text = _clean_text(description_html)

    location = (
        summary.get("cityStateCountry")
        or summary.get("location")
        or detail.get("cityStateCountry")
        or detail.get("location")
        or ""
    )

    date_label = summary.get("postedDate") or detail.get("postedDate") or ""

    job_record = {
        "title": (summary.get("title") or detail.get("title") or "").strip(),
        "location": location.strip(),
        "date": date_label,
        "link": link,
        "description": description_text,
        "metadata": _structure_metadata(summary, detail),
    }
    if description_html:
        job_record["metadata"]["description_html"] = description_html
    return job_record


def collect_jobs(session: requests.Session) -> List[Dict[str, object]]:
    results: List[Dict[str, object]] = []
    seen: set[str] = set()
    offset = 0
    page = 0
    total_hits: Optional[int] = None

    while True:
        listing_payload = _fetch_listing_page(session, offset)
        jobs: Iterable[Dict[str, object]] = listing_payload.get("jobs") or []
        jobs = list(jobs)
        if not jobs:
            break

        if total_hits is None:
            total_hits = listing_payload.get("total_hits") or len(jobs)
            emit(
                "log",
                {
                    "step": "pagination",
                    "detail": f"Discovered {total_hits} jobs (page size {len(jobs)}).",
                },
            )

        for summary in jobs:
            job_seq = (summary.get("jobSeqNo") or "").strip()
            if not job_seq or job_seq in seen:
                continue

            detail = _fetch_job_detail(session, job_seq)
            record = _build_job_record(summary, detail)
            results.append(record)
            seen.add(job_seq)

            if DETAIL_DELAY:
                time.sleep(DETAIL_DELAY)
            if MAX_RESULTS and len(results) >= MAX_RESULTS:
                return results

        offset += len(jobs)
        page += 1
        if MAX_PAGES and page >= MAX_PAGES:
            break
        if total_hits is not None and offset >= total_hits:
            break
        if PAGE_DELAY:
            time.sleep(PAGE_DELAY)

    return results


def main() -> None:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    emit("log", {"step": "start", "detail": f"Fetching listings from {ROOT_URL}"})
    try:
        jobs = collect_jobs(session)
    except ScraperError as exc:
        emit("log", {"step": "error", "detail": str(exc)})
        raise

    emit(
        "result",
        {
            "company": "GE Aerospace",
            "url": ROOT_URL,
            "jobs": jobs,
            "count": len(jobs),
        },
    )


if __name__ == "__main__":
    main()
