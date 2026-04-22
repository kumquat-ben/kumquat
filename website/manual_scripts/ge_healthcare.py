#!/usr/bin/env python3
"""Standalone scraper for the GE HealthCare careers site (Phenom platform)."""

from __future__ import annotations

import json
import os
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup

BASE_DOMAIN = "https://careers.gehealthcare.com"
LISTING_PATH = "/global/en/search-results"
DETAIL_PATH_TEMPLATE = "/global/en/job/{job_seq}"
LISTING_URL = f"{BASE_DOMAIN}{LISTING_PATH}"
REQUEST_TIMEOUT: Tuple[int, int] = (10, 30)
PAGE_DELAY = 0.2
DETAIL_DELAY = 0.05

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
)
COMMON_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://careers.gehealthcare.com/",
}


def _env_positive_int(name: str) -> Optional[int]:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return None
    try:
        parsed = int(value, 10)
    except ValueError:
        return None
    return parsed if parsed > 0 else None


MAX_JOBS = _env_positive_int("GE_HEALTHCARE_MAX_JOBS")
MAX_PAGES = _env_positive_int("GE_HEALTHCARE_MAX_PAGES")


def emit(event: str, data: Dict[str, Any]) -> None:
    print(json.dumps({"event": event, "data": data}, ensure_ascii=False))
    sys.stdout.flush()


def clean_text(fragment: Optional[str]) -> str:
    if not fragment:
        return ""
    soup = BeautifulSoup(fragment, "html.parser")
    return soup.get_text("\n", strip=True).strip()


def extract_ddo(html_text: str) -> Dict[str, Any]:
    marker = "phApp.ddo = "
    start = html_text.find(marker)
    if start == -1:
        raise ValueError("Unable to locate phApp.ddo payload")
    start += len(marker)

    while start < len(html_text) and html_text[start].isspace():
        start += 1

    if start >= len(html_text) or html_text[start] != "{":
        raise ValueError("Unexpected phApp.ddo payload structure")

    depth = 0
    end = start
    for idx in range(start, len(html_text)):
        ch = html_text[idx]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = idx
                break
    else:
        raise ValueError("Failed to parse phApp.ddo payload (unterminated object)")

    payload = html_text[start : end + 1].replace("\n", " ")
    return json.loads(payload)


def compact_metadata(items: Iterable[Tuple[str, Any]]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in items:
        if value is None:
            continue
        if isinstance(value, str):
            trimmed = value.strip()
            if not trimmed:
                continue
            result[key] = trimmed
            continue
        if isinstance(value, (list, dict)) and not value:
            continue
        result[key] = value
    return result


def fetch_listing_page(session: requests.Session, offset: int) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    params = {"from": offset} if offset else None
    response = session.get(LISTING_URL, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    ddo = extract_ddo(response.text)
    refine = ddo.get("eagerLoadRefineSearch") or {}
    data = refine.get("data") or {}
    jobs = data.get("jobs") or []
    total_hits = refine.get("totalHits") or refine.get("hits")
    return jobs, total_hits


def fetch_job_detail(session: requests.Session, job_seq: str) -> Dict[str, Any]:
    detail_url = f"{BASE_DOMAIN}{DETAIL_PATH_TEMPLATE.format(job_seq=job_seq)}"
    response = session.get(detail_url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    ddo = extract_ddo(response.text)
    job_detail = ((ddo.get("jobDetail") or {}).get("data") or {}).get("job") or {}
    return job_detail


def build_job_record(summary: Dict[str, Any], detail: Dict[str, Any]) -> Dict[str, Any]:
    job_seq = (summary.get("jobSeqNo") or detail.get("jobSeqNo") or "").strip()
    title = (summary.get("title") or detail.get("title") or "").strip()
    location = (
        summary.get("cityStateCountry")
        or summary.get("location")
        or detail.get("cityStateCountry")
        or detail.get("location")
        or ""
    )
    location = location.strip() if isinstance(location, str) else location
    posted_date = (summary.get("postedDate") or detail.get("postedDate") or "").strip()
    link = f"{BASE_DOMAIN}{DETAIL_PATH_TEMPLATE.format(job_seq=job_seq)}" if job_seq else detail.get("applyUrl") or ""

    structure = detail.get("structureData") or {}
    description_html = (
        structure.get("description")
        or detail.get("jobDescription")
        or detail.get("description")
        or detail.get("ml_Description")
        or ""
    )
    description_text = clean_text(description_html)

    metadata = compact_metadata(
        (
            ("job_seq_no", job_seq),
            ("job_id", summary.get("jobId") or detail.get("jobId")),
            ("req_id", summary.get("reqId") or detail.get("reqId")),
            ("job_requisition_id", detail.get("jobRequisitionId")),
            ("category", summary.get("category") or detail.get("category")),
            ("type", summary.get("type") or detail.get("type")),
            ("experience_level", summary.get("experienceLevel") or detail.get("experienceLevel")),
            ("business", summary.get("business") or detail.get("business")),
            ("business_segment", summary.get("businessSegment") or detail.get("businessSegment")),
            ("department", summary.get("department") or detail.get("department")),
            ("posted_date", posted_date),
            ("date_created", summary.get("dateCreated") or detail.get("dateCreated")),
            ("city", summary.get("city") or detail.get("city")),
            ("state", summary.get("state") or detail.get("state")),
            ("country", summary.get("country") or detail.get("country")),
            ("city_state", summary.get("cityState") or detail.get("cityState")),
            ("city_state_country", summary.get("cityStateCountry") or detail.get("cityStateCountry")),
            ("latitude", summary.get("latitude") or detail.get("latitude")),
            ("longitude", summary.get("longitude") or detail.get("longitude")),
            ("description_teaser", summary.get("descriptionTeaser") or detail.get("descriptionTeaser")),
            ("apply_url", summary.get("applyUrl") or detail.get("applyUrl")),
            ("external_apply", summary.get("externalApply") or detail.get("externalApply")),
            ("job_type", summary.get("jobType") or detail.get("jobType")),
            ("job_visibility", summary.get("jobVisibility") or detail.get("jobVisibility")),
            ("multi_location", summary.get("multi_location") or detail.get("multi_location")),
            ("multi_location_array", summary.get("multi_location_array") or detail.get("multi_location_array")),
            ("is_multi_location", summary.get("isMultiLocation") or detail.get("isMultiLocation")),
            ("is_multi_category", summary.get("isMultiCategory") or detail.get("isMultiCategory")),
            ("ml_skills", summary.get("ml_skills") or detail.get("ml_skills")),
            ("ml_title", detail.get("ml_title")),
            ("spotlight_job", detail.get("spotlightJob") or detail.get("SpotlightJob")),
            ("management_level", detail.get("managementLevel")),
            ("time_type", detail.get("timeType")),
            ("job_profile", detail.get("jobProfile")),
            ("job_family", detail.get("jobFamily")),
            ("job_family_group", detail.get("jobFamilyGroup")),
            ("is_remote", detail.get("isRemote")),
            ("relocation_assistance", detail.get("relocationAssistance")),
            ("location_name", detail.get("locationName")),
            ("topic", detail.get("topic")),
            ("structure_employment_type", structure.get("employmentType")),
            ("structure_identifier", structure.get("identifier")),
            ("structure_job_location", structure.get("jobLocation")),
        )
    )

    if description_html:
        metadata.setdefault("description_html", description_html)

    return {
        "title": title,
        "location": location or "",
        "date": posted_date,
        "link": link,
        "description": description_text,
        "metadata": metadata or None,
    }


def collect_jobs(session: requests.Session) -> List[Dict[str, Any]]:
    jobs: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    offset = 0
    total_hits: Optional[int] = None
    page_index = 0

    while True:
        emit("log", {"step": "listings", "detail": "fetching page", "offset": offset})
        page_jobs, hits = fetch_listing_page(session, offset)
        if total_hits is None and hits is not None:
            total_hits = int(hits)
            emit("log", {"step": "listings", "detail": "discovered total hits", "total_hits": total_hits})

        if not page_jobs:
            emit("log", {"step": "listings", "detail": "no jobs returned", "offset": offset})
            break

        for summary in page_jobs:
            job_seq = (summary.get("jobSeqNo") or "").strip()
            if not job_seq or job_seq in seen:
                continue
            seen.add(job_seq)

            detail = fetch_job_detail(session, job_seq)
            record = build_job_record(summary, detail)
            jobs.append(record)

            if MAX_JOBS and len(jobs) >= MAX_JOBS:
                emit("log", {"step": "listings", "detail": "reached max jobs limit", "max_jobs": MAX_JOBS})
                return jobs

            if DETAIL_DELAY:
                time.sleep(DETAIL_DELAY)

        offset += len(page_jobs)
        page_index += 1
        if MAX_PAGES and page_index >= MAX_PAGES:
            emit("log", {"step": "listings", "detail": "reached max pages limit", "max_pages": MAX_PAGES})
            break

        if total_hits is not None and offset >= total_hits:
            break
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
            "detail": f"Collecting jobs from {LISTING_URL}",
        },
    )

    try:
        jobs = collect_jobs(session)
    except Exception as exc:  # pragma: no cover - runtime safeguard
        emit("log", {"step": "error", "detail": str(exc)})
        raise

    emit(
        "result",
        {
            "company": "GE HealthCare",
            "url": LISTING_URL,
            "count": len(jobs),
            "jobs": jobs,
        },
    )


if __name__ == "__main__":
    main()

