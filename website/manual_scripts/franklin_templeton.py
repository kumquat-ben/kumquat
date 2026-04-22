#!/usr/bin/env python3
"""Standalone scraper for the Franklin Templeton careers site (Workday)."""

from __future__ import annotations

import json
import sys
import time
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://franklintempleton.wd5.myworkdayjobs.com"
SITE_PATH = "Primary-External-1"
LIST_URL = f"{BASE_URL}/wday/cxs/franklintempleton/{SITE_PATH}/jobs"
DETAIL_URL_TEMPLATE = (
    f"{BASE_URL}/wday/cxs/franklintempleton/{SITE_PATH}/job/{{slug}}"
)
REFERER_URL = f"{BASE_URL}/{SITE_PATH}"
PAGE_SIZE = 20
REQUEST_TIMEOUT: Tuple[int, int] = (10, 30)
PAGE_DELAY = 0.2
DETAIL_DELAY = 0.1

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
)
COMMON_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": REFERER_URL,
}


def emit(event: str, data: Dict[str, object]) -> None:
    print(json.dumps({"event": event, "data": data}, ensure_ascii=False))
    sys.stdout.flush()


def clean_html(fragment: Optional[str]) -> str:
    if not fragment:
        return ""
    soup = BeautifulSoup(fragment, "html.parser")
    return soup.get_text("\n", strip=True).strip()


def fetch_job_page(
    session: requests.Session,
    offset: int,
    limit: int = PAGE_SIZE,
) -> Dict[str, object]:
    payload = {
        "appliedFacets": {},
        "limit": limit,
        "offset": offset,
        "searchText": "",
    }
    response = session.post(
        LIST_URL,
        json=payload,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def fetch_job_detail(session: requests.Session, slug: str) -> Dict[str, object]:
    response = session.get(
        DETAIL_URL_TEMPLATE.format(slug=slug),
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def extract_job_id(posting: Dict[str, object], detail: Dict[str, object]) -> str:
    info = detail.get("jobPostingInfo") or {}
    bullet_fields = posting.get("bulletFields") or []
    if info.get("jobReqId"):
        return str(info["jobReqId"])
    if bullet_fields:
        return str(bullet_fields[0])
    slug = posting.get("externalPath") or ""
    return slug.rstrip("/").split("/")[-1]


def build_job_record(
    posting: Dict[str, object],
    detail: Dict[str, object],
) -> Dict[str, object]:
    info = detail.get("jobPostingInfo") or {}
    hiring_org = detail.get("hiringOrganization") or {}

    slug = (posting.get("externalPath") or "").rstrip("/").split("/")[-1]
    job_id = extract_job_id(posting, detail)
    description_html = info.get("jobDescription") or ""
    requisition_location = info.get("jobRequisitionLocation") or {}
    country = info.get("country") or {}

    metadata = {
        "job_posting_id": info.get("jobPostingId"),
        "job_posting_site_id": info.get("jobPostingSiteId"),
        "country": country.get("descriptor"),
        "country_id": country.get("id"),
        "job_requisition_location": requisition_location.get("descriptor"),
        "job_requisition_country": requisition_location.get("country", {}).get(
            "descriptor"
        )
        if isinstance(requisition_location.get("country"), dict)
        else None,
        "search_locations_text": posting.get("locationsText"),
        "posted_flag": info.get("posted"),
        "can_apply": info.get("canApply"),
        "include_resume_parsing": info.get("includeResumeParsing"),
        "external_path": posting.get("externalPath"),
        "slug": slug or None,
        "hiring_organization": hiring_org.get("name"),
    }
    if description_html:
        metadata["description_html"] = description_html
    metadata = {k: v for k, v in metadata.items() if v not in (None, "", [])}

    location = (
        requisition_location.get("descriptor")
        or info.get("location")
        or posting.get("locationsText")
        or ""
    )

    record = {
        "title": info.get("title") or posting.get("title") or "",
        "job_id": job_id,
        "location": location,
        "posted_on": info.get("postedOn") or posting.get("postedOn") or "",
        "time_type": info.get("timeType") or "",
        "start_date": info.get("startDate") or "",
        "url": info.get("externalUrl")
        or urljoin(BASE_URL, posting.get("externalPath") or ""),
        "description": clean_html(description_html),
        "metadata": metadata,
    }
    return record


def collect_jobs(session: requests.Session) -> List[Dict[str, object]]:
    jobs: List[Dict[str, object]] = []
    seen: Set[str] = set()

    offset = 0
    total: Optional[int] = None

    while True:
        payload = fetch_job_page(session, offset, PAGE_SIZE)
        total = total or payload.get("total")
        postings = payload.get("jobPostings") or []

        if not postings:
            break

        for posting in postings:
            external_path = posting.get("externalPath") or ""
            slug = external_path.rstrip("/").split("/")[-1]
            if not slug:
                continue

            detail = fetch_job_detail(session, slug)
            job_id = extract_job_id(posting, detail)
            if job_id in seen:
                continue
            seen.add(job_id)

            record = build_job_record(posting, detail)
            jobs.append(record)

            if DETAIL_DELAY:
                time.sleep(DETAIL_DELAY)

        offset += PAGE_SIZE
        if total is not None and offset >= int(total):
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
            "detail": f"Fetching Franklin Templeton jobs from {LIST_URL}",
            "page_size": PAGE_SIZE,
        },
    )

    try:
        jobs = collect_jobs(session)
    except Exception as exc:  # pragma: no cover - runtime safety
        emit("log", {"step": "error", "detail": str(exc)})
        raise

    emit(
        "result",
        {
            "company": "Franklin Templeton",
            "url": REFERER_URL,
            "jobs": jobs,
            "count": len(jobs),
        },
    )


if __name__ == "__main__":
    main()
