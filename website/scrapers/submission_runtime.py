from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

REQUEST_TIMEOUT = (15, 30)

REGISTRATION_KEYWORDS = (
    "create account",
    "create an account",
    "sign in",
    "log in",
    "login",
    "register",
    "password",
)

SIMPLE_FIELD_TYPES = {
    "",
    "text",
    "email",
    "tel",
    "hidden",
    "search",
    "url",
    "textarea",
    "select",
}


def fetch_page_html(url: str, *, session: Optional[requests.Session] = None) -> Dict[str, Any]:
    active_session = session or requests.Session()
    active_session.headers.update(DEFAULT_HEADERS)
    response = active_session.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return {
        "url": response.url,
        "status_code": response.status_code,
        "html": response.text,
    }


def inspect_application_page(html: str, page_url: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html or "", "html.parser")
    text_blob = soup.get_text(" ", strip=True).lower()
    forms: List[Dict[str, Any]] = []

    for form in soup.find_all("form"):
        fields: List[Dict[str, Any]] = []
        file_input_count = 0
        password_input_count = 0
        required_field_count = 0
        simple_input_count = 0

        for field in form.find_all(["input", "select", "textarea"]):
            raw_type = (field.get("type") or field.name or "").strip().lower()
            field_type = "textarea" if field.name == "textarea" else "select" if field.name == "select" else raw_type
            if field_type == "file":
                file_input_count += 1
            if field_type == "password":
                password_input_count += 1
            if field.has_attr("required"):
                required_field_count += 1
            if field_type in SIMPLE_FIELD_TYPES:
                simple_input_count += 1

            fields.append(
                {
                    "name": (field.get("name") or "").strip(),
                    "type": field_type,
                    "required": field.has_attr("required"),
                    "placeholder": (field.get("placeholder") or "").strip(),
                    "autocomplete": (field.get("autocomplete") or "").strip(),
                }
            )

        submit_buttons = []
        for button in form.find_all(["button", "input"]):
            raw_type = (button.get("type") or "").strip().lower()
            if button.name == "button" and raw_type not in ("", "submit"):
                continue
            if button.name == "input" and raw_type != "submit":
                continue
            submit_buttons.append(
                {
                    "text": button.get_text(" ", strip=True) if button.name == "button" else (button.get("value") or "").strip(),
                    "type": raw_type or "submit",
                }
            )

        forms.append(
            {
                "action": urljoin(page_url, (form.get("action") or "").strip()),
                "method": (form.get("method") or "get").upper(),
                "field_count": len(fields),
                "required_field_count": required_field_count,
                "file_input_count": file_input_count,
                "password_input_count": password_input_count,
                "simple_input_count": simple_input_count,
                "fields": fields,
                "submit_buttons": submit_buttons,
            }
        )

    registration_indicators = [keyword for keyword in REGISTRATION_KEYWORDS if keyword in text_blob]
    primary_form = max(forms, key=lambda item: item["field_count"], default=None)
    classification = classify_application_page(forms=forms, registration_indicators=registration_indicators)

    return {
        "page_url": page_url,
        "title": soup.title.string.strip() if soup.title and soup.title.string else "",
        "forms": forms,
        "primary_form": primary_form,
        "registration_indicators": registration_indicators,
        "classification": classification,
    }


def classify_application_page(*, forms: List[Dict[str, Any]], registration_indicators: List[str]) -> str:
    if not forms:
        return "no_form"

    if registration_indicators:
        return "registration_required"

    primary_form = max(forms, key=lambda item: item["field_count"], default=None)
    if not primary_form:
        return "no_form"

    if primary_form["password_input_count"] > 0:
        return "registration_required"

    field_count = primary_form["field_count"]
    simple_input_count = primary_form["simple_input_count"]
    if field_count and simple_input_count == field_count:
        return "simple_form"

    return "unsupported_form"


def _safe_attr(value: Any, attr_name: str, default: Any = None) -> Any:
    try:
        return getattr(value, attr_name, default)
    except Exception:
        return default


def _safe_related_manager_items(value: Any, attr_name: str) -> List[Any]:
    related = _safe_attr(value, attr_name)
    if related is None:
        return []
    try:
        return list(related.all())
    except Exception:
        return []


def build_applicant_profile(user: Any) -> Dict[str, str]:
    profile = _safe_attr(user, "profile")
    resume = _safe_attr(user, "resume")
    social_links = getattr(profile, "social_links", {}) if profile else {}

    def format_date_range(start_value: Optional[date], end_value: Optional[date], *, is_current: bool = False) -> str:
        parts = []
        if start_value:
            parts.append(start_value.strftime("%b %Y"))
        if is_current:
            parts.append("Present")
        elif end_value:
            parts.append(end_value.strftime("%b %Y"))
        return " - ".join(parts)

    first_name = (getattr(user, "first_name", "") or "").strip() or (getattr(profile, "first_name", "") or "").strip()
    last_name = (getattr(user, "last_name", "") or "").strip() or (getattr(profile, "last_name", "") or "").strip()
    full_name = " ".join(part for part in (first_name, last_name) if part).strip()

    experiences = []
    if resume is not None:
        for experience in _safe_related_manager_items(resume, "experiences"):
            experiences.append(
                {
                    "company": experience.company,
                    "title": experience.title,
                    "location": experience.location,
                    "date_range": format_date_range(
                        experience.start_date,
                        experience.end_date,
                        is_current=experience.is_current,
                    ),
                    "description": experience.description,
                    "highlights": experience.highlights or [],
                }
            )

    educations = []
    if resume is not None:
        for education in _safe_related_manager_items(resume, "educations"):
            educations.append(
                {
                    "institution": education.institution,
                    "degree": education.degree,
                    "field_of_study": education.field_of_study,
                    "location": education.location,
                    "date_range": format_date_range(education.start_date, education.end_date),
                    "description": education.description,
                }
            )

    return {
        "first_name": first_name,
        "last_name": last_name,
        "full_name": full_name,
        "email": (getattr(user, "email", "") or "").strip(),
        "linkedin": str(social_links.get("linkedin", "") or _safe_attr(user, "linkedin", "") or "").strip(),
        "website": str(social_links.get("website", "") or _safe_attr(user, "website", "") or "").strip(),
        "location": (_safe_attr(profile, "location", "") or _safe_attr(user, "location", "") or "").strip(),
        "headline": (_safe_attr(resume, "headline", "") or _safe_attr(user, "headline", "") or "").strip(),
        "summary": (_safe_attr(resume, "summary", "") or _safe_attr(user, "summary", "") or "").strip(),
        "skills": list(_safe_attr(resume, "skills", None) or _safe_attr(user, "skills", None) or []),
        "experiences": experiences,
        "educations": educations,
    }


def plan_field_assignments(fields: List[Dict[str, Any]], applicant: Dict[str, str]) -> Dict[str, Any]:
    assignments: List[Dict[str, str]] = []
    unmapped_required_fields: List[str] = []

    for field in fields:
        field_name = (field.get("name") or "").strip()
        field_type = (field.get("type") or "").strip()
        placeholder = (field.get("placeholder") or "").strip()
        autocomplete = (field.get("autocomplete") or "").strip()
        haystack = " ".join(part.lower() for part in (field_name, field_type, placeholder, autocomplete) if part)

        applicant_key = ""
        if any(token in haystack for token in ("first", "given-name", "given_name")):
            applicant_key = "first_name"
        elif any(token in haystack for token in ("last", "family-name", "surname")):
            applicant_key = "last_name"
        elif "email" in haystack:
            applicant_key = "email"
        elif any(token in haystack for token in ("full_name", "fullname", "name")):
            applicant_key = "full_name"
        elif any(token in haystack for token in ("linkedin", "linked_in")):
            applicant_key = "linkedin"
        elif any(token in haystack for token in ("website", "portfolio", "url")):
            applicant_key = "website"
        elif any(token in haystack for token in ("city", "location")):
            applicant_key = "location"

        applicant_value = applicant.get(applicant_key, "").strip() if applicant_key else ""
        if applicant_key and applicant_value:
            assignments.append(
                {
                    "field_name": field_name,
                    "field_type": field_type,
                    "applicant_key": applicant_key,
                }
            )
        elif field.get("required"):
            unmapped_required_fields.append(field_name or placeholder or field_type or "unknown")

    return {
        "assignments": assignments,
        "unmapped_required_fields": unmapped_required_fields,
    }
