#!/usr/bin/env python3
"""Dedicated resumable submit runner for direct Greenhouse application forms."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError, sync_playwright

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "website.settings")

import django  # noqa: E402

django.setup()

from scrapers.submission_runtime import build_applicant_profile  # noqa: E402

from generic_form_submit import (  # noqa: E402
    NAVIGATION_TIMEOUT_MS,
    _attempt_login,
    _attempt_simple_registration,
    _attempt_verification_step,
    _current_inspection,
    _find_submit_button,
    _is_verification_gate,
    _resolve_apply_url,
    _resolve_job,
    _resolve_user,
    _resume_pdf_path,
    _site_password,
    _site_username,
    _storage_state_path,
    _upload_resume_if_needed,
    _verification_code,
    build_result,
)


GREENHOUSE_MARKERS = (
    "greenhouse",
    "job-boards.greenhouse.io",
    "boards.greenhouse.io",
    "grnhse_app",
)

SUCCESS_MARKERS = (
    "application submitted",
    "thank you for applying",
    "thanks for applying",
    "application received",
)


def _value_or_empty(value: Any) -> str:
    return str(value or "").strip()


def _field_candidates(base_name: str):
    escaped = base_name.replace("\\", "\\\\").replace('"', '\\"')
    return (
        f'[name="{escaped}"]',
        f'[id="{escaped}"]',
        f'[for="{escaped}"]',
    )


def _looks_like_greenhouse(inspection: Dict[str, Any], page_url: str, page_content: str) -> bool:
    haystacks = [
        _value_or_empty(page_url).lower(),
        json.dumps(inspection).lower(),
        _value_or_empty(page_content).lower(),
    ]
    return any(marker in haystack for marker in GREENHOUSE_MARKERS for haystack in haystacks)


def _applicant_value(applicant: Dict[str, Any], key: str) -> str:
    value = applicant.get(key, "")
    if isinstance(value, list):
        return ", ".join(_value_or_empty(item) for item in value if _value_or_empty(item))
    return _value_or_empty(value)


def _fill_first_matching(page, selectors, value: str) -> bool:
    if not value:
        return False
    for selector in selectors:
        locator = page.locator(selector).first
        if locator.count() == 0:
            continue
        try:
            locator.fill(value)
            return True
        except Exception:
            continue
    return False


def _select_first_matching(page, selectors, desired_values) -> bool:
    for selector in selectors:
        locator = page.locator(selector).first
        if locator.count() == 0:
            continue
        for value in desired_values:
            try:
                locator.select_option(value=value)
                return True
            except Exception:
                pass
            try:
                locator.select_option(label=value)
                return True
            except Exception:
                pass
        try:
            options = locator.locator("option")
            option_count = options.count()
        except Exception:
            option_count = 0
        for index in range(option_count):
            option = options.nth(index)
            option_text = _value_or_empty(option.text_content()).lower()
            option_value = _value_or_empty(option.get_attribute("value"))
            if not option_value:
                continue
            if any(candidate in option_text for candidate in ("yes", "authorized", "consent", "agree", "linkedin", "other", "website")):
                try:
                    locator.select_option(value=option_value)
                    return True
                except Exception:
                    continue
        return False
    return False


def _check_first_matching(page, selectors) -> bool:
    for selector in selectors:
        locator = page.locator(selector).first
        if locator.count() == 0:
            continue
        try:
            if not locator.is_checked():
                locator.check()
            return True
        except Exception:
            try:
                locator.click()
                return True
            except Exception:
                continue
    return False


def _fill_greenhouse_fields(page, applicant: Dict[str, Any]) -> Dict[str, Any]:
    field_status: Dict[str, bool] = {}

    field_status["first_name"] = _fill_first_matching(
        page,
        (
            '[name="first_name"]',
            '[name="job_application[first_name]"]',
            '[autocomplete="given-name"]',
        ),
        _applicant_value(applicant, "first_name"),
    )
    field_status["last_name"] = _fill_first_matching(
        page,
        (
            '[name="last_name"]',
            '[name="job_application[last_name]"]',
            '[autocomplete="family-name"]',
        ),
        _applicant_value(applicant, "last_name"),
    )
    field_status["email"] = _fill_first_matching(
        page,
        (
            'input[type="email"]',
            '[name="email"]',
            '[name="job_application[email]"]',
        ),
        _applicant_value(applicant, "email"),
    )
    field_status["phone"] = _fill_first_matching(
        page,
        (
            '[name="phone"]',
            '[name="job_application[phone]"]',
            'input[type="tel"]',
        ),
        _applicant_value(applicant, "phone"),
    )
    field_status["location"] = _fill_first_matching(
        page,
        (
            '[name="location"]',
            '[name="job_application[location]"]',
            '[name="job_application[address]"]',
        ),
        _applicant_value(applicant, "location"),
    )
    field_status["linkedin"] = _fill_first_matching(
        page,
        (
            '[name*="linkedin"]',
            '[name*="linked_in"]',
            '[placeholder*="LinkedIn"]',
        ),
        _applicant_value(applicant, "linkedin"),
    )
    field_status["website"] = _fill_first_matching(
        page,
        (
            '[name*="website"]',
            '[name*="portfolio"]',
            'input[type="url"]',
        ),
        _applicant_value(applicant, "website"),
    )

    source_value = _applicant_value(applicant, "linkedin") or _applicant_value(applicant, "website") or "Other"
    field_status["source_select"] = _select_first_matching(
        page,
        (
            'select[name*="source"]',
            'select[name*="how"]',
            'select[name*="found"]',
        ),
        (
            source_value,
            "LinkedIn",
            "Other",
            "Company Website",
        ),
    )
    field_status["work_auth_select"] = _select_first_matching(
        page,
        (
            'select[name*="authorization"]',
            'select[name*="authorized"]',
            'select[name*="sponsor"]',
            'select[name*="visa"]',
        ),
        (
            "Yes",
            "No",
        ),
    )
    field_status["consent_checkbox"] = _check_first_matching(
        page,
        (
            'input[type="checkbox"][name*="consent"]',
            'input[type="checkbox"][name*="privacy"]',
            'input[type="checkbox"][name*="data"]',
        ),
    )
    return field_status


def _submit_greenhouse_application(page, applicant: Dict[str, Any], inspection: Dict[str, Any], resume_pdf_path: str) -> Dict[str, Any]:
    field_status = _fill_greenhouse_fields(page, applicant)
    if not _upload_resume_if_needed(page, inspection, resume_pdf_path):
        return build_result(
            status="incomplete",
            reason="resume_upload_required",
            apply_url=page.url,
            details={
                "inspection": inspection,
                "field_status": field_status,
            },
        )

    submit_button = _find_submit_button(page)
    if submit_button is None:
        return build_result(
            status="incomplete",
            reason="submit_button_missing",
            apply_url=page.url,
            details={
                "inspection": inspection,
                "field_status": field_status,
            },
            message="Greenhouse form was detected, but no submit button was found.",
        )

    submit_button.click()
    page.wait_for_timeout(2500)
    post_submit_inspection = _current_inspection(page)
    page_text = page.content().lower()
    if any(marker in page_text for marker in SUCCESS_MARKERS):
        return build_result(
            status="applied",
            reason="application_submitted",
            apply_url=page.url,
            details={
                "inspection": post_submit_inspection,
                "field_status": field_status,
                "classification": "greenhouse_direct_form",
            },
        )

    return build_result(
        status="action_required",
        reason="submission_clicked_manual_review_recommended",
        apply_url=page.url,
        details={
            "inspection": post_submit_inspection,
            "field_status": field_status,
            "classification": "greenhouse_direct_form",
        },
        message="Greenhouse submission was attempted, but the success state was not unambiguous.",
    )


def main() -> int:
    job = _resolve_job()
    user = _resolve_user()
    apply_url = _resolve_apply_url(job)

    if not apply_url:
        print(json.dumps(build_result(status="incomplete", reason="missing_apply_url", apply_url=""), indent=2))
        return 0

    if user is None:
        print(json.dumps(build_result(status="incomplete", reason="missing_applicant_profile", apply_url=apply_url), indent=2))
        return 0

    applicant = build_applicant_profile(user)
    storage_state_path = _storage_state_path()
    storage_state_dir = Path(storage_state_path).parent if storage_state_path else None
    if storage_state_dir is not None:
        storage_state_dir.mkdir(parents=True, exist_ok=True)

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            context_kwargs: Dict[str, Any] = {}
            if storage_state_path and Path(storage_state_path).is_file():
                context_kwargs["storage_state"] = storage_state_path
            context = browser.new_context(**context_kwargs)
            page = context.new_page()
            page.set_default_timeout(NAVIGATION_TIMEOUT_MS)
            page.goto(apply_url, wait_until="domcontentloaded")
            page.wait_for_timeout(1500)

            inspection = _current_inspection(page)
            page_content = page.content()
            if not _looks_like_greenhouse(inspection, page.url, page_content):
                print(
                    json.dumps(
                        build_result(
                            status="incomplete",
                            reason="not_greenhouse_form",
                            apply_url=page.url,
                            details={"inspection": inspection},
                        ),
                        indent=2,
                    )
                )
                context.close()
                browser.close()
                return 0

            if _verification_code():
                if not _attempt_verification_step(page, _verification_code()):
                    print(
                        json.dumps(
                            build_result(
                                status="incomplete",
                                reason="verification_code_field_not_found",
                                apply_url=page.url,
                                details={"inspection": inspection, "classification": "greenhouse_direct_form"},
                            ),
                            indent=2,
                        )
                    )
                    context.close()
                    browser.close()
                    return 0
                inspection = _current_inspection(page)

            if inspection["classification"] == "registration_required":
                registration_result = _attempt_simple_registration(
                    page,
                    applicant,
                    username=_site_username() or applicant.get("email", ""),
                    password=_site_password(),
                )
                if not registration_result.get("submitted"):
                    if not _attempt_login(page, _site_username(), _site_password()):
                        print(
                            json.dumps(
                                build_result(
                                    status="incomplete",
                                    reason=registration_result.get("reason") or "registration_not_supported",
                                    apply_url=page.url,
                                    details={"inspection": inspection, "classification": "greenhouse_direct_form"},
                                ),
                                indent=2,
                            )
                        )
                        context.close()
                        browser.close()
                        return 0
                inspection = _current_inspection(page)
                if _is_verification_gate(inspection):
                    if storage_state_path:
                        context.storage_state(path=storage_state_path)
                    print(
                        json.dumps(
                            build_result(
                                status="awaiting_email_verification",
                                reason="email_verification_required",
                                apply_url=page.url,
                                details={"inspection": inspection, "classification": "greenhouse_direct_form"},
                                message="Registration/login was submitted. Provide the email verification code to continue the Greenhouse application.",
                            ),
                            indent=2,
                        )
                    )
                    context.close()
                    browser.close()
                    return 0

            result = _submit_greenhouse_application(page, applicant, inspection, _resume_pdf_path())
            if storage_state_path:
                context.storage_state(path=storage_state_path)
            print(json.dumps(result, indent=2))
            context.close()
            browser.close()
            return 0 if result["status"] != "error" else 1
    except PlaywrightTimeoutError as exc:
        print(
            json.dumps(
                build_result(
                    status="error",
                    reason="playwright_timeout",
                    apply_url=apply_url,
                    details={"error": str(exc), "classification": "greenhouse_direct_form"},
                ),
                indent=2,
            )
        )
        return 1
    except Exception as exc:
        print(
            json.dumps(
                build_result(
                    status="error",
                    reason="unexpected_error",
                    apply_url=apply_url,
                    details={"error": str(exc), "classification": "greenhouse_direct_form"},
                ),
                indent=2,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
