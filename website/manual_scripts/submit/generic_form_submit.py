#!/usr/bin/env python3
"""Generic resumable job-application runner for simple and registration-gated forms."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "website.settings")

import django  # noqa: E402

django.setup()

from django.contrib.auth import get_user_model  # noqa: E402
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError, sync_playwright  # noqa: E402

from scrapers.models import JobPosting  # noqa: E402
from scrapers.submission_runtime import build_applicant_profile, inspect_application_page, plan_field_assignments  # noqa: E402
from scrapers.utils import get_job_apply_url  # noqa: E402

NAVIGATION_TIMEOUT_MS = 30_000


def _resolve_job() -> Optional[JobPosting]:
    job_id = (os.getenv("KUMQUAT_JOB_ID") or "").strip()
    if not job_id:
        return None
    try:
        return JobPosting.objects.select_related("scraper").get(pk=int(job_id))
    except (ValueError, JobPosting.DoesNotExist):
        return None


def _resolve_user() -> Optional[Any]:
    user_id = (os.getenv("KUMQUAT_USER_ID") or "").strip()
    if not user_id:
        return None
    user_model = get_user_model()
    try:
        return user_model.objects.get(pk=int(user_id))
    except (ValueError, user_model.DoesNotExist):
        return None


def _resolve_apply_url(job: Optional[JobPosting]) -> str:
    explicit_url = (os.getenv("KUMQUAT_JOB_URL") or "").strip()
    if explicit_url:
        return explicit_url
    if job is not None:
        return get_job_apply_url(job)
    return ""


def _storage_state_path() -> str:
    return (os.getenv("KUMQUAT_STORAGE_STATE_PATH") or "").strip()


def _site_username() -> str:
    return (os.getenv("KUMQUAT_SITE_USERNAME") or "").strip()


def _site_password() -> str:
    return (os.getenv("KUMQUAT_SITE_PASSWORD") or "").strip()


def _verification_code() -> str:
    return (os.getenv("KUMQUAT_VERIFICATION_CODE") or "").strip()


def _resume_pdf_path() -> str:
    return (os.getenv("KUMQUAT_RESUME_PDF_PATH") or "").strip()


def build_result(*, status: str, reason: str, apply_url: str, details: Optional[Dict[str, Any]] = None, message: str = "") -> Dict[str, Any]:
    payload = {
        "status": status,
        "reason": reason,
        "apply_url": apply_url,
    }
    if details:
        payload["details"] = details
    if message:
        payload["message"] = message
    return payload


def _best_input_selector(field_name: str) -> str:
    escaped = field_name.replace("\\", "\\\\").replace('"', '\\"')
    return f'[name="{escaped}"]'


def _fill_textual_fields(page, assignments: Dict[str, Any], applicant: Dict[str, str]) -> None:
    for assignment in assignments.get("assignments", []):
        field_name = assignment.get("field_name") or ""
        if not field_name:
            continue
        value = (applicant.get(assignment.get("applicant_key") or "") or "").strip()
        if not value:
            continue
        locator = page.locator(_best_input_selector(field_name)).first
        if locator.count() == 0:
            continue
        try:
            locator.fill(value)
        except Exception:
            continue


def _upload_resume_if_needed(page, inspection: Dict[str, Any], resume_pdf_path: str) -> bool:
    primary_form = inspection.get("primary_form") or {}
    if primary_form.get("file_input_count", 0) <= 0:
        return True
    if not resume_pdf_path or not Path(resume_pdf_path).is_file():
        return False
    file_inputs = page.locator('input[type="file"]')
    if file_inputs.count() == 0:
        return False
    try:
        file_inputs.first.set_input_files(resume_pdf_path)
        return True
    except Exception:
        return False


def _find_submit_button(page):
    selectors = [
        'button[type="submit"]',
        'input[type="submit"]',
        'button:has-text("Apply")',
        'button:has-text("Submit")',
        'button:has-text("Continue")',
        'button:has-text("Next")',
    ]
    for selector in selectors:
        locator = page.locator(selector).first
        if locator.count() > 0:
            return locator
    return None


def _current_inspection(page) -> Dict[str, Any]:
    return inspect_application_page(page.content(), page.url)


def _is_verification_gate(inspection: Dict[str, Any]) -> bool:
    text_blob = json.dumps(inspection).lower()
    markers = ("verification code", "verify email", "enter code", "one-time code", "otp")
    return any(marker in text_blob for marker in markers)


def _locate_verification_input(page):
    selectors = [
        'input[name*="code" i]',
        'input[id*="code" i]',
        'input[autocomplete="one-time-code"]',
        'input[name*="otp" i]',
        'input[type="tel"]',
        'input[inputmode="numeric"]',
    ]
    for selector in selectors:
        locator = page.locator(selector).first
        if locator.count() > 0:
            return locator
    return None


def _attempt_verification_step(page, code: str) -> bool:
    verification_input = _locate_verification_input(page)
    if verification_input is None:
        return False
    verification_input.fill(code)
    submit_button = _find_submit_button(page)
    if submit_button is not None:
        submit_button.click()
    else:
        verification_input.press("Enter")
    page.wait_for_timeout(1500)
    return True


def _attempt_simple_registration(page, applicant: Dict[str, str], username: str, password: str) -> Dict[str, Any]:
    field_values = {
        "first": applicant.get("first_name", ""),
        "last": applicant.get("last_name", ""),
        "email": applicant.get("email", "") or username,
        "username": username,
        "password": password,
        "confirm_password": password,
        "full_name": applicant.get("full_name", ""),
    }
    selectors = {
        "first": ['input[name*="first" i]', 'input[autocomplete="given-name"]'],
        "last": ['input[name*="last" i]', 'input[autocomplete="family-name"]'],
        "email": ['input[type="email"]', 'input[name*="email" i]'],
        "username": ['input[name*="user" i]'],
        "password": ['input[type="password"]'],
        "confirm_password": ['input[name*="confirm" i]', 'input[name*="repeat" i]'],
        "full_name": ['input[name*="full" i]'],
    }

    password_fields = page.locator('input[type="password"]')
    if password_fields.count() == 0:
        return {"submitted": False, "reason": "registration_password_field_missing"}

    for key, selector_list in selectors.items():
        value = field_values.get(key, "").strip()
        if not value:
            continue
        for selector in selector_list:
            locator = page.locator(selector).first
            if locator.count() == 0:
                continue
            try:
                locator.fill(value)
                break
            except Exception:
                continue

    submit_button = _find_submit_button(page)
    if submit_button is None:
        return {"submitted": False, "reason": "registration_submit_button_missing"}
    submit_button.click()
    page.wait_for_timeout(2000)
    inspection = _current_inspection(page)
    return {
        "submitted": True,
        "inspection": inspection,
    }


def _attempt_login(page, username: str, password: str) -> bool:
    if not username or not password:
        return False
    username_locator = None
    for selector in ('input[type="email"]', 'input[name*="email" i]', 'input[name*="user" i]'):
        locator = page.locator(selector).first
        if locator.count() > 0:
            username_locator = locator
            break
    password_locator = page.locator('input[type="password"]').first
    if username_locator is None or password_locator.count() == 0:
        return False
    username_locator.fill(username)
    password_locator.fill(password)
    submit_button = _find_submit_button(page)
    if submit_button is not None:
        submit_button.click()
    else:
        password_locator.press("Enter")
    page.wait_for_timeout(2000)
    return True


def _submit_simple_application(page, applicant: Dict[str, str], inspection: Dict[str, Any], resume_pdf_path: str) -> Dict[str, Any]:
    primary_form = inspection.get("primary_form") or {}
    mapping_plan = plan_field_assignments(primary_form.get("fields", []), applicant)
    if mapping_plan["unmapped_required_fields"]:
        return build_result(
            status="incomplete",
            reason="unmapped_required_fields",
            apply_url=page.url,
            details={
                "inspection": inspection,
                "mapping_plan": mapping_plan,
                "applicant": applicant,
            },
        )

    _fill_textual_fields(page, mapping_plan, applicant)
    if not _upload_resume_if_needed(page, inspection, resume_pdf_path):
        return build_result(
            status="incomplete",
            reason="resume_upload_required",
            apply_url=page.url,
            details={
                "inspection": inspection,
                "mapping_plan": mapping_plan,
                "resume_pdf_path": resume_pdf_path,
            },
        )

    submit_button = _find_submit_button(page)
    if submit_button is None:
        return build_result(
            status="incomplete",
            reason="submit_button_missing",
            apply_url=page.url,
            details={"inspection": inspection, "mapping_plan": mapping_plan},
        )

    submit_button.click()
    page.wait_for_timeout(2500)
    post_submit_inspection = _current_inspection(page)
    success_text = page.content().lower()
    success_markers = ("application submitted", "thank you for applying", "thanks for applying", "application received")
    if any(marker in success_text for marker in success_markers):
        return build_result(
            status="applied",
            reason="application_submitted",
            apply_url=page.url,
            details={
                "inspection": post_submit_inspection,
                "mapping_plan": mapping_plan,
            },
        )

    return build_result(
        status="action_required",
        reason="submission_clicked_manual_review_recommended",
        apply_url=page.url,
        details={
            "inspection": post_submit_inspection,
            "mapping_plan": mapping_plan,
        },
        message="Submission was attempted, but the success state was not unambiguous. Review the target page.",
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
            classification = inspection["classification"]

            if _verification_code():
                if not _attempt_verification_step(page, _verification_code()):
                    print(
                        json.dumps(
                            build_result(
                                status="incomplete",
                                reason="verification_code_field_not_found",
                                apply_url=page.url,
                                details={"inspection": inspection},
                            ),
                            indent=2,
                        )
                    )
                    context.close()
                    browser.close()
                    return 0
                inspection = _current_inspection(page)
                classification = inspection["classification"]

            if classification == "registration_required":
                registration_result = _attempt_simple_registration(
                    page,
                    applicant,
                    username=_site_username() or applicant.get("email", ""),
                    password=_site_password(),
                )
                if not registration_result.get("submitted"):
                    attempted_login = _attempt_login(page, _site_username(), _site_password())
                    if attempted_login:
                        inspection = _current_inspection(page)
                        classification = inspection["classification"]
                    else:
                        print(
                            json.dumps(
                                build_result(
                                    status="incomplete",
                                    reason=registration_result.get("reason") or "registration_not_supported",
                                    apply_url=page.url,
                                    details={"inspection": inspection},
                                ),
                                indent=2,
                            )
                        )
                        context.close()
                        browser.close()
                        return 0
                else:
                    inspection = registration_result["inspection"]
                    classification = inspection["classification"]
                    if _is_verification_gate(inspection):
                        if storage_state_path:
                            context.storage_state(path=storage_state_path)
                        print(
                            json.dumps(
                                build_result(
                                    status="awaiting_email_verification",
                                    reason="email_verification_required",
                                    apply_url=page.url,
                                    details={"inspection": inspection, "classification": classification},
                                    message="Registration was submitted. Provide the email verification code to continue the application.",
                                ),
                                indent=2,
                            )
                        )
                        context.close()
                        browser.close()
                        return 0

            if classification not in {"simple_form", "registration_required"}:
                if _is_verification_gate(inspection):
                    if storage_state_path:
                        context.storage_state(path=storage_state_path)
                    print(
                        json.dumps(
                            build_result(
                                status="awaiting_email_verification",
                                reason="email_verification_required",
                                apply_url=page.url,
                                details={"inspection": inspection, "classification": classification},
                                message="The target site is waiting on an email verification code.",
                            ),
                            indent=2,
                        )
                    )
                    context.close()
                    browser.close()
                    return 0
                print(
                    json.dumps(
                        build_result(
                            status="incomplete",
                            reason=classification,
                            apply_url=page.url,
                            details={"inspection": inspection, "classification": classification},
                        ),
                        indent=2,
                    )
                )
                context.close()
                browser.close()
                return 0

            result = _submit_simple_application(page, applicant, inspection, _resume_pdf_path())
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
                    details={"error": str(exc)},
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
                    details={"error": str(exc)},
                ),
                indent=2,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
