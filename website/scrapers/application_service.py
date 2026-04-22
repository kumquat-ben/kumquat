from __future__ import annotations

import json
import os
import secrets
import string
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from django.conf import settings
from django.utils import timezone

from .submission_runtime import build_applicant_profile
from .applicant_documents import build_runtime_artifact_dir, generate_resume_pdf
from .models import ApplicantSiteCredential, JobApplicationRun, JobApplicationSubmissionManager, JobApplicationSubmissionRequest, JobPosting
from .utils import GENERIC_SUBMIT_SCRIPT_NAME, build_job_submission_requirements, get_job_apply_url, get_manual_script_path, identify_submit_script_candidates


def _apply_method_for_script(script_name: str) -> str:
    normalized = str(script_name or "").strip()
    if not normalized:
        return JobApplicationRun.ApplyMethod.UNKNOWN
    if normalized == GENERIC_SUBMIT_SCRIPT_NAME:
        return JobApplicationRun.ApplyMethod.GENERIC_SCRIPT
    return JobApplicationRun.ApplyMethod.SITE_SPECIFIC_SCRIPT


def _current_step_for_status(status_value: str) -> str:
    step_map = {
        JobApplicationRun.Status.PENDING: "queued",
        JobApplicationRun.Status.RUNNING: "automation_running",
        JobApplicationRun.Status.AWAITING_EMAIL_VERIFICATION: "awaiting_email_verification",
        JobApplicationRun.Status.ACTION_REQUIRED: "manual_action_required",
        JobApplicationRun.Status.APPLIED: "completed",
        JobApplicationRun.Status.NEEDS_REVIEW: "systems_review",
        JobApplicationRun.Status.FAILED: "closed_as_failed",
        JobApplicationRun.Status.INCOMPLETE: "systems_review",
        JobApplicationRun.Status.ERROR: "systems_review",
    }
    return step_map.get(status_value, "")


def _build_step_sequence(run: JobApplicationRun) -> list[Dict[str, Any]]:
    runtime_state = run.runtime_state if isinstance(run.runtime_state, dict) else {}
    current_step = run.current_step or _current_step_for_status(run.status)
    method_label = run.get_apply_method_display() if run.apply_method else "Unknown"
    classification = run.form_classification or runtime_state.get("last_form_classification") or ""
    review_note = run.review_notes or run.last_error or ""

    steps = [
        {
            "key": "queued",
            "label": "Queued",
            "status": "completed",
            "detail": "The application request was accepted by Kumquat.",
        },
        {
            "key": "method_selected",
            "label": "Method Selected",
            "status": "completed",
            "detail": method_label,
        },
        {
            "key": "profile_prepared",
            "label": "Applicant Profile Prepared",
            "status": "completed" if runtime_state.get("applicant_profile") else "pending",
            "detail": "Resume and profile data prepared for runtime.",
        },
        {
            "key": "automation_running",
            "label": "Automation Attempt",
            "status": "pending",
            "detail": classification or "Script execution and form inspection are in progress.",
        },
        {
            "key": "awaiting_email_verification",
            "label": "Email Verification",
            "status": "pending",
            "detail": run.verification_prompt or "Verification is only required if the target site asks for it.",
        },
        {
            "key": "manual_action_required",
            "label": "Manual Review",
            "status": "pending",
            "detail": "A human may need to inspect the target site or complete a non-automated step.",
        },
        {
            "key": "systems_review",
            "label": "Systems Team Review",
            "status": "pending",
            "detail": review_note or "Automation did not finish cleanly, so this run is waiting for systems review.",
        },
        {
            "key": "completed",
            "label": "Completed",
            "status": "pending",
            "detail": "The application was submitted successfully.",
        },
        {
            "key": "closed_as_failed",
            "label": "Closed As Failed",
            "status": "pending",
            "detail": review_note or "The systems team marked this application as not recoverable.",
        },
    ]

    reached_steps = {"queued", "method_selected"}
    if runtime_state.get("applicant_profile"):
        reached_steps.add("profile_prepared")
    if run.status in {
        JobApplicationRun.Status.RUNNING,
        JobApplicationRun.Status.AWAITING_EMAIL_VERIFICATION,
        JobApplicationRun.Status.ACTION_REQUIRED,
        JobApplicationRun.Status.APPLIED,
        JobApplicationRun.Status.NEEDS_REVIEW,
        JobApplicationRun.Status.FAILED,
        JobApplicationRun.Status.INCOMPLETE,
        JobApplicationRun.Status.ERROR,
    }:
        reached_steps.add("automation_running")
    if run.status == JobApplicationRun.Status.AWAITING_EMAIL_VERIFICATION:
        reached_steps.add("awaiting_email_verification")
    if run.status == JobApplicationRun.Status.ACTION_REQUIRED:
        reached_steps.add("manual_action_required")
    if run.status in {
        JobApplicationRun.Status.NEEDS_REVIEW,
        JobApplicationRun.Status.FAILED,
        JobApplicationRun.Status.INCOMPLETE,
        JobApplicationRun.Status.ERROR,
    }:
        reached_steps.add("systems_review")
    if run.status == JobApplicationRun.Status.APPLIED:
        reached_steps.add("completed")
    if run.status == JobApplicationRun.Status.FAILED:
        reached_steps.add("closed_as_failed")

    for step in steps:
        if step["key"] in reached_steps:
            step["status"] = "completed"
        if step["key"] == current_step and run.status not in {JobApplicationRun.Status.APPLIED, JobApplicationRun.Status.FAILED}:
            step["status"] = "current"
    return steps


def _status_message_for_run(run: JobApplicationRun) -> str:
    if run.status == JobApplicationRun.Status.APPLIED:
        return "Application submitted."
    if run.status == JobApplicationRun.Status.AWAITING_EMAIL_VERIFICATION:
        return run.verification_prompt or "A verification code is required to continue the application."
    if run.status == JobApplicationRun.Status.ACTION_REQUIRED:
        return run.last_error or "Manual follow-up is required to finish this application."
    if run.status == JobApplicationRun.Status.NEEDS_REVIEW:
        return run.review_notes or "Automation could not finish cleanly. The systems team will review this run before it is marked failed."
    if run.status == JobApplicationRun.Status.FAILED:
        return run.review_notes or run.last_error or "This application was marked failed after systems review."
    if run.status == JobApplicationRun.Status.RUNNING:
        return "Application automation is running."
    return "Application request queued."


def build_submission_manager_defaults(job_posting, user) -> Dict[str, Any]:
    apply_url = get_job_apply_url(job_posting)
    candidate_scripts = identify_submit_script_candidates(job_posting)
    requirements = build_job_submission_requirements(job_posting)
    if candidate_scripts:
        status_value = JobApplicationSubmissionManager.Status.CANDIDATE_SCRIPT_FOUND
        matched_script_name = candidate_scripts[0]
        if matched_script_name == GENERIC_SUBMIT_SCRIPT_NAME:
            notes = (
                "No job-specific submit script matched, so the generic fallback submit script was selected. "
                "It can classify registration and simple forms, create a resumable application run, and pause for human email-code verification."
            )
        else:
            notes = (
                "A submit-script candidate was identified from backend/manual_scripts/submit/. "
                "The run state machine can still pause for human verification when the target site requires it."
            )
    else:
        status_value = JobApplicationSubmissionManager.Status.DESIGN_TIME_SCRIPT_NEEDED
        matched_script_name = ""
        notes = (
            "No submit script was identified for this job yet. "
            "A design-time script request has been recorded with the current form and job requirements."
        )

    return {
        "first_requested_by": user,
        "last_requested_by": user,
        "status": status_value,
        "matched_script_name": matched_script_name,
        "candidate_script_names": candidate_scripts,
        "apply_url": apply_url,
        "apply_domain": urlparse(apply_url).netloc if apply_url else "",
        "form_requirements": requirements,
        "requirements_summary": json.dumps(requirements, indent=2, ensure_ascii=False),
        "notes": notes,
        "request_count": 1,
    }


def get_or_create_submission_manager_for_user(job_posting, user):
    manager, created = JobApplicationSubmissionManager.objects.get_or_create(
        job=job_posting,
        defaults=build_submission_manager_defaults(job_posting, user),
    )

    request_record, request_created = JobApplicationSubmissionRequest.objects.get_or_create(
        job=job_posting,
        user=user,
        defaults={
            "manager": manager,
            "status": JobApplicationSubmissionRequest.Status.REQUESTED,
        },
    )

    if created:
        return manager, request_record, True

    if request_created:
        manager.last_requested_by = user
        manager.request_count += 1
        manager.save(update_fields=["last_requested_by", "request_count", "updated_at"])

    return manager, request_record, False


def serialize_apply_service_payload(job_posting, manager, request_record, created, run: Optional[JobApplicationRun] = None):
    payload = {
        "job": {
            "id": job_posting.id,
            "title": job_posting.title,
            "company": job_posting.scraper.company,
            "jobUrl": job_posting.link,
            "applyUrl": manager.apply_url,
        },
        "manager": {
            "id": manager.id,
            "status": manager.status,
            "browserSessionStatus": manager.browser_session_status,
            "matchedScriptName": manager.matched_script_name,
            "candidateScriptNames": manager.candidate_script_names,
            "requestCount": manager.request_count,
            "notes": manager.notes,
            "requirements": manager.form_requirements,
        },
        "request": {
            "id": request_record.id,
            "status": request_record.status,
            "createdAt": request_record.created_at.isoformat(),
            "created": created,
        },
    }
    if run is not None:
        payload["run"] = serialize_application_run(run)
    return payload


def serialize_application_run(run: JobApplicationRun) -> Dict[str, Any]:
    result = run.last_result if isinstance(run.last_result, dict) else {}
    runtime_state = run.runtime_state if isinstance(run.runtime_state, dict) else {}
    credential = run.credential
    return {
        "id": run.id,
        "status": run.status,
        "selectedScriptName": run.selected_script_name,
        "applyMethod": run.apply_method,
        "applyMethodLabel": run.get_apply_method_display(),
        "applyUrl": run.apply_url,
        "applyDomain": run.apply_domain,
        "formClassification": run.form_classification,
        "currentStep": run.current_step,
        "steps": _build_step_sequence(run),
        "statusMessage": _status_message_for_run(run),
        "isFinalFailure": run.status == JobApplicationRun.Status.FAILED,
        "verificationPrompt": run.verification_prompt,
        "submittedAt": run.submitted_at.isoformat() if run.submitted_at else None,
        "lastError": run.last_error,
        "reviewNotes": run.review_notes,
        "reviewedAt": run.reviewed_at.isoformat() if run.reviewed_at else None,
        "runtimeState": runtime_state,
        "result": result,
        "credential": (
            {
                "id": credential.id,
                "applyDomain": credential.apply_domain,
                "username": credential.username,
                "loginUrl": credential.login_url,
                "lastUsedAt": credential.last_used_at.isoformat() if credential.last_used_at else None,
            }
            if credential
            else None
        ),
    }


def _generate_site_password(length: int = 18) -> str:
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
    return "".join(secrets.choice(alphabet) for _ in range(length))


def get_or_create_site_credential(*, user, apply_domain: str, apply_url: str, credential_input: Optional[Dict[str, Any]] = None):
    credential_input = credential_input or {}
    normalized_domain = (apply_domain or urlparse(apply_url).netloc or "").strip().lower()
    defaults = {
        "login_url": credential_input.get("loginUrl") or apply_url,
        "username": credential_input.get("username") or user.email or user.get_username(),
        "password": credential_input.get("password") or _generate_site_password(),
        "metadata": {"source": "kumquat_job_apply"},
    }
    credential, created = ApplicantSiteCredential.objects.get_or_create(
        user=user,
        apply_domain=normalized_domain,
        defaults=defaults,
    )
    updated_fields = []
    if credential_input.get("username") and credential.username != credential_input["username"]:
        credential.username = credential_input["username"]
        updated_fields.append("username")
    if credential_input.get("password") and credential.password != credential_input["password"]:
        credential.password = credential_input["password"]
        updated_fields.append("password")
    if credential_input.get("loginUrl") and credential.login_url != credential_input["loginUrl"]:
        credential.login_url = credential_input["loginUrl"]
        updated_fields.append("login_url")
    if updated_fields:
        credential.save(update_fields=updated_fields + ["updated_at"])
    return credential, created


def _terminal_run_status(status_value: str) -> bool:
    return status_value in {
        JobApplicationRun.Status.APPLIED,
        JobApplicationRun.Status.NEEDS_REVIEW,
        JobApplicationRun.Status.FAILED,
        JobApplicationRun.Status.INCOMPLETE,
        JobApplicationRun.Status.ERROR,
    }


def get_or_create_active_application_run(
    *,
    manager: JobApplicationSubmissionManager,
    user,
    credential: Optional[ApplicantSiteCredential],
) -> JobApplicationRun:
    active_run = manager.application_runs.filter(user=user).order_by("-created_at", "-id").first()
    if active_run and not _terminal_run_status(active_run.status):
        return active_run

    return JobApplicationRun.objects.create(
        manager=manager,
        job=manager.job,
        user=user,
        credential=credential,
        status=JobApplicationRun.Status.PENDING,
        selected_script_name=manager.matched_script_name or GENERIC_SUBMIT_SCRIPT_NAME,
        apply_method=_apply_method_for_script(manager.matched_script_name or GENERIC_SUBMIT_SCRIPT_NAME),
        apply_url=manager.apply_url,
        apply_domain=manager.apply_domain,
        current_step=_current_step_for_status(JobApplicationRun.Status.PENDING),
    )


def _runtime_file_paths(run: JobApplicationRun) -> Dict[str, Path]:
    base_dir = build_runtime_artifact_dir("runs", str(run.user_id), str(run.id))
    return {
        "base_dir": base_dir,
        "storage_state": base_dir / "playwright_storage_state.json",
    }


def _prepare_resume_pdf(run: JobApplicationRun) -> Path:
    applicant = run.runtime_state.get("applicant_profile") if isinstance(run.runtime_state, dict) else None
    if not isinstance(applicant, dict):
        applicant = {}
    output_path = generate_resume_pdf(applicant, user_id=run.user_id, run_id=run.id)
    runtime_state = dict(run.runtime_state or {})
    runtime_state["resume_pdf_path"] = str(output_path)
    run.runtime_state = runtime_state
    run.save(update_fields=["runtime_state", "updated_at"])
    return output_path


def _execute_application_script(
    *,
    script_name: str,
    run: JobApplicationRun,
    verification_code: str = "",
) -> Dict[str, Any]:
    script_path = get_manual_script_path(script_name)
    env = os.environ.copy()
    env.setdefault("DJANGO_SETTINGS_MODULE", "website.settings")
    backend_path = str(Path(__file__).resolve().parents[1])
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = backend_path + (os.pathsep + existing_pythonpath if existing_pythonpath else "")

    paths = _runtime_file_paths(run)
    credential = run.credential
    env.update(
        {
            "KUMQUAT_JOB_ID": str(run.job_id),
            "KUMQUAT_USER_ID": str(run.user_id),
            "KUMQUAT_APPLICATION_RUN_ID": str(run.id),
            "KUMQUAT_JOB_URL": run.apply_url or "",
            "KUMQUAT_STORAGE_STATE_PATH": str(paths["storage_state"]),
            "KUMQUAT_VERIFICATION_CODE": verification_code or "",
            "KUMQUAT_SITE_USERNAME": credential.username if credential else "",
            "KUMQUAT_SITE_PASSWORD": credential.password if credential else "",
        }
    )
    resume_path = _prepare_resume_pdf(run)
    env["KUMQUAT_RESUME_PDF_PATH"] = str(resume_path)

    timeout_seconds = max(int(getattr(settings, "JOB_APPLY_SCRIPT_TIMEOUT_SECONDS", 45)), 5)
    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            env=env,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        return {
            "status": "error",
            "reason": "script_timeout",
            "details": {
                "timeoutSeconds": timeout_seconds,
                "stdout": (exc.stdout or "")[-4000:],
                "stderr": (exc.stderr or "")[-4000:],
            },
            "exit_code": None,
            "stderr": (exc.stderr or "")[-4000:],
        }
    except Exception as exc:
        return {
            "status": "error",
            "reason": "script_execution_failed",
            "details": {
                "error": str(exc),
                "scriptName": script_name,
            },
            "exit_code": None,
            "stderr": str(exc),
        }

    stdout = (result.stdout or "").strip()
    parsed_output: Dict[str, Any]
    if stdout:
        try:
            parsed_output = json.loads(stdout)
        except json.JSONDecodeError:
            parsed_output = {
                "status": "error",
                "reason": "invalid_json_output",
                "details": {"stdout": stdout},
            }
    else:
        parsed_output = {
            "status": "error",
            "reason": "empty_output",
            "details": {"stderr": result.stderr},
        }
    parsed_output.setdefault("exit_code", result.returncode)
    parsed_output.setdefault("stderr", result.stderr or "")
    return parsed_output


def _apply_script_result(run: JobApplicationRun, result: Dict[str, Any]) -> JobApplicationRun:
    status_value = str(result.get("status") or "").strip().lower()
    details = result.get("details") if isinstance(result.get("details"), dict) else {}
    inspection = details.get("inspection") if isinstance(details.get("inspection"), dict) else {}
    form_classification = str(
        result.get("form_classification")
        or details.get("classification")
        or inspection.get("classification")
        or ""
    ).strip()
    next_status = JobApplicationRun.Status.NEEDS_REVIEW
    verification_prompt = ""
    last_error = ""
    submitted_at = run.submitted_at
    review_notes = run.review_notes

    if status_value == "applied":
        next_status = JobApplicationRun.Status.APPLIED
        submitted_at = timezone.now()
        review_notes = ""
    elif status_value == "awaiting_email_verification":
        next_status = JobApplicationRun.Status.AWAITING_EMAIL_VERIFICATION
        verification_prompt = str(result.get("message") or result.get("reason") or "Enter the email verification code.")
        review_notes = ""
    elif status_value == "action_required":
        next_status = JobApplicationRun.Status.ACTION_REQUIRED
        last_error = str(result.get("message") or result.get("reason") or "Manual review is recommended.")
        review_notes = ""
    elif status_value == "ready":
        next_status = JobApplicationRun.Status.ACTION_REQUIRED
        last_error = str(result.get("message") or result.get("reason") or "Manual review is recommended.")
        review_notes = ""
    elif status_value == "error":
        next_status = JobApplicationRun.Status.NEEDS_REVIEW
        last_error = str(result.get("reason") or result.get("details", {}).get("error") or "Application script failed.")
        review_notes = "Automation hit an execution error. The systems team should inspect and decide whether to reprocess or close this run."
    elif status_value == "incomplete":
        next_status = JobApplicationRun.Status.NEEDS_REVIEW
        last_error = str(result.get("reason") or "Application could not be completed automatically.")
        review_notes = "Automation could not finish the target flow. The systems team should review this run before it is marked failed."

    runtime_state = dict(run.runtime_state or {})
    if form_classification:
        runtime_state["last_form_classification"] = form_classification
    if details:
        runtime_state["last_details"] = details
    runtime_state["last_reason"] = result.get("reason")

    run.status = next_status
    run.form_classification = form_classification
    run.current_step = _current_step_for_status(next_status)
    run.verification_prompt = verification_prompt
    run.last_result = result
    run.runtime_state = runtime_state
    run.last_error = last_error
    run.submitted_at = submitted_at
    run.review_notes = review_notes
    run.save(
        update_fields=[
            "status",
            "form_classification",
            "current_step",
            "verification_prompt",
            "last_result",
            "runtime_state",
            "last_error",
            "submitted_at",
            "review_notes",
            "updated_at",
        ]
    )

    if run.credential:
        run.credential.last_used_at = timezone.now()
        run.credential.save(update_fields=["last_used_at", "updated_at"])

    if next_status in {JobApplicationRun.Status.APPLIED, JobApplicationRun.Status.ACTION_REQUIRED}:
        run.manager.browser_session_status = JobApplicationSubmissionManager.BrowserSessionStatus.COMPLETED
        run.manager.save(update_fields=["browser_session_status", "updated_at"])

    return run


def start_or_resume_application(
    *,
    job_posting: JobPosting,
    user,
    verification_code: str = "",
    credential_input: Optional[Dict[str, Any]] = None,
) -> tuple[JobApplicationSubmissionManager, JobApplicationSubmissionRequest, bool, JobApplicationRun]:
    manager, request_record, created = get_or_create_submission_manager_for_user(job_posting, user)
    apply_url = manager.apply_url or get_job_apply_url(job_posting)
    apply_domain = manager.apply_domain or urlparse(apply_url).netloc
    credential, _credential_created = get_or_create_site_credential(
        user=user,
        apply_domain=apply_domain,
        apply_url=apply_url,
        credential_input=credential_input,
    )
    run = get_or_create_active_application_run(manager=manager, user=user, credential=credential)
    run.selected_script_name = manager.matched_script_name or GENERIC_SUBMIT_SCRIPT_NAME
    run.apply_method = _apply_method_for_script(run.selected_script_name)
    run.apply_url = apply_url
    run.apply_domain = apply_domain
    run.credential = credential
    run.status = JobApplicationRun.Status.RUNNING
    run.current_step = _current_step_for_status(JobApplicationRun.Status.RUNNING)
    runtime_state = dict(run.runtime_state or {})
    runtime_state["applicant_profile"] = build_applicant_profile(user)
    run.runtime_state = runtime_state
    run.save(
        update_fields=[
            "selected_script_name",
            "apply_method",
            "apply_url",
            "apply_domain",
            "credential",
            "status",
            "current_step",
            "runtime_state",
            "updated_at",
        ]
    )

    try:
        result = _execute_application_script(
            script_name=run.selected_script_name,
            run=run,
            verification_code=verification_code,
        )
        run = _apply_script_result(run, result)
        return manager, request_record, created, run
    except Exception as exc:
        error_result = {
            "status": "error",
            "reason": "application_service_failure",
            "details": {"error": str(exc)},
            "stderr": str(exc),
        }
        run = _apply_script_result(run, error_result)
        return manager, request_record, created, run


def queue_application_run(
    *,
    job_posting: JobPosting,
    user,
    verification_code: str = "",
    credential_input: Optional[Dict[str, Any]] = None,
) -> tuple[JobApplicationSubmissionManager, JobApplicationSubmissionRequest, bool, JobApplicationRun]:
    manager, request_record, created = get_or_create_submission_manager_for_user(job_posting, user)
    apply_url = manager.apply_url or get_job_apply_url(job_posting)
    apply_domain = manager.apply_domain or urlparse(apply_url).netloc
    credential, _credential_created = get_or_create_site_credential(
        user=user,
        apply_domain=apply_domain,
        apply_url=apply_url,
        credential_input=credential_input,
    )
    run = get_or_create_active_application_run(manager=manager, user=user, credential=credential)
    runtime_state = dict(run.runtime_state or {})
    runtime_state["applicant_profile"] = build_applicant_profile(user)
    if verification_code:
        runtime_state["queued_verification_code"] = verification_code
    elif "queued_verification_code" not in runtime_state:
        runtime_state["queued_verification_code"] = ""
    run.selected_script_name = manager.matched_script_name or GENERIC_SUBMIT_SCRIPT_NAME
    run.apply_method = _apply_method_for_script(run.selected_script_name)
    run.apply_url = apply_url
    run.apply_domain = apply_domain
    run.credential = credential
    run.status = JobApplicationRun.Status.PENDING
    run.current_step = _current_step_for_status(JobApplicationRun.Status.PENDING)
    run.runtime_state = runtime_state
    run.last_error = ""
    run.review_notes = ""
    run.save(
        update_fields=[
            "selected_script_name",
            "apply_method",
            "apply_url",
            "apply_domain",
            "credential",
            "status",
            "current_step",
            "runtime_state",
            "last_error",
            "review_notes",
            "updated_at",
        ]
    )
    return manager, request_record, created, run


def process_application_run(run: JobApplicationRun) -> JobApplicationRun:
    runtime_state = dict(run.runtime_state or {})
    verification_code = str(runtime_state.pop("queued_verification_code", "") or "")
    runtime_state["applicant_profile"] = build_applicant_profile(run.user)
    run.status = JobApplicationRun.Status.RUNNING
    run.current_step = _current_step_for_status(JobApplicationRun.Status.RUNNING)
    run.runtime_state = runtime_state
    run.save(update_fields=["status", "current_step", "runtime_state", "updated_at"])

    try:
        result = _execute_application_script(
            script_name=run.selected_script_name or GENERIC_SUBMIT_SCRIPT_NAME,
            run=run,
            verification_code=verification_code,
        )
        return _apply_script_result(run, result)
    except Exception as exc:
        error_result = {
            "status": "error",
            "reason": "application_service_failure",
            "details": {"error": str(exc)},
            "stderr": str(exc),
        }
        return _apply_script_result(run, error_result)
