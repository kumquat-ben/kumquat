#!/usr/bin/env python3
"""Start or resume a resumable job-application run from the command line."""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "website.settings")

import django  # noqa: E402

django.setup()

from django.contrib.auth import get_user_model  # noqa: E402

from scrapers.application_service import serialize_application_run, start_or_resume_application  # noqa: E402
from scrapers.models import JobPosting  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start or resume a job-application run.")
    parser.add_argument("--job-id", type=int, required=True, help="Internal JobPosting id.")
    parser.add_argument("--user-id", type=int, required=True, help="Internal user id.")
    parser.add_argument("--verification-code", default="", help="Email verification code when resuming a paused run.")
    parser.add_argument("--site-username", default="", help="Optional site username override.")
    parser.add_argument("--site-password", default="", help="Optional site password override.")
    parser.add_argument("--login-url", default="", help="Optional login URL override for the saved site credential.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    user_model = get_user_model()
    job = JobPosting.objects.select_related("scraper").get(pk=args.job_id)
    user = user_model.objects.get(pk=args.user_id)
    _manager, _request_record, _created, run = start_or_resume_application(
        job_posting=job,
        user=user,
        verification_code=args.verification_code,
        credential_input={
            "username": args.site_username,
            "password": args.site_password,
            "loginUrl": args.login_url,
        },
    )
    print(json.dumps(serialize_application_run(run), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
