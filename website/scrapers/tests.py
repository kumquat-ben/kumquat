from __future__ import annotations

import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from .models import (
    ApplicantSiteCredential,
    JobApplicationRun,
    JobApplicationSubmissionManager,
    JobApplicationSubmissionRequest,
    JobPosting,
    ManualScriptSourceURL,
    Scraper,
)
from .submission_runtime import build_applicant_profile, inspect_application_page, plan_field_assignments
from .search import _build_match_snippet, _database_fallback_search
from .utils import (
    get_manual_scripts_overview,
    get_submit_script_names,
    identify_submit_script_candidates,
    parse_manual_script_urls,
    refresh_manual_script_source_url_cache,
)


class ManualScriptSourceURLTests(TestCase):
    def test_get_submit_script_names_avoids_heavy_overview_query(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            scripts_dir = Path(temp_dir)
            (scripts_dir / "crawl_manual.py").write_text(
                'CRAWL_URL = "https://crawl.example.com/jobs"\n',
                encoding="utf-8",
            )
            submit_dir = scripts_dir / "submit"
            submit_dir.mkdir()
            (submit_dir / "apply_button.py").write_text(
                'APPLY_URL = "https://apply.example.com/job/123"\n',
                encoding="utf-8",
            )

            with patch("scrapers.utils.MANUAL_SCRIPTS_DIR", scripts_dir), patch(
                "scrapers.utils.get_manual_scripts_overview",
                side_effect=AssertionError("get_manual_scripts_overview should not be used"),
            ):
                submit_scripts = get_submit_script_names()

        self.assertEqual(submit_scripts, ["submit/apply_button.py"])

    def test_parse_manual_script_urls_extracts_literals_and_resolved_values(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            script_path = Path(temp_dir) / "sample_manual.py"
            script_path.write_text(
                "\n".join(
                    [
                        'BASE_URL = "https://example.com"',
                        'SEARCH_PATH = "/jobs"',
                        'CAREERS_URL = f"{BASE_URL}{SEARCH_PATH}"',
                        'API_URL = "https://api.example.com/v1/openings"',
                        'DOC = "See https://docs.example.com/guide"',
                    ]
                ),
                encoding="utf-8",
            )

            rows = parse_manual_script_urls(script_path)

        extracted = {(row["source_name"], row["url"]) for row in rows}
        self.assertIn(("BASE_URL", "https://example.com"), extracted)
        self.assertIn(("CAREERS_URL", "https://example.com/jobs"), extracted)
        self.assertIn(("API_URL", "https://api.example.com/v1/openings"), extracted)
        self.assertIn(("literal", "https://docs.example.com/guide"), extracted)

    def test_refresh_cache_populates_database_and_inventory_view(self):
        user_model = get_user_model()
        admin = user_model.objects.create_superuser(
            username="admin",
            email="admin@example.com",
            password="pass1234",
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            scripts_dir = Path(temp_dir)
            (scripts_dir / "alpha_manual.py").write_text(
                "\n".join(
                    [
                        'BASE_URL = "https://alpha.example.com"',
                        'SEARCH_URL = BASE_URL + "/careers"',
                    ]
                ),
                encoding="utf-8",
            )
            (scripts_dir / "beta_manual.py").write_text(
                'API_URL = "https://beta.example.com/api/jobs"\n',
                encoding="utf-8",
            )

            with patch("scrapers.utils.MANUAL_SCRIPTS_DIR", scripts_dir), patch("scrapers.views.MANUAL_SCRIPTS_DIR", scripts_dir):
                summary = refresh_manual_script_source_url_cache()
                self.assertEqual(summary["scripts"], 2)
                self.assertEqual(ManualScriptSourceURL.objects.count(), 3)

                self.client.force_login(admin)
                response = self.client.get(reverse("scrapers-manual-urls"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "alpha_manual.py")
        self.assertContains(response, "https://alpha.example.com/careers")
        self.assertContains(response, "https://beta.example.com/api/jobs")

    def test_nested_submit_scripts_are_discovered_and_cached(self):
        user_model = get_user_model()
        admin = user_model.objects.create_superuser(
            username="admin_submit_group",
            email="admin_submit_group@example.com",
            password="pass1234",
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            scripts_dir = Path(temp_dir)
            (scripts_dir / "crawl_manual.py").write_text(
                'CRAWL_URL = "https://crawl.example.com/jobs"\n',
                encoding="utf-8",
            )
            submit_dir = scripts_dir / "submit"
            submit_dir.mkdir()
            (submit_dir / "apply_button.py").write_text(
                'APPLY_URL = "https://apply.example.com/job/123"\n',
                encoding="utf-8",
            )

            with patch("scrapers.utils.MANUAL_SCRIPTS_DIR", scripts_dir), patch("scrapers.views.MANUAL_SCRIPTS_DIR", scripts_dir):
                summary = refresh_manual_script_source_url_cache()
                overview = get_manual_scripts_overview()
                self.client.force_login(admin)
                response = self.client.get(reverse("scrapers-manual"))

        self.assertEqual(summary["scripts"], 2)
        self.assertEqual(ManualScriptSourceURL.objects.count(), 2)
        submit_script = next(script for script in overview if script["group"] == "submit")
        self.assertEqual(submit_script["name"], "submit/apply_button.py")
        self.assertEqual(submit_script["display_name"], "apply_button.py")
        self.assertEqual(submit_script["group"], "submit")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Submit Scripts")
        self.assertContains(response, "apply_button.py")
        self.assertContains(response, "1 crawl")
        self.assertContains(response, "1 submit")

    def test_cached_urls_api_returns_public_paginated_json(self):
        ManualScriptSourceURL.objects.create(
            script_name="alpha_manual.py",
            source_name="SEARCH_URL",
            url="https://alpha.example.com/careers",
            file_modified_at=datetime(2026, 3, 21, 12, 0, tzinfo=timezone.utc),
        )
        ManualScriptSourceURL.objects.create(
            script_name="beta_manual.py",
            source_name="API_URL",
            url="https://beta.example.com/api/jobs",
            file_modified_at=datetime(2026, 3, 21, 12, 5, tzinfo=timezone.utc),
        )

        response = self.client.get(
            reverse("scrapers-manual-urls-api"),
            {"script": "alpha_manual.py", "page_size": 1},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["filters"]["script"], "alpha_manual.py")
        self.assertEqual(payload["page_size"], 1)
        self.assertEqual(len(payload["results"]), 1)
        self.assertEqual(payload["results"][0]["script_name"], "alpha_manual.py")
        self.assertEqual(payload["results"][0]["url"], "https://alpha.example.com/careers")
        self.assertIn("/api/jobs-docs/", payload["docs_url"])

    def test_cached_urls_api_rejects_invalid_pagination(self):
        response = self.client.get(reverse("scrapers-manual-urls-api"), {"page": 0})

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"], "must be a positive integer")

    def test_jobs_api_docs_include_manual_script_url_service(self):
        response = self.client.get(reverse("jobs-api-docs"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "/api/manual-scripts/urls.json")
        self.assertContains(response, "Cached Manual Script URLs")


class SearchSnippetTests(TestCase):
    def test_build_match_snippet_centers_on_query_match(self):
        text = (
            "Intro text that should be skipped. "
            "This role builds distributed search ranking systems for job seekers across the platform. "
            "Trailing text that should not dominate the preview."
        )

        snippet = _build_match_snippet(text, "ranking systems")

        self.assertIn("ranking systems", snippet.lower())
        self.assertNotIn("Intro text that should be skipped.", snippet)

    def test_database_fallback_search_uses_match_centered_summary(self):
        scraper = Scraper.objects.create(company="Example Co", url="https://example.com/jobs", code="[]")
        JobPosting.objects.create(
            scraper=scraper,
            title="Platform Engineer",
            location="Remote",
            link="https://example.com/jobs/platform-engineer",
            description=(
                "Opening overview. "
                "You will own observability and search relevance tuning for job discovery across the product. "
                "Closing details."
            ),
        )

        payload = _database_fallback_search("relevance tuning", page=1, page_size=10)

        self.assertEqual(payload["match_count"], 1)
        summary = payload["results"][0]["summary"]
        self.assertIn("relevance tuning", summary.lower())
        self.assertNotIn("Opening overview.", summary)


class JobPostingApplyTests(TestCase):
    def test_manual_scripts_view_redirects_anonymous_users_to_api_login(self):
        response = self.client.get(reverse("scrapers-manual"))

        self.assertEqual(response.status_code, 302)
        self.assertIn("/auth/sign-in?next=/api/manual-scripts/", response["Location"])

    def test_identify_submit_script_candidates_appends_generic_fallback(self):
        scraper = Scraper.objects.create(
            company="Acme",
            url="https://acme.example.com/jobs",
            code="print('ok')",
        )
        job = JobPosting.objects.create(
            scraper=scraper,
            title="Backend Engineer",
            location="Remote",
            date="Today",
            link="https://jobs.acme.example.com/backend-engineer",
            description="Example job",
            metadata={"apply_url": "https://jobs.acme.example.com/apply/backend-engineer"},
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            scripts_dir = Path(temp_dir)
            submit_dir = scripts_dir / "submit"
            submit_dir.mkdir()
            (submit_dir / "acme_apply.py").write_text("# acme\n", encoding="utf-8")
            (submit_dir / "generic_form_submit.py").write_text("# generic\n", encoding="utf-8")

            with patch("scrapers.utils.MANUAL_SCRIPTS_DIR", scripts_dir):
                candidates = identify_submit_script_candidates(job)

        self.assertEqual(candidates[0], "submit/acme_apply.py")
        self.assertIn("submit/generic_form_submit.py", candidates)

    def test_identify_submit_script_candidates_prefers_greenhouse_script_for_greenhouse_url(self):
        scraper = Scraper.objects.create(
            company="Webflow",
            url="https://webflow.com/company/careers",
            code="print('ok')",
        )
        job = JobPosting.objects.create(
            scraper=scraper,
            title="Product Manager",
            location="Remote",
            date="Today",
            link="https://job-boards.greenhouse.io/webflow/jobs/7721580",
            description="Example greenhouse job",
            metadata={"apply_url": "https://job-boards.greenhouse.io/webflow/jobs/7721580"},
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            scripts_dir = Path(temp_dir)
            submit_dir = scripts_dir / "submit"
            submit_dir.mkdir()
            (submit_dir / "greenhouse_apply.py").write_text("# greenhouse\n", encoding="utf-8")
            (submit_dir / "generic_form_submit.py").write_text("# generic\n", encoding="utf-8")

            with patch("scrapers.utils.MANUAL_SCRIPTS_DIR", scripts_dir):
                candidates = identify_submit_script_candidates(job)

        self.assertEqual(candidates[0], "submit/greenhouse_apply.py")
        self.assertIn("submit/generic_form_submit.py", candidates)

    def test_job_apply_manager_creates_design_time_request_when_no_submit_scripts_match(self):
        user_model = get_user_model()
        admin = user_model.objects.create_superuser(
            username="admin_apply",
            email="admin_apply@example.com",
            password="pass1234",
        )
        scraper = Scraper.objects.create(
            company="Example Co",
            url="https://example.com/jobs",
            code="print('ok')",
        )
        job = JobPosting.objects.create(
            scraper=scraper,
            title="Backend Engineer",
            location="Remote",
            date="Today",
            link="https://example.com/jobs/1",
            description="Example job",
        )

        self.client.force_login(admin)
        response = self.client.get(reverse("job-posting-apply-manager", args=[job.id]), follow=True)

        self.assertEqual(response.status_code, 200)
        manager = JobApplicationSubmissionManager.objects.get(job=job)
        request_record = JobApplicationSubmissionRequest.objects.get(job=job, user=admin)
        self.assertEqual(manager.status, JobApplicationSubmissionManager.Status.DESIGN_TIME_SCRIPT_NEEDED)
        self.assertEqual(manager.browser_session_status, JobApplicationSubmissionManager.BrowserSessionStatus.PENDING)
        self.assertEqual(manager.first_requested_by, admin)
        self.assertEqual(manager.last_requested_by, admin)
        self.assertEqual(request_record.manager, manager)
        self.assertContains(response, "Submission Manager")
        self.assertContains(response, "design-time script requirement")

    def test_job_apply_manager_uses_generic_fallback_when_available(self):
        user_model = get_user_model()
        admin = user_model.objects.create_superuser(
            username="admin_apply_fallback",
            email="admin_apply_fallback@example.com",
            password="pass1234",
        )
        scraper = Scraper.objects.create(
            company="Example Co",
            url="https://example.com/jobs",
            code="print('ok')",
        )
        job = JobPosting.objects.create(
            scraper=scraper,
            title="Backend Engineer",
            location="Remote",
            date="Today",
            link="https://example.com/jobs/1",
            description="Example job",
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            scripts_dir = Path(temp_dir)
            submit_dir = scripts_dir / "submit"
            submit_dir.mkdir()
            (submit_dir / "generic_form_submit.py").write_text("# generic\n", encoding="utf-8")

            with patch("scrapers.utils.MANUAL_SCRIPTS_DIR", scripts_dir), patch("scrapers.views.MANUAL_SCRIPTS_DIR", scripts_dir):
                self.client.force_login(admin)
                response = self.client.get(reverse("job-posting-apply-manager", args=[job.id]), follow=True)

        self.assertEqual(response.status_code, 200)
        manager = JobApplicationSubmissionManager.objects.get(job=job)
        self.assertEqual(manager.status, JobApplicationSubmissionManager.Status.CANDIDATE_SCRIPT_FOUND)
        self.assertEqual(manager.matched_script_name, "submit/generic_form_submit.py")
        self.assertContains(response, "generic fallback submit script")

    def test_job_apply_service_accepts_authenticated_session(self):
        user_model = get_user_model()
        user = user_model.objects.create_user(
            username="api_apply_user",
            email="api_apply_user@example.com",
            password="pass1234",
        )
        scraper = Scraper.objects.create(
            company="Example Co",
            url="https://example.com/jobs",
            code="print('ok')",
        )
        job = JobPosting.objects.create(
            scraper=scraper,
            title="Backend Engineer",
            location="Remote",
            date="Today",
            link="https://example.com/jobs/1",
            description="Example job",
        )

        self.client.force_login(user)
        response = self.client.post(
            reverse("job-posting-apply-service", args=[job.id]),
            data={},
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 201)
        payload = response.json()
        self.assertEqual(payload["job"]["id"], job.id)
        self.assertEqual(payload["manager"]["status"], JobApplicationSubmissionManager.Status.DESIGN_TIME_SCRIPT_NEEDED)
        self.assertEqual(payload["request"]["status"], JobApplicationSubmissionRequest.Status.REQUESTED)

    def test_job_apply_service_start_creates_queued_run_and_persists_site_credential(self):
        user_model = get_user_model()
        user = user_model.objects.create_user(
            username="runner_user",
            email="runner_user@example.com",
            password="pass1234",
            first_name="Run",
            last_name="Ner",
        )
        scraper = Scraper.objects.create(
            company="Example Co",
            url="https://example.com/jobs",
            code="print('ok')",
        )
        job = JobPosting.objects.create(
            scraper=scraper,
            title="Backend Engineer",
            location="Remote",
            date="Today",
            link="https://example.com/jobs/1",
            description="Example job",
            metadata={"apply_url": "https://apply.example.com/jobs/1"},
        )

        self.client.force_login(user)
        response = self.client.post(
            reverse("job-posting-apply-service", args=[job.id]),
            data={
                "action": "start",
                "siteUsername": "runner_user@example.com",
                "sitePassword": "Secret123!",
            },
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 202)
        payload = response.json()
        self.assertEqual(payload["run"]["status"], JobApplicationRun.Status.PENDING)
        self.assertEqual(payload["run"]["applyMethod"], JobApplicationRun.ApplyMethod.GENERIC_SCRIPT)
        self.assertEqual(payload["run"]["currentStep"], "queued")
        self.assertEqual(payload["run"]["credential"]["username"], "runner_user@example.com")
        credential = ApplicantSiteCredential.objects.get(user=user, apply_domain="apply.example.com")
        self.assertEqual(credential.password, "Secret123!")
        run = JobApplicationRun.objects.get(pk=payload["run"]["id"])
        self.assertEqual(run.status, JobApplicationRun.Status.PENDING)

    @patch("scrapers.application_service._execute_application_script")
    def test_job_apply_service_resume_accepts_verification_code(self, execute_application_script):
        execute_application_script.return_value = {
            "status": "applied",
            "reason": "application_submitted",
            "details": {"inspection": {"classification": "simple_form"}},
        }
        user_model = get_user_model()
        user = user_model.objects.create_user(
            username="resume_user",
            email="resume_user@example.com",
            password="pass1234",
            first_name="Re",
            last_name="Sume",
        )
        scraper = Scraper.objects.create(
            company="Example Co",
            url="https://example.com/jobs",
            code="print('ok')",
        )
        job = JobPosting.objects.create(
            scraper=scraper,
            title="Backend Engineer",
            location="Remote",
            date="Today",
            link="https://example.com/jobs/1",
            description="Example job",
            metadata={"apply_url": "https://apply.example.com/jobs/1"},
        )
        self.client.force_login(user)

        first_response = self.client.post(
            reverse("job-posting-apply-service", args=[job.id]),
            data={"action": "start"},
            content_type="application/json",
        )
        self.assertEqual(first_response.status_code, 202)

        second_response = self.client.post(
            reverse("job-posting-apply-service", args=[job.id]),
            data={"action": "resume", "verificationCode": "123456"},
            content_type="application/json",
        )

        self.assertEqual(second_response.status_code, 202)
        payload = second_response.json()
        self.assertEqual(payload["run"]["status"], JobApplicationRun.Status.PENDING)
        run = JobApplicationRun.objects.get(pk=payload["run"]["id"])
        self.assertEqual(run.status, JobApplicationRun.Status.PENDING)
        self.assertEqual(run.runtime_state.get("queued_verification_code"), "123456")

    @patch("scrapers.application_service._execute_application_script")
    def test_application_errors_move_run_to_needs_review_instead_of_failed(self, execute_application_script):
        execute_application_script.return_value = {
            "status": "error",
            "reason": "playwright_timeout",
            "details": {"inspection": {"classification": "unknown"}},
        }
        user_model = get_user_model()
        user = user_model.objects.create_user(
            username="needs_review_user",
            email="needs_review_user@example.com",
            password="pass1234",
        )
        scraper = Scraper.objects.create(
            company="Example Co",
            url="https://example.com/jobs",
            code="print('ok')",
        )
        job = JobPosting.objects.create(
            scraper=scraper,
            title="Backend Engineer",
            location="Remote",
            date="Today",
            link="https://example.com/jobs/1",
            description="Example job",
            metadata={"apply_url": "https://apply.example.com/jobs/1"},
        )
        self.client.force_login(user)

        queue_response = self.client.post(
            reverse("job-posting-apply-service", args=[job.id]),
            data={"action": "start"},
            content_type="application/json",
        )

        self.assertEqual(queue_response.status_code, 202)
        run = JobApplicationRun.objects.get(pk=queue_response.json()["run"]["id"])
        from .application_service import process_application_run

        processed_run = process_application_run(run)
        self.assertEqual(processed_run.status, JobApplicationRun.Status.NEEDS_REVIEW)
        self.assertEqual(processed_run.current_step, "systems_review")
        self.assertIn("systems team", processed_run.review_notes.lower())

    def test_admin_can_reprocess_and_mark_failed_application_runs(self):
        user_model = get_user_model()
        admin_user = user_model.objects.create_superuser(
            username="apply_admin",
            email="apply_admin@example.com",
            password="pass1234",
        )
        applicant = user_model.objects.create_user(
            username="apply_target",
            email="apply_target@example.com",
            password="pass1234",
        )
        scraper = Scraper.objects.create(
            company="Example Co",
            url="https://example.com/jobs",
            code="print('ok')",
        )
        job = JobPosting.objects.create(
            scraper=scraper,
            title="Backend Engineer",
            location="Remote",
            date="Today",
            link="https://example.com/jobs/1",
            description="Example job",
        )
        manager = JobApplicationSubmissionManager.objects.create(
            job=job,
            status=JobApplicationSubmissionManager.Status.CANDIDATE_SCRIPT_FOUND,
        )
        run = JobApplicationRun.objects.create(
            manager=manager,
            job=job,
            user=applicant,
            status=JobApplicationRun.Status.NEEDS_REVIEW,
            selected_script_name="submit/greenhouse_apply.py",
            apply_method=JobApplicationRun.ApplyMethod.SITE_SPECIFIC_SCRIPT,
            current_step="systems_review",
            review_notes="Needs manual review.",
        )

        self.client.force_login(admin_user)
        reprocess_response = self.client.get(reverse("admin:scrapers_jobapplicationrun_reprocess", args=[run.id]))
        self.assertEqual(reprocess_response.status_code, 302)
        run.refresh_from_db()
        self.assertEqual(run.status, JobApplicationRun.Status.PENDING)
        self.assertEqual(run.current_step, "queued")
        self.assertEqual(run.reprocess_count, 1)

        failed_response = self.client.get(reverse("admin:scrapers_jobapplicationrun_mark_failed", args=[run.id]))
        self.assertEqual(failed_response.status_code, 302)
        run.refresh_from_db()
        self.assertEqual(run.status, JobApplicationRun.Status.FAILED)
        self.assertEqual(run.current_step, "closed_as_failed")
        self.assertEqual(run.reviewed_by, admin_user)


class SubmissionRuntimeTests(TestCase):
    def test_inspect_application_page_marks_registration_required(self):
        inspection = inspect_application_page(
            """
            <html><body>
              <h1>Create account to apply</h1>
              <form action="/register" method="post">
                <input type="email" name="email" required>
                <input type="password" name="password" required>
                <button type="submit">Register</button>
              </form>
            </body></html>
            """,
            "https://jobs.example.com/apply/123",
        )

        self.assertEqual(inspection["classification"], "registration_required")

    def test_inspect_application_page_marks_simple_form(self):
        inspection = inspect_application_page(
            """
            <html><body>
              <form action="/apply" method="post">
                <input type="text" name="first_name" required>
                <input type="text" name="last_name" required>
                <input type="email" name="email" required>
                <button type="submit">Apply</button>
              </form>
            </body></html>
            """,
            "https://jobs.example.com/apply/123",
        )

        self.assertEqual(inspection["classification"], "simple_form")

    def test_plan_field_assignments_flags_unmapped_required_fields(self):
        user_model = get_user_model()
        user = user_model.objects.create_user(
            username="candidate",
            email="candidate@example.com",
            password="pass1234",
            first_name="Jane",
            last_name="Doe",
        )
        applicant = build_applicant_profile(user)
        plan = plan_field_assignments(
            [
                {"name": "first_name", "type": "text", "required": True, "placeholder": "", "autocomplete": ""},
                {"name": "email", "type": "email", "required": True, "placeholder": "", "autocomplete": ""},
                {"name": "cover_letter", "type": "text", "required": True, "placeholder": "", "autocomplete": ""},
            ],
            applicant,
        )

        self.assertEqual(len(plan["assignments"]), 2)
        self.assertEqual(plan["unmapped_required_fields"], ["cover_letter"])


class JobPostingDuplicateTrackingTests(TestCase):
    def test_update_or_create_increments_duplicate_metrics_and_dashboard_summary(self):
        user_model = get_user_model()
        admin = user_model.objects.create_superuser(
            username="admin_dupes",
            email="admin_dupes@example.com",
            password="pass1234",
        )
        scraper = Scraper.objects.create(
            company="Dupes Co",
            url="https://dupes.example.com/jobs",
            code="print('ok')",
        )

        job, created = JobPosting.objects.update_or_create(
            scraper=scraper,
            link="https://dupes.example.com/jobs/123",
            defaults={
                "title": "Platform Engineer",
                "location": "Remote",
                "date": "2026-03-21",
                "description": "First crawl",
            },
        )
        self.assertTrue(created)
        self.assertEqual(job.duplicate_hit_count, 0)

        job, created = JobPosting.objects.update_or_create(
            scraper=scraper,
            link="https://dupes.example.com/jobs/123",
            defaults={
                "title": "Platform Engineer",
                "location": "Remote",
                "date": "2026-03-22",
                "description": "Seen again",
            },
        )
        self.assertFalse(created)
        self.assertEqual(job.duplicate_hit_count, 1)
        self.assertIsNotNone(job.last_crawled_at)
        self.assertIsNotNone(job.last_duplicate_seen_at)

        self.client.force_login(admin)
        dashboard_response = self.client.get(reverse("scrapers-manual"))
        detail_response = self.client.get(reverse("job-posting-detail", args=[job.id]))

        self.assertEqual(dashboard_response.status_code, 200)
        self.assertContains(dashboard_response, "Duplicate Hits")
        self.assertContains(dashboard_response, "1 jobs seen again")
        self.assertEqual(detail_response.status_code, 200)
        self.assertContains(detail_response, "Last Crawl Update")
        self.assertContains(detail_response, "Last Duplicate Seen")
        self.assertContains(detail_response, ">1</dd>", html=True)

    def test_get_or_create_marks_existing_job_as_duplicate_hit(self):
        scraper = Scraper.objects.create(
            company="Existing Co",
            url="https://existing.example.com/jobs",
            code="print('ok')",
        )
        job = JobPosting.objects.create(
            scraper=scraper,
            title="Backend Engineer",
            location="Remote",
            date="2026-03-21",
            link="https://existing.example.com/jobs/1",
            description="Original row",
        )

        same_job, created = JobPosting.objects.get_or_create(
            scraper=scraper,
            link=job.link,
            defaults={
                "title": job.title,
                "location": job.location,
                "date": job.date,
                "description": job.description,
            },
        )

        self.assertFalse(created)
        self.assertEqual(same_job.pk, job.pk)
        self.assertEqual(same_job.duplicate_hit_count, 1)
        self.assertIsNotNone(same_job.last_duplicate_seen_at)
