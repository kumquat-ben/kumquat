from django.contrib import admin, messages
from django.http import HttpRequest, HttpResponseRedirect
from django.shortcuts import get_object_or_404
from django.urls import path, reverse
from django.utils import timezone
from django.utils.html import format_html

from .models import ApplicantSiteCredential, JobApplicationRun, JobApplicationSubmissionManager, JobApplicationSubmissionRequest
from .tasks import dispatch_pending_job_application_runs


@admin.register(ApplicantSiteCredential)
class ApplicantSiteCredentialAdmin(admin.ModelAdmin):
    list_display = ("user", "apply_domain", "username", "last_used_at", "updated_at")
    search_fields = ("user__username", "user__email", "apply_domain", "username")
    readonly_fields = ("created_at", "updated_at", "last_used_at")


@admin.register(JobApplicationSubmissionManager)
class JobApplicationSubmissionManagerAdmin(admin.ModelAdmin):
    list_display = ("job", "status", "matched_script_name", "apply_domain", "request_count", "updated_at")
    list_filter = ("status", "browser_session_status")
    search_fields = ("job__title", "job__scraper__company", "matched_script_name", "apply_domain")
    readonly_fields = ("created_at", "updated_at")


@admin.register(JobApplicationSubmissionRequest)
class JobApplicationSubmissionRequestAdmin(admin.ModelAdmin):
    list_display = ("job", "user", "status", "created_at")
    list_filter = ("status",)
    search_fields = ("job__title", "job__scraper__company", "user__username", "user__email")
    readonly_fields = ("created_at", "updated_at")


@admin.register(JobApplicationRun)
class JobApplicationRunAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "job",
        "user",
        "status",
        "apply_method",
        "current_step",
        "reprocess_count",
        "reviewed_at",
        "action_links",
        "updated_at",
    )
    list_filter = ("status", "apply_method", "current_step", "apply_domain")
    search_fields = (
        "job__title",
        "job__scraper__company",
        "user__username",
        "user__email",
        "selected_script_name",
        "apply_domain",
    )
    readonly_fields = (
        "created_at",
        "updated_at",
        "submitted_at",
        "reviewed_at",
        "reviewed_by",
        "action_links",
    )

    def get_urls(self):
        return [
            path(
                "<int:run_id>/reprocess/",
                self.admin_site.admin_view(self.reprocess_view),
                name="scrapers_jobapplicationrun_reprocess",
            ),
            path(
                "<int:run_id>/mark-failed/",
                self.admin_site.admin_view(self.mark_failed_view),
                name="scrapers_jobapplicationrun_mark_failed",
            ),
        ] + super().get_urls()

    @admin.display(description="Actions")
    def action_links(self, obj: JobApplicationRun) -> str:
        reprocess_url = reverse("admin:scrapers_jobapplicationrun_reprocess", args=[obj.pk])
        mark_failed_url = reverse("admin:scrapers_jobapplicationrun_mark_failed", args=[obj.pk])
        return format_html(
            '<a class="button" href="{}">Reprocess</a>&nbsp;<a class="button" href="{}">Mark Failed</a>',
            reprocess_url,
            mark_failed_url,
        )

    def reprocess_view(self, request: HttpRequest, run_id: int):
        run = get_object_or_404(JobApplicationRun, pk=run_id)
        runtime_state = dict(run.runtime_state or {})
        runtime_state["last_reprocess_requested_at"] = timezone.now().isoformat()
        runtime_state["last_reprocess_requested_by"] = request.user.get_username()
        run.runtime_state = runtime_state
        run.status = JobApplicationRun.Status.PENDING
        run.current_step = "queued"
        run.review_notes = ""
        run.last_error = ""
        run.reviewed_at = None
        run.reviewed_by = None
        run.reprocess_count += 1
        run.save(
            update_fields=[
                "runtime_state",
                "status",
                "current_step",
                "review_notes",
                "last_error",
                "reviewed_at",
                "reviewed_by",
                "reprocess_count",
                "updated_at",
            ]
        )
        dispatch_pending_job_application_runs()
        self.message_user(request, f"Application run {run.pk} was re-queued for processing.", level=messages.SUCCESS)
        return HttpResponseRedirect(request.META.get("HTTP_REFERER") or reverse("admin:scrapers_jobapplicationrun_changelist"))

    def mark_failed_view(self, request: HttpRequest, run_id: int):
        run = get_object_or_404(JobApplicationRun, pk=run_id)
        note = run.review_notes or run.last_error or "Marked failed by systems review."
        run.status = JobApplicationRun.Status.FAILED
        run.current_step = "closed_as_failed"
        run.review_notes = note
        run.reviewed_at = timezone.now()
        run.reviewed_by = request.user
        run.save(update_fields=["status", "current_step", "review_notes", "reviewed_at", "reviewed_by", "updated_at"])
        self.message_user(request, f"Application run {run.pk} was marked as failed.", level=messages.WARNING)
        return HttpResponseRedirect(request.META.get("HTTP_REFERER") or reverse("admin:scrapers_jobapplicationrun_changelist"))
