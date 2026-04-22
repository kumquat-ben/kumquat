import json
import os
import subprocess
from datetime import datetime
from urllib.parse import urlencode, urlparse

import requests
from django.contrib import messages
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator
from django.db.models import Count, Max, Q, Subquery, Sum
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, redirect
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.http import require_http_methods
from django.views.generic import TemplateView, View
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from .mixins import SuperuserRequiredMixin
from .ai import generate_scraper_code
from .application_service import (
    queue_application_run,
    get_or_create_submission_manager_for_user,
    serialize_apply_service_payload,
)
from .forms import ScraperCreateForm
from .models import (
    ApplicantSiteCredential,
    JobApplicationRun,
    JobApplicationSubmissionManager,
    JobApplicationSubmissionRequest,
    JobPosting,
    ManualScriptQueue,
    ManualScriptRun,
    ManualScriptSourceURL,
    Scraper,
    ScraperRun,
)
from .tasks import (
    get_manual_script_controller,
    get_manual_script_queue_concurrency,
    schedule_all,
    schedule_manual_script,
    start_manual_script_queue,
    stop_manual_script_queue,
)
from .utils import (
    GENERIC_SUBMIT_SCRIPT_NAME,
    MANUAL_SCRIPTS_DIR,
    build_job_submission_requirements,
    deduplicate_job_postings,
    get_manual_script_source_url_stats,
    get_manual_scripts_overview,
    get_job_apply_url,
    get_submit_script_names,
    identify_submit_script_candidates,
    refresh_manual_script_source_url_cache,
    run_scraper,
)


@require_http_methods(["GET", "POST"])
@login_required
def create_scraper(request):
    if not request.user.is_superuser:
        return JsonResponse({"detail": "Forbidden"}, status=403)

    company = request.GET.get("company") or request.POST.get("company")
    url = request.GET.get("url") or request.POST.get("url")
    interval_param = request.GET.get("interval_hours") or request.POST.get("interval_hours")
    timeout_param = request.GET.get("timeout_seconds") or request.POST.get("timeout_seconds")
    active_param = request.GET.get("active") or request.POST.get("active")
    interval_hours = None
    timeout_seconds = Scraper._meta.get_field("timeout_seconds").default
    is_active = True

    if request.method == "POST" and (not company or not url):
        if request.content_type and "application/json" in request.content_type:
            try:
                payload = json.loads(request.body.decode() or "{}")
            except json.JSONDecodeError:
                return JsonResponse({"error": "Invalid JSON body"}, status=400)
            company = company or payload.get("company")
            url = url or payload.get("url")
            interval_param = interval_param or payload.get("interval_hours")
            timeout_param = timeout_param or payload.get("timeout_seconds")
            active_param = active_param or payload.get("active")

    if request.method == "GET" and not company and not url:
        return JsonResponse(
            {
                "instructions": (
                    "Provide company and url via query parameters (?company=...&url=...) "
                    "or POST JSON {'company': ..., 'url': ...}. Optional: interval_hours (>=1, default 24), "
                    "timeout_seconds (>=30, default 180), active (true/false)."
                ),
                "example": (
                    "/api/scrapers/create/?company=Example&url=https://example.com"
                    "&interval_hours=12&timeout_seconds=240"
                ),
            }
        )

    if not company or not url:
        return JsonResponse({"error": "company and url are required"}, status=400)

    if interval_param not in (None, ""):
        try:
            interval_hours = int(interval_param)
            if interval_hours < 1:
                raise ValueError
        except (TypeError, ValueError):
            return JsonResponse({"error": "interval_hours must be a positive integer"}, status=400)
    else:
        interval_hours = 24

    if timeout_param not in (None, ""):
        try:
            timeout_seconds = int(timeout_param)
            if timeout_seconds < 30:
                raise ValueError
        except (TypeError, ValueError):
            return JsonResponse({"error": "timeout_seconds must be an integer >= 30"}, status=400)

    if isinstance(active_param, str):
        is_active = active_param.lower() not in ("0", "false", "off")
    elif isinstance(active_param, bool):
        is_active = active_param
    else:
        is_active = True

    code = generate_scraper_code(url, company)
    scraper = Scraper.objects.create(
        company=company,
        url=url,
        code=code,
        interval_hours=interval_hours,
        timeout_seconds=timeout_seconds,
        active=is_active,
    )
    schedule_all()
    return JsonResponse({"id": scraper.id, "status": "saved"})


class ScraperManagementView(SuperuserRequiredMixin, TemplateView):
    """UI for operations staff to manage scraper scripts."""

    template_name = "scrapers/manage.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['scrapers'] = Scraper.objects.order_by("-created_at")
        context['recent_runs'] = ScraperRun.objects.select_related("scraper").order_by("-started_at")[:20]
        context.setdefault('create_form', ScraperCreateForm())
        return context

    def post(self, request, *args, **kwargs):
        action = request.POST.get("action", "create")
        if action == "create":
            form = ScraperCreateForm(request.POST)
            if form.is_valid():
                data = form.cleaned_data
                code = generate_scraper_code(data["url"], data["company"])
                scraper = Scraper.objects.create(
                    company=data["company"],
                    url=data["url"],
                    code=code,
                    interval_hours=data["interval_hours"],
                    timeout_seconds=data["timeout_seconds"],
                    active=data["active"],
                )
                schedule_all()
                messages.success(request, f"Scraper '{scraper.company}' created and scheduled every {scraper.interval_hours} hour(s).")
                return redirect("scrapers-manage")

            messages.error(request, "Please correct the errors below.")
            return self.render_to_response(self.get_context_data(create_form=form))

        scraper_id = request.POST.get("scraper_id")
        if not scraper_id:
            messages.error(request, "No scraper selected.")
            return redirect("scrapers-manage")

        try:
            scraper = Scraper.objects.get(id=scraper_id)
        except Scraper.DoesNotExist:
            messages.error(request, "Scraper not found.")
            return redirect("scrapers-manage")

        if action == "toggle":
            scraper.active = not scraper.active
            scraper.save(update_fields=["active"])
            schedule_all()
            state = "activated" if scraper.active else "paused"
            messages.success(request, f"Scraper '{scraper.company}' {state}.")
            return redirect("scrapers-manage")

        if action == "update":
            try:
                interval_hours = int(request.POST.get("interval_hours", scraper.interval_hours))
                if interval_hours < 1:
                    raise ValueError
                timeout_seconds = int(request.POST.get("timeout_seconds", scraper.timeout_seconds))
                if timeout_seconds < 30:
                    raise ValueError
            except (TypeError, ValueError):
                messages.error(request, "Interval must be >= 1 hour and timeout >= 30 seconds.")
                return redirect("scrapers-manage")

            scraper.interval_hours = interval_hours
            scraper.timeout_seconds = timeout_seconds
            scraper.save(update_fields=["interval_hours", "timeout_seconds"])
            schedule_all()
            messages.success(
                request,
                f"Scraper '{scraper.company}' interval updated to every {interval_hours} hour(s) with timeout {timeout_seconds}s.",
            )
            return redirect("scrapers-manage")

        if action == "run":
            result = run_scraper(scraper.id)
            run_id = result.get("run_id")
            if result.get("status") == "error":
                messages.error(
                    request,
                    f"Scraper '{scraper.company}' run #{run_id} failed: {result.get('error')}",
                )
            else:
                summary = result.get("summary", {})
                created = summary.get("created", 0)
                duplicates = summary.get("duplicates", 0)
                total = summary.get("total", 0)
                messages.success(
                    request,
                    f"Scraper '{scraper.company}' run #{run_id} stored {created} new job(s) "
                    f"(checked {total}, {duplicates} duplicates).",
                )
            return redirect("scrapers-manage")

        messages.error(request, "Unknown action.")
        return redirect("scrapers-manage")


class ScraperRunDetailView(SuperuserRequiredMixin, TemplateView):
    template_name = "scrapers/run_detail.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        run = get_object_or_404(
            ScraperRun.objects.select_related("scraper"),
            pk=kwargs["pk"],
        )
        context["run"] = run
        payload = run.payload if isinstance(run.payload, dict) else {}
        raw_logs = [entry for entry in payload.get("logs") or [] if isinstance(entry, dict)]
        context["summary"] = payload.get("summary") or {}
        context["logs"] = [
            {
                **entry,
                "dt": (
                    datetime.fromtimestamp(entry["timestamp"], tz=timezone.utc)
                    if isinstance(entry.get("timestamp"), (int, float))
                    else None
                ),
            }
            for entry in raw_logs
        ]
        context["stderr_output"] = payload.get("stderr") or ""
        context["payload_pretty"] = json.dumps(payload, indent=2, default=str) if payload else ""
        return context


class ManualScriptsView(SuperuserRequiredMixin, TemplateView):
    template_name = "scrapers/manual_scripts.html"

    @staticmethod
    def _kubernetes_api_request(method, path, payload=None):
        host = os.environ.get("KUBERNETES_SERVICE_HOST")
        port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
        token_path = "/var/run/secrets/kubernetes.io/serviceaccount/token"
        ca_path = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
        if not host or not os.path.exists(token_path):
            raise RuntimeError("Kubernetes in-cluster credentials are not available.")

        token = open(token_path, "r", encoding="utf-8").read().strip()
        url = f"https://{host}:{port}{path}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        if payload is not None:
            headers["Content-Type"] = "application/merge-patch+json"

        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            json=payload,
            timeout=10,
            verify=ca_path if os.path.exists(ca_path) else True,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"Kubernetes API request failed ({response.status_code}): {response.text[:300]}")
        return response.json() if response.content else {}

    @classmethod
    def _get_manual_worker_scale(cls):
        namespace = os.environ.get("MANUAL_SCRIPT_WORKER_NAMESPACE", "jobs")
        deployment = os.environ.get("MANUAL_SCRIPT_WORKER_DEPLOYMENT", "manual-script-worker")
        path = f"/apis/apps/v1/namespaces/{namespace}/deployments/{deployment}/scale"
        data = cls._kubernetes_api_request("GET", path)
        spec = data.get("spec") or {}
        status = data.get("status") or {}
        return {
            "desired_replicas": spec.get("replicas", 0),
            "current_replicas": status.get("replicas", 0),
            "available_replicas": status.get("availableReplicas", 0),
        }

    @classmethod
    def _set_manual_worker_scale(cls, replicas):
        namespace = os.environ.get("MANUAL_SCRIPT_WORKER_NAMESPACE", "jobs")
        deployment = os.environ.get("MANUAL_SCRIPT_WORKER_DEPLOYMENT", "manual-script-worker")
        path = f"/apis/apps/v1/namespaces/{namespace}/deployments/{deployment}/scale"
        cls._kubernetes_api_request("PATCH", path, payload={"spec": {"replicas": replicas}})

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        scripts = get_manual_scripts_overview()
        url_stats = get_manual_script_source_url_stats()
        crawl_job_stats = JobPosting.objects.aggregate(
            total_duplicate_hits=Sum("duplicate_hit_count"),
            jobs_with_duplicates=Count("id", filter=Q(duplicate_hit_count__gt=0)),
            last_job_crawl=Max("last_crawled_at"),
        )
        crawl_scripts = [script for script in scripts if script.get("group") == "crawl"]
        submit_scripts = [script for script in scripts if script.get("group") == "submit"]
        other_scripts = [script for script in scripts if script.get("group") not in {"crawl", "submit"}]
        runs_queryset = ManualScriptRun.objects.order_by("-scheduled_at", "-id")
        successful_runs = runs_queryset.filter(status=ManualScriptRun.Status.SUCCESS).count()
        failed_runs = runs_queryset.filter(status=ManualScriptRun.Status.ERROR).count()

        context["scripts"] = scripts
        context["crawl_scripts"] = crawl_scripts
        context["submit_scripts"] = submit_scripts
        context["other_scripts"] = other_scripts
        context["scripts_dir"] = str(MANUAL_SCRIPTS_DIR)
        controller = get_manual_script_controller()
        context["controller"] = controller
        context["active_queue"] = ManualScriptQueue.objects.filter(
            status__in=[ManualScriptQueue.Status.PENDING, ManualScriptQueue.Status.RUNNING]
        ).order_by("-created_at", "-id").first()
        context["recent_stopped_queue"] = ManualScriptQueue.objects.filter(
            status=ManualScriptQueue.Status.STOPPED
        ).order_by("-finished_at", "-id").first()
        context["queue_concurrency"] = get_manual_script_queue_concurrency()
        context["active_queue_running_count"] = (
            context["active_queue"].runs.filter(status=ManualScriptRun.Status.RUNNING).count()
            if context["active_queue"] else 0
        )
        try:
            context["worker_scale"] = self._get_manual_worker_scale()
            context["worker_scale_error"] = ""
        except Exception as exc:  # pragma: no cover - depends on cluster auth
            context["worker_scale"] = {
                "desired_replicas": controller.desired_worker_replicas,
                "current_replicas": None,
                "available_replicas": None,
            }
            context["worker_scale_error"] = str(exc)

        runs_paginator = Paginator(runs_queryset, 5)
        runs_page_number = self.request.GET.get("page")
        runs_page_obj = runs_paginator.get_page(runs_page_number)
        base_query_params = self.request.GET.copy()
        if "page" in base_query_params:
            base_query_params.pop("page")
        base_query_string = base_query_params.urlencode()

        context["summary"] = {
            "total_scripts": len(scripts),
            "crawl_scripts": len(crawl_scripts),
            "submit_scripts": len(submit_scripts),
            "other_scripts": len(other_scripts),
            "total_runs": runs_queryset.count(),
            "successful_runs": successful_runs,
            "failed_runs": failed_runs,
            "total_cached_urls": url_stats["total_urls"],
            "scripts_with_cached_urls": url_stats["scripts_with_urls"],
            "urls_last_refresh": url_stats["last_refresh"],
            "total_duplicate_hits": crawl_job_stats["total_duplicate_hits"] or 0,
            "jobs_with_duplicates": crawl_job_stats["jobs_with_duplicates"] or 0,
            "last_job_crawl": crawl_job_stats["last_job_crawl"],
        }
        context["runs_page_obj"] = runs_page_obj
        context["runs_paginator"] = runs_paginator
        context["runs"] = list(runs_page_obj.object_list)
        context["runs_page_size"] = runs_paginator.per_page
        context["runs_query_prefix"] = f"{base_query_string}&" if base_query_string else ""
        return context

    def post(self, request, *args, **kwargs):
        action = request.POST.get("action", "schedule")
        if action == "refresh-url-cache":
            try:
                summary = refresh_manual_script_source_url_cache()
                messages.success(
                    request,
                    f"Manual script URL cache refreshed for {summary['scripts']} scripts. Stored {summary['urls']} URLs.",
                )
            except Exception as exc:  # pragma: no cover - runtime handling
                messages.error(request, f"Failed to refresh manual script URL cache: {exc}")
            return redirect("scrapers-manual")

        if action in {"update-controller", "start-queue"}:
            try:
                desired_replicas = int(request.POST.get("desired_worker_replicas") or get_manual_script_controller().desired_worker_replicas or 1)
                desired_replicas = max(desired_replicas, 1)
                desired_concurrency = int(request.POST.get("queue_concurrency") or get_manual_script_controller().queue_concurrency or 1)
                desired_concurrency = max(desired_concurrency, 1)
                controller = get_manual_script_controller()
                controller.desired_worker_replicas = desired_replicas
                controller.queue_concurrency = desired_concurrency
                update_fields = ["desired_worker_replicas", "queue_concurrency", "updated_at"]
                if action == "start-queue":
                    controller.is_enabled = True
                    controller.last_started_at = timezone.now()
                    update_fields.extend(["is_enabled", "last_started_at"])
                controller.save(update_fields=update_fields)
                scale_message = ""
                try:
                    self._set_manual_worker_scale(desired_replicas)
                    scale_message = f" Worker deployment scaling requested to {desired_replicas} replica(s)."
                except Exception as exc:
                    scale_message = f" Worker deployment scaling could not be applied automatically: {exc}"
                if action == "start-queue":
                    queue = start_manual_script_queue()
                    messages.success(
                        request,
                        (
                            f"Background loop is running from queue #{queue.id}. "
                            f"It will keep cycling until stopped, using concurrency {desired_concurrency}."
                            f"{scale_message}"
                        ),
                    )
                else:
                    messages.success(
                        request,
                        (
                            f"Loop controller updated. "
                            f"Concurrency is now {desired_concurrency} and worker replicas are set to {desired_replicas}."
                            f"{scale_message}"
                        ),
                    )
            except FileNotFoundError:
                messages.error(request, "No manual scripts found.")
            except ValueError:
                messages.error(request, "Worker replica count and queue concurrency must be positive integers.")
            except Exception as exc:  # pragma: no cover - runtime handling
                messages.error(request, f"Failed to start background queue: {exc}")
            return redirect("scrapers-manual")

        if action == "stop-queue":
            try:
                stop_manual_script_queue()
                messages.success(
                    request,
                    "Background loop stop requested. Running scripts can finish, pending scripts were cancelled, and no new loop queue will be created.",
                )
            except Exception as exc:  # pragma: no cover - runtime handling
                messages.error(request, f"Failed to stop background queue: {exc}")
            return redirect("scrapers-manual")

        script_name = request.POST.get("script_name")
        if not script_name:
            messages.error(request, "No script selected.")
            return redirect("scrapers-manual")

        try:
            run_record = schedule_manual_script(script_name)
            messages.success(
                request,
                f"Manual script '{script_name}' scheduled (run id {run_record.id}). Check the run history below for status.",
            )
        except FileNotFoundError:
            messages.error(request, "Script not found.")
        except Exception as exc:  # pragma: no cover - runtime handling
            messages.error(request, f"Failed to schedule script '{script_name}': {exc}")

        return redirect("scrapers-manual")


class ManualScriptSourceURLListView(SuperuserRequiredMixin, TemplateView):
    template_name = "scrapers/manual_script_urls.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        selected_script = (self.request.GET.get("script") or "").strip()
        urls_queryset = ManualScriptSourceURL.objects.order_by("script_name", "source_name", "url")
        if selected_script:
            urls_queryset = urls_queryset.filter(script_name=selected_script)

        paginator = Paginator(urls_queryset, 100)
        page_obj = paginator.get_page(self.request.GET.get("page"))
        base_query_params = self.request.GET.copy()
        if "page" in base_query_params:
            base_query_params.pop("page")
        base_query_string = base_query_params.urlencode()

        context["selected_script"] = selected_script
        context["scripts"] = get_manual_scripts_overview()
        context["stats"] = get_manual_script_source_url_stats()
        context["page_obj"] = page_obj
        context["urls"] = list(page_obj.object_list)
        context["query_prefix"] = f"{base_query_string}&" if base_query_string else ""
        return context


class ManualScriptSourceURLApiView(View):
    """Public JSON API for cached manual-script source URLs."""

    default_page_size = 100
    max_page_size = 500

    def _parse_positive_int(self, raw_value, default):
        if raw_value in (None, ""):
            return default
        try:
            value = int(raw_value)
        except (TypeError, ValueError):
            raise ValueError("must be a positive integer")
        if value < 1:
            raise ValueError("must be a positive integer")
        return value

    def _build_page_url(self, request, page_number):
        params = request.GET.copy()
        params["page"] = page_number
        return request.build_absolute_uri(f"{request.path}?{params.urlencode()}")

    def get(self, request, *args, **kwargs):
        selected_script = (request.GET.get("script") or "").strip()

        try:
            page_number = self._parse_positive_int(request.GET.get("page"), 1)
            page_size = min(
                self._parse_positive_int(request.GET.get("page_size"), self.default_page_size),
                self.max_page_size,
            )
        except ValueError as exc:
            return JsonResponse({"detail": str(exc)}, status=400)

        urls_queryset = ManualScriptSourceURL.objects.order_by("script_name", "source_name", "url")
        if selected_script:
            urls_queryset = urls_queryset.filter(script_name=selected_script)

        paginator = Paginator(urls_queryset, page_size)
        page_obj = paginator.get_page(page_number)
        stats = get_manual_script_source_url_stats()

        return JsonResponse(
            {
                "count": paginator.count,
                "page": page_obj.number,
                "page_size": page_size,
                "num_pages": paginator.num_pages,
                "next": self._build_page_url(request, page_obj.next_page_number()) if page_obj.has_next() else None,
                "previous": self._build_page_url(request, page_obj.previous_page_number()) if page_obj.has_previous() else None,
                "filters": {
                    "script": selected_script or None,
                },
                "stats": {
                    "total_cached_urls": stats["total_urls"],
                    "scripts_with_cached_urls": stats["scripts_with_urls"],
                    "last_refresh": stats["last_refresh"].isoformat() if stats["last_refresh"] else None,
                },
                "results": [
                    {
                        "id": entry.id,
                        "script_name": entry.script_name,
                        "source_name": entry.source_name or "literal",
                        "url": entry.url,
                        "file_modified_at": entry.file_modified_at.isoformat(),
                        "created_at": entry.created_at.isoformat(),
                        "updated_at": entry.updated_at.isoformat(),
                    }
                    for entry in page_obj.object_list
                ],
                "docs_url": request.build_absolute_uri(reverse("jobs-api-docs")),
            }
        )


class JobPostingDeduplicationView(SuperuserRequiredMixin, TemplateView):
    """Superuser tool to inspect and trigger job posting deduplication runs."""

    template_name = "scrapers/job_deduplicate.html"
    preview_limit = 50
    per_group_preview_limit = 5

    @staticmethod
    def _bool_from_param(value):
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        if isinstance(value, (int, float)):
            return value != 0
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _resolve_scraper(identifier):
        if not identifier:
            return None, False
        try:
            return Scraper.objects.get(pk=int(identifier)), False
        except (ValueError, Scraper.DoesNotExist):
            return None, True

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        scraper_param = self.request.GET.get("scraper")
        active_scraper, invalid_scope = self._resolve_scraper(scraper_param)

        base_qs = JobPosting.objects.select_related("scraper")
        if active_scraper:
            base_qs = base_qs.filter(scraper=active_scraper)
        elif invalid_scope:
            messages.error(self.request, "Selected scraper not found; showing duplicates across all scrapers.")

        duplicate_links_qs = (
            base_qs.values("link")
            .annotate(total=Count("id"))
            .filter(total__gt=1)
        )

        total_groups = duplicate_links_qs.count()
        if total_groups:
            duplicate_links_subquery = Subquery(duplicate_links_qs.values("link"))
            total_rows = base_qs.filter(link__in=duplicate_links_subquery).count()
        else:
            total_rows = 0
        rows_to_remove = max(total_rows - total_groups, 0) if total_groups else 0

        preview_entries = list(
            duplicate_links_qs.order_by("-total", "link")[: self.preview_limit]
        )
        preview_links = [entry["link"] for entry in preview_entries]

        cluster_map = {}
        if preview_links:
            cluster_queryset = (
                base_qs.filter(link__in=preview_links)
                .select_related("scraper")
                .order_by("link", "-created_at", "-id")
            )
            for posting in cluster_queryset:
                bucket = cluster_map.setdefault(posting.link, [])
                if len(bucket) < self.per_group_preview_limit:
                    bucket.append(posting)

        preview_groups = []
        for entry in preview_entries:
            link = entry["link"]
            postings = cluster_map.get(link, [])
            keep_posting = postings[0] if postings else None
            duplicate_postings = postings[1:] if postings else []
            remaining = entry["total"] - len(postings)
            preview_groups.append(
                {
                    "link": link,
                    "total": entry["total"],
                    "keep": keep_posting,
                    "duplicates": duplicate_postings,
                    "remaining_count": remaining if remaining > 0 else 0,
                }
            )

        previewed_group_count = len(preview_groups)
        remaining_groups = max(total_groups - previewed_group_count, 0)

        context.update(
            {
                "scrapers": Scraper.objects.order_by("company"),
                "active_scraper": active_scraper,
                "duplicate_stats": {
                    "groups": total_groups,
                    "rows": total_rows,
                    "rows_to_remove": rows_to_remove,
                    "previewed_groups": previewed_group_count,
                    "remaining_groups": remaining_groups,
                },
                "duplicate_groups": preview_groups,
                "preview_limit": self.preview_limit,
                "per_group_preview_limit": self.per_group_preview_limit,
            }
        )
        return context

    def post(self, request, *args, **kwargs):
        scraper_param = request.POST.get("scraper")
        target_scraper, invalid_scope = self._resolve_scraper(scraper_param)

        if invalid_scope:
            messages.error(request, "Unable to deduplicate: selected scraper not found.")
            redirect_url = request.path
            if scraper_param:
                redirect_url = f"{redirect_url}?{urlencode({'scraper': scraper_param})}"
            return redirect(redirect_url)

        dry_run = self._bool_from_param(request.POST.get("dry_run"))
        summary = deduplicate_job_postings(scraper=target_scraper, dry_run=dry_run)
        scope_label = f"scraper '{target_scraper.company}'" if target_scraper else "all scrapers"

        if dry_run:
            messages.info(
                request,
                (
                    f"Dry run ({scope_label}): found {summary['duplicate_groups']} duplicate group(s); "
                    f"{summary['removed']} row(s) would be removed while keeping {summary['kept']} posting(s)."
                ),
            )
        else:
            messages.success(
                request,
                (
                    f"Deduplication complete ({scope_label}): removed {summary['removed']} row(s) "
                    f"across {summary['duplicate_groups']} group(s); kept {summary['kept']} posting(s)."
                ),
            )

        params = {}
        if target_scraper:
            params["scraper"] = str(target_scraper.id)

        redirect_url = request.path
        if params:
            redirect_url = f"{redirect_url}?{urlencode(params)}"
        return redirect(redirect_url)


class JobPostingListView(SuperuserRequiredMixin, TemplateView):
    template_name = "scrapers/job_list.html"
    paginate_by = 25

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        scraper_id = self.request.GET.get("scraper")
        queryset = JobPosting.objects.select_related("scraper").order_by("-last_crawled_at", "-created_at")

        if scraper_id:
            queryset = queryset.filter(scraper_id=scraper_id)
            context["active_scraper"] = get_object_or_404(Scraper, id=scraper_id)

        paginator = Paginator(queryset, self.paginate_by)
        page_number = self.request.GET.get("page")
        page_obj = paginator.get_page(page_number)

        context.update(
            {
                "page_obj": page_obj,
                "paginator": paginator,
                "job_postings": page_obj.object_list,
                "scrapers": Scraper.objects.order_by("company"),
            }
        )
        return context


class JobPostingDetailView(SuperuserRequiredMixin, TemplateView):
    template_name = "scrapers/job_detail.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        job_posting = get_object_or_404(JobPosting.objects.select_related("scraper"), pk=kwargs["pk"])
        metadata = job_posting.metadata if isinstance(job_posting.metadata, dict) else None
        context.update(
            {
                "job": job_posting,
                "metadata": metadata,
                "metadata_pretty": json.dumps(metadata, indent=2, ensure_ascii=False) if metadata else None,
            }
        )
        return context

class JobApplicationSubmissionManagerMixin:
    def _build_manager_defaults_for_user(self, job_posting, user):
        from .application_service import build_submission_manager_defaults

        return build_submission_manager_defaults(job_posting, user)

    def _get_or_create_manager_for_user(self, job_posting, user):
        return get_or_create_submission_manager_for_user(job_posting, user)

    @staticmethod
    def _serialize_apply_service_payload(job_posting, manager, request_record, created):
        return serialize_apply_service_payload(job_posting, manager, request_record, created)


class JobApplicationSubmissionManagerView(JobApplicationSubmissionManagerMixin, LoginRequiredMixin, TemplateView):
    template_name = "scrapers/job_apply_manager.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        job_posting = get_object_or_404(JobPosting.objects.select_related("scraper"), pk=kwargs["pk"])
        manager, existing_request, created = self._get_or_create_manager_for_user(job_posting, self.request.user)
        submit_script_names = get_submit_script_names()
        latest_run = manager.application_runs.filter(user=self.request.user).select_related("credential").first()
        saved_credential = ApplicantSiteCredential.objects.filter(
            user=self.request.user,
            apply_domain=manager.apply_domain,
        ).order_by("-updated_at", "-id").first()

        if created:
            if manager.status == JobApplicationSubmissionManager.Status.CANDIDATE_SCRIPT_FOUND:
                messages.success(
                    self.request,
                    f"Submission manager initialized for '{job_posting.title}'. Candidate submit script: {manager.matched_script_name}.",
                )
            else:
                messages.warning(
                    self.request,
                    (
                        f"Submission manager initialized for '{job_posting.title}'. "
                        "No submit script matched yet, so a design-time script requirement entry was recorded."
                    ),
                )

        context.update(
            {
                "job": job_posting,
                "manager": manager,
                "request_record": existing_request,
                "candidate_scripts": manager.candidate_script_names,
                "available_submit_scripts": submit_script_names,
                "latest_run": latest_run,
                "saved_credential": saved_credential,
                "browser_session_pending": (
                    manager.browser_session_status == JobApplicationSubmissionManager.BrowserSessionStatus.PENDING
                ),
            }
        )
        return context


class JobApplicationSubmissionServiceView(JobApplicationSubmissionManagerMixin, APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, pk: int):
        job_posting = get_object_or_404(JobPosting.objects.select_related("scraper"), pk=pk)
        manager, request_record, created = self._get_or_create_manager_for_user(job_posting, request.user)
        run_id = str(request.query_params.get("runId") or "").strip()
        latest_run_qs = manager.application_runs.filter(user=request.user).select_related("credential")
        if run_id:
            latest_run = latest_run_qs.filter(pk=run_id).first()
        else:
            latest_run = latest_run_qs.first()
        payload = serialize_apply_service_payload(job_posting, manager, request_record, created, run=latest_run)
        response_status = status.HTTP_201_CREATED if created else status.HTTP_200_OK
        return Response(payload, status=response_status)

    def post(self, request, pk: int):
        job_posting = get_object_or_404(JobPosting.objects.select_related("scraper"), pk=pk)
        action = str(request.data.get("action") or "prepare").strip().lower()
        verification_code = str(request.data.get("verificationCode") or "").strip()
        credential_input = {
            "username": str(request.data.get("siteUsername") or "").strip(),
            "password": str(request.data.get("sitePassword") or "").strip(),
            "loginUrl": str(request.data.get("loginUrl") or "").strip(),
        }

        if action in {"start", "resume", "continue"}:
            try:
                manager, request_record, created, run = queue_application_run(
                    job_posting=job_posting,
                    user=request.user,
                    verification_code=verification_code,
                    credential_input=credential_input,
                )
            except Exception as exc:
                return Response(
                    {
                        "detail": "The application run could not be queued.",
                        "error": str(exc),
                    },
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR,
                )
            payload = serialize_apply_service_payload(job_posting, manager, request_record, created, run=run)
            payload["queued"] = True
            payload["statusUrl"] = request.build_absolute_uri(f"{request.path}?runId={run.id}")
            return Response(payload, status=status.HTTP_202_ACCEPTED)

        manager, request_record, created = self._get_or_create_manager_for_user(job_posting, request.user)
        latest_run = manager.application_runs.filter(user=request.user).select_related("credential").first()
        payload = serialize_apply_service_payload(job_posting, manager, request_record, created, run=latest_run)
        response_status = status.HTTP_201_CREATED if created else status.HTTP_200_OK
        return Response(payload, status=response_status)


class ScraperCodeView(SuperuserRequiredMixin, TemplateView):
    template_name = "scrapers/script_detail.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        scraper = get_object_or_404(Scraper, pk=kwargs["pk"])
        context["scraper"] = scraper
        context["code"] = scraper.code
        return context
