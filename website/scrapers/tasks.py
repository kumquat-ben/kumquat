import atexit

from apscheduler.schedulers.background import BackgroundScheduler
from django.conf import settings
from django.db import transaction
from django.db.utils import OperationalError, ProgrammingError
from django.utils import timezone

from .application_service import process_application_run
from .models import JobApplicationRun, ManualScriptController, ManualScriptQueue, ManualScriptRun, Scraper, ScraperRun
from .utils import get_manual_scripts_overview, run_manual_script, run_scraper

scheduler = BackgroundScheduler()
_scheduler_started = False
_scheduler_stopping = False
MANUAL_SCRIPT_QUEUE_DISPATCH_JOB_ID = "manual-script-queue-dispatch"
MANUAL_SCRIPT_RUN_DISPATCH_JOB_ID = "manual-script-run-dispatch"
JOB_APPLICATION_RUN_DISPATCH_JOB_ID = "job-application-run-dispatch"


def manual_script_scheduler_enabled() -> bool:
    return bool(getattr(settings, "ENABLE_MANUAL_SCRIPT_SCHEDULER", True))


def scraper_scheduler_enabled() -> bool:
    return bool(getattr(settings, "ENABLE_SCRAPER_SCHEDULER", True))


def get_manual_script_queue_concurrency() -> int:
    try:
        controller = get_manual_script_controller()
        return max(int(controller.queue_concurrency), 1)
    except Exception:
        return max(int(getattr(settings, "MANUAL_SCRIPT_QUEUE_CONCURRENCY", 3)), 1)


def get_manual_script_queue_poll_seconds() -> int:
    return max(int(getattr(settings, "MANUAL_SCRIPT_QUEUE_POLL_SECONDS", 2)), 1)


def get_job_application_queue_concurrency() -> int:
    return max(int(getattr(settings, "JOB_APPLICATION_QUEUE_CONCURRENCY", 2)), 1)


def get_job_application_queue_poll_seconds() -> int:
    return max(int(getattr(settings, "JOB_APPLICATION_QUEUE_POLL_SECONDS", 2)), 1)


def get_manual_script_controller() -> ManualScriptController:
    controller, _ = ManualScriptController.objects.get_or_create(
        pk=1,
        defaults={
            "is_enabled": False,
            "loop_mode": True,
            "queue_concurrency": max(int(getattr(settings, "MANUAL_SCRIPT_QUEUE_CONCURRENCY", 2)), 1),
            "desired_worker_replicas": 1,
        },
    )
    return controller


def _scheduler_available() -> bool:
    return _scheduler_started and scheduler.running and not _scheduler_stopping


def shutdown_scheduler() -> None:
    global _scheduler_stopping, _scheduler_started

    if _scheduler_stopping or not _scheduler_started:
        return

    _scheduler_stopping = True
    try:
        if scheduler.running:
            scheduler.shutdown(wait=False)
    except RuntimeError:
        pass
    finally:
        _scheduler_started = False


atexit.register(shutdown_scheduler)


def schedule_all() -> None:
    if not _scheduler_available():
        return

    try:
        scheduler.remove_all_jobs()
        if scraper_scheduler_enabled():
            for scraper in Scraper.objects.filter(active=True):
                interval = max(scraper.interval_hours, 1)
                scheduler.add_job(
                    run_scraper,
                    "interval",
                    hours=interval,
                    args=[scraper.id],
                    kwargs={"triggered_by": ScraperRun.Trigger.SCHEDULER},
                    id=f"scraper-{scraper.id}",
                    replace_existing=True,
                )
        if manual_script_scheduler_enabled():
            scheduler.add_job(
                dispatch_pending_manual_script_runs,
                "interval",
                seconds=get_manual_script_queue_poll_seconds(),
                id=MANUAL_SCRIPT_RUN_DISPATCH_JOB_ID,
                replace_existing=True,
                max_instances=1,
            )
            scheduler.add_job(
                dispatch_manual_script_queue,
                "interval",
                seconds=get_manual_script_queue_poll_seconds(),
                id=MANUAL_SCRIPT_QUEUE_DISPATCH_JOB_ID,
                replace_existing=True,
                max_instances=1,
            )
            scheduler.add_job(
                dispatch_pending_job_application_runs,
                "interval",
                seconds=get_job_application_queue_poll_seconds(),
                id=JOB_APPLICATION_RUN_DISPATCH_JOB_ID,
                replace_existing=True,
                max_instances=1,
            )
    except (ProgrammingError, OperationalError, RuntimeError):
        # Database tables might not be ready yet (e.g. during migrate).
        return


def start_scheduler() -> None:
    global _scheduler_started, _scheduler_stopping

    if _scheduler_started:
        return

    _scheduler_stopping = False
    scheduler.start()
    _scheduler_started = True
    recover_manual_script_queue_state()
    schedule_all()
    dispatch_manual_script_queue()
    dispatch_pending_job_application_runs()


def schedule_manual_script(script_name: str) -> ManualScriptRun:
    run_record = ManualScriptRun.objects.create(script_name=script_name)
    if not _scheduler_available() or not manual_script_scheduler_enabled():
        return run_record
    dispatch_pending_manual_script_runs()
    return run_record


def _create_manual_script_queue() -> ManualScriptQueue:
    scripts = get_manual_scripts_overview()
    if not scripts:
        raise FileNotFoundError("No manual scripts found.")

    queue = ManualScriptQueue.objects.create(
        status=ManualScriptQueue.Status.PENDING,
        total_scripts=len(scripts),
    )
    pending_runs = [
        ManualScriptRun(
            script_name=script["name"],
            queue=queue,
            queue_position=index,
        )
        for index, script in enumerate(scripts, start=1)
    ]
    ManualScriptRun.objects.bulk_create(pending_runs)
    return queue


def start_manual_script_queue() -> ManualScriptQueue:
    controller = get_manual_script_controller()
    if not controller.is_enabled:
        controller.is_enabled = True
        controller.last_started_at = timezone.now()
        controller.save(update_fields=["is_enabled", "last_started_at", "updated_at"])

    active_queue = ManualScriptQueue.objects.filter(
        status__in=[ManualScriptQueue.Status.PENDING, ManualScriptQueue.Status.RUNNING]
    ).order_by("-created_at", "-id").first()
    if active_queue:
        return active_queue

    queue = _create_manual_script_queue()
    dispatch_manual_script_queue()
    return queue


def stop_manual_script_queue() -> None:
    controller = get_manual_script_controller()
    controller.is_enabled = False
    controller.last_stopped_at = timezone.now()
    controller.save(update_fields=["is_enabled", "last_stopped_at", "updated_at"])

    active_queue = ManualScriptQueue.objects.filter(
        status__in=[ManualScriptQueue.Status.PENDING, ManualScriptQueue.Status.RUNNING]
    ).order_by("-created_at", "-id").first()
    if not active_queue:
        return

    with transaction.atomic():
        active_queue = ManualScriptQueue.objects.select_for_update().get(pk=active_queue.pk)
        cancelled_count = active_queue.runs.filter(status=ManualScriptRun.Status.PENDING).update(
            status=ManualScriptRun.Status.CANCELLED,
            finished_at=timezone.now(),
            error="Stopped by operator.",
        )
        running_count = active_queue.runs.filter(status=ManualScriptRun.Status.RUNNING).count()
        active_queue.completed_scripts = active_queue.runs.filter(
            status__in=[
                ManualScriptRun.Status.SUCCESS,
                ManualScriptRun.Status.ERROR,
                ManualScriptRun.Status.CANCELLED,
            ]
        ).count()
        active_queue.current_script_name = ""
        active_queue.error = "Stopped by operator."
        if running_count == 0:
            active_queue.status = ManualScriptQueue.Status.STOPPED
            active_queue.finished_at = timezone.now()
            active_queue.save(
                update_fields=["completed_scripts", "current_script_name", "error", "status", "finished_at"]
            )
        else:
            active_queue.status = ManualScriptQueue.Status.STOPPED
            active_queue.save(update_fields=["completed_scripts", "current_script_name", "error", "status"])


def dispatch_pending_manual_script_runs() -> None:
    if not _scheduler_available() or not manual_script_scheduler_enabled():
        return

    try:
        jobs_to_schedule = []
        with transaction.atomic():
            pending_runs = list(
                ManualScriptRun.objects.select_for_update()
                .filter(queue__isnull=True, status=ManualScriptRun.Status.PENDING)
                .order_by("scheduled_at", "id")[: get_manual_script_queue_concurrency()]
            )
            dispatch_time = timezone.now()
            for pending_run in pending_runs:
                pending_run.status = ManualScriptRun.Status.RUNNING
                pending_run.started_at = dispatch_time
                pending_run.save(update_fields=["status", "started_at"])
                jobs_to_schedule.append((pending_run.id, pending_run.script_name))

        for run_id, script_name in jobs_to_schedule:
            try:
                scheduler.add_job(
                    run_manual_script,
                    "date",
                    args=[script_name],
                    kwargs={"run_id": run_id},
                    id=f"manual-script-{run_id}",
                    replace_existing=False,
                )
            except Exception:
                ManualScriptRun.objects.filter(pk=run_id).update(status=ManualScriptRun.Status.PENDING, started_at=None)
                raise
    except (ProgrammingError, OperationalError, RuntimeError):
        return


def dispatch_manual_script_queue() -> None:
    if not _scheduler_available() or not manual_script_scheduler_enabled():
        return

    try:
        jobs_to_schedule = []
        controller = get_manual_script_controller()
        with transaction.atomic():
            queue = ManualScriptQueue.objects.select_for_update().filter(
                status__in=[ManualScriptQueue.Status.PENDING, ManualScriptQueue.Status.RUNNING]
            ).order_by("created_at", "id").first()
            if not queue and controller.is_enabled and controller.loop_mode:
                queue = _create_manual_script_queue()
            if not queue:
                return

            running_qs = queue.runs.filter(status=ManualScriptRun.Status.RUNNING).order_by("queue_position", "id")
            running_count = running_qs.count()
            completed_count = queue.runs.filter(
                status__in=[ManualScriptRun.Status.SUCCESS, ManualScriptRun.Status.ERROR]
            ).count()
            pending_qs = queue.runs.filter(status=ManualScriptRun.Status.PENDING).order_by("queue_position", "id")
            pending_count = pending_qs.count()

            if running_count == 0 and pending_count == 0:
                last_error_run = queue.runs.filter(status=ManualScriptRun.Status.ERROR).order_by("-finished_at", "-id").first()
                queue.status = ManualScriptQueue.Status.ERROR if queue.runs.filter(status=ManualScriptRun.Status.ERROR).exists() else ManualScriptQueue.Status.SUCCESS
                queue.current_script_name = ""
                queue.completed_scripts = completed_count
                queue.finished_at = timezone.now()
                queue.error = last_error_run.error if last_error_run else ""
                queue.save(update_fields=["status", "current_script_name", "completed_scripts", "finished_at", "error"])
                return

            queue.started_at = queue.started_at or timezone.now()
            queue.status = ManualScriptQueue.Status.RUNNING
            queue.current_script_name = running_qs.first().script_name if running_count else ""
            queue.completed_scripts = completed_count
            queue.save(update_fields=["started_at", "status", "current_script_name", "completed_scripts"])

            available_slots = max(get_manual_script_queue_concurrency() - running_count, 0)
            if available_slots == 0:
                return

            next_runs = list(pending_qs[:available_slots])
            dispatch_time = timezone.now()
            for next_run in next_runs:
                next_run.status = ManualScriptRun.Status.RUNNING
                next_run.started_at = dispatch_time
                next_run.save(update_fields=["status", "started_at"])
                jobs_to_schedule.append((next_run.id, next_run.script_name))

            if not running_count and next_runs:
                queue.current_script_name = next_runs[0].script_name
                queue.save(update_fields=["current_script_name"])

        for run_id, script_name in jobs_to_schedule:
            try:
                scheduler.add_job(
                    run_manual_script,
                    "date",
                    args=[script_name],
                    kwargs={"run_id": run_id},
                    id=f"manual-script-{run_id}",
                    replace_existing=False,
                )
            except Exception:
                ManualScriptRun.objects.filter(pk=run_id).update(status=ManualScriptRun.Status.PENDING, started_at=None)
                raise
    except (ProgrammingError, OperationalError, RuntimeError):
        return


def run_job_application(run_id: int) -> None:
    try:
        run = JobApplicationRun.objects.select_related("user", "manager", "credential").get(pk=run_id)
    except JobApplicationRun.DoesNotExist:
        return
    process_application_run(run)


def dispatch_pending_job_application_runs() -> None:
    if not _scheduler_available() or not manual_script_scheduler_enabled():
        return

    try:
        jobs_to_schedule = []
        with transaction.atomic():
            pending_runs = list(
                JobApplicationRun.objects.select_for_update()
                .filter(status=JobApplicationRun.Status.PENDING)
                .order_by("created_at", "id")[: get_job_application_queue_concurrency()]
            )
            dispatch_time = timezone.now()
            for run in pending_runs:
                run.status = JobApplicationRun.Status.RUNNING
                runtime_state = dict(run.runtime_state or {})
                runtime_state["queued_at"] = runtime_state.get("queued_at") or dispatch_time.isoformat()
                run.runtime_state = runtime_state
                run.current_step = "automation_running"
                run.save(update_fields=["status", "current_step", "runtime_state", "updated_at"])
                jobs_to_schedule.append(run.id)

        for run_id in jobs_to_schedule:
            try:
                scheduler.add_job(
                    run_job_application,
                    "date",
                    args=[run_id],
                    id=f"job-application-{run_id}",
                    replace_existing=False,
                )
            except Exception:
                JobApplicationRun.objects.filter(pk=run_id).update(
                    status=JobApplicationRun.Status.PENDING,
                    current_step="queued",
                )
                raise
    except (ProgrammingError, OperationalError, RuntimeError):
        return


def recover_manual_script_queue_state() -> None:
    try:
        scheduled_job_ids = {job.id for job in scheduler.get_jobs()}
        interrupted_runs = list(
            ManualScriptRun.objects.filter(status=ManualScriptRun.Status.RUNNING)
        )
        recovered_queue_ids = set()

        for run in interrupted_runs:
            if f"manual-script-{run.id}" in scheduled_job_ids:
                continue

            if run.queue_id:
                recovered_queue_ids.add(run.queue_id)
            run.status = ManualScriptRun.Status.PENDING
            run.started_at = None
            run.finished_at = None
            run.output = ""
            run.error = ""
            run.save(update_fields=["status", "started_at", "finished_at", "output", "error"])

        for queue_id in recovered_queue_ids:
            if not queue_id:
                continue
            ManualScriptQueue.objects.filter(pk=queue_id).update(
                status=ManualScriptQueue.Status.PENDING,
                current_script_name="",
                finished_at=None,
                error="",
            )
    except (ProgrammingError, OperationalError):
        return
