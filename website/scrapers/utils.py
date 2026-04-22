import ast
import json
import os
import re
import select
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

from django.conf import settings
from django.db import transaction
from django.db.models import Count
from django.utils import timezone

from .models import JobPosting, ManualScriptQueue, ManualScriptRun, ManualScriptSourceURL, Scraper, ScraperRun

MANUAL_SCRIPTS_DIR = Path(settings.BASE_DIR) / "manual_scripts"
MANUAL_SCRIPTS_DIR.mkdir(parents=True, exist_ok=True)
URL_LITERAL_RE = re.compile(r"https?://[^\s'\"<>]+")
URL_NAME_HINT_RE = re.compile(r"(?:^|_)(?:url|urls|endpoint|base|root|host|api)(?:$|_)", re.IGNORECASE)
GENERIC_SUBMIT_SCRIPT_NAME = "submit/generic_form_submit.py"


def _iter_manual_script_paths() -> List[Path]:
    return sorted(
        path
        for path in MANUAL_SCRIPTS_DIR.rglob("*.py")
        if path.is_file() and "__pycache__" not in path.parts
    )


def _manual_script_name(path: Path) -> str:
    return path.relative_to(MANUAL_SCRIPTS_DIR).as_posix()


def _manual_script_group(script_name: str) -> str:
    parts = Path(script_name).parts
    if len(parts) > 1:
        return parts[0]
    return "crawl"


def get_manual_script_path(script_name: str) -> Path:
    script_path = (MANUAL_SCRIPTS_DIR / script_name).resolve()
    if not script_path.is_file() or MANUAL_SCRIPTS_DIR.resolve() not in script_path.parents:
        raise FileNotFoundError("Manual script not found or outside allowed directory.")
    return script_path


def get_submit_script_names() -> List[str]:
    submit_scripts: List[str] = []
    for path in _iter_manual_script_paths():
        script_name = _manual_script_name(path)
        if _manual_script_group(script_name) == "submit":
            submit_scripts.append(script_name)
    return submit_scripts


def get_job_apply_url(job_posting: JobPosting) -> str:
    metadata = job_posting.metadata if isinstance(job_posting.metadata, dict) else {}
    candidate_values = [
        metadata.get("apply_url"),
        metadata.get("custom_apply_link"),
        metadata.get("external_url"),
        metadata.get("externalUrl"),
    ]
    apply_links = metadata.get("apply_links")
    if isinstance(apply_links, list):
        candidate_values.extend(apply_links)
    candidate_values.append(job_posting.link)

    for candidate in candidate_values:
        if isinstance(candidate, str) and candidate.strip().startswith(("http://", "https://")):
            return candidate.strip()
    return ""


def _tokenize_script_match_value(value: str) -> List[str]:
    if not value:
        return []
    return [token for token in re.split(r"[^a-z0-9]+", value.lower()) if len(token) >= 3]


def identify_submit_script_candidates(job_posting: JobPosting) -> List[str]:
    submit_script_names = get_submit_script_names()
    if not submit_script_names:
        return []

    apply_url = get_job_apply_url(job_posting)
    parsed_apply = urlparse(apply_url) if apply_url else None
    token_sources = [
        job_posting.scraper.company,
        job_posting.title,
        parsed_apply.netloc if parsed_apply else "",
        parsed_apply.path if parsed_apply else "",
    ]
    match_tokens = []
    for source in token_sources:
        match_tokens.extend(_tokenize_script_match_value(source))
    match_tokens = list(dict.fromkeys(match_tokens))

    scored_matches = []
    company_slug = "_".join(_tokenize_script_match_value(job_posting.scraper.company))
    for script_name in submit_script_names:
        script_lower = script_name.lower()
        score = 0
        if company_slug and company_slug in script_lower:
            score += 10
        for token in match_tokens:
            if token in script_lower:
                score += 2
        if score > 0:
            scored_matches.append((score, script_name))

    scored_matches.sort(key=lambda item: (-item[0], item[1]))
    candidates = [script_name for _, script_name in scored_matches[:5]]
    if GENERIC_SUBMIT_SCRIPT_NAME in submit_script_names and GENERIC_SUBMIT_SCRIPT_NAME not in candidates:
        candidates.append(GENERIC_SUBMIT_SCRIPT_NAME)
    return candidates[:5]


def build_job_submission_requirements(job_posting: JobPosting) -> Dict[str, Any]:
    metadata = job_posting.metadata if isinstance(job_posting.metadata, dict) else {}
    interesting_metadata = {}
    for key, value in metadata.items():
        lowered = key.lower()
        if any(
            marker in lowered
            for marker in ("apply", "form", "resume", "cover", "email", "phone", "external", "question")
        ):
            interesting_metadata[key] = value

    return {
        "job_title": job_posting.title,
        "company": job_posting.scraper.company,
        "location": job_posting.location,
        "job_link": job_posting.link,
        "apply_url": get_job_apply_url(job_posting),
        "description_excerpt": (job_posting.description or "")[:500],
        "metadata_signals": interesting_metadata,
    }


def persist_job_results(scraper: Scraper, payload: Dict[str, Any]) -> Dict[str, int]:
    jobs = payload.get("jobs") or []
    created = 0
    duplicates = 0
    truncated = 0

    for job in jobs:
        link = job.get("link")
        title = (job.get("title") or "").strip()
        if not link or not title:
            truncated += 1
            continue

        defaults = {
            "title": title[:255],
            "location": (job.get("location") or "")[:255],
            "date": (job.get("date") or "")[:100],
            "description": (job.get("description") or "")[:10000],
        }

        _, created_flag = JobPosting.objects.get_or_create(
            scraper=scraper,
            link=link,
            defaults=defaults,
        )

        if created_flag:
            created += 1
        else:
            duplicates += 1

    return {
        "total": len(jobs),
        "created": created,
        "duplicates": duplicates,
        "invalid": truncated,
    }


def get_manual_scripts_overview() -> List[Dict[str, Any]]:
    scripts: List[Dict[str, Any]] = []
    script_names: List[str] = []

    for path in _iter_manual_script_paths():
        stats = path.stat()
        script_name = _manual_script_name(path)
        script_names.append(script_name)
        scripts.append(
            {
                "name": script_name,
                "display_name": path.name,
                "group": _manual_script_group(script_name),
                "size": stats.st_size,
                "modified": datetime.fromtimestamp(stats.st_mtime),
                "last_run": None,
                "last_run_status": None,
                "last_run_status_code": None,
            }
        )

    if script_names:
        latest_runs: Dict[str, ManualScriptRun] = {}
        latest_run_queryset = (
            ManualScriptRun.objects.filter(script_name__in=script_names)
            .order_by("script_name", "-scheduled_at", "-id")
        )
        for run in latest_run_queryset:
            latest_runs.setdefault(run.script_name, run)

        for script in scripts:
            last_run_record = latest_runs.get(script["name"])
            if not last_run_record:
                continue

            last_run_timestamp = (
                last_run_record.finished_at
                or last_run_record.started_at
                or last_run_record.scheduled_at
            )
            script["last_run"] = last_run_timestamp
            script["last_run_status"] = last_run_record.get_status_display()
            script["last_run_status_code"] = last_run_record.status

    def script_sort_key(entry: Dict[str, Any]) -> Any:
        last_run = entry["last_run"]
        if last_run is None:
            return (0, datetime.min, entry["modified"], entry["name"])
        if timezone.is_aware(last_run):
            last_run = timezone.make_naive(last_run)
        return (1, last_run, entry["modified"], entry["name"])

    scripts.sort(key=script_sort_key)
    return scripts


def _extract_urls_from_text(text: str) -> List[str]:
    urls: List[str] = []
    seen = set()
    for match in URL_LITERAL_RE.findall(text or ""):
        cleaned = match.rstrip(").,;")
        if cleaned in seen:
            continue
        seen.add(cleaned)
        urls.append(cleaned)
    return urls


def _resolve_manual_script_ast_value(node: ast.AST, resolved: Dict[str, Any]) -> Any:
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        return resolved.get(node.id)
    if isinstance(node, ast.JoinedStr):
        parts: List[str] = []
        for value in node.values:
            resolved_value = _resolve_manual_script_ast_value(value, resolved)
            if resolved_value is None:
                return None
            parts.append(str(resolved_value))
        return "".join(parts)
    if isinstance(node, ast.FormattedValue):
        return _resolve_manual_script_ast_value(node.value, resolved)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _resolve_manual_script_ast_value(node.left, resolved)
        right = _resolve_manual_script_ast_value(node.right, resolved)
        if left is None or right is None:
            return None
        if isinstance(left, str) and isinstance(right, str):
            return left + right
        return None
    if isinstance(node, ast.Call):
        func_name = None
        if isinstance(node.func, ast.Name):
            func_name = node.func.id
        elif isinstance(node.func, ast.Attribute):
            func_name = node.func.attr
        if func_name == "urljoin" and len(node.args) >= 2:
            base = _resolve_manual_script_ast_value(node.args[0], resolved)
            target = _resolve_manual_script_ast_value(node.args[1], resolved)
            if isinstance(base, str) and isinstance(target, str):
                return urljoin(base, target)
        return None
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        values = []
        for item in node.elts:
            resolved_value = _resolve_manual_script_ast_value(item, resolved)
            if resolved_value is None:
                return None
            values.append(resolved_value)
        return values
    if isinstance(node, ast.Dict):
        data = {}
        for key_node, value_node in zip(node.keys, node.values):
            key = _resolve_manual_script_ast_value(key_node, resolved)
            value = _resolve_manual_script_ast_value(value_node, resolved)
            if key is None or value is None:
                return None
            data[key] = value
        return data
    return None


def parse_manual_script_urls(path: Path) -> List[Dict[str, str]]:
    try:
        source = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        source = path.read_text(encoding="latin-1")

    discovered: Dict[str, Dict[str, str]] = {}

    for url in _extract_urls_from_text(source):
        discovered.setdefault(url, {"source_name": "literal", "url": url})

    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError:
        return sorted(discovered.values(), key=lambda entry: (entry["url"], entry["source_name"]))

    resolved: Dict[str, Any] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            value = _resolve_manual_script_ast_value(node.value, resolved)
            targets = [target.id for target in node.targets if isinstance(target, ast.Name)]
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            value = _resolve_manual_script_ast_value(node.value, resolved) if node.value else None
            targets = [node.target.id]
        else:
            continue

        for target_name in targets:
            if value is not None:
                resolved[target_name] = value

            candidate_values = value if isinstance(value, list) else [value]
            for candidate in candidate_values:
                if not isinstance(candidate, str):
                    continue
                urls = _extract_urls_from_text(candidate)
                if not urls and URL_NAME_HINT_RE.search(target_name):
                    continue
                for url in urls:
                    discovered[url] = {"source_name": target_name, "url": url}

    return sorted(discovered.values(), key=lambda entry: (entry["url"], entry["source_name"]))


def refresh_manual_script_source_url_cache() -> Dict[str, int]:
    script_paths = _iter_manual_script_paths()
    created_rows = 0
    deleted_rows = 0
    script_count = 0

    with transaction.atomic():
        current_script_names = [_manual_script_name(path) for path in script_paths]
        stale_qs = ManualScriptSourceURL.objects.exclude(script_name__in=current_script_names)
        deleted_rows += stale_qs.count()
        stale_qs.delete()

        for path in script_paths:
            script_count += 1
            script_name = _manual_script_name(path)
            modified_at = timezone.make_aware(datetime.fromtimestamp(path.stat().st_mtime))
            parsed_rows = [
                ManualScriptSourceURL(
                    script_name=script_name,
                    source_name=entry["source_name"],
                    url=entry["url"],
                    url_digest=ManualScriptSourceURL.build_url_digest(entry["url"]),
                    file_modified_at=modified_at,
                )
                for entry in parse_manual_script_urls(path)
            ]
            existing_qs = ManualScriptSourceURL.objects.filter(script_name=script_name)
            deleted_rows += existing_qs.count()
            existing_qs.delete()
            if parsed_rows:
                ManualScriptSourceURL.objects.bulk_create(parsed_rows)
                created_rows += len(parsed_rows)

    return {
        "scripts": script_count,
        "urls": created_rows,
        "replaced_rows": deleted_rows,
    }


def get_manual_script_source_url_stats() -> Dict[str, Any]:
    rows = ManualScriptSourceURL.objects.all()
    latest_refresh = rows.order_by("-updated_at").values_list("updated_at", flat=True).first()
    return {
        "total_urls": rows.count(),
        "scripts_with_urls": rows.values("script_name").distinct().count(),
        "last_refresh": latest_refresh,
    }


def run_scraper(scraper_id: int, *, triggered_by: str = ScraperRun.Trigger.MANUAL) -> Dict[str, Any]:
    scraper = Scraper.objects.get(id=scraper_id)
    temp_file = None
    start_time = timezone.now()
    run = ScraperRun.objects.create(
        scraper=scraper,
        status=ScraperRun.Status.SUCCESS,
        triggered_by=triggered_by,
        payload={"logs": [], "summary": None, "stderr": ""},
    )

    process = None
    payload_state: Dict[str, Any] = {"logs": [], "summary": None, "stderr": ""}

    def save_payload() -> None:
        run.payload = {
            "logs": payload_state["logs"],
            "summary": payload_state["summary"],
            "stderr": payload_state["stderr"],
        }
        run.save(update_fields=["payload"])

    try:
        project_root = Path(__file__).resolve().parents[1]
        backend_path = str(project_root)
        preamble = (
            "import os, sys\n"
            f"sys.path.insert(0, r\"{backend_path}\")\n"
            "os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'website.settings')\n"
            "import django\n"
            "django.setup()\n\n"
        )

        with tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode="w") as f:
            f.write(preamble)
            f.write(scraper.code)
            temp_file = f.name

        env = os.environ.copy()
        env.setdefault("DJANGO_SETTINGS_MODULE", "website.settings")
        existing_pythonpath = env.get("PYTHONPATH", "")
        if existing_pythonpath:
            env["PYTHONPATH"] = backend_path + os.pathsep + existing_pythonpath
        else:
            env["PYTHONPATH"] = backend_path

        process = subprocess.Popen(
            [sys.executable, temp_file],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=env,
        )

        deadline = start_time + timedelta(seconds=max(scraper.timeout_seconds, 30))
        stdout = process.stdout
        stderr = process.stderr
        stderr_lines: List[str] = []
        result_data: Optional[Dict[str, Any]] = None

        def handle_stdout_line(raw_line: str) -> None:
            nonlocal result_data
            line = raw_line.strip()
            if not line:
                return
            try:
                message = json.loads(line)
            except json.JSONDecodeError:
                entry = {"step": "stdout", "detail": line, "timestamp": time.time()}
                payload_state["logs"].append(entry)
                save_payload()
                return

            event = message.get("event")
            if event == "log":
                data = message.get("data")
                if isinstance(data, dict):
                    payload_state["logs"].append(data)
                    save_payload()
            elif event == "result":
                data = message.get("data")
                if isinstance(data, dict):
                    result_data = data
                    save_payload()
            else:
                payload_state["logs"].append(
                    {"step": event or "message", "detail": message, "timestamp": time.time()}
                )
                save_payload()

        while True:
            if timezone.now() > deadline:
                process.kill()
                raise TimeoutError(f"Scraper exceeded timeout of {scraper.timeout_seconds} seconds")

            if process.poll() is not None:
                if stdout:
                    remaining = stdout.read()
                    if remaining:
                        for line in remaining.splitlines():
                            handle_stdout_line(line)
                if stderr:
                    remaining_err = stderr.read()
                    if remaining_err:
                        stderr_lines.extend(remaining_err.splitlines())
                break

            channels = [channel for channel in (stdout, stderr) if channel and not channel.closed]
            if not channels:
                time.sleep(0.1)
                continue

            ready, _, _ = select.select(channels, [], [], 1.0)

            if stdout in ready:
                line = stdout.readline()
                if line:
                    handle_stdout_line(line)

            if stderr in ready:
                err_line = stderr.readline()
                if err_line:
                    stderr_lines.append(err_line.rstrip("\n"))

        if process.returncode:
            raise RuntimeError(f"Scraper exited with status {process.returncode}")

        if result_data is None:
            raise RuntimeError("Scraper finished without emitting a result payload.")

        summary = persist_job_results(scraper, result_data)
        payload_state["summary"] = summary
        payload_state["stderr"] = "\n".join([line for line in stderr_lines if line])
        save_payload()

        scraper.last_run = timezone.now()
        scraper.save(update_fields=["last_run"])

        finished_at = timezone.now()
        run.finished_at = finished_at
        run.duration_ms = int((finished_at - start_time).total_seconds() * 1000)
        run.status = ScraperRun.Status.SUCCESS
        run.error = ""
        run.save(update_fields=["finished_at", "duration_ms", "payload", "status", "error"])
        return {"run_id": run.id, "status": "success", "summary": summary}
    except Exception as exc:
        finished_at = timezone.now()
        run.finished_at = finished_at
        run.duration_ms = int((finished_at - start_time).total_seconds() * 1000)
        run.status = ScraperRun.Status.ERROR
        run.error = str(exc)
        stderr_dump = "\n".join([line for line in stderr_lines if line])
        if process and process.stderr:
            extra_err = process.stderr.read()
            if extra_err:
                stderr_dump = "\n".join(filter(None, [stderr_dump, extra_err]))
        payload_state["stderr"] = stderr_dump
        save_payload()
        run.save(update_fields=["finished_at", "duration_ms", "status", "error"])
        return {"run_id": run.id, "status": "error", "error": str(exc)}
    finally:
        if process and process.poll() is None:
            process.kill()
        if process and process.stdout:
            process.stdout.close()
        if process and process.stderr:
            process.stderr.close()
        if temp_file and os.path.exists(temp_file):
            os.unlink(temp_file)


def deduplicate_job_postings(*, scraper: Optional[Scraper] = None, dry_run: bool = False) -> Dict[str, Any]:
    """
    Remove older duplicates that share the same ``link``.

    When ``scraper`` is provided, the scope is limited to that scraper; otherwise
    the entire job table is considered. If ``dry_run`` is True, no records are
    deleted and IDs of the would-be deletions are returned.
    """
    base_qs = JobPosting.objects.all()
    if scraper:
        base_qs = base_qs.filter(scraper=scraper)

    duplicate_links = (
        base_qs.values("link")
        .annotate(total=Count("id"))
        .filter(total__gt=1)
        .values_list("link", flat=True)
    )

    duplicates_groups = 0
    removed_ids: List[int] = []
    kept = 0
    removed_total = 0

    for link in duplicate_links.iterator(chunk_size=200):
        cluster_qs = (
            base_qs.filter(link=link)
            .order_by("-created_at", "-id")
        )
        cluster_ids = list(cluster_qs.values_list("id", flat=True))
        if len(cluster_ids) <= 1:
            continue

        keep_id = cluster_ids[0]
        to_remove_ids = cluster_ids[1:]
        if not to_remove_ids:
            continue

        duplicates_groups += 1
        removed_total += len(to_remove_ids)
        if keep_id is not None:
            kept += 1

        if dry_run:
            removed_ids.extend(to_remove_ids)
        else:
            JobPosting.objects.filter(id__in=to_remove_ids).delete()

    result: Dict[str, Any] = {
        "scope": "scraper" if scraper else "global",
        "scraper_id": scraper.id if scraper else None,
        "duplicate_groups": duplicates_groups,
        "removed": removed_total,
        "kept": kept,
        "dry_run": dry_run,
    }

    if dry_run:
        result["would_remove_ids"] = removed_ids
    return result


def _execute_manual_script(script_name: str) -> Dict[str, Any]:
    script_path = get_manual_script_path(script_name)

    env = os.environ.copy()
    env.setdefault("DJANGO_SETTINGS_MODULE", "website.settings")
    backend_path = str(Path(__file__).resolve().parents[1])
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = backend_path + (os.pathsep + existing_pythonpath if existing_pythonpath else "")

    timeout_seconds = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 300), 30)

    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True,
        env=env,
        timeout=timeout_seconds,
    )

    return {
        "exit_code": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def run_manual_script(script_name: str, run_id: Optional[int] = None) -> Dict[str, Any]:
    run_record: Optional[ManualScriptRun] = None
    if run_id:
        try:
            run_record = ManualScriptRun.objects.get(pk=run_id)
            run_record.status = ManualScriptRun.Status.RUNNING
            run_record.started_at = timezone.now()
            run_record.save(update_fields=["status", "started_at"])
        except ManualScriptRun.DoesNotExist:
            run_record = None

    try:
        result = _execute_manual_script(script_name)
        if run_record:
            run_record.status = (
                ManualScriptRun.Status.SUCCESS if result.get("exit_code") == 0 else ManualScriptRun.Status.ERROR
            )
            run_record.output = (result.get("stdout") or "")
            run_record.error = (result.get("stderr") or "")
            run_record.finished_at = timezone.now()
            run_record.save(update_fields=["status", "output", "error", "finished_at"])
            _refresh_manual_script_queue(run_record.queue_id)
            if run_record.queue_id:
                from .tasks import dispatch_manual_script_queue

                dispatch_manual_script_queue()
        return result
    except Exception as exc:
        if run_record:
            run_record.status = ManualScriptRun.Status.ERROR
            run_record.error = str(exc)
            run_record.finished_at = timezone.now()
            run_record.save(update_fields=["status", "error", "finished_at"])
            _refresh_manual_script_queue(run_record.queue_id)
            if run_record.queue_id:
                from .tasks import dispatch_manual_script_queue

                dispatch_manual_script_queue()
        raise


def _refresh_manual_script_queue(queue_id: Optional[int]) -> None:
    if not queue_id:
        return

    try:
        queue = ManualScriptQueue.objects.get(pk=queue_id)
    except ManualScriptQueue.DoesNotExist:
        return

    runs_qs = queue.runs.all()
    has_running = runs_qs.filter(status=ManualScriptRun.Status.RUNNING).exists()
    completed_scripts = runs_qs.filter(
        status__in=[ManualScriptRun.Status.SUCCESS, ManualScriptRun.Status.ERROR, ManualScriptRun.Status.CANCELLED]
    ).count()
    queue.completed_scripts = completed_scripts

    if queue.status == ManualScriptQueue.Status.STOPPED:
        if has_running:
            queue.save(update_fields=["completed_scripts"])
            return
        queue.current_script_name = ""
        queue.finished_at = queue.finished_at or timezone.now()
        queue.save(update_fields=["completed_scripts", "current_script_name", "finished_at"])
        return

    if has_running:
        current_run = runs_qs.filter(status=ManualScriptRun.Status.RUNNING).order_by("queue_position", "id").first()
        queue.status = ManualScriptQueue.Status.RUNNING
        queue.current_script_name = current_run.script_name if current_run else ""
        if queue.started_at is None:
            queue.started_at = timezone.now()
        queue.save(update_fields=["completed_scripts", "status", "current_script_name", "started_at"])
        return

    next_pending = runs_qs.filter(status=ManualScriptRun.Status.PENDING).order_by("queue_position", "id").first()
    if next_pending:
        queue.status = ManualScriptQueue.Status.RUNNING if completed_scripts else ManualScriptQueue.Status.PENDING
        queue.current_script_name = ""
        if completed_scripts and queue.started_at is None:
            queue.started_at = timezone.now()
        queue.save(update_fields=["completed_scripts", "status", "current_script_name", "started_at"])
        return

    queue.status = ManualScriptQueue.Status.ERROR if runs_qs.filter(status=ManualScriptRun.Status.ERROR).exists() else ManualScriptQueue.Status.SUCCESS
    queue.current_script_name = ""
    queue.finished_at = timezone.now()
    last_error_run = runs_qs.filter(status=ManualScriptRun.Status.ERROR).order_by("-finished_at", "-id").first()
    queue.error = last_error_run.error if last_error_run else ""
    if queue.started_at is None:
        queue.started_at = queue.created_at
    queue.save(
        update_fields=[
            "completed_scripts",
            "status",
            "current_script_name",
            "finished_at",
            "error",
            "started_at",
        ]
    )
