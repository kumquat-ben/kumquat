import sys
from pathlib import Path

from django.apps import AppConfig


class ScrapersConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "scrapers"

    def ready(self) -> None:
        argv0 = Path(sys.argv[0])

        # Manual scripts are launched as plain Python files and import Django to
        # persist results. They must not auto-start APScheduler, or each child
        # process will recursively dispatch more scripts.
        if "manual_scripts" in argv0.parts:
            return

        if argv0.name == "manage.py" and len(sys.argv) > 1:
            if sys.argv[1] in {"migrate", "collectstatic", "shell", "run_manual_script_worker"}:
                return

        # Import late to avoid scheduler setup during migrations before apps loaded.
        from .tasks import start_scheduler

        start_scheduler()
