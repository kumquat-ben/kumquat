import signal
import time

from django.core.management.base import BaseCommand

from scrapers.tasks import shutdown_scheduler, start_scheduler


class Command(BaseCommand):
    help = "Run the dedicated manual script worker process."

    def handle(self, *args, **options):
        stop_requested = False

        def _handle_signal(signum, frame):  # pragma: no cover - signal driven
            nonlocal stop_requested
            stop_requested = True

        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

        start_scheduler()
        self.stdout.write(self.style.SUCCESS("Manual script worker started."))

        try:
            while not stop_requested:
                time.sleep(1)
        finally:
            shutdown_scheduler()
            self.stdout.write(self.style.WARNING("Manual script worker stopped."))
