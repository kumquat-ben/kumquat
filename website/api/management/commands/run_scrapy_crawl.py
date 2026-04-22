from django.core.management.base import BaseCommand, CommandError

from api.scrapy_runner import crawl_target_with_scrapy, discover_domains_with_scrapy


class Command(BaseCommand):
    help = "Run a Scrapy-backed website crawl or domain discovery job."

    def add_arguments(self, parser):
        parser.add_argument("--mode", choices=["target", "discovery"], required=True)
        parser.add_argument("--id", type=int, required=True)

    def handle(self, *args, **options):
        mode = options["mode"]
        identifier = options["id"]
        if mode == "target":
            crawl_target_with_scrapy(identifier)
            return
        if mode == "discovery":
            discover_domains_with_scrapy(identifier)
            return
        raise CommandError(f"Unsupported mode: {mode}")
