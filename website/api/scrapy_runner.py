import hashlib
from urllib.parse import urlsplit, urlunsplit

from django.utils import timezone
from scrapy import Request, Spider
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings as ScrapySettings

from .models import SearchCrawlTarget, SearchDocument, WebsiteCrawlerDefinition, WebsiteDiscoveredDomain
from .search import SearchCrawlerError, normalize_crawl_url


SCRAPY_DEFAULT_SETTINGS = {
    "LOG_ENABLED": False,
    "ROBOTSTXT_OBEY": False,
    "COOKIES_ENABLED": False,
    "TELNETCONSOLE_ENABLED": False,
    "USER_AGENT": "KumquatScrapyBot/0.1 (+https://kumquat.info)",
}


def _base_domain(hostname):
    hostname = (hostname or "").lower().strip(".")
    if not hostname:
        return ""
    parts = hostname.split(".")
    if len(parts) <= 2:
        return hostname
    return ".".join(parts[-2:])


def _normalized_domain_root(url):
    normalized_url = normalize_crawl_url(url)
    parsed = urlsplit(normalized_url)
    return urlunsplit((parsed.scheme, parsed.netloc, "/", "", ""))


class _SearchTargetSpider(Spider):
    name = "kumquat_search_target"

    def __init__(self, target_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.target = SearchCrawlTarget.objects.get(pk=target_id)
        self.start_urls = [self.target.normalized_url]
        self.allowed_domains = [self.target.scope_netloc]
        self.max_depth = self.target.max_depth
        self.max_pages = self.target.max_pages
        self.visited_count = 0
        self.now = timezone.now

    def parse(self, response, depth=0):
        if self.visited_count >= self.max_pages:
            return

        self.visited_count += 1
        title = (response.css("title::text").get() or "").strip()[:255]
        text_parts = [part.strip() for part in response.xpath("//body//text()").getall() if part.strip()]
        text = " ".join(text_parts)
        links = response.css("a::attr(href)").getall()
        SearchDocument.objects.update_or_create(
            normalized_url=normalize_crawl_url(response.url),
            defaults={
                "crawl_target": self.target,
                "url": response.url,
                "title": title,
                "summary": text[:280].strip(),
                "content": text,
                "content_hash": hashlib.sha256(text.encode("utf-8")).hexdigest() if text else "",
                "depth": depth,
                "http_status": response.status,
                "link_count": len(links),
                "crawled_at": self.now(),
            },
        )

        if depth >= self.max_depth or self.visited_count >= self.max_pages:
            return

        for href in links:
            next_url = response.urljoin(href)
            try:
                normalized = normalize_crawl_url(next_url)
            except SearchCrawlerError:
                continue
            if urlsplit(normalized).netloc.lower() != self.target.scope_netloc.lower():
                continue
            yield Request(normalized, callback=self.parse, cb_kwargs={"depth": depth + 1})


class _DomainDiscoverySpider(Spider):
    name = "kumquat_domain_discovery"

    def __init__(self, crawler_definition_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.crawler_definition = WebsiteCrawlerDefinition.objects.get(pk=crawler_definition_id)
        seed_parsed = urlsplit(self.crawler_definition.seed_url)
        self.start_urls = [self.crawler_definition.seed_url]
        self.allowed_domains = [seed_parsed.netloc]
        self.max_pages = int((self.crawler_definition.config or {}).get("discovery_max_pages") or 25)
        self.source_base_domain = _base_domain(seed_parsed.hostname)
        self.visited_count = 0

    def parse(self, response):
        if self.visited_count >= self.max_pages:
            return

        self.visited_count += 1
        for href in response.css("a::attr(href)").getall():
            absolute = response.urljoin(href)
            try:
                normalized = normalize_crawl_url(absolute)
            except SearchCrawlerError:
                continue
            hostname = urlsplit(normalized).hostname or ""
            if not hostname:
                continue
            if _base_domain(hostname) == self.source_base_domain:
                yield Request(normalized, callback=self.parse)
                continue

            normalized_root = _normalized_domain_root(normalized)
            discovered, created = WebsiteDiscoveredDomain.objects.get_or_create(
                crawler_definition=self.crawler_definition,
                domain=urlsplit(normalized_root).netloc.lower(),
                defaults={
                    "source_url": response.url,
                    "normalized_url": normalized_root,
                    "status": WebsiteDiscoveredDomain.STATUS_NEW,
                },
            )
            if not created:
                discovered.discovery_count += 1
                discovered.source_url = response.url
                discovered.normalized_url = normalized_root
                discovered.last_seen_at = timezone.now()
                discovered.save(
                    update_fields=["discovery_count", "source_url", "normalized_url", "last_seen_at", "updated_at"]
                )


def _run_spider(spider_cls, *args):
    settings = ScrapySettings(values=SCRAPY_DEFAULT_SETTINGS)
    process = CrawlerProcess(settings=settings)
    process.crawl(spider_cls, *args)
    process.start()


def crawl_target_with_scrapy(target_id):
    target = SearchCrawlTarget.objects.get(pk=target_id)
    target.status = SearchCrawlTarget.STATUS_RUNNING
    target.started_at = timezone.now()
    target.finished_at = None
    target.last_error = ""
    target.save(update_fields=["status", "started_at", "finished_at", "last_error", "updated_at"])

    try:
        _run_spider(_SearchTargetSpider, target_id)
    except Exception as exc:
        target.status = SearchCrawlTarget.STATUS_FAILED
        target.last_error = str(exc)
    else:
        target.status = SearchCrawlTarget.STATUS_COMPLETED
        target.document_count = SearchDocument.objects.filter(crawl_target=target).count()
    finally:
        target.finished_at = timezone.now()
        if target.status != SearchCrawlTarget.STATUS_COMPLETED:
            target.document_count = SearchDocument.objects.filter(crawl_target=target).count()
        target.save(
            update_fields=["status", "last_error", "document_count", "finished_at", "updated_at"]
        )
    return target


def discover_domains_with_scrapy(crawler_definition_id):
    crawler_definition = WebsiteCrawlerDefinition.objects.get(pk=crawler_definition_id)
    _run_spider(_DomainDiscoverySpider, crawler_definition_id)
    return crawler_definition
