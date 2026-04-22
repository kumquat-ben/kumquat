# Copyright (c) 2026 Benjamin Levin. All Rights Reserved.
# Unauthorized use or distribution is strictly prohibited.
import hashlib
import re
from collections import deque
from html.parser import HTMLParser
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlsplit, urlunsplit
from urllib.request import Request, urlopen

from django.conf import settings
from django.db.models import Q
from django.utils import timezone
from elastic_transport import ConnectionError as ElasticsearchConnectionError
from elasticsearch import ApiError

from .documents import SearchDocumentDocument
from .models import SearchCrawlTarget, SearchDocument


TOKEN_RE = re.compile(r"[a-z0-9]{2,}")
WHITESPACE_RE = re.compile(r"\s+")
PARKING_PAGE_MARKERS = (
    "domain for sale",
    "buy this domain",
    "this domain is for sale",
    "this web page is parked",
    "this domain is parked",
    "parked free",
    "sedo domain parking",
    "hugedomains",
    "afternic",
    "parkingcrew",
    "bodis",
    "dan.com",
)


class SearchCrawlerError(Exception):
    pass


class _HTMLIndexParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []
        self.title = ""
        self._text_parts = []
        self._skip_depth = 0
        self._in_title = False

    def handle_starttag(self, tag, attrs):
        if tag in {"script", "style", "noscript"}:
            self._skip_depth += 1
            return
        if tag == "title":
            self._in_title = True
            return
        if tag != "a":
            return
        href = dict(attrs).get("href")
        if href:
            self.links.append(href)

    def handle_endtag(self, tag):
        if tag in {"script", "style", "noscript"} and self._skip_depth:
            self._skip_depth -= 1
        elif tag == "title":
            self._in_title = False

    def handle_data(self, data):
        if self._skip_depth:
            return
        cleaned = WHITESPACE_RE.sub(" ", data or "").strip()
        if not cleaned:
            return
        if self._in_title:
            self.title = f"{self.title} {cleaned}".strip()
            return
        self._text_parts.append(cleaned)

    @property
    def text(self):
        return WHITESPACE_RE.sub(" ", " ".join(self._text_parts)).strip()


def normalize_crawl_url(url):
    raw_url = (url or "").strip()
    parsed = urlsplit(raw_url)
    if not parsed.scheme:
        parsed = urlsplit(f"https://{raw_url}")
    if parsed.scheme not in {"http", "https"}:
        raise SearchCrawlerError("Only http and https URLs can be crawled.")
    if not parsed.netloc:
        raise SearchCrawlerError("A valid URL is required.")

    hostname = (parsed.hostname or "").lower()
    if not hostname:
        raise SearchCrawlerError("A valid URL is required.")

    port = parsed.port
    if port and not ((parsed.scheme == "http" and port == 80) or (parsed.scheme == "https" and port == 443)):
        netloc = f"{hostname}:{port}"
    else:
        netloc = hostname
    path = parsed.path or "/"
    normalized = urlunsplit((parsed.scheme.lower(), netloc, path, parsed.query, ""))
    return normalized


def _extract_page_payload(html):
    parser = _HTMLIndexParser()
    parser.feed(html)
    parser.close()
    text = WHITESPACE_RE.sub(" ", parser.text).strip()
    summary = text[:280].strip()
    return {
        "title": parser.title[:255],
        "text": text,
        "summary": summary,
        "links": parser.links,
    }


def _fetch_html(url):
    request = Request(
        url,
        headers={
            "User-Agent": getattr(
                settings,
                "SEARCH_CRAWLER_USER_AGENT",
                "KumquatSearchBot/0.1 (+https://kumquat.info)",
            ),
        },
    )
    timeout = getattr(settings, "SEARCH_CRAWLER_TIMEOUT_SECONDS", 10)
    with urlopen(request, timeout=timeout) as response:
        status = getattr(response, "status", 200)
        content_type = response.headers.get("Content-Type", "")
        if "text/html" not in content_type.lower():
            raise SearchCrawlerError(f"Unsupported content type: {content_type or 'unknown'}")
        charset = response.headers.get_content_charset() or "utf-8"
        body = response.read().decode(charset, errors="replace")
        return status, body


def is_probable_parking_page(url, fetch_html=_fetch_html):
    try:
        status_code, html = fetch_html(url)
    except (HTTPError, URLError, SearchCrawlerError):
        return False

    if status_code >= 400:
        return False

    page_payload = _extract_page_payload(html)
    combined = f"{page_payload['title']} {page_payload['text']}".lower()
    return any(marker in combined for marker in PARKING_PAGE_MARKERS)


def _same_scope(candidate_url, scope_netloc):
    return urlsplit(candidate_url).netloc.lower() == (scope_netloc or "").lower()


def crawl_target(target_id, fetch_html=_fetch_html):
    target = SearchCrawlTarget.objects.get(pk=target_id)
    target.status = SearchCrawlTarget.STATUS_RUNNING
    target.started_at = timezone.now()
    target.finished_at = None
    target.last_error = ""
    target.save(update_fields=["status", "started_at", "finished_at", "last_error", "updated_at"])

    queue = deque([(target.normalized_url, 0)])
    visited = set()
    crawled_count = 0

    try:
        while queue and crawled_count < target.max_pages:
            current_url, depth = queue.popleft()
            normalized_url = normalize_crawl_url(current_url)
            if normalized_url in visited:
                continue
            visited.add(normalized_url)

            try:
                status_code, html = fetch_html(normalized_url)
            except HTTPError as exc:
                status_code = exc.code
                html = ""
                page_payload = {"title": "", "text": "", "summary": "", "links": []}
            except URLError as exc:
                raise SearchCrawlerError(f"Failed to crawl {normalized_url}: {exc.reason}") from exc
            else:
                page_payload = _extract_page_payload(html)

            SearchDocument.objects.update_or_create(
                normalized_url=normalized_url,
                defaults={
                    "crawl_target": target,
                    "url": normalized_url,
                    "title": page_payload["title"],
                    "summary": page_payload["summary"],
                    "content": page_payload["text"],
                    "content_hash": hashlib.sha256(page_payload["text"].encode("utf-8")).hexdigest()
                    if page_payload["text"]
                    else "",
                    "depth": depth,
                    "http_status": status_code,
                    "link_count": len(page_payload["links"]),
                    "crawled_at": timezone.now(),
                },
            )
            crawled_count += 1

            if depth >= target.max_depth:
                continue

            for href in page_payload["links"]:
                absolute_url = urljoin(normalized_url, href)
                try:
                    next_url = normalize_crawl_url(absolute_url)
                except SearchCrawlerError:
                    continue
                if not _same_scope(next_url, target.scope_netloc):
                    continue
                if next_url in visited:
                    continue
                queue.append((next_url, depth + 1))

    except SearchCrawlerError as exc:
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


def _tokenize(text):
    return TOKEN_RE.findall((text or "").lower())


def _build_snippet(text, query):
    if not text:
        return ""
    lowered = text.lower()
    query_tokens = _tokenize(query)
    start = 0
    for token in query_tokens:
        position = lowered.find(token)
        if position >= 0:
            start = max(position - 80, 0)
            end = min(position + 180, len(text))
            snippet = text[start:end].strip()
            if start > 0:
                snippet = f"...{snippet}"
            if end < len(text):
                snippet = f"{snippet}..."
            return snippet
    return text[:220].strip()


def search_documents(query, limit=5):
    query = (query or "").strip()
    if not query:
        return {
            "results": [],
            "document_count": SearchDocument.objects.count(),
            "match_count": 0,
            "backend": "database",
        }

    tokens = _tokenize(query)
    if not tokens:
        return {
            "results": [],
            "document_count": SearchDocument.objects.count(),
            "match_count": 0,
            "backend": "database",
        }

    try:
        search = SearchDocumentDocument.search()
        search = search.query(
            "multi_match",
            query=query,
            fields=["title^3", "summary^2", "content"],
            fuzziness="AUTO",
        )[:limit]
        response = search.execute()
    except (ElasticsearchConnectionError, ApiError):
        response = None
    if response is not None:
        results = []
        for hit in response:
            content = getattr(hit, "content", "") or getattr(hit, "summary", "")
            results.append(
                {
                    "title": getattr(hit, "title", "") or getattr(hit, "normalized_url", ""),
                    "url": getattr(hit, "normalized_url", ""),
                    "summary": getattr(hit, "summary", ""),
                    "snippet": _build_snippet(content, query),
                    "score": getattr(hit.meta, "score", 0),
                    "crawled_at": getattr(hit, "crawled_at", None),
                }
            )
        return {
            "results": results,
            "document_count": SearchDocument.objects.count(),
            "match_count": len(results),
            "backend": "elasticsearch",
        }

    combined_filter = Q()
    for token in tokens:
        combined_filter |= Q(title__icontains=token) | Q(summary__icontains=token) | Q(content__icontains=token)

    candidates = list(
        SearchDocument.objects.filter(combined_filter).order_by("-crawled_at", "-updated_at")[:100]
    )
    scored_results = []
    for document in candidates:
        haystack = " ".join([document.title, document.summary, document.content]).lower()
        score = sum(haystack.count(token) for token in tokens)
        if score <= 0:
            continue
        scored_results.append(
            {
                "title": document.title or document.normalized_url,
                "url": document.normalized_url,
                "summary": document.summary,
                "snippet": _build_snippet(document.content or document.summary, query),
                "score": score,
                "crawled_at": document.crawled_at,
            }
        )

    scored_results.sort(key=lambda item: (item["score"], item["crawled_at"]), reverse=True)
    return {
        "results": scored_results[:limit],
        "document_count": SearchDocument.objects.count(),
        "match_count": len(scored_results),
        "backend": "database",
    }
