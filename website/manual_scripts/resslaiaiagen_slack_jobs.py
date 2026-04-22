#!/usr/bin/env python3
"""Custom scraper for Resslaiaiagen Slack job postings."""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence

import requests

# ---------------------------------------------------------------------------
# Django setup
# ---------------------------------------------------------------------------
BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "website.settings")

import django  # noqa: E402

django.setup()

from django.conf import settings  # noqa: E402

from scrapers.models import JobPosting, Scraper  # noqa: E402
from scrapers.utils import deduplicate_job_postings  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
INVITE_URL = (
    "https://join.slack.com/t/resslaiaiagen-czp2639/shared_invite/"
    "zt-2vpd5vabp-D9LpsJZRiweb7_OFnvIvhA"
)
WORKSPACE_DOMAIN = "resslaiaiagen-czp2639"
WORKSPACE_URL = f"https://{WORKSPACE_DOMAIN}.slack.com"
COMPANY_NAME = "Resslaiaiagen Slack"
API_BASE = "https://slack.com/api"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}

DEFAULT_TIMEOUT_SECONDS = max(getattr(settings, "MANUAL_SCRIPT_TIMEOUT_SECONDS", 1800), 120)
DEFAULT_DELAY = 0.3
REQUEST_TIMEOUT = (10, 30)
DEFAULT_CHANNEL_HINTS = ("job", "jobs", "career", "hiring", "recruit")

SCRAPER_QS = Scraper.objects.filter(company=COMPANY_NAME, url=INVITE_URL).order_by("id")
if SCRAPER_QS.exists():
    SCRAPER = SCRAPER_QS.first()
    if SCRAPER_QS.count() > 1:
        logging.warning("Multiple Resslaiaiagen Slack scrapers found; using id=%s", SCRAPER.id)
else:
    SCRAPER = Scraper.objects.create(
        company=COMPANY_NAME,
        url=INVITE_URL,
        code="custom-script",
        interval_hours=24,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


class SlackApiError(Exception):
    """Raised when the Slack API request fails."""


@dataclass
class SlackJob:
    title: str
    link: str
    description: str
    posted_at: Optional[str]
    channel_id: str
    channel_name: str
    message_ts: str
    metadata: Dict[str, Any]


class SlackClient:
    def __init__(self, token: str, *, delay: float = DEFAULT_DELAY) -> None:
        self.token = token
        self.delay = max(0.0, delay)
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    def request(self, endpoint: str, *, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{API_BASE}/{endpoint}"
        headers = {"Authorization": f"Bearer {self.token}"}
        attempts = 0

        while True:
            attempts += 1
            response = self.session.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", "1"))
                self.logger.warning("Rate limited by Slack API; sleeping %s seconds", retry_after)
                time.sleep(retry_after)
                continue
            response.raise_for_status()
            payload = response.json()
            if not payload.get("ok", False):
                error = payload.get("error", "unknown_error")
                raise SlackApiError(f"Slack API error on {endpoint}: {error}")
            if self.delay:
                time.sleep(self.delay)
            return payload


class SlackJobScraper:
    def __init__(
        self,
        token: str,
        *,
        delay: float = DEFAULT_DELAY,
        include_private: bool = False,
        channel_ids: Optional[Sequence[str]] = None,
        channel_names: Optional[Sequence[str]] = None,
        use_all_channels: bool = False,
        channel_hints: Sequence[str] = DEFAULT_CHANNEL_HINTS,
        oldest: Optional[float] = None,
    ) -> None:
        self.client = SlackClient(token, delay=delay)
        self.include_private = include_private
        self.channel_ids = {cid for cid in channel_ids or [] if cid}
        self.channel_names = {name for name in channel_names or [] if name}
        self.use_all_channels = use_all_channels
        self.channel_hints = [hint.lower() for hint in channel_hints]
        self.oldest = oldest
        self.logger = logging.getLogger(self.__class__.__name__)

    def scrape(self, *, limit: Optional[int] = None) -> Iterator[SlackJob]:
        channels = list(self._iter_channels())
        self.logger.info("Discovered %s Slack channels to scan", len(channels))
        yielded = 0

        for channel in channels:
            for message in self._iter_messages(channel["id"]):
                job = _message_to_job(message, channel)
                if not job:
                    continue
                yield job
                yielded += 1
                if limit is not None and yielded >= limit:
                    self.logger.info("Reached limit %s; stopping.", limit)
                    return

    def _iter_channels(self) -> Iterable[Dict[str, Any]]:
        types = ["public_channel"]
        if self.include_private:
            types.append("private_channel")

        cursor = None
        while True:
            params = {
                "limit": 200,
                "types": ",".join(types),
                "exclude_archived": True,
            }
            if cursor:
                params["cursor"] = cursor
            payload = self.client.request("conversations.list", params=params)
            channels = payload.get("channels") or []
            for channel in channels:
                if self._channel_selected(channel):
                    yield channel
            cursor = (payload.get("response_metadata") or {}).get("next_cursor")
            if not cursor:
                break

    def _channel_selected(self, channel: Dict[str, Any]) -> bool:
        channel_id = channel.get("id") or ""
        channel_name = channel.get("name") or ""

        if self.channel_ids and channel_id in self.channel_ids:
            return True
        if self.channel_names and channel_name in self.channel_names:
            return True
        if self.channel_ids or self.channel_names:
            return False
        if self.use_all_channels:
            return True

        lowered = channel_name.lower()
        return any(hint in lowered for hint in self.channel_hints)

    def _iter_messages(self, channel_id: str) -> Iterable[Dict[str, Any]]:
        cursor = None
        while True:
            params: Dict[str, Any] = {
                "channel": channel_id,
                "limit": 200,
            }
            if cursor:
                params["cursor"] = cursor
            if self.oldest is not None:
                params["oldest"] = str(self.oldest)
            payload = self.client.request("conversations.history", params=params)
            messages = payload.get("messages") or []
            for message in messages:
                yield message
            cursor = (payload.get("response_metadata") or {}).get("next_cursor")
            if not cursor:
                break


def _message_to_job(message: Dict[str, Any], channel: Dict[str, Any]) -> Optional[SlackJob]:
    subtype = message.get("subtype")
    if subtype in {"channel_join", "channel_leave", "channel_topic", "channel_purpose", "channel_name"}:
        return None
    if subtype in {"channel_archive", "channel_unarchive", "message_deleted"}:
        return None

    text = message.get("text") or ""
    blocks_text = _extract_text_from_blocks(message.get("blocks") or [])
    attachments_text = _extract_text_from_attachments(message.get("attachments") or [])
    description = _build_description([text, blocks_text, attachments_text])
    if not description:
        return None

    title = _derive_title(text, blocks_text, message.get("attachments") or [])
    ts = message.get("ts") or ""
    link = _build_permalink(channel.get("id") or "", ts)
    posted_at = _format_ts(ts)
    metadata = _build_metadata(message, channel, link, blocks_text)

    return SlackJob(
        title=title,
        link=link,
        description=description,
        posted_at=posted_at,
        channel_id=channel.get("id") or "",
        channel_name=channel.get("name") or "",
        message_ts=ts,
        metadata=metadata,
    )


def _extract_text_from_blocks(blocks: Sequence[Dict[str, Any]]) -> str:
    parts: List[str] = []
    for block in blocks:
        block_type = block.get("type")
        if block_type == "section":
            text = (block.get("text") or {}).get("text")
            if text:
                parts.append(text)
        elif block_type == "context":
            for element in block.get("elements") or []:
                text = element.get("text")
                if text:
                    parts.append(text)
        elif block_type == "rich_text":
            elements = block.get("elements") or []
            parts.extend(_flatten_rich_text(elements))
    return _normalize_text(parts)


def _flatten_rich_text(elements: Sequence[Dict[str, Any]]) -> List[str]:
    lines: List[str] = []
    for element in elements:
        if element.get("type") == "rich_text_section":
            for child in element.get("elements") or []:
                text = child.get("text")
                if text:
                    lines.append(text)
        elif element.get("type") == "rich_text_list":
            for child in element.get("elements") or []:
                for item in child.get("elements") or []:
                    text = item.get("text")
                    if text:
                        lines.append(text)
    return lines


def _extract_text_from_attachments(attachments: Sequence[Dict[str, Any]]) -> str:
    parts: List[str] = []
    for attachment in attachments:
        title = attachment.get("title")
        text = attachment.get("text")
        fallback = attachment.get("fallback")
        if title:
            parts.append(title)
        if text:
            parts.append(text)
        if fallback and fallback not in (title, text):
            parts.append(fallback)
    return _normalize_text(parts)


def _normalize_text(chunks: Sequence[str]) -> str:
    cleaned = [chunk.strip() for chunk in chunks if chunk and chunk.strip()]
    return "\n".join(cleaned)


def _build_description(parts: Sequence[str]) -> str:
    cleaned = [part.strip() for part in parts if part and part.strip()]
    if not cleaned:
        return ""
    return "\n\n".join(dict.fromkeys(cleaned))


def _derive_title(text: str, blocks_text: str, attachments: Sequence[Dict[str, Any]]) -> str:
    for attachment in attachments:
        title = attachment.get("title")
        if title:
            return title.strip()[:255]
    for candidate in (text, blocks_text):
        if candidate:
            first_line = candidate.splitlines()[0].strip()
            if first_line:
                return first_line[:255]
    return "Slack job posting"


def _build_permalink(channel_id: str, ts: str) -> str:
    ts_compact = ts.replace(".", "")
    return f"{WORKSPACE_URL}/archives/{channel_id}/p{ts_compact}"


def _format_ts(ts: str) -> Optional[str]:
    if not ts:
        return None
    try:
        return datetime.utcfromtimestamp(float(ts)).isoformat() + "Z"
    except ValueError:
        return None


def _simplify_files(files: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    simplified: List[Dict[str, Any]] = []
    for file_info in files:
        simplified.append(
            {
                "id": file_info.get("id"),
                "name": file_info.get("name"),
                "title": file_info.get("title"),
                "mimetype": file_info.get("mimetype"),
                "filetype": file_info.get("filetype"),
                "size": file_info.get("size"),
                "url_private": file_info.get("url_private"),
            }
        )
    return simplified


def _simplify_attachments(attachments: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    simplified: List[Dict[str, Any]] = []
    for attachment in attachments:
        simplified.append(
            {
                "title": attachment.get("title"),
                "title_link": attachment.get("title_link"),
                "text": attachment.get("text"),
                "fallback": attachment.get("fallback"),
                "service_name": attachment.get("service_name"),
                "service_url": attachment.get("service_url"),
            }
        )
    return simplified


def _build_metadata(
    message: Dict[str, Any],
    channel: Dict[str, Any],
    permalink: str,
    blocks_text: str,
) -> Dict[str, Any]:
    metadata: Dict[str, Any] = {
        "workspace_domain": WORKSPACE_DOMAIN,
        "channel_id": channel.get("id"),
        "channel_name": channel.get("name"),
        "message_ts": message.get("ts"),
        "thread_ts": message.get("thread_ts"),
        "user": message.get("user"),
        "bot_id": message.get("bot_id"),
        "subtype": message.get("subtype"),
        "permalink": permalink,
        "blocks_text": blocks_text or None,
        "reactions": message.get("reactions"),
        "reply_count": message.get("reply_count"),
    }
    attachments = message.get("attachments") or []
    if attachments:
        metadata["attachments"] = _simplify_attachments(attachments)
    files = message.get("files") or []
    if files:
        metadata["files"] = _simplify_files(files)
    return {key: value for key, value in metadata.items() if value not in (None, "", [], {})}


def store_listing(listing: SlackJob) -> None:
    JobPosting.objects.update_or_create(
        scraper=SCRAPER,
        link=listing.link,
        defaults={
            "title": listing.title[:255],
            "location": "",
            "date": (listing.posted_at or "")[:100],
            "description": listing.description[:10000],
            "metadata": listing.metadata,
        },
    )


def run_scrape(
    *,
    token: str,
    limit: Optional[int],
    delay: float,
    include_private: bool,
    channel_ids: Sequence[str],
    channel_names: Sequence[str],
    use_all_channels: bool,
    oldest: Optional[float],
) -> int:
    scraper = SlackJobScraper(
        token,
        delay=delay,
        include_private=include_private,
        channel_ids=channel_ids,
        channel_names=channel_names,
        use_all_channels=use_all_channels,
        oldest=oldest,
    )
    count = 0
    for job in scraper.scrape(limit=limit):
        store_listing(job)
        count += 1
    return count


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resslaiaiagen Slack job scraper")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY)
    parser.add_argument("--include-private", action="store_true", default=False)
    parser.add_argument("--all-channels", action="store_true", default=False)
    parser.add_argument("--channel-ids", type=str, default="")
    parser.add_argument("--channel-names", type=str, default="")
    parser.add_argument("--oldest", type=float, default=None)
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
    )
    return parser.parse_args(argv)


def _split_csv(value: str) -> List[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))

    token = (
        os.getenv("SLACK_TOKEN")
        or os.getenv("SLACK_USER_TOKEN")
        or os.getenv("SLACK_BOT_TOKEN")
        or ""
    )
    if not token:
        logging.error("Missing Slack token. Set SLACK_TOKEN, SLACK_USER_TOKEN, or SLACK_BOT_TOKEN.")
        return 1

    channel_ids = _split_csv(args.channel_ids)
    channel_names = _split_csv(args.channel_names)

    start = time.time()
    try:
        count = run_scrape(
            token=token,
            limit=args.limit,
            delay=args.delay,
            include_private=args.include_private,
            channel_ids=channel_ids,
            channel_names=channel_names,
            use_all_channels=args.all_channels,
            oldest=args.oldest,
        )
    except SlackApiError as exc:
        logging.error("Scrape failed: %s", exc)
        return 1

    dedupe_summary = deduplicate_job_postings(scraper=SCRAPER)
    duration = time.time() - start
    summary = {
        "company": COMPANY_NAME,
        "workspace": WORKSPACE_URL,
        "invite_url": INVITE_URL,
        "count": count,
        "elapsed_seconds": duration,
        "dedupe": dedupe_summary,
    }
    logging.info("Summary: %s", json.dumps(summary))
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
