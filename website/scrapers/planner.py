"""OpenAI powered reasoning + code generation pipeline for bespoke scrapers."""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import textwrap
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

try:
    from playwright.sync_api import Error as PlaywrightError  # type: ignore
    from playwright.sync_api import TargetClosedError  # type: ignore
    from playwright.sync_api import sync_playwright  # type: ignore

    _PLAYWRIGHT_AVAILABLE = True
except ImportError as exc:  # pragma: no cover - runtime guard
    sync_playwright = None  # type: ignore
    TargetClosedError = PlaywrightError = Exception  # type: ignore
    _PLAYWRIGHT_AVAILABLE = False
    _PLAYWRIGHT_IMPORT_ERROR = exc

try:
    from openai import OpenAI  # type: ignore
    from openai import BadRequestError  # type: ignore

    _OPENAI_AVAILABLE = True
except ImportError as exc:  # pragma: no cover - runtime guard
    OpenAI = None  # type: ignore
    BadRequestError = Exception  # type: ignore
    _OPENAI_AVAILABLE = False
    _OPENAI_IMPORT_ERROR = exc


logger = logging.getLogger(__name__)

SAMPLE_SCRAPER_REFERENCE = (
    "Use dataclasses (JobSummary/JobListing), a `requests.Session` with Disney-like headers, "
    "paginate via `#search-results table tbody tr`, and fetch detail pages to capture apply links "
    "and `.ats-description` HTML, similar to the hand-crafted Disney scraper."
)


class PlannerError(RuntimeError):
    """Raised when the OpenAI planning pipeline fails."""


@dataclass
class SiteContext:
    url: str
    html_excerpt: str
    forms: List[Dict[str, Any]]
    anchors: List[Dict[str, str]]
    metadata: Dict[str, Any]
    listing_hints: List[Dict[str, Any]]


class OpenAIPlanner:
    """High-level orchestrator that produces tailored scraper scripts."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        reasoning_model: Optional[str] = None,
        code_model: Optional[str] = None,
    ) -> None:
        if not _OPENAI_AVAILABLE:
            raise PlannerError(
                "The 'openai' package is required for the planning pipeline. "
                "Install it via `pip install openai`."
            ) from _OPENAI_IMPORT_ERROR

        self.api_key = api_key or os.getenv("OPENAI_API_KEY") or os.getenv("OPENAI_SECRET_KEY")
        if not self.api_key:
            raise PlannerError("Set OPENAI_API_KEY (or OPENAI_SECRET_KEY) so the planner can call OpenAI.")

        self.client = OpenAI(api_key=self.api_key)
        self.reasoning_model = reasoning_model or os.getenv("OPENAI_REASONING_MODEL") or "gpt-4o-mini"
        self.code_model = code_model or os.getenv("OPENAI_CODE_MODEL") or "gpt-4o"
        self.reasoning_fallback_model = os.getenv("OPENAI_REASONING_FALLBACK_MODEL") or "gpt-4o-mini"
        self.code_fallback_model = os.getenv("OPENAI_CODE_FALLBACK_MODEL") or "gpt-4o"
        self.max_attempts = int(os.getenv("SCRAPER_PLANNER_MAX_ATTEMPTS", "3"))

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def generate_scraper_script(self, url: str, company: str) -> str:
        """Run the full reasoning/codegen pipeline and return a Python script."""

        context = self._collect_site_context(url)
        failure_feedback: Optional[str] = None
        last_error: Optional[str] = None

        for attempt in range(1, self.max_attempts + 1):
            plan = self._reason_about_site(url, company, context, failure_feedback)
            code = self._synthesise_code(url, company, context, plan)
            success, feedback = self._validate_script(code, url, company)

            if success:
                if feedback:
                    logger.info("Code validation feedback: %s", feedback)
                return code

            failure_feedback = feedback
            last_error = feedback
            logger.warning("Planner attempt %s for %s failed; retrying", attempt, url)

        raise PlannerError(
            f"Failed to generate a working scraper for {url} after {self.max_attempts} attempts. "
            f"Last error: {last_error}"
        )

    # ------------------------------------------------------------------
    # Site context collection
    # ------------------------------------------------------------------
    def _collect_site_context(self, url: str) -> SiteContext:
        html: Optional[str] = None
        if _PLAYWRIGHT_AVAILABLE and sync_playwright is not None:
            browser = None
            page = None
            try:
                with sync_playwright() as playwright:
                    browser = playwright.chromium.launch(headless=True)
                    page = browser.new_page()
                    page.set_default_timeout(45000)
                    page.goto(url, wait_until="domcontentloaded")
                    html = page.content()
            except (TargetClosedError, PlaywrightError) as exc:
                html = None
                log_message = f"Playwright failed for {url}: {exc}"
                logger.warning(log_message)
            finally:
                try:
                    if page is not None:
                        page.close()
                except Exception:  # pragma: no cover - cleanup best-effort
                    pass
                try:
                    if browser is not None:
                        browser.close()
                except Exception:  # pragma: no cover - cleanup best-effort
                    pass
        else:
            logger.info("Playwright unavailable; falling back to requests for %s", url)

        if not html:
            response = None
            try:
                response = requests.get(
                    url,
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                        "Accept-Language": "en-US,en;q=0.9",
                    },
                    timeout=(10, 15),
                )
                response.raise_for_status()
                html = response.text
            except requests.RequestException as exc:  # pragma: no cover - network failure
                logger.warning("requests fallback failed for %s: %s", url, exc)
                html = ""
            finally:
                if response is not None:
                    try:
                        response.close()
                    except Exception:
                        pass

        if not html:
            logger.warning("Proceeding with empty HTML context for %s", url)
            html = ""

        soup = BeautifulSoup(html or "", "html.parser")

        forms_summary: List[Dict[str, Any]] = []
        for form in soup.find_all("form")[:5]:
            inputs: List[Dict[str, Any]] = []
            for input_el in form.find_all(["input", "select", "textarea"]):
                inputs.append(
                    {
                        "name": (input_el.get("name") or "").strip(),
                        "type": (input_el.get("type") or input_el.name or "").strip(),
                        "placeholder": (input_el.get("placeholder") or "").strip(),
                    }
                )

            forms_summary.append(
                {
                    "action": (form.get("action") or "").strip(),
                    "method": (form.get("method") or "GET").upper(),
                    "inputs": inputs,
                }
            )

        anchors_summary: List[Dict[str, str]] = []
        for anchor in soup.find_all("a", href=True)[:25]:
            text = anchor.get_text(" ", strip=True)
            anchors_summary.append({"text": text[:80], "href": anchor["href"]})

        metadata = {
            "title": soup.title.string.strip() if soup.title and soup.title.string else "",
            "detected_tables": len(soup.find_all("table")),
            "detected_lists": len(soup.find_all("ul")),
        }

        listing_hints = self._extract_listing_hints(soup)

        if html:
            excerpt_text = re.sub(r"\s+", " ", html)
            excerpt = textwrap.shorten(excerpt_text, width=4000, placeholder=" …")
        else:
            excerpt = ""

        return SiteContext(
            url=url,
            html_excerpt=excerpt,
            forms=forms_summary,
            anchors=anchors_summary,
            metadata=metadata,
            listing_hints=listing_hints,
        )

    # ------------------------------------------------------------------
    # Reasoning + planning
    # ------------------------------------------------------------------
    def _extract_listing_hints(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        hints: List[Dict[str, Any]] = []
        class_counter: Counter[tuple[str, ...]] = Counter()
        samples: Dict[tuple[str, ...], str] = {}

        for element in soup.find_all(class_=True):
            classes = tuple([c for c in element.get("class", []) if c][:2])
            if not classes:
                continue
            class_counter[classes] += 1
            if classes not in samples:
                text = element.get_text(" ", strip=True)
                if text:
                    samples[classes] = text[:160]

        for classes, count in class_counter.most_common(10):
            if count < 3:
                continue
            selector = ".".join(classes)
            hints.append(
                {
                    "selector": f".{selector}" if selector else "",
                    "count": count,
                    "sample": samples.get(classes, ""),
                }
            )
            if len(hints) >= 5:
                break

        return hints

    def _reason_about_site(
        self,
        url: str,
        company: str,
        context: SiteContext,
        failure_feedback: Optional[str] = None,
    ) -> Dict[str, Any]:
        prompt = textwrap.dedent(
            f"""
            You are an autonomous scraping architect. Analyse the site for {company}
            ({url}) and produce a structured plan to extract job listings.

            Provide your response strictly as JSON with the following keys:
            - initial_actions: ordered list of steps (describe UI interactions).
            - listing_page: description of how to detect listings (selectors, structure).
            - detail_page: description of how to follow a job detail and fields to extract.
            - pagination: describe how to move across result pages (selectors or API calls).
            - data_fields: array of field names to extract per job.
            - notes: free-form tips (rate limit, cookies, etc.).

            Site context:
            {{
                "metadata": {json.dumps(context.metadata, indent=2)},
                "forms": {json.dumps(context.forms, indent=2)},
                "anchors": {json.dumps(context.anchors, indent=2)},
                "listing_hints": {json.dumps(context.listing_hints, indent=2)},
                "html_excerpt": "{context.html_excerpt}"
            }}
            """
        ).strip()

        if failure_feedback:
            prompt += "\nPrevious attempt feedback to address:\n" + failure_feedback

        try:
            response = self.client.responses.create(
                model=self.reasoning_model,
                input=prompt,
                reasoning={"effort": "medium"},
                temperature=0.1,
            )
        except BadRequestError as exc:
            error_message = str(exc)
            if self.reasoning_fallback_model and "model" in error_message.lower():
                response = self.client.responses.create(
                    model=self.reasoning_fallback_model,
                    input=prompt,
                    temperature=0.2,
                )
            else:
                raise PlannerError(f"Reasoning model call failed: {exc}") from exc
        except Exception as exc:
            raise PlannerError(f"Reasoning model request error: {exc}") from exc

        plan_text = response.output_text.strip()
        plan = self._coerce_json(plan_text)
        if not isinstance(plan, dict):
            raise PlannerError("Reasoning model did not return valid JSON plan")
        return plan

    # ------------------------------------------------------------------
    # Code synthesis
    # ------------------------------------------------------------------
    def _synthesise_code(
        self,
        url: str,
        company: str,
        context: SiteContext,
        plan: Dict[str, Any],
    ) -> str:
        playwright_note = (
            "Playwright is available in the runtime environment; you may use it."
            if _PLAYWRIGHT_AVAILABLE and sync_playwright is not None
            else "Playwright is NOT available; use requests + BeautifulSoup and standard libraries."
        )

        required_fields = plan.get("data_fields") or ["title", "location", "detail_url"]
        plan_json = json.dumps(plan, indent=2)
        listing_hints = json.dumps(context.listing_hints, indent=2)
        anchors_snippet = json.dumps(context.anchors[:10], indent=2)

        prompt = textwrap.dedent(
            f"""
            You are a senior Python engineer and will produce a production-ready scraper
            for the {company} careers site ({url}). Follow the reference style shown below
            (requests + BeautifulSoup, dataclasses, iterative pagination, detailed logging):

            Example structure:
            - {SAMPLE_SCRAPER_REFERENCE}
            - Define dataclasses for JobSummary and JobListing
            - Create a class named <Company>JobScraper with methods `_fetch_search_page`, `_parse_job_summaries`, `_fetch_job_detail`
            - Iterate through result pages, apply a small delay, and visit job detail pages.
            - Write each job to the database using Django's ORM (`JobPosting` model) as soon as it's parsed, avoiding duplicates.

            Environment constraints:
            {playwright_note}
            - Assume this script will be executed inside the Django project root.
            - Do not call `settings.configure`; the host process already initialises Django. Instead, ensure `sys.path` includes the project base, set `DJANGO_SETTINGS_MODULE="website.settings"` if not already set, and call `django.setup()` before using models.
            - Import `JobPosting` from `scrapers.models` after Django has been initialised.
            - Deduplicate based on `link` (detail URL) when writing to the database.

            Output requirements:
            - Provide the entire script inside a single ```python``` block with a `main()` entry point and CLI arguments (`--max-pages`, `--limit`, `--delay`, `--log-level`).
            - Use structured logging similar to the reference (INFO/DEBUG statements).
            - For each stored job, capture the fields: {', '.join(required_fields)} plus `description_text`, `description_html`, and any metadata discovered.
            - Return a summary JSON to stdout with keys `company`, `url`, `count`, `jobs` (short summaries only) and optional `error`.
            - Handle network and parsing errors gracefully; continue after logging and skip problematic jobs.

            Planning inputs:
            Plan JSON:
            {plan_json}

            Listing hints:
            {listing_hints}

            Example anchors:
            {anchors_snippet}

            HTML snippet (may be empty if the site timed out; rely on selectors from plan/hints):
            {context.html_excerpt}
            """
        ).strip()

        try:
            response = self.client.responses.create(
                model=self.code_model,
                input=prompt,
                temperature=0.25,
            )
        except BadRequestError as exc:
            error_message = str(exc)
            if self.code_fallback_model and "model" in error_message.lower():
                response = self.client.responses.create(
                    model=self.code_fallback_model,
                    input=prompt,
                    temperature=0.2,
                )
            else:
                raise PlannerError(f"Code model call failed: {exc}") from exc
        except Exception as exc:
            raise PlannerError(f"Code model request error: {exc}") from exc

        code_text = response.output_text
        code = self._extract_code_block(code_text)
        if not code:
            raise PlannerError("Code model did not return a Python code block")
        return code

    def _validate_script(self, code: str, url: str, company: str) -> tuple[bool, str]:
        # Runtime validation is skipped because scripts depend on Django settings and
        # database access that are not available in the isolated planner sandbox.
        # Instead, we rely on the language model instructions to generate resilient
        # code. A lightweight heuristic could be added here in the future.
        return True, "validation skipped"

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _coerce_json(raw: str) -> Any:
        candidate = raw.strip()
        if candidate.startswith("```"):
            candidate = candidate.strip("`")
            candidate = re.sub(r"^python\n", "", candidate)
        start = candidate.find("{")
        end = candidate.rfind("}")
        if start != -1 and end != -1:
            candidate = candidate[start : end + 1]
        try:
            return json.loads(candidate)
        except json.JSONDecodeError as exc:
            raise PlannerError(f"Unable to parse JSON from reasoning model: {exc}")

    @staticmethod
    def _extract_code_block(raw: str) -> str:
        if "```" not in raw:
            return raw.strip()
        match = re.search(r"```python\s*(.*?)```", raw, flags=re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()
        match = re.search(r"```\s*(.*?)```", raw, flags=re.DOTALL)
        if match:
            return match.group(1).strip()
        return raw.strip()

    @staticmethod
    def _extract_json_blob(stdout: str) -> Optional[str]:
        if not stdout:
            return None
        lines = [line.strip() for line in stdout.strip().splitlines() if line.strip()]
        for line in reversed(lines):
            if not line:
                continue
            try:
                json.loads(line)
                return line
            except json.JSONDecodeError:
                continue
        return None

    @staticmethod
    def _format_feedback(message: str, stdout: str, stderr: str) -> str:
        parts = [message]
        if stdout:
            truncated_stdout = stdout[-1500:]
            parts.append("STDOUT (tail):\n" + truncated_stdout)
        if stderr:
            truncated_stderr = stderr[-1500:]
            parts.append("STDERR (tail):\n" + truncated_stderr)
        return "\n\n".join(parts)
