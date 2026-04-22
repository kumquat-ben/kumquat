"""Scraper code generator that leverages OpenAI reasoning and code models."""

from __future__ import annotations

import logging
import textwrap
from typing import Optional

from .planner import OpenAIPlanner, PlannerError

logger = logging.getLogger(__name__)


def generate_scraper_code(url: str, company: str) -> str:
    """
    Generate a Playwright-based scraper tailored to the given URL/company.

    The generator first invokes the OpenAI-powered planner to reason about the
    target site and synthesise a scraper. If that pipeline fails (e.g. missing
    API key, network outage, unexpected LLM response), we fall back to a minimal
    template that simply fetches the page so that deployment is never blocked.
    """
    planner: Optional[OpenAIPlanner] = None

    try:
        planner = OpenAIPlanner()
        return planner.generate_scraper_script(url, company)
    except PlannerError as exc:
        logger.error("Planner failure while generating scraper for %s: %s", url, exc)
    except Exception:  # pragma: no cover - defensive logging
        logger.exception("Unexpected error while generating scraper for %s", url)

    logger.warning("Falling back to minimal scraper template for %s", url)
    return _fallback_scraper(url, company)


def _fallback_scraper(url: str, company: str) -> str:
    """Return a minimal scraper template as a last-resort fallback."""
    return textwrap.dedent(
        f'''
        """
        Fallback scraper for {company}. This template simply fetches the landing
        page with `requests` and prints its length. Replace the implementation
        once the planner pipeline succeeds.
        """

import json
import requests


def main():
    result = {{"company": "{company}", "url": "{url}", "jobs": [], "count": 0}}
    try:
        response = requests.get(
            "{url}",
            headers={{
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            }},
            timeout=30,
        )
        response.raise_for_status()
        html = response.text
        result["html_length"] = len(html)
    except Exception as exc:
        result["error"] = str(exc)

    print(json.dumps(result))


if __name__ == "__main__":
    main()
        '''
    ).strip()
