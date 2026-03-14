"""Stage 2: Discover -- pydantic-ai agent finds and saves URLs autonomously."""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any

from pydantic_ai import Agent, UsageLimits
from pydantic_ai.exceptions import UsageLimitExceeded

from ..config import Config, _to_pydantic_ai_model
from ..cost import CostTracker
from ..db import get_connection, get_discovered_urls
from ..display import show_detail, show_stage, show_warning
from . import tools
from .prompts import DISCOVER_SYSTEM

_CONTEXT_TOKEN_CAP = 8_000


def _truncate_context(text: str, max_tokens: int = _CONTEXT_TOKEN_CAP) -> str:
    """Truncate text to roughly max_tokens using tiktoken."""
    try:
        import tiktoken
        enc = tiktoken.get_encoding("cl100k_base")
        tokens = enc.encode(text)
        if len(tokens) <= max_tokens:
            return text
        truncated = enc.decode(tokens[:max_tokens])
        return truncated + "\n\n[...context truncated to fit token budget...]"
    except ImportError:
        return text


def _build_search_tools(config: Config) -> tuple[list, list]:
    """Build search tool functions and builtin tools based on search_provider config.

    Returns (tool_functions, builtin_tools) for pydantic-ai Agent.
    """
    tool_functions: list = []
    builtin_tools: list = []
    provider = config.search_provider

    if provider == "openai":
        from pydantic_ai.tools import WebSearchTool
        builtin_tools.append(WebSearchTool())
    elif provider == "exa" and config.exa_api_key:
        from exa_py import Exa
        exa_client = Exa(api_key=config.exa_api_key)

        def web_search(query: str, num_results: int = 10) -> list[dict]:
            """Search the web using Exa. Returns list of results with title, url, and text."""
            results = exa_client.search(
                query,
                num_results=num_results,
                contents={"text": {"max_characters": 3000}},
            )
            return [
                {"title": r.title, "url": r.url, "text": getattr(r, "text", "")}
                for r in results.results
            ]

        tool_functions.append(web_search)
    elif provider == "brave":
        api_key = os.environ.get("BRAVE_API_KEY", "")
        if api_key:
            import httpx as _httpx

            def web_search(query: str, count: int = 10) -> list[dict]:
                """Search the web using Brave Search API."""
                resp = _httpx.get(
                    "https://api.search.brave.com/res/v1/web/search",
                    headers={"X-Subscription-Token": api_key, "Accept": "application/json"},
                    params={"q": query, "count": count},
                )
                resp.raise_for_status()
                return [
                    {"title": r["title"], "url": r["url"], "description": r.get("description", "")}
                    for r in resp.json().get("web", {}).get("results", [])
                ]

            tool_functions.append(web_search)
    elif provider == "tavily":
        api_key = os.environ.get("TAVILY_API_KEY", "")
        if api_key:
            import httpx as _httpx

            def web_search(query: str, max_results: int = 10) -> list[dict]:
                """Search the web using Tavily Search API."""
                resp = _httpx.post(
                    "https://api.tavily.com/search",
                    json={"api_key": api_key, "query": query, "max_results": max_results},
                )
                resp.raise_for_status()
                return [
                    {"title": r.get("title", ""), "url": r["url"], "content": r.get("content", "")}
                    for r in resp.json().get("results", [])
                ]

            tool_functions.append(web_search)

    return tool_functions, builtin_tools


async def run_discover(
    issuer_id: str,
    context_doc: str,
    config: Config,
    costs: CostTracker | None = None,
) -> list[dict[str, Any]]:
    """Run the discover agent. Returns list of discovered URLs from database.

    Args:
        issuer_id: The issuer/company identifier (ISIN or similar).
        context_doc: Formatted company profile + context string passed as system context.
        config: Pipeline configuration.
    """
    show_stage(2, "Discovering URLs")
    tools.init_tools(config, issuer_id, costs)

    context_doc = _truncate_context(context_doc)
    system_prompt = f"{context_doc}\n\n---\n\n{DISCOVER_SYSTEM}"

    search_tools, builtin_tools = _build_search_tools(config)

    agent = Agent(
        _to_pydantic_ai_model(config.discover_model),
        system_prompt=system_prompt,
        tools=[
            tools.fetch_sitemap,
            tools.crawl_page,
            tools.map_domain,
            tools.probe_urls,
            tools.save_urls,
            tools.get_saved_urls,
        ] + search_tools + builtin_tools,
    )

    start = time.monotonic()
    timeout = config.max_discover_minutes * 60

    async with agent:
        try:
            result = await asyncio.wait_for(
                agent.run(
                    "Discover all URLs containing physical asset information for this company. "
                    "Work systematically: primary site first, then subsidiaries, then regulatory/external. "
                    "Save URLs as you go.",
                    usage_limits=UsageLimits(
                        tool_calls_limit=config.max_discover_tool_calls,
                        request_limit=None,
                    ),
                ),
                timeout=timeout,
            )
            if costs and result:
                costs.track_pydantic_ai(result.usage(), config.discover_model, "discover")
        except asyncio.TimeoutError:
            show_detail(f"Discover timed out after {config.max_discover_minutes}m — using URLs saved so far")
        except UsageLimitExceeded:
            show_detail(f"Discover hit {config.max_discover_tool_calls} tool call limit — using URLs saved so far")

    elapsed = time.monotonic() - start

    conn = get_connection(config)
    try:
        discovered = get_discovered_urls(conn, issuer_id)
    finally:
        conn.close()

    show_detail(f"Found {len(discovered)} URLs in {elapsed:.0f}s")

    return discovered
