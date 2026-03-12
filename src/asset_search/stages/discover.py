"""Stage 2: Discover -- pydantic-ai agent finds and saves URLs autonomously."""

from __future__ import annotations

import asyncio
import time
from typing import Any

from pydantic_ai import Agent
from pydantic_ai.mcp import MCPServerStdio

from ..config import Config
from ..db import get_connection, get_discovered_urls
from ..display import show_detail, show_stage
from . import tools
from .prompts import DISCOVER_SYSTEM


async def run_discover(
    issuer_id: str,
    context_doc: str,
    config: Config,
) -> list[dict[str, Any]]:
    """Run the discover agent. Returns list of discovered URLs from database.

    Args:
        issuer_id: The issuer/company identifier (ISIN or similar).
        context_doc: Formatted company profile + context string passed as system context.
        config: Pipeline configuration.
    """
    show_stage(2, "Discovering URLs")
    tools.init_tools(config, issuer_id)

    system_prompt = f"{context_doc}\n\n---\n\n{DISCOVER_SYSTEM}"

    toolsets = []
    if config.search_provider == "exa" and config.exa_api_key:
        toolsets.append(
            MCPServerStdio(
                "npx",
                ["-y", "@anthropic/exa-mcp-server"],
                env={"EXA_API_KEY": config.exa_api_key},
            )
        )

    agent = Agent(
        config.discover_model,
        system_prompt=system_prompt,
        tools=[
            tools.fetch_sitemap,
            tools.crawl_page,
            tools.map_domain,
            tools.save_urls,
            tools.get_saved_urls,
        ],
        toolsets=toolsets or None,
    )

    start = time.monotonic()
    timeout = config.max_discover_minutes * 60

    async with agent:
        try:
            await asyncio.wait_for(
                agent.run(
                    "Discover all URLs containing physical asset information for this company. "
                    "Work systematically: primary site first, then subsidiaries, then regulatory/external. "
                    "Save URLs as you go."
                ),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            show_detail(f"Discover timed out after {config.max_discover_minutes}m — using URLs saved so far")

    elapsed = time.monotonic() - start

    conn = get_connection(config)
    try:
        discovered = get_discovered_urls(conn, issuer_id)
    finally:
        conn.close()

    show_detail(f"Found {len(discovered)} URLs in {elapsed:.0f}s")

    return discovered
