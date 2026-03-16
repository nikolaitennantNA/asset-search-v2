"""Stage 2: Discover -- supervisor + parallel worker agents find URLs."""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any

from pydantic import BaseModel
from pydantic_ai import Agent, UsageLimits
from pydantic_ai.exceptions import UsageLimitExceeded

from ..config import Config, _to_pydantic_ai_model
from ..cost import CostTracker
from ..db import get_connection, get_discovered_urls, delete_discovered_urls
from ..display import show_detail, show_stage, show_warning
from . import tools
from .prompts import (
    DISCOVER_SYSTEM,
    DISCOVER_SUPERVISOR_PROMPT,
    DISCOVER_WORKER_PREAMBLE,
    DISCOVER_REVIEW_TEMPLATE,
)

_CONTEXT_TOKEN_CAP = 8_000
_SUPERVISOR_MAX_TOOL_CALLS = 20


# ---------------------------------------------------------------------------
# Structured types for supervisor output
# ---------------------------------------------------------------------------

class DiscoverTask(BaseModel):
    focus: str
    instructions: str
    search_queries: list[str] = []


class DiscoverPlan(BaseModel):
    tasks: list[DiscoverTask] = []
    done: bool = False
    remove_urls: list[str] = []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _truncate_context(text: str, max_tokens: int = _CONTEXT_TOKEN_CAP) -> str:
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
    """Build search tool functions and builtin tools based on search_provider."""
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
            """Search the web using Exa."""
            results = exa_client.search(
                query, num_results=num_results,
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


def _build_review_message(issuer_id: str, round_num: int, config: Config) -> str:
    """Build the review message showing all discovered URLs."""
    conn = get_connection(config)
    try:
        urls = get_discovered_urls(conn, issuer_id)
    finally:
        conn.close()

    categories: dict[str, int] = {}
    for u in urls:
        cat = u.get("category", "unknown")
        categories[cat] = categories.get(cat, 0) + 1
    category_lines = "\n".join(f"- {cat}: {cnt}" for cat, cnt in sorted(categories.items(), key=lambda x: -x[1]))

    url_lines = []
    for u in urls:
        cat = u.get("category", "unknown")
        notes = u.get("notes", "")
        url_lines.append(f"- [{cat}] {u['url']} — \"{notes}\"")

    return DISCOVER_REVIEW_TEMPLATE.format(
        round_num=round_num,
        total_count=len(urls),
        category_breakdown=category_lines or "  (none)",
        url_list="\n".join(url_lines) or "  (none)",
    )


# ---------------------------------------------------------------------------
# Worker runner
# ---------------------------------------------------------------------------

async def _run_worker(
    task: DiscoverTask,
    config: Config,
    context_doc: str,
    search_tools: list,
    builtin_tools: list,
    tool_call_budget: int,
    timeout: float,
    costs: CostTracker | None,
    verbose: bool,
) -> str:
    """Run a single worker agent. Returns focus label."""
    preamble = DISCOVER_WORKER_PREAMBLE.format(
        focus=task.focus,
        instructions=task.instructions,
        starting_queries=", ".join(task.search_queries) if task.search_queries else "(use your judgement)",
    )
    system_prompt = f"{context_doc}\n\n---\n\n{preamble}\n\n---\n\n{DISCOVER_SYSTEM}"

    worker_tools = [
        tools.fetch_sitemap,
        tools.crawl_page,
        tools.map_domain,
        tools.probe_urls,
        tools.save_urls,
        tools.get_saved_urls,
    ] + search_tools + builtin_tools

    agent = Agent(
        _to_pydantic_ai_model(config.discover_worker_model),
        system_prompt=system_prompt,
        tools=worker_tools,
        retries=2,
    )

    start = time.monotonic()
    tool_call_count = 0

    async with agent:
        try:
            async with agent.iter(
                f"Discover URLs for your assigned area: {task.focus}. "
                f"Instructions: {task.instructions}. "
                "Save URLs as you find them.",
                usage_limits=UsageLimits(
                    tool_calls_limit=tool_call_budget,
                    request_limit=None,
                ),
            ) as agent_run:
                async for node in agent_run:
                    if verbose and hasattr(node, 'tool_name'):
                        tool_call_count += 1
                        args_str = str(getattr(node, 'args', ''))
                        if len(args_str) > 100:
                            args_str = args_str[:100] + '...'
                        show_detail(f"[{task.focus}] [{tool_call_count}] {node.tool_name}({args_str})")
            if costs and agent_run.result:
                costs.track_pydantic_ai(agent_run.result.usage(), config.discover_worker_model, f"discover_worker:{task.focus}")
        except asyncio.TimeoutError:
            show_detail(f"[{task.focus}] timed out — using URLs saved so far")
        except UsageLimitExceeded:
            show_detail(f"[{task.focus}] hit tool call limit — using URLs saved so far")

    elapsed = time.monotonic() - start
    show_detail(f"[{task.focus}] done in {elapsed:.0f}s ({tool_call_count} tool calls)")
    return task.focus


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def run_discover(
    issuer_id: str,
    context_doc: str,
    config: Config,
    costs: CostTracker | None = None,
    verbose: bool = False,
) -> list[dict[str, Any]]:
    """Run parallel discovery: supervisor plans, workers collect, supervisor reviews."""
    show_stage(2, "Discovering URLs")
    tools.init_tools(config, issuer_id, costs)

    context_doc = _truncate_context(context_doc)
    search_tools, builtin_tools = _build_search_tools(config)

    overall_start = time.monotonic()
    overall_timeout = config.max_discover_minutes * 60
    remaining_tool_budget = config.max_discover_tool_calls

    # ── Phase 1: Supervisor plans (no tools — fast structured output) ─────
    show_detail("Supervisor planning...")
    supervisor_system = f"{context_doc}\n\n---\n\n{DISCOVER_SUPERVISOR_PROMPT}"

    supervisor = Agent(
        _to_pydantic_ai_model(config.discover_supervisor_model),
        system_prompt=supervisor_system,
        output_type=DiscoverPlan,
        retries=2,
    )

    plan: DiscoverPlan | None = None
    async with supervisor:
        try:
            result = await asyncio.wait_for(
                supervisor.run(
                    "Plan the URL discovery for this company. Break into parallel tasks for worker agents.",
                ),
                timeout=min(90, overall_timeout),
            )
            plan = result.output
            if costs:
                costs.track_pydantic_ai(result.usage(), config.discover_supervisor_model, "discover_supervisor")
        except (asyncio.TimeoutError, Exception) as e:
            show_warning(f"Supervisor planning failed: {e}")

    if not plan or not plan.tasks:
        show_detail("Falling back to single-agent discovery")
        plan = DiscoverPlan(tasks=[DiscoverTask(
            focus="full_discovery",
            instructions="Discover all URLs for this company. Work systematically: primary site, subsidiaries, regulatory, external.",
            search_queries=[],
        )])

    tasks = plan.tasks[:config.max_discover_workers]
    show_detail(f"Plan: {len(tasks)} workers — " + ", ".join(t.focus for t in tasks))
    if verbose:
        for t in tasks:
            show_detail(f"  [{t.focus}] {t.instructions}")

    # ── Phase 2: Workers execute in parallel ───────────────────────────────
    worker_budget = max(10, remaining_tool_budget // len(tasks))
    worker_timeout = max(60, (overall_timeout - (time.monotonic() - overall_start)) / 2)

    worker_coros = [
        asyncio.wait_for(
            _run_worker(
                task=t, config=config, context_doc=context_doc,
                search_tools=search_tools, builtin_tools=builtin_tools,
                tool_call_budget=worker_budget, timeout=worker_timeout,
                costs=costs, verbose=verbose,
            ),
            timeout=worker_timeout,
        )
        for t in tasks
    ]

    results = await asyncio.gather(*worker_coros, return_exceptions=True)
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            show_warning(f"Worker [{tasks[i].focus}] failed: {r}")

    # ── Phase 3: Supervisor reviews ────────────────────────────────────────
    for round_num in range(1, config.max_discover_rounds + 1):
        remaining_time = overall_timeout - (time.monotonic() - overall_start)
        if remaining_time < 30:
            show_detail("Time budget exhausted, skipping review")
            break

        show_detail(f"Supervisor reviewing (round {round_num})...")
        review_msg = _build_review_message(issuer_id, round_num, config)

        review_supervisor = Agent(
            _to_pydantic_ai_model(config.discover_supervisor_model),
            system_prompt=supervisor_system,
            output_type=DiscoverPlan,
        )

        review_plan: DiscoverPlan | None = None
        async with review_supervisor:
            try:
                review_result = await asyncio.wait_for(
                    review_supervisor.run(review_msg),
                    timeout=min(120, remaining_time),
                )
                review_plan = review_result.output
                if costs:
                    costs.track_pydantic_ai(review_result.usage(), config.discover_supervisor_model, "discover_supervisor_review")
            except (asyncio.TimeoutError, Exception) as e:
                show_warning(f"Supervisor review failed: {e}")
                break

        if not review_plan:
            break

        if review_plan.remove_urls:
            conn = get_connection(config)
            try:
                removed = delete_discovered_urls(conn, issuer_id, review_plan.remove_urls)
            finally:
                conn.close()
            show_detail(f"Supervisor removed {removed} URLs")

        if review_plan.done:
            show_detail("Supervisor satisfied — discovery complete")
            break

        if review_plan.tasks:
            extra_tasks = review_plan.tasks[:config.max_discover_workers]
            show_detail(f"Supervisor requesting {len(extra_tasks)} more workers: " + ", ".join(t.focus for t in extra_tasks))

            extra_timeout = max(60, (overall_timeout - (time.monotonic() - overall_start)) / 2)
            extra_coros = [
                asyncio.wait_for(
                    _run_worker(
                        task=t, config=config, context_doc=context_doc,
                        search_tools=search_tools, builtin_tools=builtin_tools,
                        tool_call_budget=worker_budget, timeout=extra_timeout,
                        costs=costs, verbose=verbose,
                    ),
                    timeout=extra_timeout,
                )
                for t in extra_tasks
            ]
            extra_results = await asyncio.gather(*extra_coros, return_exceptions=True)
            for i, r in enumerate(extra_results):
                if isinstance(r, Exception):
                    show_warning(f"Worker [{extra_tasks[i].focus}] failed: {r}")
        else:
            break

    # ── Return final URL list ──────────────────────────────────────────────
    elapsed = time.monotonic() - overall_start
    conn = get_connection(config)
    try:
        discovered = get_discovered_urls(conn, issuer_id)
    finally:
        conn.close()

    show_detail(f"Found {len(discovered)} URLs in {elapsed:.0f}s")
    return discovered
