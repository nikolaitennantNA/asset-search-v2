"""Stage 6: QA — pydantic-ai agent evaluates coverage and fills gaps."""

from __future__ import annotations

from typing import Any

from pydantic_ai import Agent
from pydantic_ai.mcp import MCPServerStdio

from ..config import Config
from ..db import get_connection, save_discovered_assets
from ..display import show_stage
from ..models import Asset, QAReport
from .prompts import QA_SYSTEM


async def run_qa(
    issuer_id: str, context_doc: str, assets: list[Asset], config: Config,
    rag_store=None,
) -> QAReport:
    """Run QA agent to evaluate asset coverage and fill gaps."""
    show_stage(6, "Quality assurance")

    asset_summary = _build_asset_summary(assets)
    system_prompt = f"{context_doc}\n\n---\n\n{QA_SYSTEM}"

    toolsets = []
    if config.search_provider == "exa" and config.exa_api_key:
        toolsets.append(
            MCPServerStdio(
                "npx",
                ["-y", "@anthropic/exa-mcp-server"],
                env={"EXA_API_KEY": config.exa_api_key},
            )
        )

    qa_tools = []

    if rag_store:
        async def rag_query(text: str, top_k: int = 20) -> list[dict]:
            """Search already-scraped pages for missed information."""
            return await rag_store.query(text, namespace=issuer_id, top_k=top_k)
        qa_tools.append(rag_query)

    async def scrape_and_extract(urls: list[str]) -> list[dict[str, Any]]:
        """Scrape new URLs, extract assets, persist everything."""
        from .scrape import run_scrape
        from .extract import run_extract
        url_dicts = [{"url": u, "category": "external", "notes": "QA gap-fill"} for u in urls]
        pages = await run_scrape(issuer_id, url_dicts, config, rag_store)
        new_assets = await run_extract(issuer_id, "", pages, config)
        if new_assets:
            conn = get_connection(config)
            try:
                save_discovered_assets(conn, issuer_id, [a.model_dump() for a in new_assets])
            finally:
                conn.close()
            assets.extend(new_assets)
        return [a.model_dump() for a in new_assets]

    qa_tools.append(scrape_and_extract)

    agent = Agent(
        config.qa_model,
        system_prompt=system_prompt,
        output_type=QAReport,
        tools=qa_tools,
        toolsets=toolsets or None,
    )

    qa_report = QAReport()
    async with agent:
        for _iteration in range(config.max_qa_iterations):
            result = await agent.run(
                f"## Current assets ({len(assets)} total)\n{asset_summary}\n\n"
                "Evaluate coverage. If gaps, use rag_query first, then scrape_and_extract. "
                "Return QAReport."
            )
            qa_report = result.output
            if not qa_report.should_enrich:
                break
            asset_summary = _build_asset_summary(assets)

    return qa_report


def _build_asset_summary(assets: list[Asset]) -> str:
    by_type: dict[str, int] = {}
    for a in assets:
        t = a.naturesense_asset_type or a.asset_type_raw or "Unknown"
        by_type[t] = by_type.get(t, 0) + 1
    lines = [f"Total: {len(assets)} assets"]
    if by_type:
        lines.append(
            "By type: " + ", ".join(
                f"{t}: {c}" for t, c in sorted(by_type.items(), key=lambda x: -x[1])
            )
        )
    return "\n".join(lines)
