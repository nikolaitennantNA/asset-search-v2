"""Stage 6: QA — pydantic-ai agent evaluates coverage and fills gaps."""

from __future__ import annotations

from typing import Any

from pydantic_ai import Agent

from ..config import Config, _to_pydantic_ai_model
from ..cost import CostTracker
from ..db import get_connection, save_discovered_assets, save_discovered_urls
from ..display import show_stage
from ..models import Asset, QAReport
from .discover import _build_search_tools
from .prompts import QA_SYSTEM


async def run_qa(
    issuer_id: str, context_doc: str, assets: list[Asset], config: Config,
    rag_store=None, costs: CostTracker | None = None,
) -> QAReport:
    """Run QA agent to evaluate asset coverage and fill gaps."""
    show_stage(6, "Quality assurance")

    asset_summary = _build_asset_summary(assets)
    system_prompt = f"{context_doc}\n\n---\n\n{QA_SYSTEM}"

    search_tools, builtin_tools = _build_search_tools(config)

    qa_tools = []

    if rag_store:
        async def rag_query(text: str, top_k: int = 20) -> list[dict]:
            """Search already-scraped pages for missed information."""
            from rag import Usage as RAGUsage
            rag_usage = RAGUsage()
            results = await rag_store.query(text, namespace=issuer_id, top_k=top_k, usage=rag_usage)
            if costs:
                costs.track_embedding(rag_usage.embedding_tokens)
                costs.track_cohere_rerank(rag_usage.rerank_calls)
            return results
        qa_tools.append(rag_query)

    async def scrape_and_extract(urls: list[str]) -> list[dict[str, Any]]:
        """Scrape new URLs, extract assets, persist everything."""
        from .scrape import run_scrape
        from .extract import run_extract
        url_dicts = [{"url": u, "category": "external", "notes": "QA gap-fill"} for u in urls]
        # Persist QA-discovered URLs to discovered_urls table
        conn = get_connection(config)
        try:
            save_discovered_urls(conn, issuer_id, url_dicts)
        finally:
            conn.close()
        pages = await run_scrape(issuer_id, url_dicts, config, rag_store, costs=costs)
        new_assets = await run_extract(issuer_id, "", pages, config, costs=costs)
        if new_assets:
            # Quick name-based dedup against existing assets
            existing_names = {a.asset_name.lower().strip() for a in assets if a.asset_name}
            deduped = [a for a in new_assets if a.asset_name.lower().strip() not in existing_names]
            if deduped:
                conn = get_connection(config)
                try:
                    save_discovered_assets(conn, issuer_id, [a.model_dump() for a in deduped])
                finally:
                    conn.close()
                assets.extend(deduped)
            new_assets = deduped
        return [a.model_dump() for a in new_assets]

    qa_tools.append(scrape_and_extract)

    agent = Agent(
        _to_pydantic_ai_model(config.qa_model),
        system_prompt=system_prompt,
        output_type=QAReport,
        tools=qa_tools + search_tools + builtin_tools,
    )

    qa_report = QAReport()
    async with agent:
        for _iteration in range(config.max_qa_iterations):
            result = await agent.run(
                f"## Current assets ({len(assets)} total)\n{asset_summary}\n\n"
                "Evaluate coverage. If gaps, use rag_query first, then scrape_and_extract. "
                "Return QAReport."
            )
            if costs:
                costs.track_pydantic_ai(result.usage(), config.qa_model, "qa")
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
